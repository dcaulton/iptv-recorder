from tv_detection_common.models import Channel, Schedule, Recording, RecordingStatus
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DatabaseError
from datetime import datetime, timedelta, timezone
import logging
import sys
import os
import subprocess
import time
import requests
import threading

print(f"just waking up")
# Force logging to stdout (Docker-friendly), level INFO
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout,  # explicit stdout (Docker captures this)
    force=True          # override any existing config
)
logger = logging.getLogger(__name__)
logger.info(f"just waking up in the logger")

# Config
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    logger.error("DB_URL environment variable not set. Exiting.")
    exit(1)
engine = create_engine(DB_URL, echo=False)
Session = sessionmaker(bind=engine)

VPN_MANAGER_BASE_URL = "http://localhost:8080/"

def check_database_connection():
    # Quick connectivity + table test
    logger.info(f"testing Database connection")
    try:
        with Session() as session:
            # Simple count query (doesn't care if table is empty)
            result = session.execute(text("SELECT COUNT(*) FROM schedules"))
            count = result.scalar()
            logger.info(f"Database connection OK. Found {count} entries in schedules table.")
    except OperationalError as e:
        logger.error(f"Connection failed (will retry later): {e}")
    except DatabaseError as e:
        logger.error(f"Database error (table missing or permission issue?): {e}")
    except Exception as e:
        logger.error(f"Unexpected error during DB test: {e}")

def vpn_connect(country: str):
    restart_url = f"{VPN_MANAGER_BASE_URL}restart?country={country}"
    try:
        response = requests.get(restart_url)
        response_data = response.json() if response.content else {}
        if response.status_code != 200 or response_data.get("status") != "started":
            logger.error(f"Failed to request VPN connect: {response.status_code} - {response_data}")
            print(f"Connect response: {response_data}")  # Debug print
            return
        logger.info("VPN connect request sent successfully.")
        print(f"Connect response: {response_data}")  # Debug print to see PID if started
    except Exception as e:
        logger.error(f"Error requesting VPN connect: {e}")
        return

def test_vpn_connect_and_stream(country: str):
    vpn_connect(country)

# TODO make this async with timeout, remove the wait
    # Step 2: Wait for success by polling /status endpoint
    # Assumption: /status should eventually return something like {"status": "connected", "country": "uk"}
    # If it's always {}, implement /status in vpn-manager to check process.poll() is None (alive) and parse OpenVPN output for "Initialization Sequence Completed"
    status_url = f"{VPN_MANAGER_BASE_URL}status"
    connected = False
    max_attempts = 6  # Poll every 5 seconds for up to 1.2 minutes
    for attempt in range(max_attempts):
        try:
            status_response = requests.get(status_url)
            status_data = status_response.json() if status_response.content else {}
            logger.info(f"Status check (attempt {attempt + 1}): {status_data}")
            print(f"Status response: {status_data}")  # Debug print
            if status_data.get("pid") and status_data.get("country") == "uk":
                connected = True
                logger.info("VPN connected successfully to UK.")
                break
            elif status_data.get("status") == "failed":
                logger.error(f"VPN connection failed: {status_data.get('message', 'Unknown error')}")
                return
        except Exception as e:
            logger.error(f"Error checking status (attempt {attempt + 1}): {e}")
        time.sleep(5)
    
    if not connected:
        logger.error("Failed to confirm VPN connection after polling. Check vpn-manager logs for OpenVPN errors (e.g., auth failure).")
        print("Tip: NordVPN requires credentials. Ensure vpn-manager provides --auth-user-pass to OpenVPN.")
        return

    # Step 3: Tune (probe) the BBC Two Northern Ireland stream using ffprobe
    stream_url = "https://vs-hls-pushb-uk-live.akamaized.net/x=4/i=urn:bbc:pips:service:bbc_two_northern_ireland_hd/pc_hd_abr_v2.m3u8"
    try:
        # Run ffprobe to get stream info; -v error suppresses verbose but captures errors
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_format", "-show_streams", stream_url],
            capture_output=True,
            text=True,
            timeout=60  # Longer timeout for potential network delays
        )
        if result.returncode == 0:
            logger.info("Successfully probed the stream.")
            print("Stream info:\n" + result.stdout)
        else:
            logger.error(f"Failed to probe the stream (return code {result.returncode}): {result.stderr}")
            print(f"Probe error: {result.stderr}")  # Debug print for geo-block or other issues
    except subprocess.TimeoutExpired:
        logger.error("ffprobe timed out while probing the stream.")
    except Exception as e:
        logger.error(f"Error probing the stream: {e}")

def get_proxy_base(vpn_country):
    # Map vpn_country to proxy container name/IP
    if vpn_country == "uk":
        return "http://vpn-uk:8080/p/"  # or the proxy's single-stream path
    # Add more for ca, etc.
    return ""  # direct

def record_stream(schedule_id):
    session = Session()
    schedule = session.query(Schedule).get(schedule_id)
    if not schedule:
        return

    channel = schedule.channel
    program = schedule.program

    recording = Recording(
        schedule_id=schedule.id,
        channel_id=channel.id,
        program_id=program.id,
        start_time=schedule.start_time,
        end_time=schedule.end_time,
        status=RecordingStatus.RECORDING
    )
    session.add(recording)
    session.commit()

    url = channel.tuning_json.get("url")
    if channel.geo_blocked:
        url = get_proxy_base(channel.vpn_country) + url

    duration = (schedule.end_time - schedule.start_time).total_seconds()
    output_file = f"/mnt/recordings/{channel.name}_{program.title}_{schedule.start_time.strftime('%Y%m%d_%H%M')}.ts"

    cmd = [
        "ffmpeg",
        "-i", url,
        "-t", str(duration),
        "-c", "copy",
        output_file
    ]

    process = POPEN(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    recording.completed_at = datetime.now(timezone.utc)
    if process.returncode == 0:
        recording.status = RecordingStatus.COMPLETED
        recording.file_path = output_file
    else:
        recording.status = RecordingStatus.FAILED
        recording.error_message = stderr.decode()

    session.commit()
    session.close()

def scheduler_loop():
    print("entering scheduler loop")
    while True:
        session = Session()
        now = datetime.now(timezone.utc)
        pending = session.query(Schedule).filter(Schedule.start_time <= now + timedelta(minutes=5), Schedule.start_time > now - timedelta(minutes=5)).all()

        threads = []
        for sch in pending:
            if not sch.recording:  # no recording yet
                t = threading.Thread(target=record_stream, args=(sch.id,))
                t.start()
                threads.append(t)

        for t in threads:
            t.join()

        session.close()
        time.sleep(60)  # check every minute

if __name__ == "__main__":
    check_database_connection()
    test_vpn_connect_and_stream('uk')
    vpn_restart('uk')
    scheduler_loop()
