from tv_detection_common.models import Channel, Schedule, Recording, RecordingStatus
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DatabaseError
from datetime import datetime, timedelta, timezone
import logging
import sys
import os
import subprocess
import json
import time
import requests
import threading
from collections import defaultdict
from difflib import SequenceMatcher

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

channels = []
streams = []

def verify_database_connection():
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

def test_stream_url_with_vpn(country: str, stream_url: str):
    connected = False
    max_attempts = 10
    for attempt in range(max_attempts):
        vpn_connect(country)
        return_code = probe_stream_url(stream_url)
        if return_code == 0:
            return 0
        elif return_code == 1:  # we got a 403, try another vpn endpoint
            time.sleep(1)
            continue
        elif return_code == 2:
            return 2
    return 1
    
def probe_stream_url(stream_url: str):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_format", "-show_streams", stream_url],
            capture_output=True,
            text=True,
            timeout=60  # Longer timeout for potential network delays
        )
        if result.returncode == 0:
            logger.info("Successfully probed the stream.")
            return 0
        else:
            logger.error(f"Failed attempt to probe the stream (return code {result.returncode}): [{result.stderr.strip()}]")
            # TODO if it's 403 return 1, else return 2, for now our code assumes 403 if failure, kinda brittle
            return 1
    except subprocess.TimeoutExpired:
        logger.error("ffprobe timed out while probing the stream, aborting")
    except Exception as e:
        logger.error(f"Error probing the stream, aborting: {e}")
    return 2

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

def test_two_streams():
    resp = test_stream_url_with_vpn('uk', "https://vs-hls-pushb-uk-live.akamaized.net/x=4/i=urn:bbc:pips:service:bbc_two_northern_ireland_hd/pc_hd_abr_v2.m3u8")
    if not resp:
        print('VPN stream OK')
    else:
        print('VPN stream FAILED')
    resp = probe_stream_url("https://unlimited1-cl-isp.dps.live/ucvtv2/ucvtv2.smil/playlist.m3u8")
    if not resp:
        print('Non-VPN stream OK')
    else:
        print('Non-VPN stream FAILED')


def load_channels():
  with open('./channels.json') as file:
    global channels
    channels = json.load(file)
    print(f"num chans is [{len(channels)}]")
    print(f"0th chan is {channels[0]}")
    print(f"100th chan is {channels[100]}")

def load_streams():
  with open('./streams.json') as file:
    global streams
    streams = json.load(file)
    print(f"num streams is [{len(streams)}]")
    print(f"0th stream is {streams[0]}")
    print(f"100th stream is {streams[100]}")

def get_info_for_stream(stream):
    cid = stream.get('channel')
    if cid:  # Direct match
        return {
            'id': cid,
            'country': id_to_country.get(cid),
            'name': id_to_name.get(cid)
        }
    else:  # Fuzzy match title to channels.json name
        title_clean = stream['title'].lower().replace(' hd', '').replace(' sd', '').replace(' tv', '').replace(' channel', '').strip()
        best_id = None
        best_score = 0
        for name_lower, possible_id in name_to_id.items():
            score = SequenceMatcher(None, title_clean, name_lower).ratio()
            if score > best_score and score >= 0.85:  # Threshold: adjust lower if too strict
                best_score = score
                best_id = possible_id
        if best_id:
            return {
                'id': best_id,
                'country': id_to_country.get(best_id),
                'name': id_to_name.get(best_id)
            }
        else:
            # Fallback: infer from title/feed/url (optional)
            inferred_country = None
            if 'uk' in stream['feed'].lower(): inferred_country = 'gb'
            # Or parse URL domain, etc.
            return {
                'id': None,  # Flag as unmatched
                'country': inferred_country,
                'name': stream['title']
            }

def scan_for_valid_streams():
    # Your scanning loop
    valid_streams = []  # For ones with ID/country/EPG
    for stream in streams_data:
        info = get_info_for_stream(stream)
        if info['id']:  # Has universal ID
            # Get EPG (from previous code)
            country = info['country'].lower() if info['country'] else ''
            providers = []  # Fetch from SITES.md as before
            epg = get_epg_for_channel(info['id'], country, providers)
            if epg:
                valid_streams.append({
                    'stream_url': stream['url'],
                    'id': info['id'],
                    'country': info['country'],
                    'name': info['name'],
                    'epg_count': len(epg)
                })
        # Else: skip as useless

    print(f"Valid streams with EPG: {len(valid_streams)}")
    return valid_streams

if __name__ == "__main__":
    verify_database_connection()
    load_channels()
    load_streams()

    valid_streams = scan_for_valid_streams()


#    test_two_streams()
#    scheduler_loop()
