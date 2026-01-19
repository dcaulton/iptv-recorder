from tv_detection_common.models import Channel, Schedule, Recording, RecordingStatus
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
import time
import threading

# Config
DB_URL = "sqlite:///tv_detection.db"  # or postgresql://user:pass@host/db
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

def get_proxy_base(vpn_country):
    # Map vpn_country to proxy container name/IP
    if vpn_country == "gb":
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

    process = POPEN(cmd, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    recording.completed_at = datetime.utcnow()
    if process.returncode == 0:
        recording.status = RecordingStatus.COMPLETED
        recording.file_path = output_file
    else:
        recording.status = RecordingStatus.FAILED
        recording.error_message = stderr.decode()

    session.commit()
    session.close()

def scheduler_loop():
    while True:
        session = Session()
        now = datetime.utcnow()
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
    scheduler_loop()
