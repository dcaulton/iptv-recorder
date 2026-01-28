from tv_detection_common.models import Channel, Schedule, Recording, RecordingStatus
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
from utils.database_connection import DatabaseConnection
from utils.vpn_manager import VpnManager


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout,  # explicit stdout (Docker captures this)
    force=True          # override any existing config
)
logger = logging.getLogger(__name__)
logger.info(f"starting...")


channels = []
streams = []
countries = []
md_text = ''

def get_proxy_base(vpn_country):
    # Map vpn_country to proxy container name/IP
    if vpn_country == "uk":
        return "http://vpn-uk:8080/p/"  # or the proxy's single-stream path
    # Add more for ca, etc.
    return ""  # direct

def record_stream(schedule_id):
    session = db_conn.Session()
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
        session = db_conn.Session()
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
    resp = vpn_manager.test_stream_url_with_vpn('uk', "https://vs-hls-pushb-uk-live.akamaized.net/x=4/i=urn:bbc:pips:service:bbc_two_northern_ireland_hd/pc_hd_abr_v2.m3u8")
    if not resp:
        print('VPN stream OK')
    else:
        print('VPN stream FAILED')
    resp = vpn_manager.probe_stream_url("https://unlimited1-cl-isp.dps.live/ucvtv2/ucvtv2.smil/playlist.m3u8")
    if not resp:
        print('Non-VPN stream OK')
    else:
        print('Non-VPN stream FAILED')


def load_channels_etc():
  with open('/channel_files/channels.json') as file:
    global channels_data
    channels = json.load(file)
    print(f"num chans is [{len(channels)}]")

  with open('/channel_files/streams.json') as file:
    global streams
    streams = json.load(file)
    print(f"num streams is [{len(streams)}]")

  with open('/channel_files/countries.json') as file:
    global countries 
    countries = json.load(file)
    print(f"num countries is [{len(countries)}]")

  with open('/channel_files/sites.md') as file:
    global md_text
    md_text = ''.join(file.readlines())

def parse_sites():
    country_to_providers = defaultdict(list)
    current_country = None
    for line in md_text.splitlines():
        if line.startswith('## '):
            current_country = line.strip('# ').lower()
            code = name_to_code.get(current_country)  # e.g., 'united kingdom' → 'gb'
            if code:
                current_code = code
        elif line.startswith('- '):
            provider = line.strip('- `').rstrip('`')
            if current_code:
                country_to_providers[current_code].append(provider)
    return country_to_providers


def get_epg_for_channel(channel_id: str, country: str, providers: list) -> list:
    if not providers:
        logger.info(f"No providers found for country '{country}' - skipping EPG for {channel_id}")
        return []
    logger.debug(f"Attempting EPG for {channel_id} in {country} with providers: {providers}")
    for provider in providers:
        xml_url = f"https://iptv-org.github.io/epg/guides/{xml_dir_map.get(country, country)}/{provider}.xml"
        logger.debug(f"Trying {xml_url}")
        try:
            response = requests.get(xml_url, timeout=10)
            if response.status_code != 200:
                continue
            root = ET.fromstring(response.content)
            # Check if channel exists
            if any(ch.get('id') == channel_id for ch in root.findall('channel')):
                programs = []
                for prog in root.findall('programme'):
                    if prog.get('channel') == channel_id:
                        title = prog.find('title').text if prog.find('title') is not None else None
                        start = prog.get('start')  # e.g., "20260122180000 +0000"
                        stop = prog.get('stop')
                        desc = prog.find('desc').text if prog.find('desc') is not None else None
                        category = [cat.text for cat in prog.findall('category')] if prog.findall('category') else []
                        programs.append({
                            'start': start,
                            'stop': stop,
                            'title': title,
                            'desc': desc,
                            'categories': category
                        })
                if programs:  # Found listings
                    return programs  # List of dicts, sorted by start time naturally
        except Exception:
            continue
    return []  # No EPG found across providers

def get_info_for_stream(stream, id_to_country, id_to_name, name_to_id):
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
            if score > best_score and score >= 0.75:
                best_score = score
                best_id = possible_id
        if best_id:
            return {
                'id': best_id,
                'country': id_to_country.get(best_id),
                'name': id_to_name.get(best_id)
            }
        else:
            return {
                'id': None,
                'country': None,
                'name': stream['title']
            }

def scan_for_valid_streams(id_to_country, id_to_name, name_to_id, country_to_providers):
    # ...
    for stream in streams:
        # ...
        info = get_info_for_stream(stream, id_to_country, id_to_name, name_to_id)
        if info['id']:  # Has universal ID
            country = info['country'].lower() if info['country'] else ''
            providers = country_to_providers.get(country, [])  # ← use the dict here!
            epg = get_epg_for_channel(info['id'], country, providers)
            if epg:
                valid_count += 1
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
    db_conn = DatabaseConnection(logger=logger, test_conn=True)
    vpn_manager = VpnManager(logger=logger)
    load_channels_etc()
    id_to_country = {ch['id']: ch.get('country') for ch in channels}
    id_to_name = {ch['id']: ch['name'] for ch in channels}
    name_to_id = {ch['name'].lower(): ch['id'] for ch in channels}
    code_to_name = {ch['code'].lower(): ch['name'].lower() for ch in countries}
    name_to_code = {v: k for k, v in code_to_name.items()}  # Reverse for parsing
    country_to_providers = parse_sites()
    xml_dir_map = {'gb': 'uk'}
#    valid_streams = scan_for_valid_streams(id_to_country, id_to_name, name_to_id, country_to_providers)
#
#    with open('/channel_files/valid_streams.json', 'w') as file:
#        file.write(json.dumps(valid_streams))


#    test_two_streams()
#    scheduler_loop()
