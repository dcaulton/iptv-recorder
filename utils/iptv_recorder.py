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
from .database_connection import DatabaseConnection
from .vpn_manager import VpnManager



class IptvRecorder():
    def __init__(self, logger):
        self.logger = logger
        self.db_conn = DatabaseConnection(self.logger)
        self.vpn_manager = VpnManager(self.logger)
        self.channels = []
        self.streams = []
        self.countries = []
        self.md_text = ''
        self.load_channels_etc()
        self.id_to_country = {ch['id']: ch.get('country') for ch in self.channels}
        self.id_to_name = {ch['id']: ch['name'] for ch in self.channels}
        self.name_to_id = {ch['name'].lower(): ch['id'] for ch in self.channels}
        self.code_to_name = {ch['code'].lower(): ch['name'].lower() for ch in self.countries}
        self.name_to_code = {v: k for k, v in self.code_to_name.items()}  # Reverse for parsing
        self.country_to_providers = self.parse_sites()
        self.xml_dir_map = {'gb': 'uk'}


    def main_loop(self):
        self.logger.info("entering scheduler loop")
        while True:
            session = self.db_conn.Session()
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

    def test_two_streams(self):
        self.logger.info("testing two streams")
        resp = self.vpn_manager.test_stream_url_with_vpn('uk', "https://vs-hls-pushb-uk-live.akamaized.net/x=4/i=urn:bbc:pips:service:bbc_two_northern_ireland_hd/pc_hd_abr_v2.m3u8")
        if not resp:
            print('VPN stream OK')
        else:
            print('VPN stream FAILED')
        resp = self.vpn_manager.probe_stream_url("https://unlimited1-cl-isp.dps.live/ucvtv2/ucvtv2.smil/playlist.m3u8")
        if not resp:
            print('Non-VPN stream OK')
        else:
            print('Non-VPN stream FAILED')

    def load_channels_etc(self):
      self.logger.info("loading channels")
      with open('/channel_files/channels.json') as file:
        self.channels = json.load(file)
        print(f"num chans is [{len(self.channels)}]")

      with open('/channel_files/streams.json') as file:
        self.streams = json.load(file)
        print(f"num streams is [{len(self.streams)}]")

      with open('/channel_files/countries.json') as file:
        self.countries = json.load(file)
        print(f"num countries is [{len(self.countries)}]")

      with open('/channel_files/sites.md') as file:
        self.md_text = ''.join(file.readlines())

    def parse_sites(self):
        self.logger.info("parsing sites")
        country_to_providers = defaultdict(list)
        current_country = None
        for line in self.md_text.splitlines():
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

    def get_info_for_stream(self, stream):
        self.logger.info(f"getting info for stream {stream}")
        cid = stream.get('channel')
        if cid:  # Direct match
            return {
                'id': cid,
                'country': self.id_to_country.get(cid),
                'name': self.id_to_name.get(cid)
            }
        else:  # Fuzzy match title to channels.json name
            title_clean = stream['title'].lower().replace(' hd', '').replace(' sd', '').replace(' tv', '').replace(' channel', '').strip()
            best_id = None
            best_score = 0
            for name_lower, possible_id in self.name_to_id.items():
                score = SequenceMatcher(None, title_clean, name_lower).ratio()
                if score > best_score and score >= 0.75:
                    best_score = score
                    best_id = possible_id
            if best_id:
                return {
                    'id': best_id,
                    'country': self.id_to_country.get(best_id),
                    'name': self.id_to_name.get(best_id)
                }
            else:
                return {
                    'id': None,
                    'country': None,
                    'name': stream['title']
                }

    def scan_for_valid_streams(self):
        self.logger.info("scanning for valid streams")
        for stream in self.streams:
            # ...
            info = self.get_info_for_stream(stream)
            if info['id']:  # Has universal ID
                country = info['country'].lower() if info['country'] else ''
                providers = self.country_to_providers.get(country, [])  # ← use the dict here!
                epg = self.get_epg_for_channel(info['id'], country, providers)
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

    def get_epg_for_channel(self, channel_id: str, country: str, providers: list) -> list:
        self.logger.info(f"getting epg for channel {channel_id}, country {country}, providers {providers}")
        if not providers:
            self.logger.info(f"No providers found for country '{country}' - skipping EPG for {channel_id}")
            return []
        self.logger.debug(f"Attempting EPG for {channel_id} in {country} with providers: {providers}")
        for provider in providers:
            xml_url = f"https://iptv-org.github.io/epg/guides/{xml_dir_map.get(country, country)}/{provider}.xml"
            self.logger.debug(f"Trying {xml_url}")
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


#def get_proxy_base(vpn_country):
#    # Map vpn_country to proxy container name/IP
#    if vpn_country == "uk":
#        return "http://vpn-uk:8080/p/"  # or the proxy's single-stream path
#    # Add more for ca, etc.
#    return ""  # direct
#
#def record_stream(schedule_id):
#    session = db_conn.Session()
#    schedule = session.query(Schedule).get(schedule_id)
#    if not schedule:
#        return
#
#    channel = schedule.channel
#    program = schedule.program
#
#    recording = Recording(
#        schedule_id=schedule.id,
#        channel_id=channel.id,
#        program_id=program.id,
#        start_time=schedule.start_time,
#        end_time=schedule.end_time,
#        status=RecordingStatus.RECORDING
#    )
#    session.add(recording)
#    session.commit()
#
#    url = channel.tuning_json.get("url")
#    if channel.geo_blocked:
#        url = get_proxy_base(channel.vpn_country) + url
#
#    duration = (schedule.end_time - schedule.start_time).total_seconds()
#    output_file = f"/mnt/recordings/{channel.name}_{program.title}_{schedule.start_time.strftime('%Y%m%d_%H%M')}.ts"
#
#    cmd = [
#        "ffmpeg",
#        "-i", url,
#        "-t", str(duration),
#        "-c", "copy",
#        output_file
#    ]
#
#    process = POPEN(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#    stdout, stderr = process.communicate()
#    recording.completed_at = datetime.now(timezone.utc)
#    if process.returncode == 0:
#        recording.status = RecordingStatus.COMPLETED
#        recording.file_path = output_file
#    else:
#        recording.status = RecordingStatus.FAILED
#        recording.error_message = stderr.decode()
#
#    session.commit()
#    session.close()

