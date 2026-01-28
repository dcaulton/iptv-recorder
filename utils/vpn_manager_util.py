import sys
import os
import subprocess
import json
import time
import requests


VPN_MANAGER_BASE_URL = "http://localhost:8080/"

class VpnManager():
    def __init__(self, logger=None):
        self.logger = logger

    def connect(self, country: str):
        restart_url = f"{VPN_MANAGER_BASE_URL}restart?country={country}"
        try:
            response = requests.get(restart_url)
            response_data = response.json() if response.content else {}
            if response.status_code != 200 or response_data.get("status") != "started":
                self.logger.error(f"Failed to request VPN connect: {response.status_code} - {response_data}")
                print(f"Connect response: {response_data}")  # Debug print
                return
            self.logger.info("VPN connect request sent successfully.")
            print(f"Connect response: {response_data}")  # Debug print to see PID if started
        except Exception as e:
            self.logger.error(f"Error requesting VPN connect: {e}")
            return

    def test_stream_url_with_vpn(self, country: str, stream_url: str):
        connected = False
        max_attempts = 10
        for attempt in range(max_attempts):
            self.connect(country)
            return_code = self.probe_stream_url(stream_url)
            if return_code == 0:
                return 0
            elif return_code == 1:  # we got a 403, try another vpn endpoint
                time.sleep(1)
                continue
            elif return_code == 2:
                return 2
        return 1
        
    def probe_stream_url(self, stream_url: str):
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-show_format", "-show_streams", stream_url],
                capture_output=True,
                text=True,
                timeout=60  # Longer timeout for potential network delays
            )
            if result.returncode == 0:
                self.logger.info("Successfully probed the stream.")
                return 0
            else:
                self.logger.error(f"Failed attempt to probe the stream (return code {result.returncode}): [{result.stderr.strip()}]")
                # TODO if it's 403 return 1, else return 2, for now our code assumes 403 if failure, kinda brittle
                return 1
        except subprocess.TimeoutExpired:
            self.logger.error("ffprobe timed out while probing the stream, aborting")
        except Exception as e:
            self.logger.error(f"Error probing the stream, aborting: {e}")
        return 2


