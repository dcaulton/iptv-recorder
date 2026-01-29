import logging
import sys
from utils.iptv_recorder import IptvRecorder

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout,  # explicit stdout (Docker captures this)
    force=True          # override any existing config
)
logger = logging.getLogger(__name__)
logger.info(f"starting...")

iptv_recorder = IptvRecorder(logger)

if __name__ == "__main__":
    iptv_recorder.narrow_channels('uk')
    iptv_recorder.streams_for_channels()
    iptv_recorder.test_channels_with_streams('uk')
#    iptv_recorder.scan_for_valid_streams()
#    iptv_recorder.scan_for_valid_streams(country_in='gb')
#    iptv_recorder.scan_for_valid_streams(country_in='uk')
    iptv_recorder.main_loop()
