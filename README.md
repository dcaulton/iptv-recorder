# iptv-recorder
Simple ffmpeg-based recorder for IPTV streams, with VPN support via sidecars

- Install: `pip install -r requirements.txt`
- In code, import from the shared lib:
```python
from tv_detection_common.models import Base, Channel, Schedule, Recording, RecordingStatus
