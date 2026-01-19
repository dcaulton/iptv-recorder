# this is needed, otherwise the build can't find external DNS for whatever reason

docker build --network host -t ghcr.io/dcaulton/iptv-recorder:latest .

