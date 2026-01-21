#!/bin/sh

docker buildx build --platform linux/arm64 -f Dockerfile_iptv_recorder -t ghcr.io/dcaulton/iptv-recorder:latest --push .
