#!/bin/sh

docker buildx build --platform linux/arm64 -f Dockerfile_iptv_recorder -t ghcr.io/dcaulton/iptv-recorder:v0.1 --push .
