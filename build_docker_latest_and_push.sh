#!/bin/sh

docker buildx build --platform linux/arm64 -t ghcr.io/dcaulton/iptv-recorder:latest --push .
