#!/bin/sh

docker buildx build --platform linux/arm64 -f Dockerfile_vpn_manager -t ghcr.io/dcaulton/vpn-manager:latest --push .
