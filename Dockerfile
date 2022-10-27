# syntax=docker/dockerfile:1
FROM ubuntu:latest
WORKDIR /

COPY puller puller
COPY solution solution
COPY peakload peakload
COPY extend extend

COPY run.sh run.sh

ENTRYPOINT ["bash", "run.sh"]