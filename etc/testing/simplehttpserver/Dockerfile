# Local-mode fixture for pipeline-service tests that pin
# trinitronx/python-simplehttpserver (an external image): the tests run
# `python -m SimpleHTTPServer <port>`, which is Python 2 syntax. Ubuntu
# 16.04 ships /usr/bin/python -> python2.7, so this image satisfies those
# pipelines without network access. Built locally:
#
#   docker build -t trinitronx/python-simplehttpserver:latest etc/testing/simplehttpserver
FROM ubuntu:16.04
RUN apt-get update && apt-get install -y --no-install-recommends python && rm -rf /var/lib/apt/lists/*
