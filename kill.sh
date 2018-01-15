#!/bin/bash
set -x

docker run -it --rm \
--network apachestormstatefulboltwarmcache_default \
storm \
storm kill topologyName
