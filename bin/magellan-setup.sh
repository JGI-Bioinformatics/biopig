#!/bin/sh
#
# configure hadoop environment with biopig on NERSC's magellan cloud testbed
# to run:
# ./magellan-setup.sh

DIRECTORY=$(cd `dirname $0` && pwd)

module unload hadoop
module load tig hadoop/magellan

export HADOOPDIR=/usr/common/tig/hadoop/hadoop-0.20.2+228/conf-magellan/
export PIG_CLASSPATH=/usr/common/tig/hadoop/hadoop-0.20.2+228/conf-magellan/:$DIRECTORY/../lib/biopig-core-0.3.0-job.jar
export PIG_OPTS="-Xmx1G -Dmapred.compress.map.output=true -Dmapred.child.java.opts=-Xmx2G"
