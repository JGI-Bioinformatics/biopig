#!/usr/bin/env bash

#
# bash script to setup the meta environment
#

# the root of the Pig installation

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
    ls=`ls -ld "$this"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '.*/.*' > /dev/null; then
        this="$link"
    else
        this=`dirname "$this"`/"$link"
    fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`unset CDPATH; cd "$bin"; pwd`
this="$bin/$script"
export META_HOME=`dirname "$this"`/..

echo "setting META_HOME = ${META_HOME}"

export PATH=${META_HOME}/bin:${PATH}
export MANPATH=${META_HOME}/man:${MANPATH}
export JAVA_OPTS=" -Dmeta.home=${META_HOME}"
export HADOOP_CLASSPATH=${META_HOME}/lib:${META_HOME}/conf

#
# check groovy-startup file
# 


if [ -f "${HOME}/.groovy/groovy-starter.conf" ]; then

    grep meta.home ${HOME}/.groovy/groovy-starter.conf > /dev/null

    if [ $? -eq 0 ]; then
       echo "groovy-starter.conf already modified with meta.home... skipping"
    else
       echo "adjusting ${HOME}/.groovy/groovy-starter.conf"
       echo -e "\nload !{meta.home}/lib/*.jar\n " >> ${HOME}/.groovy/groovy-starter.conf
    fi

else

    echo "creating ${HOME}/.groovy/groovy-starter.conf"
    
    echo -e "# load required libraries
load !{groovy.home}/lib/*.jar
# load user specific libraries
load !{user.home}/.groovy/lib/*.jar
# tools.jar for ant tasks
load \${tools.jar}

load !{meta.home}/lib/*.jar
" >>  ${HOME}/.groovy/groovy-starter.conf

fi
