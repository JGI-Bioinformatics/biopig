#!/usr/bin/env bash

#
# bash script to setup the meta environment
#

#
# first check to see if java is setup, if not, use the jgi-specific location
#
if [! -n $JAVA_HOME ]; then
    export JAVA_HOME=/jgi/tools/SUN/jdk/DEFAULT
    export JDK_HOME=/jgi/tools/SUN/jdk/DEFAULT
fi

#
# now do the same with groovy
#
if [! -n $JAVA_HOME ]; then
   export GROOVY_HOME=/jgi/tools/lang/groovy/current/
   export GROOVY_CONF=~/.groovy/groovy-starter.conf
   export PATH=${PATH}:${GROOVY_HOME}/bin
fi

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


# check to see if groovy is in the path already

which groovy > /dev/null

if [ $? -eq 0 ]; then

   echo "found groovy in the path, skipping"

else

    for x in  /jgi/tools/lang/groovy /opt/local/groovy /usr/local/groovy ${HOME}/groovy ${HOME}/local/groovy
     do
        if [ -f $x ]; then
            echo "found groovy at $x";
            break;
        fi
        echo "no groovy found!  make sure you have groovy in your path"
     done
fi



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
    mkdir ${HOME}/.groovy

    echo -e "# load required libraries
load !{groovy.home}/lib/*.jar
# load user specific libraries
load !{user.home}/.groovy/lib/*.jar
# tools.jar for ant tasks
load \${tools.jar}

load !{meta.home}/lib/*.jar
" >>  ${HOME}/.groovy/groovy-starter.conf

fi
