#!/bin/sh

mvn install:install-file -Dfile=apache-cassandra-0.6.0-beta2.jar -DgroupId=apache-cassandra -DartifactId=apache-cassandra -Dversion=0.6.0 -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=libthrift-r917130.jar -DgroupId=thrift -DartifactId=libthrift -Dversion=0.6.0 -Dpackaging=jar -DgeneratePom=true

