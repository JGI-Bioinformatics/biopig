#!/bin/sh

mvn install:install-file -Dfile=apache-cassandra-0.6.1.jar -DgroupId=apache-cassandra -DartifactId=apache-cassandra -Dversion=0.6.1 -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=libthrift-r917130.jar -DgroupId=thrift -DartifactId=libthrift -Dversion=0.6.0 -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=flexjson.jar -DgroupId=flexjson -DartifactId=flexjson -Dversion=1.7.0 -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=json-simple-1.1.jar -DgroupId=jsonsimple -DartifactId=jsonsimple -Dversion=1.1 -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=high-scale-lib.jar -DgroupId=high-scale-lib -DartifactId=high-scale-lib -Dversion=UNKNOWN -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=clhm-production.jar -DgroupId=com.reardencommerce -DartifactId=clhm -Dversion=UNKNOWN -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=biojava.jar -DgroupId=org.biojava -DartifactId=biojava -Dversion=UNKNOWN -Dpackaging=jar -DgeneratePom=true

