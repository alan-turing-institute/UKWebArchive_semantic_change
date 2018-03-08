#!/bin/sh
#---------------------------------#
# dynamically build the classpath #
#---------------------------------#
THE_CLASSPATH=./:./target/ukwebarchive-0.1-SNAPSHOT.jar
for i in `ls ./target/lib/*.jar`
do
  THE_CLASSPATH=${THE_CLASSPATH}:${i}
done

#---------------------------#
# run                       #
#---------------------------#
java -Xmx16G -cp ".:${THE_CLASSPATH}" $1
