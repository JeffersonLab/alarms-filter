#!/bin/bash

[ -z "$BOOTSTRAP_SERVERS" ] && echo "BOOTSTRAP_SERVERS environment required" && exit 1;

# Grab first SERVER from SERVERS CSV env
IFS=','
read -ra tmpArray <<< "$BOOTSTRAP_SERVERS"

BOOTSTRAP_SERVER=${tmpArray[0]}

help=$'Usage:\n'
help+="  Set:   $0 [-n] name [-e] expression"
help+=$'\n'
help+="  Unset: $0 [-n] name -u"

while getopts ":c:t:m:o:u" opt; do
  case ${opt} in
    u )
      unset=true
      ;;
    n )
      name=$OPTARG
      ;;
    e )
      expression=$OPTARG
      ;;
    \? ) echo "$help"
      ;;
  esac
done

if ((OPTIND == 1))
then
    echo "$help"
    exit
fi

if [ ! "$channel" ]
then
  echo "$help"
  exit;
fi

if [ "$unset" ]
then
  expression = null

CWD=$(readlink -f "$(dirname "$0")")

APP_HOME=$CWD/..

FILTER_JAR=`ls $APP_HOME/lib/alarms-filter*`
CLIENTS_JAR=`ls $APP_HOME/libs/kafka-clients-*`
JACK_CORE=`ls $APP_HOME/libs/jackson-core-*`
JACK_BIND=`ls $APP_HOME/libs/jackson-databind-*`
JACK_ANN=`ls $APP_HOME/libs/jackson-annotations-*`
SLF4J_API=`ls $APP_HOME/libs/slf4j-api-*`
SLF4J_IMP=`ls $APP_HOME/libs/slf4j-log4j*`
LOG4J_IMP=`ls $APP_HOME/libs/log4j-*`
LOG4J_CONF=$APP_HOME/config

RUN_CP="$FILTER_JAR:$CLIENTS_JAR:$SLF4J_API:$SLF4J_IMP:$LOG4J_IMP:$LOG4J_CONF:$JACK_CORE:$JACK_BIND:$JACK_ANN"

java -Dlog.dir=$APP_HOME/logs -cp $RUN_CP org.jlab.alarms.client.CommandProducer $BOOTSTRAP_SERVERS filter-commands $name $expression