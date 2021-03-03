#!/bin/bash

[ -z "$BOOTSTRAP_SERVERS" ] && echo "BOOTSTRAP_SERVERS environment required" && exit 1;

# Grab first SERVER from SERVERS CSV env
IFS=','
read -ra tmpArray <<< "$BOOTSTRAP_SERVERS"

BOOTSTRAP_SERVER=${tmpArray[0]}

help=$'Usage:\n'
help+="  Set:   $0 [-n] filterName [-o] outTopic [-a] alarmNameCsv [-c] categoryCsv [-l] locationCsv"
help+=$'\n'
help+="  Unset: $0 [-n] filterName -u"

while getopts ":u:o:n:a:c:l" opt; do
  case ${opt} in
    u )
      unset=true
      ;;
    o )
      outTopic=$OPTARG
      ;;
    n )
      filterName=$OPTARG
      ;;
    a )
      alarmNameCsv=$OPTARG
      ;;
    c )
      categoryCsv=$OPTARG
      ;;
    l )
      locationCsv=$OPTARG
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


CWD=$(readlink -f "$(dirname "$0")")

APP_HOME=$CWD/..

FILTER_JAR=`ls $APP_HOME/lib/alarms-filter*`
CLIENTS_JAR=`ls $APP_HOME/lib/kafka-clients-*`
JACK_CORE=`ls $APP_HOME/lib/jackson-core-*`
JACK_BIND=`ls $APP_HOME/lib/jackson-databind-*`
JACK_ANN=`ls $APP_HOME/lib/jackson-annotations-*`
SLF4J_API=`ls $APP_HOME/lib/slf4j-api-*`
SLF4J_IMP=`ls $APP_HOME/lib/slf4j-log4j*`
LOG4J_IMP=`ls $APP_HOME/lib/log4j-*`
LOG4J_CONF=$APP_HOME/config

RUN_CP="$FILTER_JAR:$CLIENTS_JAR:$SLF4J_API:$SLF4J_IMP:$LOG4J_IMP:$LOG4J_CONF:$JACK_CORE:$JACK_BIND:$JACK_ANN"

java -Dlog.dir=$APP_HOME/logs -cp $RUN_CP org.jlab.alarms.client.CommandProducer $BOOTSTRAP_SERVERS filter-commands "$filterName" "$unset" "$outTopic" "$alarmNameCsv" "$locationCsv" "$categoryCsv"