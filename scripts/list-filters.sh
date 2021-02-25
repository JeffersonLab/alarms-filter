#!/bin/bash

[ -z "$BOOTSTRAP_SERVERS" ] && echo "BOOTSTRAP_SERVERS environment required" && exit 1;

CWD=$(readlink -f "$(dirname "$0")")

APP_HOME=$CWD/..

FILTER_JAR=`ls $APP_HOME/lib/alarms-filter-*`
CLIENTS_JAR=`ls $APP_HOME/lib/kafka-clients-*`
JACK_CORE=`ls $APPP_HOME/lib/jackson-core-*`
JACK_BIND=`ls $APP_HOME/lib/jackson-databind-*`
JACK_ANN=`ls $APP_HOME/lib/jackson-annotations-*`
SLF4J_API=`ls $APP_HOME/lib/slf4j-api-*`
SLF4J_IMP=`ls $APP_HOME/lib/slf4j-log4j*`
LOG4J_IMP=`ls $APP_HOME/lib/log4j-*`
LOG4J_CONF=$CWD

RUN_CP="$FILTER_JAR:$CLIENTS_JAR:$SLF4J_API:$SLF4J_IMP:$LOG4J_IMP:$LOG4J_CONF:$JACK_CORE:$JACK_BIND:$JACK_ANN"

java -cp $RUN_CP CommandConsumer $BOOTSTRAP_SERVERS filter-commands