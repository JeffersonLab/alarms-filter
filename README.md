# alarms-filter [![Java CI with Gradle](https://github.com/JeffersonLab/alarms-filter/workflows/Java%20CI%20with%20Gradle/badge.svg)](https://github.com/JeffersonLab/alarms-filter/actions?query=workflow%3A%22Java+CI+with+Gradle%22) [![Docker Image Version (latest semver)](https://img.shields.io/docker/v/slominskir/alarms-filter?sort=semver&label=DockerHub)](https://hub.docker.com/r/slominskir/alarms-filter)
A [Kafka Streams](https://kafka.apache.org/documentation/streams/) application to filter active alarms into separate topics in [JAWS](https://github.com/JeffersonLab/jaws).  This app provides the ability to suppress alarms by design.  For example, when a group of alarms need to be disabled due to a portion of the machinery being turned off.  The alarms-filter app monitors a command topic for a set of filtered output topics to create and maintain (with compact cleanup policy).   Each consumer of the active-alarms topic can always create their own local filters, but the alarms-filter app provides a global set of shared filtered topics for all consumers to read from.

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/alarms-filter#quick-start-with-compose)
 - [Build](https://github.com/JeffersonLab/alarms-filter#build)
 - [Configure](https://github.com/JeffersonLab/alarms-filter#configure)
 - [Deploy](https://github.com/JeffersonLab/alarms-filter#deploy)
 - [Docker](https://github.com/JeffersonLab/alarms-filter#docker)
 - [See Also](https://github.com/JeffersonLab/alarms-filter#see-also)
 ---

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/alarms-filter
cd alarms-filter
```
2. Launch Docker
```
docker-compose up
```
3. Create a filter!
```
docker exec filter /opt/alarms-filter/bin/set-filter.sh -n only-alarm1 -o my-filtered-topic -a alarm1
```
4. Trips some alarms!
```
docker exec jaws /scripts/client/set-alarming.py alarm1
docker exec jaws /scripts/client/set-alarming.py alarm2
```
5. Verify our filtered topic only contains alarm1
```
docker exec jaws /scripts/client/list-active.py
docker exec jaws /scripts/client/list-active.py --topic my-filtered-topic
```
## Build
This [Java 11](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/alarms-filter
cd alarms-filter
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

**Note**: When developing the app you can mount the build artifact into the container by substituting the `docker-compose up` command with:
```
docker-compose -f docker-compose.yml -f docker-compose-dev.yml up
```

## Configure
Environment Variables

| Name | Description |
|---|---|
| BOOTSTRAP_SERVERS | Comma-separated list of host and port pairs pointing to a Kafka server to bootstrap the client connection to a Kafka Cluser; example: `kafka:9092` |
| SCHEMA_REGISTRY | URL to Confluent Schema Registry; example: `http://registry:8081` |

## Deploy
The Kafka Streams app is a regular Java application, and start scripts are created and dependencies collected by the Gradle distribution targets:

```
gradlew assembleDist
```

[Releases](https://github.com/JeffersonLab/alarms-filter/releases)

Launch with:

UNIX:
```
bin/alarms-filter
```
Windows:
```
bin/alarms-filter.bat
```

## Docker
```
docker pull slominskir/alarms-filter
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/alarms-filter)

## See Also
 - [Developer Notes](https://github.com/JeffersonLab/alarms-filter/wiki/Developer-Notes)
