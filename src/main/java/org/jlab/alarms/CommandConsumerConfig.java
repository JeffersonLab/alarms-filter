package org.jlab.alarms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class CommandConsumerConfig extends AbstractConfig {
    public static final String COMMAND_TOPIC = "command.topic";
    public static final String COMMAND_GROUP = "command.group";
    public static final String COMMAND_POLL_MILLIS = "command.poll.millis";
    public static final String COMMAND_BOOTSTRAP_SERVERS = "command.bootstrap.servers";

    public CommandConsumerConfig(Map originals) {
        super(configDef(), originals, false);
    }

    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(COMMAND_TOPIC,
                        ConfigDef.Type.STRING,
                        "filter-commands",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka topic to monitor for filter commands")
                .define(COMMAND_GROUP,
                        ConfigDef.Type.STRING,
                        "alarms-filter",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka consumer group to use when monitoring the COMMAND_TOPIC")
                .define(COMMAND_POLL_MILLIS,
                        ConfigDef.Type.LONG,
                        5000l,
                        ConfigDef.Importance.HIGH,
                        "Milliseconds between polls for command topic changes - update delay is twice this value since the command thread waits for 'no changes' poll response before requesting update")
                .define(COMMAND_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        "localhost:9092",
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of host and port pairs that are the addresses of the Kafka brokers used to query the command topic");
    }
}
