package org.jlab.alarms.client.extras;

import org.jlab.alarms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class ConsumerAPITest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerAPITest.class);

    public static void main(String[] args) throws InterruptedException {
        final String servers = args[0];

        final Properties props = new Properties();

        props.put(EventSourceConfig.EVENT_SOURCE_TOPIC, "registered-alarms");
        props.put(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, servers);
        props.put(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_SCHEMA_REGISTRY_URL, "http://registry:8081");

        final EventSourceConsumer<String, RegisteredAlarm> consumer = new EventSourceConsumer<>(props);

        consumer.addListener(new EventSourceListener<>() {
            @Override
            public void update(List<EventSourceRecord<String, RegisteredAlarm>> changes) {
                for (EventSourceRecord<String, RegisteredAlarm> record : changes) {
                    System.out.println(record);
                }
                consumer.close();
            }
        });

        consumer.start();
        consumer.join(); // block until first update, which contains current state of topic
    }
}

