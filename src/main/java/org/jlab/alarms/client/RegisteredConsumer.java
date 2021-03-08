package org.jlab.alarms.client;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jlab.alarms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class RegisteredConsumer {

    private static final Logger log = LoggerFactory.getLogger(RegisteredConsumer.class);

    public static void main(String[] args) throws InterruptedException {
        final String servers = args[0];

        final Properties props = new Properties();

        props.put(EventSourceConfig.EVENT_SOURCE_TOPIC, "registered-alarms");
        props.put(EventSourceConfig.EVENT_SOURCE_GROUP, "RegisteredConsumer");
        props.put(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, servers);
        props.put(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(EventSourceConfig.EVENT_SOURCE_SCHEMA_REGISTRY_URL, "http://registry:8081");

        final EventSourceConfig config = new EventSourceConfig(props);
        final EventSourceConsumer<String, RegisteredAlarm> con = new EventSourceConsumer<>(config);

        con.addListener(new EventSourceListener<String, RegisteredAlarm>() {
            @Override
            public void update(List<EventSourceRecord<String, RegisteredAlarm>> changes) {
                for(EventSourceRecord<String, RegisteredAlarm> record: changes) {
                    System.out.println(record);
                }
                con.close();
            }
        });

        con.start();
        con.join(); // block until first update, which contains current state of topic
    }
}

