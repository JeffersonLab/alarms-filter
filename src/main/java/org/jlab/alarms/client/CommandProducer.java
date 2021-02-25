package org.jlab.alarms.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CommandProducer {
    public static void main(String[] args) {
        String servers = args[0];
        String commandTopic = args[1];
        String name = args[2];
        String expression = args[3];

        String key = "{\"name\":\"" + name + "\"}";
        String value;

        if(expression == null) {
            value = null;
        } else {
            value = "{\"filterExpression\":\"" + expression + "\"}";
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<String, String>(commandTopic, key, value));
        }
    }
}

