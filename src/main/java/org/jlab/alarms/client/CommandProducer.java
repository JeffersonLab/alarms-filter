package org.jlab.alarms.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jlab.alarms.CommandRecord;

import java.util.Properties;

public class CommandProducer {
    public static void main(String[] args) {
        String servers = args[0];
        String commandTopic = args[1];
        String filterName = args[2];
        String unset = args[3];
        String outTopic = args[4];
        String alarmNameCsv = args[5];
        String locationCsv = args[6];
        String categoryCsv = args[7];

        String key = "{\"name\":\"" + filterName + "\"}";
        String value;

        if("true".equals(unset)) {
            value = null;
        } else {
            CommandRecord record = new CommandRecord();
            record.setFilterName(filterName);
            record.setOutputTopic(outTopic);
            record.setAlarmNames(fromCsv(alarmNameCsv));
            record.setLocations(fromCsv(locationCsv));
            record.setCategories(fromCsv(categoryCsv));

            value = record.getValue().toJSON();
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<String, String>(commandTopic, key, value));
        }
    }

    static String[] fromCsv(String csv) {
        String[] result = null;

        if(csv != null && !csv.isEmpty()) {
            result = csv.split(",");
        }

        return result;
    }
}