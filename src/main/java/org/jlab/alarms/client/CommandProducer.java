package org.jlab.alarms.client;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jlab.alarms.CommandRecord;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class CommandProducer {
    public static void main(String[] args) {
        String servers = args[0];
        String commandTopic = args[1];
        String outTopic = args[2];
        String unset = args[3];
        String filterName = args[4];
        String alarmNameCsv = args[5];
        String locationCsv = args[6];
        String categoryCsv = args[7];

        CommandRecord record = new CommandRecord();
        record.setOutputTopic(outTopic);

        String key = record.getKey().toJSON();
        String value;

        if("true".equals(unset)) {
            value = null;
        } else {
            record.setFilterName(filterName);
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

    static Set<String> fromCsv(String csv) {
        Set<String> result = null;

        if(csv != null && !csv.isEmpty()) {
            String[] resultArray = csv.split(",");
            result = new HashSet<String>(Arrays.asList(resultArray));
        }

        return result;
    }
}