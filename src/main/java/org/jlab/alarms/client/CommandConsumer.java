package org.jlab.alarms.client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class CommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(CommandConsumer.class);

    public static void main(String[] args) {
        String servers = args[0];
        String topic = args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", "CommandConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

         TopicInfo info = new TopicInfo();

        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    consumer.seekToBeginning(partitions);
                    
                    info.lastOffset = consumer.endOffsets(partitions).values().toArray(new Long[0])[0] - 1;

                    if(info.lastOffset == -1) {
                        info.empty = true; // Never been any messages in topic!
                    }
                }
            });

            while (!info.empty) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("%s=%s%n", record.key(), record.value());
                    info.empty = (info.lastOffset == record.offset());
		        }
           }
	}
    }

    static class TopicInfo {
       public long lastOffset = 0;
       public boolean empty = false;
    }
}

