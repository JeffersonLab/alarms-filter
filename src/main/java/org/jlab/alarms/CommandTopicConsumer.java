package org.jlab.alarms;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Rewinds and Replays a command topic to determine the output topics and filters to apply, and then listens for
 * changes so that the Transformer can be notified to reconfigure.
 *
 * The command topic is Event Sourced so records with duplicate keys are allowed and records later in the
 * stream overwrite ones earlier in the stream if they have the same key.  Tombstone records also are observed.
 * Topic compaction should be enabled to minimize duplicate keys.  Command topic keys are unique Strings representing
 * the name of the filter and values are JSON structures containing the name of the output topic and filter criteria.
 * Therefore it is possible to have multiple filters writing to the same output topic.
 */
public class CommandTopicConsumer extends Thread implements AutoCloseable {
    private final Logger log = LoggerFactory.getLogger(CommandTopicConsumer.class);
    private AtomicReference<TRI_STATE> state = new AtomicReference<>(TRI_STATE.INITIALIZED);
    private final CommandChangeListener listener;
    private final Object config;
    private HashMap<CommandRecord.CommandKey, CommandRecord> commands = new HashMap<>();
    private KafkaConsumer<String, String> consumer;
    private Map<Integer, TopicPartition> assignedPartitionsMap = new HashMap<>(); // empty map initially to avoid NPE
    private Map<TopicPartition, Long> endOffsets;
    private boolean reachedEnd = false;
    private Long pollMillis;

    public CommandTopicConsumer(CommandChangeListener listener, CommandConsumerConfig config) {
        this.listener = listener;
        this.config = config;

        log.debug("Creating CommandTopicConsumer");

        String kafkaUrl = config.getString(CommandConsumerConfig.COMMAND_BOOTSTRAP_SERVERS);
        String commandTopic = config.getString(CommandConsumerConfig.COMMAND_TOPIC);
        String commandGroup = config.getString(CommandConsumerConfig.COMMAND_GROUP);
        pollMillis = config.getLong(CommandConsumerConfig.COMMAND_POLL_MILLIS);

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("group.id", commandGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        log.info("Consume command channel from: " + kafkaUrl);

        consumer = new KafkaConsumer<>(props);

        // Read all records up to the high water mark (most recent records) / end offsets
        Map<Integer, Boolean> partitionEndReached = new HashMap<>();

        consumer.subscribe(Collections.singletonList(commandTopic), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.debug("Seeking to beginning of topic");
                if(partitions.size() != 1) { // I think the code below actually handles multiple partitions fine, but  it's the principle of the matter!
                    throw new IllegalArgumentException("The command topic must have exactly one partition");
                }
                assignedPartitionsMap = partitions.stream().collect(Collectors.toMap(TopicPartition::partition, p -> p));
                consumer.seekToBeginning(partitions);
                endOffsets = consumer.endOffsets(partitions);
                for(TopicPartition p: endOffsets.keySet()) {
                    Long value = endOffsets.get(p);
                    if(value == 0) {
                        partitionEndReached.put(p.partition(), true);
                        log.info("Empty channels list to begin with on partition: " + p.partition());
                    }
                }
            }
        });

        // Note: first poll triggers seek to beginning
        int tries = 0;

        // Maybe we should just ensure single partition to make this simpler?
        while(!reachedEnd) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMillis));

            log.debug("found " + records.count() + " records");

            for (ConsumerRecord<String, String> record : records) {
                commandUpdate(record);

                log.trace("Looking for last index: {}, found: {}", endOffsets.get(assignedPartitionsMap.get(record.partition())), record.offset() + 1);

                if(record.offset() + 1 == endOffsets.get(assignedPartitionsMap.get(record.partition()))) {
                    log.debug("end of partition {} reached", record.partition());
                    partitionEndReached.put(record.partition(), true);
                }
            }

            if(++tries > 10) {
                // We only poll a few times before saying enough is enough.
                throw new RuntimeException("Took too long to obtain initial list of channels");
            }

            // If all partitions ends are reached, we are up-to-date
            int endedCount = 0;
            for(Integer partition: assignedPartitionsMap.keySet()) {
                Boolean ended = partitionEndReached.getOrDefault(partition, false);

                if(ended) {
                    endedCount++;
                }
            }
            if(endedCount == assignedPartitionsMap.size()) {
                reachedEnd = true;
            }
        }

        log.trace("done with ChannelManager constructor");
    }

    private void commandUpdate(ConsumerRecord<String, String> record) {
        log.debug("Command update: {}={}", record.key(), record.value());

        CommandRecord.CommandKey key = null;

        try {
            key = CommandRecord.CommandKey.fromJSON(record.key());
        } catch(JsonProcessingException e) {
            throw new RuntimeException("Unable to parse JSON key from command topic", e);
        }

        if(record.value() == null) {
            log.info("Removing record: {}", key.getName());
            commands.remove(key);
        } else {
            log.info("Adding record: {}", key.getName());
            CommandRecord cr = null;
            CommandRecord.CommandValue value = null;
            try {
                value = CommandRecord.CommandValue.fromJSON(record.value());
            } catch(JsonProcessingException e) {
                throw new RuntimeException("Unable to parse JSON value from command topic", e);
            }
            cr = new CommandRecord(key, value);
            commands.put(key, cr);
        }
    }

    @Override
    public void run() {
        log.debug("Starting CommandTopicConsumer run method");
        try {
            // Only move to running state if we are currently initialized (don't move to running if closed)
            boolean transitioned = state.compareAndSet(TRI_STATE.INITIALIZED, TRI_STATE.RUNNING);

            log.trace("State transitioned?: " + transitioned);

            // Once set, we wait until changes have settled to avoid call this too frequently with changes happening
            boolean needUpdate = false;

            // Listen for changes
            while (state.get() == TRI_STATE.RUNNING) {
                log.debug("polling for changes");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollMillis));

                if (records.count() > 0) { // We have changes
                    for(ConsumerRecord<String, String> record: records) {
                        commandUpdate(record);
                    }

                    log.info("Change in command list: request update once settled");
                    needUpdate = true;

                } else { // No changes, settled
                    if(needUpdate) {
                        log.info("No changes (we've settled), so notify we have an update");
                        listener.update();
                        needUpdate = false;
                    }
                }
            }
        } catch (WakeupException e) {
            log.debug("Change monitor thread WakeupException caught");
            // Only a problem if running, ignore exception if closing
            if (state.get() == TRI_STATE.RUNNING) throw e;
        } finally {
            consumer.close();
        }

        log.trace("Change monitor thread exited cleanly");
    }

    public Set<CommandRecord> getCommands() {
        log.debug("getCommands()");

        return new HashSet<CommandRecord>(commands.values());
    }

    @Override
    public void close() {
        log.info("Shutting down the channels topic monitoring thread");

        TRI_STATE previousState = state.getAndSet(TRI_STATE.CLOSED);

        // If never started, just release resources immediately
        if(previousState == TRI_STATE.INITIALIZED) {
            consumer.close();
        } else {
            consumer.wakeup();
        }
    }

    private enum TRI_STATE {
        INITIALIZED, RUNNING, CLOSED;
    }
}
