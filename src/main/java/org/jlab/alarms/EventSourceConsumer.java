package org.jlab.alarms;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EventSourceConsumer<K, V> extends Thread implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(EventSourceConsumer.class);

    private final KafkaConsumer<K, V> consumer;
    private final EventSourceConfig config;
    private final Set<EventSourceListener<K, V>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private long endOffset = 0;
    private boolean endReached = false;

    // Both current state and changes since last listener notification are tracked
    private final HashMap<K, EventSourceRecord<K, V>> state = new HashMap<>();
    private final List<EventSourceRecord<K, V>> changes = new ArrayList<>();

    public EventSourceConsumer(EventSourceConfig config) {

        this.config = config;

        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS));
        props.put("group.id", config.getString(EventSourceConfig.EVENT_SOURCE_GROUP));
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // TODO: This must be configurable
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
    }

    public void addListener(EventSourceListener<K, V> listener) {
        listeners.add(listener);
    }

    public void removeListener(EventSourceListener<K, V> listener) {
        listeners.remove(listener);
    }

    public void start() {
        // TODO: Maintain state machine to ensure only call start once!
        try {
            init();
            monitorChanges();
        } finally {
            consumer.close();
        }
    }

    private void init() {
        consumer.subscribe(Collections.singletonList(config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC)), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.debug("Seeking to beginning of topic");

                if(partitions.size() != 1) {
                    throw new IllegalStateException("An Event Sourced topic must have exactly one partition else message ordering is not guaranteed");
                }

                consumer.seekToBeginning(partitions);

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

                endOffset = endOffsets.get(0); // Exactly one partition verified above

                if(endOffset == 0) {
                    log.info("Empty topic to begin with");
                    endReached = true;
                }
            }
        });

        // Note: first poll triggers seek to beginning (so some empty polls are expected)
        int emptyPollCount = 0;

        while(!endReached) {

            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));

            log.debug("found " + records.count() + " records");

            for (ConsumerRecord<K, V> record : records) {
                updateState(record);

                log.trace("Looking for last index: {}, found: {}", endOffset, record.offset() + 1);

                if(record.offset() + 1 == endOffset) {
                    log.debug("end of partition {} reached", record.partition());
                    endReached = true;
                }
            }

            if(records.count() == 0) {
                emptyPollCount++;
            }

            if(emptyPollCount > 10) {
                // Scenario where topic compaction is not configured properly.
                // Last message (highest endOffset) deleted before consumer connected (so never received)!
                // It is also possible server just isn't delivering messages timely, so this may be bad check...
                throw new RuntimeException("Took too long to obtain initial list; verify topic compact policy!");
            }
        }

        if(!changes.isEmpty()) {
            // Toss out old messages on first update
            changes.clear();
            changes.addAll(state.values());
            notifyListeners();
        }

        log.trace("Done with EventSourceConsumer constructor");
    }

    private void monitorChanges() {
        // We wait until changes have settled to avoid notifying listeners of individual changes
        // We use a simple strategy of waiting for a single poll without changes to flush
        // But we won't let changes build up too long either so we check for max poll with changes
        int pollsWithChangesSinceLastFlush = 0;
        boolean hasChanges = false;

        boolean monitor = true;

        while(monitor) {
            log.debug("polling for changes");
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));

            if (records.count() > 0) { // We have changes
                for(ConsumerRecord<K, V> record: records) {
                    updateState(record);
                }

                log.info("Change in command list: request update once settled");
                hasChanges = true;
            } else {
                if(hasChanges) {
                    log.info("Flushing changes since we've settled (we had a poll with no changes)");
                    notifyListeners();
                    hasChanges = false;
                    pollsWithChangesSinceLastFlush = 0;
                }
            }

            // This is an escape hatch in case poll consistently returns changes; otherwise we'd never flush!
            if(pollsWithChangesSinceLastFlush >= config.getLong(EventSourceConfig.EVENT_SOURCE_MAX_POLL_BEFORE_FLUSH)) {
                log.info("Flushing changes due to max poll with changes");
                notifyListeners();
                hasChanges = false;
                pollsWithChangesSinceLastFlush = 0;
            }

            if(hasChanges) {
                pollsWithChangesSinceLastFlush++;
            }
        }
    }

    private void notifyListeners() {
        for(EventSourceListener<K, V> listener: listeners) {
            listener.update(changes);
        }
        changes.clear();
    }

    private void updateState(ConsumerRecord<K, V> record) {
        log.debug("State update: {}={}", record.key(), record.value());

        EventSourceRecord<K, V> esr = new EventSourceRecord<>(record.key(), record.value());
        changes.add(esr);

        if(record.value() == null) {
            log.info("Removing record: {}", record.key());
            state.remove(record.key());
        } else {
            log.info("Adding record: {}", record.key());
            state.put(record.key(), esr);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
