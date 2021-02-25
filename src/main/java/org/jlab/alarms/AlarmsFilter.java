package org.jlab.alarms;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmsFilter {
    private static final Logger log = LoggerFactory.getLogger(AlarmsFilter.class);

    public static final String INPUT_TOPIC = "active-alarms";
    public static final String OUTPUT_TOPIC = "filtered-active-alarms";

    public static final SpecificAvroSerde<ActiveAlarmKey> INPUT_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarmValue> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarmKey> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
    public static final SpecificAvroSerde<ActiveAlarmValue> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

    /**
     * Enumerations of all channels with expiration timers, mapped to the cancellable Executor handle.
     */
    public static Map<String, Cancellable> channelHandleMap = new ConcurrentHashMap<>();

    static Properties getStreamsConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");

        registry = (registry == null) ? "http://localhost:8081" : registry;

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shelved-timer");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching
        props.put(SCHEMA_REGISTRY_URL_CONFIG, registry);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * Create the Kafka Streams Domain Specific Language (DSL) Topology.
     *
     * @param props The streams configuration
     * @return The Topology
     */
    static Topology createTopology(Properties props) {
        final StreamsBuilder builder = new StreamsBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        INPUT_VALUE_SERDE.configure(config, false);

        final KStream<ActiveAlarmKey, ActiveAlarmValue> input = builder.stream(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        final KStream<ActiveAlarmKey, ActiveAlarmValue> output = input.transform(new MsgTransformerFactory());

        output.to(OUTPUT_TOPIC, Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    /**
     * Factory to create Kafka Streams Transformer instances; references a stateStore to maintain previous
     * RegisteredAlarms.
     */
    private static final class MsgTransformerFactory implements TransformerSupplier<ActiveAlarmKey, ActiveAlarmValue, KeyValue<ActiveAlarmKey, ActiveAlarmValue>> {

        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        @Override
        public Transformer<ActiveAlarmKey, ActiveAlarmValue, KeyValue<ActiveAlarmKey, ActiveAlarmValue>> get() {
            return new Transformer<ActiveAlarmKey, ActiveAlarmValue, KeyValue<ActiveAlarmKey, ActiveAlarmValue>>() {
                private ProcessorContext context;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<ActiveAlarmKey, ActiveAlarmValue> transform(ActiveAlarmKey key, ActiveAlarmValue value) {
                    KeyValue<ActiveAlarmKey, ActiveAlarmValue> result = null; // null returned to mean no record

                    log.debug("Handling message: {}={}", key, value);

                    if(key.getType() == ActiveMessageType.EPICSAck) { // We only let EPICS acks through (as a test!)
                        result = new KeyValue<>(key, value);
                    }

                    return result;
                }

                @Override
                public void close() {
                    // Nothing to do
                }
            };
        }
    }

    /**
     * Entrypoint of the application.
     *
     * @param args The command line arguments
     */
    public static void main(String[] args) {
        final Properties props = getStreamsConfig();
        final Topology top = createTopology(props);
        final KafkaStreams streams = new KafkaStreams(top, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
