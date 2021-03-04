package org.jlab.alarms;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.health.ConnectClusterDetails;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmsFilter {
    private static final Logger log = LoggerFactory.getLogger(AlarmsFilter.class);

    public static final String INPUT_TOPIC = "active-alarms";

    public static final SpecificAvroSerde<ActiveAlarmKey> INPUT_KEY_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarmValue> INPUT_VALUE_SERDE = new SpecificAvroSerde<>();
    public static final SpecificAvroSerde<ActiveAlarmKey> OUTPUT_KEY_SERDE = INPUT_KEY_SERDE;
    public static final SpecificAvroSerde<ActiveAlarmValue> OUTPUT_VALUE_SERDE = INPUT_VALUE_SERDE;

    static AdminClient admin;

    final static Map<CommandRecord.CommandKey, KafkaStreams> streamsList = new ConcurrentHashMap<>();

    final static Map<String, RegisteredAlarm> registeredAlarms = new ConcurrentHashMap<>();

    final static CountDownLatch latch = new CountDownLatch(1);

    static Properties getAdminConfig() {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        final Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "alarms-filter-admin");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return props;
    }

    static Properties getStreamsConfig(String outputTopic) {

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        bootstrapServers = (bootstrapServers == null) ? "localhost:9092" : bootstrapServers;

        String registry = System.getenv("SCHEMA_REGISTRY");

        registry = (registry == null) ? "http://localhost:8081" : registry;

        final Properties props = new Properties();
        props.put("OUTPUT_TOPIC", outputTopic);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alarms-filter-" + outputTopic);
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
    static Topology createTopology(Properties props, CommandRecord command) {
        final StreamsBuilder builder = new StreamsBuilder();

        // If you get an unhelpful NullPointerException in the depths of the AVRO deserializer it's likely because you didn't set registry config
        Map<String, String> config = new HashMap<>();
        config.put(SCHEMA_REGISTRY_URL_CONFIG, props.getProperty(SCHEMA_REGISTRY_URL_CONFIG));

        INPUT_KEY_SERDE.configure(config, true);
        INPUT_VALUE_SERDE.configure(config, false);


        final KStream<ActiveAlarmKey, ActiveAlarmValue> input = builder.stream(INPUT_TOPIC, Consumed.with(INPUT_KEY_SERDE, INPUT_VALUE_SERDE));

        final KStream<ActiveAlarmKey, ActiveAlarmValue> output = input.transform(new MsgTransformerFactory(command));

        output.to(props.getProperty("OUTPUT_TOPIC"), Produced.with(OUTPUT_KEY_SERDE, OUTPUT_VALUE_SERDE));

        return builder.build();
    }

    /**
     * Factory to create Kafka Streams Transformer instances.
     */
    private static final class MsgTransformerFactory implements TransformerSupplier<ActiveAlarmKey, ActiveAlarmValue, KeyValue<ActiveAlarmKey, ActiveAlarmValue>> {

        private final CommandRecord command;

        public MsgTransformerFactory(CommandRecord command) {
            this.command = command;
        }

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

                    log.debug("Applying filter: {}", command.getFilterName());

                    Set<String> alarmNames = command.getAlarmNames();
                    Set<String> locations = command.getLocations();
                    Set<String> categories = command.getCategories();

                    String alarmName = key.getName();
                    String location = null;
                    String category = null;

                    RegisteredAlarm alarm = registeredAlarms.get(alarmName);

                    if(alarm != null) {
                        location = alarm.getLocation().name();
                        category = alarm.getCategory().name();
                    }

                    boolean nameMatch = true;
                    boolean locationMatch = true;
                    boolean categoryMatch = true;

                    if(alarmNames != null) {
                        nameMatch = alarmNames.contains(alarmName);
                    }

                    if(locations != null) {
                        locationMatch = locations.contains(location);
                    }

                    if(categories != null) {
                        categoryMatch = categories.contains(category);
                    }

                    if(nameMatch && locationMatch && categoryMatch) {
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
        Properties adminProps = getAdminConfig();
        admin = AdminClient.create(adminProps);

        RegisteredAlarmsConsumer registeredConsumer = new RegisteredAlarmsConsumer();

        CommandConsumerConfig commandConfig = new CommandConsumerConfig(new HashMap<>());

        CommandTopicConsumer commandConsumer = new CommandTopicConsumer(new CommandChangeListener() {
            @Override
            public void update(List<CommandRecord> changes) {
                // 1. Stop Stream and destroy outputTopic
                // 2. If set command, create new stream with new topic
                try {
                    for (CommandRecord command : changes) {
                        unsetStream(command); // Always attempt to clear stream first when new command comes

                        if (command.getValue() != null) { // Only set new stream if command value is not null
                            setStream(command);
                        }
                    }
                } catch(ExecutionException | InterruptedException e) {
                    shutdown(e);
                }
            }
        }, commandConfig);

        commandConsumer.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                shutdown(null);
            }
        });

        try {
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void shutdown(Exception e) {
        log.info("Shutting Down Streams");
        if(e != null) {
            e.printStackTrace();
        }

        for(KafkaStreams streams: streamsList.values()) {
            streams.close(); // blocks...
        }

        if(admin != null) {
            admin.close();
        }

        latch.countDown();
    }

    public static void setStream(CommandRecord command) {
        log.debug("setStream: {}", command.getOutputTopic());

        final Properties props = getStreamsConfig(command.getOutputTopic());
        final Topology top = createTopology(props, command);
        final KafkaStreams streams = new KafkaStreams(top, props);

        streamsList.put(command.getKey(), streams);

        streams.start();
    }

    public static void unsetStream(CommandRecord command) throws ExecutionException, InterruptedException {
        log.debug("unsetStream: {}", command.getOutputTopic());
        KafkaStreams streams = streamsList.remove(command.getKey());

        if(streams != null) {
            streams.close(); // blocks...
        }

        // Always attempt to destroy (cleanup topic) too
        String topic = command.getOutputTopic();

        DeleteTopicsResult result = admin.deleteTopics(Arrays.asList(topic));

        result.all().get(); // blocks...
    }
}
