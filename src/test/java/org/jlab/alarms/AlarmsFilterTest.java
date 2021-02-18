package org.jlab.alarms;

import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmsFilterTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ActiveAlarmValue> inputTopic;
    private TestOutputTopic<String, ActiveAlarmValue> outputTopic;
    private ActiveAlarmValue alarm1;
    private ActiveAlarmValue alarm2;

    @Before
    public void setup() {
        final Properties streamsConfig = AlarmsFilter.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = AlarmsFilter.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        // setup test topics
        inputTopic = testDriver.createInputTopic(AlarmsFilter.INPUT_TOPIC, AlarmsFilter.INPUT_KEY_SERDE.serializer(), AlarmsFilter.INPUT_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(AlarmsFilter.OUTPUT_TOPIC, AlarmsFilter.OUTPUT_KEY_SERDE.deserializer(), AlarmsFilter.OUTPUT_VALUE_SERDE.deserializer());

        alarm1 = new ActiveAlarmValue();

        alarm2 = new ActiveAlarmValue();
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void tombstoneMsg() throws InterruptedException {
        List<KeyValue<String, ActiveAlarmValue>> keyValues = new ArrayList<>();
        keyValues.add(KeyValue.pair("alarm1", alarm1));
        keyValues.add(KeyValue.pair("alarm1", alarm2));
        inputTopic.pipeKeyValueList(keyValues, Instant.now(), Duration.ofSeconds(5));
        testDriver.advanceWallClockTime(Duration.ofSeconds(5));
        KeyValue<String, ActiveAlarmValue> result = outputTopic.readKeyValuesToList().get(0);
        Assert.assertNull(result.value);
    }

    @Test
    public void notYetExpired() {
        inputTopic.pipeInput("alarm1", alarm1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        inputTopic.pipeInput("alarm2", alarm2);
        KeyValue<String, ActiveAlarmValue> result = outputTopic.readKeyValuesToList().get(0);
        Assert.assertEquals("alarm1", result.key);
        Assert.assertNull(result.value);
    }

    @Test
    public void expired() {
        inputTopic.pipeInput("alarm1", alarm1);
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        KeyValue<String, ActiveAlarmValue> result = outputTopic.readKeyValue();
        Assert.assertEquals("alarm1", result.key);
        Assert.assertNull(result.value);
    }
}
