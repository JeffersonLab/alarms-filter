package org.jlab.alarms;

import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AlarmsFilterTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<ActiveAlarmKey, ActiveAlarmValue> inputTopic;
    private TestOutputTopic<ActiveAlarmKey, ActiveAlarmValue> outputTopic;
    private ActiveAlarmKey alarmKey1;
    private ActiveAlarmKey alarmKey2;
    private ActiveAlarmValue alarmValue1;
    private ActiveAlarmValue alarmValue2;

    @Before
    public void setup() {
        final Properties streamsConfig = AlarmsFilter.getStreamsConfig();
        streamsConfig.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://testing");
        final Topology top = AlarmsFilter.createTopology(streamsConfig);
        testDriver = new TopologyTestDriver(top, streamsConfig);

        // setup test topics
        inputTopic = testDriver.createInputTopic(AlarmsFilter.INPUT_TOPIC, AlarmsFilter.INPUT_KEY_SERDE.serializer(), AlarmsFilter.INPUT_VALUE_SERDE.serializer());
        outputTopic = testDriver.createOutputTopic(AlarmsFilter.OUTPUT_TOPIC, AlarmsFilter.OUTPUT_KEY_SERDE.deserializer(), AlarmsFilter.OUTPUT_VALUE_SERDE.deserializer());

        alarmKey1 = new ActiveAlarmKey();
        alarmKey2 = new ActiveAlarmKey();
        alarmValue1 = new ActiveAlarmValue();
        alarmValue2 = new ActiveAlarmValue();

        alarmKey1.setName("alarm1");
        alarmKey2.setName("alarm2");

        alarmKey1.setType(ActiveMessageType.EPICSAlarming);
        alarmKey2.setType(ActiveMessageType.EPICSAck);

        EPICSAlarming alarming = new EPICSAlarming();
        alarming.setSevr(SevrEnum.MAJOR);
        alarming.setStat(StatEnum.HIHI);

        EPICSAck ack = new EPICSAck();
        ack.setAck(EPICSAcknowledgementEnum.MAJOR_ACK);

        alarmValue1.setMsg(alarming);
        alarmValue2.setMsg(ack);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void filterByMsgType() {
        System.out.println("Key: " + alarmKey1);
        System.out.println("Value: " + alarmValue1);
        System.out.println("Topic: " + inputTopic);

        inputTopic.pipeInput(alarmKey1, alarmValue1);
        inputTopic.pipeInput(alarmKey2, alarmValue2);
        List<KeyValue<ActiveAlarmKey, ActiveAlarmValue>> resultList = outputTopic.readKeyValuesToList();

        Assert.assertEquals(1, resultList.size());

        KeyValue<ActiveAlarmKey, ActiveAlarmValue> result = resultList.get(0);

        Assert.assertEquals(alarmKey2, result.key);
        Assert.assertEquals(alarmValue2, result.value);
    }
}
