package org.jlab.alarms;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.jlab.alarms.serde.JsonDeserializer;
import org.jlab.alarms.serde.JsonSerializer;

public final class FilterCommandSerde {
    static public final class CommandRecordKeySerde
            extends Serdes.WrapperSerde<CommandRecordKey> {
        public CommandRecordKeySerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(CommandRecordKey.class));
        }
    }

    static public final class CommandRecordValueSerde
            extends Serdes.WrapperSerde<CommandRecordValue> {
        public CommandRecordValueSerde() {
            super(new JsonSerializer<>(),
                    new JsonDeserializer<>(CommandRecordValue.class));
        }
    }

    public static Serde<CommandRecordKey> key() {
        return new FilterCommandSerde.CommandRecordKeySerde();
    }

    public static Serde<CommandRecordValue> value() {
        return new FilterCommandSerde.CommandRecordValueSerde();
    }
}