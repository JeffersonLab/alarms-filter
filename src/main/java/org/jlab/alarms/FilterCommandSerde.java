package org.jlab.alarms;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.jlab.alarms.serde.JsonDeserializer;
import org.jlab.alarms.serde.JsonSerializer;

public final class FilterCommandSerde {
    // We require a no-args constructor so we create a new class that wraps JsonDeserializer
    static public final class CommandRecordKeyDeserializer extends JsonDeserializer<FilterCommandKey> {

        public CommandRecordKeyDeserializer() {
            super(FilterCommandKey.class);
        }
    }

    static public final class CommandRecordValueDeserializer extends JsonDeserializer<FilterCommandValue> {

        public CommandRecordValueDeserializer() {
            super(FilterCommandValue.class);
        }
    }


    static public final class CommandRecordKeySerde
            extends Serdes.WrapperSerde<FilterCommandKey> {
        public CommandRecordKeySerde() {
            super(new JsonSerializer<>(),
                    new CommandRecordKeyDeserializer());
        }
    }

    static public final class CommandRecordValueSerde
            extends Serdes.WrapperSerde<FilterCommandValue> {
        public CommandRecordValueSerde() {
            super(new JsonSerializer<>(),
                    new CommandRecordValueDeserializer());
        }
    }

    public static Serde<FilterCommandKey> key() {
        return new FilterCommandSerde.CommandRecordKeySerde();
    }

    public static Serde<FilterCommandValue> value() {
        return new FilterCommandSerde.CommandRecordValueSerde();
    }
}