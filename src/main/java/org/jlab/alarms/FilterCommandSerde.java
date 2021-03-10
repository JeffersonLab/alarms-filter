package org.jlab.alarms;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.jlab.alarms.serde.JsonDeserializer;
import org.jlab.alarms.serde.JsonSerializer;

public final class FilterCommandSerde {
    // We require a no-args constructor so we create a new class that wraps JsonDeserializer
    static public final class CommandRecordKeyDeserializer extends JsonDeserializer<CommandRecordKey> {

        public CommandRecordKeyDeserializer() {
            super(CommandRecordKey.class);
        }
    }

    static public final class CommandRecordValueDeserializer extends JsonDeserializer<CommandRecordValue> {

        public CommandRecordValueDeserializer() {
            super(CommandRecordValue.class);
        }
    }


    static public final class CommandRecordKeySerde
            extends Serdes.WrapperSerde<CommandRecordKey> {
        public CommandRecordKeySerde() {
            super(new JsonSerializer<>(),
                    new CommandRecordKeyDeserializer());
        }
    }

    static public final class CommandRecordValueSerde
            extends Serdes.WrapperSerde<CommandRecordValue> {
        public CommandRecordValueSerde() {
            super(new JsonSerializer<>(),
                    new CommandRecordValueDeserializer());
        }
    }

    public static Serde<CommandRecordKey> key() {
        return new FilterCommandSerde.CommandRecordKeySerde();
    }

    public static Serde<CommandRecordValue> value() {
        return new FilterCommandSerde.CommandRecordValueSerde();
    }
}