package org.jlab.alarms;

public class EventSourceConsumer<E> extends Thread implements AutoCloseable {

    public EventSourceConsumer(EventSourceConfig config, EventSourceListener<E> listener) {

    }

    @Override
    public void close() throws Exception {

    }
}
