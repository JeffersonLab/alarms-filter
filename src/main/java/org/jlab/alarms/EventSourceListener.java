package org.jlab.alarms;

import java.util.List;

public interface EventSourceListener<E> {
    public abstract void update(List<E> changes);
}
