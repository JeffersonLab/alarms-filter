package org.jlab.alarms;

public class EventSourceRecord<K,V> {
    private K key;
    private V value;

    public EventSourceRecord(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public String toString() {
        return key.toString() + "=" + value.toString();
    }
}
