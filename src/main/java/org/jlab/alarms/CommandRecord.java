package org.jlab.alarms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;
import java.util.Set;

public class CommandRecord {
    @JsonIgnore
    private CommandKey key;
    @JsonIgnore
    private CommandValue value;

    public CommandRecord() {
        key = new CommandKey();
        value = new CommandValue();
    }

    public CommandRecord(CommandKey key, CommandValue value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandRecord that = (CommandRecord) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public CommandKey getKey() {
        return key;
    }

    public CommandValue getValue() {
        return value;
    }

    public String getFilterName() {
        return value.getFilterName();
    }

    public Set<String> getAlarmNames() {
        return value.getAlarmNames();
    }

    public Set<String> getLocations() {
        return value.getLocations();
    }

    public Set<String> getCategories() {
        return value.getCategories();
    }

    public void setFilterName(String filterName) {
        value.setFilterName(filterName);
    }

    public void setAlarmNames(Set<String> names) {
        value.setAlarmNames(names);
    }

    public void setLocations(Set<String> locations) {
        value.setLocations(locations);
    }

    public void setCategories(Set<String> categories) {
        value.setCategories(categories);
    }

    public void setOutputTopic(String outTopic) {
        key.setOutputTopic(outTopic);
    }

    public String getOutputTopic() {
        return key.getOutputTopic();
    }


    public String toJSON() {
        ObjectMapper objectMapper = new ObjectMapper();

        String json = null;

        try {
            json = objectMapper.writeValueAsString(this);
        } catch(JsonProcessingException e) {
            throw new RuntimeException("Nothing a user can do about this; JSON couldn't be created!", e);
        }

        return json;
    }

    @Override
    public String toString() {
        return "CommandRecord{" +
                "outputTopic='" + key.getOutputTopic() + '\'' +
                ", filterName='" + value.getFilterName() + '\'' +
                '}';
    }

    public static class CommandKey {
    private String outputTopic;

    public CommandKey() {

    }

    public CommandKey(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandKey key = (CommandKey) o;
        return Objects.equals(outputTopic, key.outputTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputTopic);
    }

    public String toJSON() {
        ObjectMapper objectMapper = new ObjectMapper();

        String json = null;

        try {
            json = objectMapper.writeValueAsString(this);
        } catch(JsonProcessingException e) {
            throw new RuntimeException("Nothing a user can do about this; JSON couldn't be created!", e);
        }

        return json;
    }

    public static CommandKey fromJSON(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(json, CommandKey.class);
    }
}

public static class CommandValue {
    private String filterName;
    private Set<String> alarmNames;
    private Set<String> locations;
    private Set<String> categories;

    public CommandValue() {
    }

    public CommandValue(String filterName, Set<String> alarmNames, Set<String> locations, Set<String> categories) {
        this.filterName = filterName;
        this.alarmNames = alarmNames;
        this.locations = locations;
        this.categories = categories;
    }

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public Set<String> getAlarmNames() {
        return alarmNames;
    }

    public void setAlarmNames(Set<String> alarmNames) {
        this.alarmNames = alarmNames;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public void setLocations(Set<String> locations) {
        this.locations = locations;
    }

    public Set<String> getCategories() {
        return categories;
    }

    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    public String toJSON() {
        ObjectMapper objectMapper = new ObjectMapper();

        String json = null;

        try {
            json = objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Nothing a user can do about this; JSON couldn't be created!", e);
        }

        return json;
    }

    public static CommandValue fromJSON(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(json, CommandValue.class);
    }
}
}
