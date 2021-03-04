package org.jlab.alarms;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

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

    public void setFilterName(String filterName) {
        value.setFilterName(filterName);
    }

    public void setAlarmNames(String[] names) {
        value.setAlarmNames(names);
    }

    public void setLocations(String[] locations) {
        value.setLocations(locations);
    }

    public void setCategories(String[] categories) {
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
    private String[] alarmNames;
    private String[] locations;
    private String[] categories;

    public CommandValue() {
    }

    public CommandValue(String filterName, String[] alarmNames, String[] locations, String[] categories) {
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

    public String[] getAlarmNames() {
        return alarmNames;
    }

    public void setAlarmNames(String[] alarmNames) {
        this.alarmNames = alarmNames;
    }

    public String[] getLocations() {
        return locations;
    }

    public void setLocations(String[] locations) {
        this.locations = locations;
    }

    public String[] getCategories() {
        return categories;
    }

    public void setCategories(String[] categories) {
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
