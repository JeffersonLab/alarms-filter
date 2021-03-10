package org.jlab.alarms;

import java.util.Set;

public class CommandRecordValue {
    private String filterName;
    private Set<String> alarmNames;
    private Set<String> locations;
    private Set<String> categories;

    public CommandRecordValue() {

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

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }
}
