package org.jlab.alarms;

import java.util.List;

public interface CommandChangeListener {
    public abstract void update(List<CommandRecord> changes);
}
