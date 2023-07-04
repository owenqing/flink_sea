package org.owenqing.example.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
public class Event {
    public String name;
    public long eventTime;

    public Event() {}
}
