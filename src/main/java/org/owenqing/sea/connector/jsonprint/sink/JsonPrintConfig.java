package org.owenqing.sea.connector.jsonprint.sink;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@Getter
public class JsonPrintConfig implements Serializable {
    public Integer batchSize;
}
