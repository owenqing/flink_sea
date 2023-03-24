package org.owenqing.sea.connector.jsonprint;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class WithOptions {
    // connector name
    public static final String IDENTIFIER = "json-print";
    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size").intType().noDefaultValue();
}
