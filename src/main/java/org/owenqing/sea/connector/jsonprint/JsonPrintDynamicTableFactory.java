package org.owenqing.sea.connector.jsonprint;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.owenqing.sea.connector.jsonprint.sink.JsonPrintDynamicTableSink;

import java.util.HashSet;
import java.util.Set;

/**
 * 定义 with 参数
 */
public class JsonPrintDynamicTableFactory implements DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        // options and schema
        return new JsonPrintDynamicTableSink(options, schema);
    }

    // connector name
    @Override
    public String factoryIdentifier() {
        return WithOptions.IDENTIFIER;
    }

    // connector required args
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(WithOptions.BATCH_SIZE);
        return options;
    }

    // connector optional args
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<ConfigOption<?>>();
    }
}
