package org.owenqing.sea.connector.jsonprint.sink;

import lombok.var;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.owenqing.sea.connector.jsonprint.WithOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JsonPrintDynamicTableSink implements DynamicTableSink, Serializable {
    ReadableConfig options;
    ResolvedSchema schema;

    public JsonPrintDynamicTableSink(ReadableConfig options, ResolvedSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // support change log mode
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // args
        var batchSize = options.get(WithOptions.BATCH_SIZE);

        JsonPrintConfig cfg = new JsonPrintConfig(batchSize);
        // schema is very important
        // we can get columns info and primary keys info from schema
        List<Column> columns = schema.getColumns();
        ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            var col = columns.get(i);
            cols.add(new Tuple2<>(col.getName(), col.getDataType().getConversionClass().getSimpleName()));
        }
        return SinkFunctionProvider.of(new JsonPrintSinkFunction(cfg, cols));
    }

    @Override
    public DynamicTableSink copy() {
        return new JsonPrintDynamicTableSink(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "JsonPrint Table Sink";
    }
}
