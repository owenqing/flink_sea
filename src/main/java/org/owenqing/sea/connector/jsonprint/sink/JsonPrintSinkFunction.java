package org.owenqing.sea.connector.jsonprint.sink;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class JsonPrintSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {
    private final JsonPrintConfig config;
    private final ArrayList<Tuple2<String, String>> columns;
    private final List<RowData> buffer  = new LinkedList<>();

    public JsonPrintSinkFunction(JsonPrintConfig config, ArrayList<Tuple2<String, String>> columns) {
        this.config = config;
        this.columns = columns;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 在这里建立数据库连接...
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        super.invoke(row, context);
        if (row == null) {
            return;
        }
        buffer.add(row);
        if (buffer.size() >= config.getBatchSize()) {
            doSink();
        }
    }

    private void doSink() {
        if (buffer.size() <= 0) {
            return;
        }
        for (var row : buffer) {
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                var col = columns.get(i);
                var feName = col.f0;
                var feType = col.f1;
                switch (feType) {
                    case "String":
                        map.put(feName, row.getString(i).toString());
                        break;
                    case "Integer":
                        map.put(feName, row.getInt(i));
                        break;
                }
            }
            System.out.printf("%s => %s\n", row.getRowKind().toString() ,JSON.toJSONString(map));
        }
        buffer.clear();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        doSink();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
