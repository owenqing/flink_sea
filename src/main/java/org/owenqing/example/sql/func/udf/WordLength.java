package org.owenqing.example.sql.func.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, w_len INT>"))
public class WordLength extends TableFunction<Row> {
    public void eval(String str) {
        collect(Row.of(str, str.length()));
    }
}
