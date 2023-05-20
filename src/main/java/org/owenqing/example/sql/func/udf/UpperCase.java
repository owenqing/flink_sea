package org.owenqing.example.sql.func.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Locale;

@FunctionHint(output = @DataTypeHint("STRING"))
public class UpperCase extends ScalarFunction {
    public String eval(String s) {
        return s.toUpperCase(Locale.ROOT);
    }
}
