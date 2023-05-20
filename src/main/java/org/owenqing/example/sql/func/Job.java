package org.owenqing.example.sql.func;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.owenqing.example.sql.func.udf.UpperCase;
import org.owenqing.example.sql.func.udf.WordLength;

public class Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60);
//        env.disableOperatorChaining();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // config
        Configuration config = tEnv.getConfig().getConfiguration();
        config.setString("pipeline.name", "udf demo");
        config.set(CoreOptions.DEFAULT_PARALLELISM, 1);

        // register user define function
        tEnv.createTemporaryFunction("upFunc", UpperCase.class);
        tEnv.createTemporaryFunction("wordLen", WordLength.class);

        tEnv.executeSql("CREATE TABLE event (" +
                "               id STRING," +
                "               name STRING," +
                "               val INT," +
                "               process_time AS PROCTIME()" +
                "           ) WITH (" +
                "               'connector' = 'datagen'," +
                "               'rows-per-second' = '1'," +
                "               'fields.id.length' = '32'," +
                "               'fields.name.length' = '4'," +
                "               'fields.val.min' = '0'," +
                "               'fields.val.max' = '100'" +
                ")");

        tEnv.executeSql("SELECT" +
                "               upFunc(name) AS uName," +
                "               word," +
                "               len" +
                "           FROM event" +
                "           LEFT JOIN LATERAL TABLE(wordLen(name)) AS T(word, len) ON TRUE").print();

        env.execute();
    }
}
