package org.owenqing.example.connector.jsonprint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60);
        env.disableOperatorChaining();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("pipeline.name", "json.print.connector.test");

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

        tEnv.executeSql("CREATE TABLE foo (" +
                "               id STRING," +
                "               name STRING," +
                "               val INT" +
                "           ) WITH (" +
                "               'connector' = 'json-print'," +
                "               'batch-size' = '10'" +
                ")");

        tEnv.executeSql("INSERT INTO foo" +
                "           SELECT id, name, val FROM event");
        env.execute();
    }
}
