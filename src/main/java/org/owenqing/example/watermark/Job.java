package org.owenqing.example.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import scala.Tuple1;

import java.time.Duration;

/**
 * nc -lk 9000
 * input:
 * Click,1
 * Click,2
 * Click,12
 *
 * 观测 watermark 对窗口输出的影响
 */
public class Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Event> eventStream = sourceStream.map((MapFunction<String, Event>) s -> {
            String[] tokens = s.trim().split(",");
            Event event = new Event();
            event.setName(tokens[0]);
            event.setEventTime(Long.parseLong(tokens[1]));
            return event;
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.getEventTime() * 1000)
        ).setParallelism(1);

        eventStream
                .keyBy((KeySelector<Event, String>) Event::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new AggregateFunction<Event, Tuple1<Integer>, Integer>() {
                            @Override
                            public Tuple1<Integer> createAccumulator() {
                                return new Tuple1<Integer>(0);
                            }

                            @Override
                            public Tuple1<Integer> add(Event event, Tuple1<Integer> acc) {
                                return new Tuple1<>(acc._1 + 1);
                            }

                            @Override
                            public Integer getResult(Tuple1<Integer> acc) {
                                return acc._1;
                            }

                            @Override
                            public Tuple1<Integer> merge(Tuple1<Integer> acc, Tuple1<Integer> acc1) {
                                return new Tuple1<>(acc._1 + acc1._1);
                            }
                        }).print().setParallelism(1);
        env.execute();
    }
}
