package org.owenqing.example.broadcast;

import com.alibaba.fastjson2.JSONObject;
import lombok.val;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.owenqing.example.broadcast.pojo.Person;
import org.owenqing.example.broadcast.pojo.PersonTag;

import java.util.ArrayList;
import java.util.HashMap;

public class Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // stream 1: broadcast stream
        val personTagList = new ArrayList<PersonTag>();
        personTagList.add(new PersonTag("Jack", "X1"));
        personTagList.add(new PersonTag("Bob", "C1"));
        DataStreamSource<PersonTag> tagStream = env.fromCollection(personTagList);
        MapStateDescriptor<String, PersonTag> personTagStateDescriptor = new MapStateDescriptor<String, PersonTag>(
                "PersonTagBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<PersonTag>() {
                })
        );
        // broadcast stream data
        BroadcastStream<PersonTag> personTagBroadcastStream = tagStream.broadcast(personTagStateDescriptor);

        // stream 2: regular stream
        DataStreamSource<String> nameStream = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Person> personStream = nameStream.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String s) throws Exception {
                JSONObject json = JSONObject.parse(s);
                Person p = new Person();
                p.setName(json.getString("name"));
                p.setAge(json.getInteger("age"));
                return p;
            }
        });

        // 广播状态需要时间，初始化后不一定立马就能完成数据加载
        // 如果需要保证较好的一致性，可以在 open 中直接请求 DB 数据
        personStream
                .connect(personTagBroadcastStream)
                .process(new BroadcastProcessFunction<Person, PersonTag, String>() {
                    private final HashMap<String, String> tagMap = new HashMap<>();
                    @Override
                    public void processElement(Person person,
                                               BroadcastProcessFunction<Person, PersonTag, String>.ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        String tag = tagMap.getOrDefault(person.getName(), "unknown");
                        out.collect(String.format("name: %s, age: %d, tag: %s", person.name, person.age, tag));
                    }

                    @Override
                    public void processBroadcastElement(PersonTag personTag,
                                                        BroadcastProcessFunction<Person, PersonTag, String>.Context ctx,
                                                        Collector<String> out) throws Exception {
                        tagMap.put(personTag.getName(), personTag.getTag());
                    }
                })
                .print();



        env.execute("broadcast-demo");
    }
}
