package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class DbusLogETLMetricTask {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.55.4:9092";
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );





        //      1. 历史天 + 当天 每个页面的总体访问量
        source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector)  {
                JSONObject jsonObject = JSONObject.parseObject(s);
                Long ts = jsonObject.getLong("ts");
                if (ts == null) return;
                // ✅ 判断是秒还是毫秒
                if (ts < 1000000000000L) { // 小于 1 万亿说明是秒级
                    ts = ts * 1000;
                }

                // 3️⃣ 转为日期字符串（本地时区）
                LocalDate localDate = Instant.ofEpochMilli(ts)
                        .atZone(ZoneId.of("Asia/Shanghai"))
                        .toLocalDate();

                jsonObject.put("log_date",localDate.toString());

                collector.collect(jsonObject);
            }
        }).keyBy(jsonObject -> jsonObject.getString("log_date") +"_"+ jsonObject.getString("log_type"))
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject jsonObject)  {
                        String logDate = jsonObject.getString("log_date");
                        String logType = jsonObject.getString("log_type");
                        return Tuple3.of(logDate, logType, 1L);
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .timeWindow(Time.days(1))
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value)  {
                        return String.format("日期: %s, 页面: %s, PV: %d", value.f0, value.f1, value.f2);
                    }
                });
                .print();



        env.execute("DbusLogETLMetricTask");
    }
}
