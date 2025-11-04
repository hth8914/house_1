package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class DbusLogETLMetricTask2 {

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




        // 2. 历史天 + 当天 共计搜索词TOP10(每天的词云)
        SingleOutputStreamOperator<JSONObject> parsed = source.flatMap(new FlatMapFunction<String, JSONObject>() {
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

                jsonObject.put("log_date", localDate.toString());

                collector.collect(jsonObject);
            }
        });


        // 2️⃣ 保留 search 日志并展开 keywords
        DataStream<Tuple3<String, String, Long>> keywordStream = parsed.flatMap(new FlatMapFunction<JSONObject, Tuple3<String, String, Long>>() {
            @Override
            public void flatMap(JSONObject json, Collector<Tuple3<String, String, Long>> out)  {
                if (!"search".equals(json.getString("log_type"))) return;
                JSONArray kws = json.getJSONArray("keywords");
                if (kws == null || kws.isEmpty()) return;

                String logDate = json.getString("log_date");
                for (Object kwObj : kws) {
                    String kw = kwObj.toString().trim();
                    if (kw.length() > 0) {
                        out.collect(Tuple3.of(logDate, kw, 1L));
                    }
                }
            }
        });

        // 3️⃣ 每天每个关键词计数
        DataStream<Tuple3<String, String, Long>> keywordCount = keywordStream
                .keyBy(t -> t.f0 + "_" + t.f1)
                .sum(2);

        // 4️⃣ 按天聚合 Top10
        keywordCount
                .keyBy(t -> t.f0)
                .process(new TopNProcessFunction(10))
                .keyBy(v -> 1)
                .process(new SortAndDedupOutput())
                .print();

        env.execute("Daily Search Keyword Top10");
    }

    // 🔟 每日 TopN 计算函数
    public static class TopNProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, String> {
        private final int topSize;
        private transient ListState<Tuple3<String, String, Long>> listState;
        private transient ValueState<Boolean> hasOutput; // 防重复输出

        public TopNProcessFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters)  {
            ListStateDescriptor<Tuple3<String, String, Long>> descriptor =
                    new ListStateDescriptor<>(
                            "keywordState",
                            TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {})
                    );
            listState = getRuntimeContext().getListState(descriptor);

            ValueStateDescriptor<Boolean> hasOutputDesc = new ValueStateDescriptor<>("hasOutput", Boolean.class);
            hasOutput = getRuntimeContext().getState(hasOutputDesc);
        }

        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            // 定时器：延迟触发汇总
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 2000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Boolean printed = hasOutput.value();
            if (printed != null && printed) return; // ✅ 防重复输出

            List<Tuple3<String, String, Long>> allKeywords = new ArrayList<>();
            for (Tuple3<String, String, Long> kw : listState.get()) {
                allKeywords.add(kw);
            }

            // ✅ 聚合同关键词计数
            Map<String, Long> merged = new HashMap<>();
            for (Tuple3<String, String, Long> t : allKeywords) {
                merged.put(t.f1, merged.getOrDefault(t.f1, 0L) + t.f2);
            }

            // ✅ 排序并取 TopN
            List<Map.Entry<String, Long>> sorted = new ArrayList<>(merged.entrySet());
            sorted.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

            StringBuilder sb = new StringBuilder();
            sb.append("📅 日期: ").append(ctx.getCurrentKey()).append("\n");
            sb.append("🔥 热门搜索词 TOP ").append(topSize).append(":\n");
            int rank = 1;
            for (Map.Entry<String, Long> e : sorted.subList(0, Math.min(topSize, sorted.size()))) {
                sb.append(rank++).append(". ").append(e.getKey()).append(" -> ").append(e.getValue()).append("\n");
            }

            out.collect(sb.toString());
            hasOutput.update(true);
            listState.clear();
        }
    }

    // ✅ 汇总排序输出（按日期升序 + 去重）
    public static class SortAndDedupOutput extends KeyedProcessFunction<Integer, String, String> {
        private transient ListState<String> allResults;

        @Override
        public void open(Configuration parameters)  {
            allResults = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("allResults", String.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            allResults.add(value);
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 2000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<String> results = new ArrayList<>();
            for (String r : allResults.get()) {
                results.add(r);
            }

            // 去重：相同日期保留最后一条
            Map<String, String> dedup = new HashMap<>();
            for (String s : results) {
                int start = s.indexOf("📅 日期: ") + 6;
                int end = s.indexOf("\n", start);
                String date = s.substring(start, end).trim();
                dedup.put(date, s);
            }

            // 按日期升序
            List<String> sortedKeys = new ArrayList<>(dedup.keySet());
            sortedKeys.sort(Comparator.naturalOrder());

            for (String date : sortedKeys) {
                out.collect(dedup.get(date));
            }

            allResults.clear();
        }







//        env.execute("DbusLogETLMetricTask");
    }
}
