import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject; // Added
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner; // Added
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows; // Changed
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.time.Duration; // Added
import java.util.ArrayList;
import java.util.List;

public class HotSearchRankingApp {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);

        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage()
        );

        // 获取外部配置 (支持 K8s 环境变量注入)
        String kafkaServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaServers == null) kafkaServers = "localhost:9092"; // 默认本地

        String redisHost = System.getenv("REDIS_HOST");
        if (redisHost == null) redisHost = "redis";

        String esHost = System.getenv("ES_HOST");
        if (esHost == null) esHost = "elasticsearch";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics("global_behavior_log")
                .setGroupId("hot_search_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        //升级 Watermark 策略，容忍 5 秒乱序
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        try {
                            JSONObject json = JSON.parseObject(element);
                            return json.getLong("ts");
                        } catch (Exception e) {
                            return recordTimestamp;
                        }
                    }
                });

        DataStream<String> kafkaStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

        DataStream<String> resultStream = kafkaStream
                .map(json -> JSON.parseObject(json))
                .filter(data -> "search".equals(data.getString("behavior")))
                .map(data -> data.getString("searchTerm"))
                .keyBy(word -> word)

                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new HotWordAgg(), new HotWordWindowResult())
                .keyBy(res -> res.windowEnd)
                .process(new TopNProcessor(3));

        resultStream.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(6379)
                .build();

        resultStream.addSink(new RedisSink<>(conf, new HotSearchRedisMapper()));

        // 如果需要 final 变量给 lambda 使用
        final String finalEsHost = esHost;

        org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder<String> esSinkBuilder =
                new org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder<String>()

                        .setHosts(new org.apache.http.HttpHost(finalEsHost, 9200, "http"))
                        // 每积攒 50 条数据，或者达到一定体积就批量写入，提高吞吐量
                        .setBulkFlushMaxActions(50)
                        .setEmitter(
                                (element, context, indexer) -> {

                                    org.elasticsearch.action.index.IndexRequest request = org.elasticsearch.client.Requests.indexRequest()
                                            .index("global_behavior")
                                            .source(element, org.elasticsearch.common.xcontent.XContentType.JSON);
                                    indexer.add(request);
                                }
                        );

        kafkaStream.sinkTo(esSinkBuilder.build()).name("Elasticsearch Sink");

        env.execute("GlobalInsight Hot Search App");


    }


    public static class HotWordAgg implements AggregateFunction<String, Long, Long> {
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(String v, Long acc) { return acc + 1; }
        @Override public Long getResult(Long acc) { return acc; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    public static class HotWordWindowResult implements WindowFunction<Long, SearchCount, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow w, Iterable<Long> input, Collector<SearchCount> out) {
            out.collect(new SearchCount(key, w.getEnd(), input.iterator().next()));
        }
    }

    public static class TopNProcessor extends KeyedProcessFunction<Long, SearchCount, String> {

        private transient org.apache.flink.metrics.Counter searchCounter;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {

            this.searchCounter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("total_search_count");

            dataState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("topn-state", SearchCount.class));
        }

        @Override
        public void processElement(SearchCount v, Context ctx, Collector<String> out) throws Exception {

            this.searchCounter.inc();

            dataState.add(v);
            // 注册事件时间定时器 (Watermark 到达窗口结束时间时触发)
            ctx.timerService().registerEventTimeTimer(v.windowEnd + 1);
        }

        private final int topN;
        private transient ListState<SearchCount> dataState;
        public TopNProcessor(int n) { this.topN = n; }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<SearchCount> list = new ArrayList<>();
            for (SearchCount s : dataState.get()) { list.add(s); }
            dataState.clear();
            list.sort((a, b) -> Long.compare(b.count, a.count));

            StringBuilder sb = new StringBuilder();
            sb.append("\n🔥 [实时热搜] ").append(new java.sql.Timestamp(ts - 1)).append("\n");
            for (int i = 0; i < Math.min(topN, list.size()); i++) {
                sb.append("Top ").append(i + 1).append(": ").append(list.get(i).word).append(" (").append(list.get(i).count).append("次)\n");
            }
            out.collect(sb.toString());
        }

    }

    public static class SearchCount {
        public String word;
        public long windowEnd;
        public long count;
        public SearchCount() {}
        public SearchCount(String w, long we, long c) { this.word = w; this.windowEnd = we; this.count = c; }
    }

    public static class HotSearchRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "GLOBAL_INSIGHT_REPORT");
        }

        @Override
        public String getKeyFromData(String s) {
            return "last_hot_search_result";
        }

        @Override
        public String getValueFromData(String s) {
            return s;
        }
    }
}
