import com.alibaba.fastjson.JSON;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;
import java.util.List;

public class HotSearchRankingApp {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 每 5 秒启动一次检查点（存档频率）
        env.enableCheckpointing(5000);

        // 2. 设置模式为 EXACTLY_ONCE（精准一次消费，不丢不重）
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);

        // 3. 两次检查点之间的最小间隔，防止存档太频繁压垮系统
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000);

        // 4. 检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 5. 任务取消时保留检查点（非常重要，方便手动重启恢复）
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 6. 状态后端设置：将状态存储在内存中（本地开发环境建议）
        env.setStateBackend(new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                new org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage()
        );



        KafkaSource<String> source = KafkaSource.<String>builder()
                // 修改这里：使用 docker-compose 中定义的内部监听地址 kafka:29092
                .setBootstrapServers("kafka:29092")
                .setTopics("global_behavior_log")
                .setGroupId("hot_search_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 1. 获取计算结果流
        DataStream<String> resultStream = kafkaStream
                .map(json -> JSON.parseObject(json))
                .filter(data -> "search".equals(data.getString("behavior")))
                .map(data -> data.getString("searchTerm"))
                .keyBy(word -> word)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(10)))
                .aggregate(new HotWordAgg(), new HotWordWindowResult())
                .keyBy(res -> res.windowEnd)
                .process(new TopNProcessor(3));

        // 2. 打印到控制台
        resultStream.print();

        // 3. 接入 Redis Sink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("redis")
                .setPort(6379)
                .build();

        resultStream.addSink(new RedisSink<>(conf, new HotSearchRedisMapper()));

        // ==========================================
        // 新增：Elasticsearch Sink (双写架构)
        // ==========================================
        org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder<String> esSinkBuilder =
                new org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder<String>()
                        // 注意这里的主机名：在 Docker 网络中，ES 的服务名就是 elasticsearch
                        .setHosts(new org.apache.http.HttpHost("elasticsearch", 9200, "http"))
                        // 每积攒 50 条数据，或者达到一定体积就批量写入，提高吞吐量
                        .setBulkFlushMaxActions(50)
                        .setEmitter(
                                (element, context, indexer) -> {
                                    // 构建向 ES 写入的请求
                                    org.elasticsearch.action.index.IndexRequest request = org.elasticsearch.client.Requests.indexRequest()
                                            .index("global_behavior") // ES 中的索引名称（相当于数据库的表名）
                                            .source(element, org.elasticsearch.common.xcontent.XContentType.JSON); // 直接写入 JSON
                                    indexer.add(request);
                                }
                        );

        // 将最原始的 kafkaStream（未做过滤的明细数据）写入 ES
        kafkaStream.sinkTo(esSinkBuilder.build()).name("Elasticsearch Sink");

        // 核心修复：只保留这一个 execute
        env.execute("GlobalInsight Hot Search App");


    }

    // --- 支撑类 ---

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
            // 注册一个名为 "total_search_count" 的自定义指标
            this.searchCounter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("total_search_count");

            // 之前的 dataState 初始化保持不变
            dataState = getRuntimeContext().getListState(new org.apache.flink.api.common.state.ListStateDescriptor<>("topn-state", SearchCount.class));
        }

        @Override
        public void processElement(SearchCount v, Context ctx, Collector<String> out) throws Exception {
            // 每处理一个搜索词，计数器加 1
            this.searchCounter.inc();

            dataState.add(v);
            ctx.timerService().registerProcessingTimeTimer(v.windowEnd + 1);
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

    // 修复：填入 Redis 的具体映射逻辑
    public static class HotSearchRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "GLOBAL_INSIGHT_REPORT");
        }

        @Override
        public String getKeyFromData(String s) {
            return "last_hot_search_result"; // Redis 里的 Field 名
        }

        @Override
        public String getValueFromData(String s) {
            return s; // 写入计算出的热搜 String
        }
    }
}