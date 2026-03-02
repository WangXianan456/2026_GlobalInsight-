import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class RealtimeGMVApp {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 初期开发先设为 1，方便观察结果

        // 2. 配置 Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("global_behavior_log")
                .setGroupId("gmv_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 读取数据流
        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 核心逻辑：解析 JSON -> 过滤 'buy' 行为 -> 提取价格 -> 滚动窗口聚合
        kafkaStream
                .map(jsonStr -> JSON.parseObject(jsonStr))
                .filter(data -> "buy".equals(data.getString("behavior"))) // 只统计购买行为
                .map(data -> data.getDouble("priceCNY")) // 提取人民币价格
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 5秒一个滚动窗口
                .reduce((price1, price2) -> price1 + price2) // 累加 GMV
                .map(gmv -> "💰 [实时大盘] 过去 5 秒成交额 (CNY): " + String.format("%.2f", gmv))
                .print();

        // 5. 启动任务
        env.execute("GlobalInsight Real-time GMV Stat");
    }
}