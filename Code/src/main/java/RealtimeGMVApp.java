import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject; // Import JSONObject
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner; // Import SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows; // Change to EventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration; // Import Duration

public class RealtimeGMVApp {
    public static void main(String[] args) throws Exception {
  
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 初期开发先设为 1，方便观察结果

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("global_behavior_log")
                .setGroupId("gmv_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // CHANGE: 升级 Watermark 策略，容忍 5 秒乱序 (Matching MockDataGenerator's logic)
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String jsonStr, long recordTimestamp) {
                        try {
                            JSONObject json = JSON.parseObject(jsonStr);
                            return json.getLong("ts"); 
                        } catch (Exception e) {
                            return recordTimestamp;
                        }
                    }
                });

        DataStream<String> kafkaStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

        // 核心逻辑：解析 JSON -> 过滤 'buy' 行为 -> 提取价格 -> 滚动窗口聚合
        kafkaStream
                .map(jsonStr -> JSON.parseObject(jsonStr))
                .filter(data -> "buy".equals(data.getString("behavior"))) 
                .map(data -> data.getDouble("priceCNY")) 
                // CHANGE: 使用 EventTime 窗口，而不是 ProcessingTime
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((price1, price2) -> price1 + price2) 
                .map(gmv -> "💰 [实时大盘] 过去 5 秒成交额 (CNY): " + String.format("%.2f", gmv))
                .print();

        env.execute("GlobalInsight Real-time GMV Stat");
    }
}
