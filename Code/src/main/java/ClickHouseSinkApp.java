import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSONObject;
import java.util.Random;
import java.util.UUID;

public class ClickHouseSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        boolean isLocal = System.getProperty("os.name").toLowerCase().contains("win");
        String kafkaBootstrap = isLocal ? "localhost:9092" : "kafka:9092";
        String clickhouseUrl = isLocal
                ? "jdbc:clickhouse://localhost:8123/globalinsight"
                : "jdbc:clickhouse://clickhouse:8123/globalinsight";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("global_behavior_log")
                .setGroupId("clickhouse-sink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        ObjectMapper mapper = new ObjectMapper();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(json -> {
                    try {
                        JSONObject obj = JSONObject.parseObject(json);
                        Behavior b = new Behavior();
                        b.userId = obj.getString("userId");
                        b.eventType = obj.getString("behavior"); // MockDataGenerator 中使用的是 behavior 字段
                        b.itemId = obj.getString("itemId");
                        b.ts = obj.getLongValue("ts");
                        b.age = obj.containsKey("age") ? obj.getInteger("age") : 18;
                        b.level = obj.containsKey("level") ? obj.getString("level") : "Bronze";
                        b.sessionId = obj.containsKey("sessionId") ? obj.getString("sessionId") : UUID.randomUUID().toString();
                        return b;
                    }
                    catch (Exception e) { return null; }
                })
                .filter(v -> v != null)
                .addSink(JdbcSink.sink(
                        "INSERT INTO globalinsight.behavior_olap " +
                                "(user_id,event_type,item_id,ts,age,level,session_id) VALUES (?,?,?,?,?,?,?)",
                        (ps, v) -> {
                            ps.setString(1, v.userId);
                            ps.setString(2, v.eventType);
                            ps.setString(3, v.itemId);
                            ps.setLong(4, v.ts);
                            ps.setInt(5, v.age);
                            ps.setString(6, v.level);
                            ps.setString(7, v.sessionId);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(500).withBatchIntervalMs(2000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(clickhouseUrl)
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()
                ));

        env.execute("ClickHouse Sink App");
    }

    public static class Behavior {
        public String userId;
        public String eventType;
        public String itemId;
        public long ts;
        public int age;
        public String level;
        public String sessionId;
    }
}
