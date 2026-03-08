import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.api.SqlDialect;

public class HiveSinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String catalogName = "myhive";
        String defaultDb = "default";
        // 宿主机运行和容器内运行路径不同，通过环境变量或配置文件动态指定
        boolean isLocal = System.getProperty("os.name").toLowerCase().contains("win");

        String hiveConfDir = isLocal
            ? "F:\\AAARepository\\PDD\\GlobalInsight\\GlobalInsight-Env\\hive-conf"
            : "/opt/hive-conf";

        String kafkaBootstrap = isLocal ? "localhost:9092" : "kafka:9092";

        String hiveVersion = "2.3.2";

        HiveCatalog hive = new HiveCatalog(catalogName, defaultDb, hiveConfDir, hiveVersion);
        tEnv.registerCatalog(catalogName, hive);
        tEnv.useCatalog(catalogName);

        // 设置方言为 Hive 模式以支持 Hive 语法
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS global_insight");
        tEnv.useDatabase("global_insight");

        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS behavior_dwd (" +
                        " user_id STRING, behavior STRING, item_id STRING, ts BIGINT, country_code STRING, site_id STRING, price DOUBLE, currency STRING " +
                        ") PARTITIONED BY (dt STRING) STORED AS PARQUET " +
                        "TBLPROPERTIES (" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt'," +
                        "  'sink.partition-commit.trigger'='process-time'," +
                        "  'sink.partition-commit.delay'='1 min'," +
                        "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
                        ")"
        );

        // 切换回默认方言以支持 Kafka DDL
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tEnv.executeSql(
                "CREATE TABLE kafka_src (" +
                        " userId STRING, behavior STRING, itemId STRING, ts BIGINT, countryCode STRING, siteId STRING, price DOUBLE, currency STRING, " +
                        " proctime AS PROCTIME() " +
                        ") WITH (" +
                        " 'connector'='kafka'," +
                        " 'topic'='global_behavior_log'," +
                        " 'properties.bootstrap.servers'='" + kafkaBootstrap + "'," +
                        " 'properties.group.id'='hive-sink-group'," +
                        " 'scan.startup.mode'='latest-offset'," +
                        " 'format'='json'," +
                        " 'json.ignore-parse-errors'='true'" +
                        ")"
        );

        tEnv.executeSql(
                "INSERT INTO myhive.global_insight.behavior_dwd " +
                        "SELECT userId, behavior, itemId, ts, countryCode, siteId, price, currency, " +
                        "FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd') AS dt " +
                        "FROM kafka_src"
        );
    }
}
