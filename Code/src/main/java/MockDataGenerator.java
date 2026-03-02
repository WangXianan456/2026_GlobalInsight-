import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MockDataGenerator {

    public static class GlobalBehavior {
        public String userId;
        public String eventId;
        public String countryCode;  // 加入 "CN" (中国)
        public String siteId;
        public String behavior;
        public String itemId;
        public String searchTerm;
        public double price;        // 原始价格
        public String currency;     // 原始币种 (USD, CNY, etc.)
        public double priceCNY;     // 换算后的 CNY (展示你对结算业务的理解)
        public long ts;

        @Override
        public String toString() {
            return JSONObject.toJSONString(this);
        }
    }

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "global_behavior_log";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = createKafkaProducer();
        ExecutorService executor = Executors.newFixedThreadPool(5); // 模拟高并发

        System.out.println(">>> GlobalInsight 模拟器：已切换为 CNY 本位币模式...");

        for (int i = 0; i < 5; i++) {
            executor.execute(() -> {
                Random r = new Random();
                while (!Thread.currentThread().isInterrupted()) {
                    GlobalBehavior log = generateData(r);
                    producer.send(new ProducerRecord<>(TOPIC, log.userId, log.toString()));
                    try { TimeUnit.MILLISECONDS.sleep(r.nextInt(50) + 10); } catch (Exception e) { break; }
                }
            });
        }
    }

    public static GlobalBehavior generateData(Random r) {
        GlobalBehavior log = new GlobalBehavior();
        // 1. 国家和货币列表加入中国 (CN)
        String[] countries = {"US", "UK", "JP", "CN"};
        String[] currencies = {"USD", "GBP", "JPY", "CNY"};
        double[] exchangeRates = {7.2, 9.1, 0.048, 1.0}; // 模拟实时汇率

        int idx = r.nextInt(countries.length);
        log.countryCode = countries[idx];
        log.currency = currencies[idx];
        log.siteId = "TEMU_" + log.countryCode;
        log.userId = "U" + (1000 + r.nextInt(5000));
        log.eventId = UUID.randomUUID().toString();

        // 2. 模拟业务逻辑
        int dice = r.nextInt(100);
        if (dice < 30) {
            log.behavior = "search";
            String[] kws = {"手机壳", "筋膜枪", "无线耳机", "瑜伽垫"};
            log.searchTerm = kws[r.nextInt(kws.length)];
            log.itemId = "";
        } else {
            log.behavior = (dice < 85) ? "click" : "buy";
            log.itemId = "ITEM-" + (2000 + r.nextInt(1000));
            if (log.behavior.equals("buy")) {
                log.price = 10.0 + r.nextDouble() * 500;
                // 3. 计算本位币 CNY 价格 (核心面试亮点)
                log.priceCNY = log.price * exchangeRates[idx];
            }
        }
        log.ts = System.currentTimeMillis();
        return log;
    }


    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}