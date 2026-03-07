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
        public String countryCode;
        public String siteId;
        public String behavior;
        public String itemId;
        public String searchTerm;
        public double price;
        public String currency;
        public double priceCNY;
        public long ts;

        @Override
        public String toString() {
            return JSONObject.toJSONString(this);
        }
    }

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "global_behavior_log";

    // 预定义搜索词库
    private static final String[] SEARCH_KEYWORDS = {
            "iPhone 15 Case", "Bluetooth Earphones", "Yoga Mat", "Running Shoes", "T-Shirt",
            "Screen Protector", "USB-C Cable", "Water Bottle", "Socks", "Backpack",
            "Gaming Mouse", "Mechanical Keyboard", "Smart Watch", "Desk Lamp", "Towel",
            "Shampoo", "Face Mask", "Lipstick", "Coffee Maker", "Blender"
    };

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

                    // 慢速模式：每条数据休眠 10~60 毫秒
                    try { TimeUnit.MILLISECONDS.sleep(r.nextInt(50) + 10); } catch (Exception e) { break; }
                    // --- 高速模式 ---
                    // try { TimeUnit.MILLISECONDS.sleep(r.nextInt(4) + 1); } catch (Exception e) { break; }
                    // --- 极限模式 ---
                    // try { TimeUnit.MILLISECONDS.sleep(0); } catch (Exception e) { break; }

                }
            });
        }
    }

    public static GlobalBehavior generateData(Random r) {
        GlobalBehavior log = new GlobalBehavior();

        // 1. 优化国家分布（由均匀分布改为加权分布）
        // 扩充更多国家和币种，体现全球化业务
        String countryCode;
        String currency;
        double rate; // 对 CNY 的汇率

        int countryDice = r.nextInt(100);

        if (countryDice < 30) {
            // 北美市场 (30%)
            countryCode = "US"; currency = "USD"; rate = 7.2;
        } else if (countryDice < 45) {
            // 欧洲市场-德国 (15%)
            countryCode = "DE"; currency = "EUR"; rate = 7.8;
        } else if (countryDice < 55) {
            // 英国市场 (10%)
            countryCode = "UK"; currency = "GBP"; rate = 9.2;
        } else if (countryDice < 65) {
            // 日本市场 (10%)
            countryCode = "JP"; currency = "JPY"; rate = 0.048;
        } else if (countryDice < 75) {
            // 澳洲市场 (10%)
            countryCode = "AU"; currency = "AUD"; rate = 4.7;
        } else if (countryDice < 85) {
            // 加拿大市场 (10%)
            countryCode = "CA"; currency = "CAD"; rate = 5.3;
        } else if (countryDice < 95) {
            // 中国本土 (10%)
            countryCode = "CN"; currency = "CNY"; rate = 1.0;
        } else {
            // 韩国市场 (5%)
            countryCode = "KR"; currency = "KRW"; rate = 0.0054;
        }

        log.countryCode = countryCode;
        log.currency = currency;
        log.siteId = "TEMU_" + countryCode;
        log.userId = "U" + (1000 + r.nextInt(10000));
        log.eventId = UUID.randomUUID().toString();

        // 2. 优化行为漏斗（Search 60% -> Click 35% -> Buy 5%）
        int behaviorDice = r.nextInt(100);
        if (behaviorDice < 60) {

            log.behavior = "search";
            log.searchTerm = SEARCH_KEYWORDS[r.nextInt(SEARCH_KEYWORDS.length)];
            log.itemId = "";
        } else {

            log.behavior = (behaviorDice < 95) ? "click" : "buy";

            int itemNum = r.nextInt(5000);
            log.itemId = "ITEM-" + (10000 + itemNum);

            if ("buy".equals(log.behavior)) {

                // 3. 价格一致性优化：价格由 ItemID 决定，而不是完全随机

                double basePrice = 10.0 + (itemNum % 100) * 5.0; // 假定价格跟ID有关
                double fluctuation = r.nextDouble() * 4.0 - 2.0;
                log.price = Math.max(0.01, basePrice + fluctuation);

                log.priceCNY = log.price * rate;
            }
        }

        // 4. 模拟乱序数据
        // 90% 的数据是实时的，10% 的数据会有 0~5秒 的延迟

        long delay = (r.nextInt(10) < 1) ? r.nextInt(5000) : 0;
        log.ts = System.currentTimeMillis() - delay;

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
