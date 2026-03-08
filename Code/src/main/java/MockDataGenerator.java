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
        public String sessionId;
        public String countryCode;
        public String siteId;
        public String behavior;
        public String itemId;
        public String searchTerm;
        public int age;
        public String level;
        public double price;
        public String currency;
        public double priceCNY;
        public long ts;

        @Override
        public String toString() {
            return JSONObject.toJSONString(this);
        }
    }

    private static final String TOPIC = "global_behavior_log";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final java.util.Map<String, UserProfile> USER_PROFILES = new java.util.HashMap<>();

    private static final java.util.Map<String, UserSession> USER_SESSIONS = new java.util.concurrent.ConcurrentHashMap<>();

    private static class UserProfile {
        int age;
        String level;
    }

    private static class UserSession {
        String sessionId;
        long lastTs;
        int step; // 0: search, 1: click, 2: cart, 3: buy
    }

    private static final String[] SEARCH_KEYWORDS = {
            "iPhone 15 Case", "Bluetooth Earphones", "Yoga Mat", "Running Shoes", "T-Shirt",
            "Screen Protector", "USB-C Cable", "Water Bottle", "Socks", "Backpack",
            "Gaming Mouse", "Mechanical Keyboard", "Smart Watch", "Desk Lamp", "Towel",
            "Shampoo", "Face Mask", "Lipstick", "Coffee Maker", "Blender"
    };

    public static void main(String[] args) throws Exception {

        String kafkaServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaServers == null) kafkaServers = DEFAULT_BOOTSTRAP_SERVERS;

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random r = new Random();
        String[] levels = {"Bronze", "Silver", "Gold", "Diamond"};
        for (int i = 0; i < 1000; i++) {
            String uid = "U" + (1000 + i);
            UserProfile p = new UserProfile();
            p.age = 18 + r.nextInt(42);
            p.level = levels[r.nextInt(levels.length)];
            USER_PROFILES.put(uid, p);
        }

        ExecutorService executor = Executors.newFixedThreadPool(5); // 模拟高并发

        System.out.println(">>> GlobalInsight 模拟器：已切换为 CNY 本位币模式...");

        for (int i = 0; i < 5; i++) {
            executor.execute(() -> {
                Random r1 = new Random();
                while (!Thread.currentThread().isInterrupted()) {
                    GlobalBehavior log = generateData(r1);
                    producer.send(new ProducerRecord<>(TOPIC, log.userId, log.toString()));

                    System.out.println("成功发送 -> 用户: " + log.userId + " | 行为: " + log.behavior + " | 会话: " + log.sessionId);

                    // 慢速模式：每条数据休眠 10~60 毫秒
                    try { TimeUnit.MILLISECONDS.sleep(r1.nextInt(50) + 10); } catch (Exception e) { break; }
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

        String countryCode;
        String currency;
        double rate;

        int countryDice = r.nextInt(100);

        if (countryDice < 30) {

            countryCode = "US"; currency = "USD"; rate = 7.2;
        } else if (countryDice < 45) {

            countryCode = "DE"; currency = "EUR"; rate = 7.8;
        } else if (countryDice < 55) {

            countryCode = "UK"; currency = "GBP"; rate = 9.2;
        } else if (countryDice < 65) {

            countryCode = "JP"; currency = "JPY"; rate = 0.048;
        } else if (countryDice < 75) {

            countryCode = "AU"; currency = "AUD"; rate = 4.7;
        } else if (countryDice < 85) {

            countryCode = "CA"; currency = "CAD"; rate = 5.3;
        } else if (countryDice < 95) {

            countryCode = "CN"; currency = "CNY"; rate = 1.0;
        } else {

            countryCode = "KR"; currency = "KRW"; rate = 0.0054;
        }

        log.countryCode = countryCode;
        log.currency = currency;
        log.siteId = "TEMU_" + countryCode;

        String userId = "U" + (1000 + r.nextInt(1000));
        log.userId = userId;
        log.eventId = UUID.randomUUID().toString();

        UserSession session = USER_SESSIONS.get(userId);
        long now = System.currentTimeMillis();
        if (session == null || (now - session.lastTs) > 300000) { // 5分钟超时
            session = new UserSession();
            session.sessionId = UUID.randomUUID().toString();
            session.step = 0;
            USER_SESSIONS.put(userId, session);
        }
        session.lastTs = now;
        log.sessionId = session.sessionId;

        UserProfile profile = USER_PROFILES.get(userId);
        if (profile != null) {
            log.age = profile.age;
            log.level = profile.level;
        }

        // 会话漏斗逻辑模拟 (Search -> Click -> Buy)
        if (session.step == 0) {
            log.behavior = "search";
            log.searchTerm = SEARCH_KEYWORDS[r.nextInt(SEARCH_KEYWORDS.length)];
            log.itemId = "";
            if (r.nextBoolean()) session.step = 1; // 50% 概率进入下一步
        } else if (session.step == 1) {
            log.behavior = "click";
            int itemNum = r.nextInt(5000);
            log.itemId = "ITEM-" + (10000 + itemNum);
            log.searchTerm = "";
            if (r.nextInt(10) < 2) session.step = 2; // 20% 概率下单
            else if (r.nextBoolean()) session.step = 0; // 50% 概率重新搜索
        } else {
            log.behavior = "buy";
            int itemNum = r.nextInt(5000);
            log.itemId = "ITEM-" + (10000 + itemNum);
            log.searchTerm = "";

            double basePrice = 10.0 + (itemNum % 100) * 5.0;
            double fluctuation = r.nextDouble() * 4.0 - 2.0;
            log.price = Math.max(0.01, basePrice + fluctuation);
            log.priceCNY = log.price * rate;

            session.step = 0; // 购买完重置
            session.sessionId = UUID.randomUUID().toString(); // 重置 Session
        }

        long delay = (r.nextInt(10) < 1) ? r.nextInt(5000) : 0;
        log.ts = now - delay;

        return log;
    }


    public static KafkaProducer<String, String> createKafkaProducer() {
        String kafkaServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaServers == null) kafkaServers = DEFAULT_BOOTSTRAP_SERVERS;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
