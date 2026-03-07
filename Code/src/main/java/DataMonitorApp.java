import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

public class DataMonitorApp {

    private static final String TOPIC = "global_behavior_log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "monitor_dashboard_group_" + System.currentTimeMillis(); // 随机组名，保证每次启动都能以此看到所有数据(如果设置earliest)或者最新数据

    // 统计指标容器
    private static final AtomicLong intervalCount = new AtomicLong(0);
    private static final AtomicLong intervalBytes = new AtomicLong(0);
    // 使用 DoubleAdder 替换 AtomicReference<Double> 以提高并发性能
    private static final DoubleAdder intervalGmv = new DoubleAdder();
    private static final AtomicLong searchCount = new AtomicLong(0);
    private static final AtomicLong clickCount = new AtomicLong(0);
    private static final AtomicLong buyCount = new AtomicLong(0);
    private static final AtomicLong maxDelay = new AtomicLong(0);

    public static void main(String[] args) {
        System.out.println(">>> 正在启动实时数据监控大屏 (Monitor Dashboard)...");

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2000);
                    printDashboard();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }).start();

        startConsumer();
    }

    private static void startConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 从最新数据开始读，只监控当前实时的流量
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println(">>> Monitor 已连接 Kafka，正在等待数据流...");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        // 1. 基础流量统计
        intervalCount.incrementAndGet();
        intervalBytes.addAndGet((long) record.value().length());

        try {
            // 2. 业务解析
            JSONObject json = JSON.parseObject(record.value());
            String behavior = json.getString("behavior");
            Long ts = json.getLong("ts");

            // 3. 统计各事件类型
            if ("search".equals(behavior)) {
                searchCount.incrementAndGet();
            } else if ("click".equals(behavior)) {
                clickCount.incrementAndGet();
            } else if ("buy".equals(behavior)) {
                buyCount.incrementAndGet();
                Double priceCNY = json.getDouble("priceCNY");
                if (priceCNY != null) {
                    intervalGmv.add(priceCNY);
                }
            }

            // 4. 延迟监控 (当前系统时间 - 事件时间)
            if (ts != null) {
                long delay = System.currentTimeMillis() - ts;
                // 去最大值
                if (delay > maxDelay.get()) {
                    maxDelay.set(delay);
                }
            }

        } catch (Exception e) {

        }
    }

    private static void printDashboard() {
        // 获取快照并重置关键计数器 (实现 Interval 统计)
        long count = intervalCount.getAndSet(0);
        long bytes = intervalBytes.getAndSet(0);
        double gmv = intervalGmv.sumThenReset();
        long searches = searchCount.getAndSet(0);
        long clicks = clickCount.getAndSet(0);
        long buys = buyCount.getAndSet(0);
        long delay = maxDelay.getAndSet(0); // 获取这2秒内的最大延迟

        // 计算速率 (因为是2秒打印一次，所以除以2得到每秒数据)
        double qps = count / 2.0;
        double throughputKB = (bytes / 1024.0) / 2.0;

        // 计算转化率 (简单漏斗)
        double clickRate = searches == 0 ? 0 : (double) clicks / searches * 100;
        double buyRate = clicks == 0 ? 0 : (double) buys / clicks * 100;

        StringBuilder sb = new StringBuilder();
        sb.append("\n================ [  REAL-TIME DASHBOARD  ] ================\n");
        sb.append(String.format(" 🚀 吞吐性能  : QPS: %-8.1f msg/s  | 流量: %-8.1f KB/s\n", qps, throughputKB));
        sb.append(String.format(" 💰 实时GMV   : ¥ %-10.2f (本窗口成交额)\n", gmv));
        sb.append(String.format(" 📊 行为分布  : Search: %-5d | Click: %-5d | Buy: %-5d\n", searches, clicks, buys));
        sb.append(String.format(" 📉 转化漏斗  : Search->Click: %5.1f%%  | Click->Buy: %5.1f%%\n", clickRate, buyRate));
        sb.append(String.format(" 🐢 最大延迟  : %d ms (乱序程度检测)\n", delay));
        sb.append("===========================================================\n");

        System.out.print(sb.toString());
    }
}

