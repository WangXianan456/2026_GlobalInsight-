import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class HotSearchRedisMapper implements RedisMapper<String> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        // 使用 HSET (Hash) 存储，Key 为 GLOBAL_INSIGHT_HOT_SEARCH
        return new RedisCommandDescription(RedisCommand.HSET, "GLOBAL_INSIGHT_HOT_SEARCH");
    }

    @Override
    public String getKeyFromData(String data) {
        // 假设 data 是 "Top 1: 手机壳 (50次)"，提取排名作为 Field
        return data.split(":")[0].trim();

    }

    @Override
    public String getValueFromData(String data) {
        return data;
    }
}