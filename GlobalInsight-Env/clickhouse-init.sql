-- 创建数据库
CREATE DATABASE IF NOT EXISTS globalinsight;

-- 创建行为日志表
CREATE TABLE IF NOT EXISTS globalinsight.behavior_olap (
    user_id String,
    event_type String,
    item_id String,
    ts UInt64,
    age UInt8,
    level String,
    session_id String
) ENGINE = MergeTree()
ORDER BY ts;

