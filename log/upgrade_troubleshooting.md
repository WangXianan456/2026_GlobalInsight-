# GlobalInsight 架构升级与故障排查复盘日志

**日期**: 2026-03-08
**任务**: 升级实时数仓架构，引入 ClickHouse (实时 OLAP) 和 Hive (离线归档)，打通 Kafka -> Flink -> ClickHouse/Hive 全链路。

---

## 1. 🏗️ 环境与基础设施故障排查 (Infrastructure Issues)

### 🔴 故障一：物理资源与磁盘空间不足
**现象**:
- **JVM 内存分配失败**：`os::commit_memory` 报错，提示页面文件太小。
- **Docker 引擎崩溃**：迁移磁盘位置后出现 `context canceled` 和 `starting wsl-bootstrap` 错误。

**解决方案**:
1. **解决虚拟内存不足**：将 Windows 虚拟内存（PageFile）迁出 C 盘，在 F 盘设置 32GB 以上的自定义大小，解决了 `1455` 错误码。
2. **Docker 数据迁移**：通过 Docker Desktop 界面将 `Disk image location` 从 C 盘迁移至 F 盘，防止系统盘空间耗尽。
3. **WSL 重置**：针对迁移后的报错，通过 `Reset to factory defaults` 修复了 WSL2 虚拟磁盘的初始化问题。

### 🔴 故障二：端口冲突与进程残留
**现象**:
- NameNode 和本地服务争抢 `9000` 端口，导致 ClickHouse 无法启动（ClickHouse 默认 TCP 端口也是 9000）。
- Java 模拟器报错 `Bootstrap broker disconnected`。

**解决方案**:
1. **端口规划治理**:
   - 修改 `docker-compose.yml`，将 ClickHouse 的 TCP 映射端口从 `9000:9000` 调整为 `9001:9000`，有效避开 NameNode 和本地服务的冲突。
2. **清理残留进程**: 使用 `netstat -ano` 结合 `taskkill` 杀掉了已挂起但仍占用资源的 Java/Docker 进程。
3. **Kafka 连通性修复**: 确保 Zookeeper 正常启动后，Kafka 成功连接 2181 端口。

### 🔴 故障三：Docker 权限与数据卷异常
**现象**:
- Zookeeper 和 NameNode 无法读写挂载的数据目录，日志报错 Permission denied。

**解决方案**:
- **数据卷清理**: 删除本地残留的 `data` 目录及执行 `docker-compose down -v`，重建容器卷权限。

---

## 2. 🛠️ 应用构建与依赖故障排查 (Build & Dependency Issues)

### 🔴 故障四：Hadoop 依赖冲突 (Dependency Hell)
**现象**:
启动 `ClickHouseSinkApp` 时抛出致命错误：
```text
Exception in thread "main" java.lang.NoSuchMethodError: org.apache.hadoop.security.HadoopKerberosName.setRuleMechanism(Ljava/lang/String;)V
```

**原因分析**:
这是一个经典的 Java Classpath 冲突问题。
*   项目显式依赖了 `hadoop-client:3.3.4` (新版)。
*   同时依赖了 `hive-exec` (旧版本自带旧 Hadoop)。
由于 Maven 加载顺序的不确定，旧版类被优先加载。

**解决方案**:
修改 `pom.xml` 进行强力干预：
1.  **调整顺序**: 将 `hadoop-client` 的依赖声明移动到 `hive-exec` 之前。
2.  **强制排除**: 在 `hive-exec` 中显式排除 `org.apache.hadoop:*`。

### 🔴 故障五：Hive 版本不兼容与 Pentaho 缺失
**现象**:
1. 启动 `HiveSinkApp` 报错 `Unexpected error when trying to load service provider... HiveServer2EndpointFactory not found`。
2. 编译时报错 `Could not find artifact org.pentaho:pentaho-aggdesigner-algorithm`。

**原因分析**:
1. **服务端不匹配**: `docker-compose.yml` 使用 Hive 2.3.2，而 `pom.xml` 原本引入 Hive 3.1.3。大版本不兼容导致 Metastore 协议和 ServiceLoader 加载失败。
2. **依赖缺失**: Flink Hive 连接器需要 `hive-service` 和 `hive-metastore` 才能完整工作。
3. **幽灵依赖**: Pentaho 聚合算法包在 Maven 中央仓库缺失。

**解决方案**:
1. **版本对齐**: 将所有 Hive 依赖（exec, service, metastore）统一降级为 **2.3.9**（兼容 2.3.2）。
2. **补全依赖**: 显式添加 `hive-service` 和 `hive-metastore`。
3. **彻底排除**: 在所有 Hive 依赖中通过 `<exclusion>` 标签排除 `pentaho-aggdesigner-algorithm`。

### 🔴 故障六：ClickHouse 连接拒绝
**现象**:
Flink 任务反复重启，报错 `Connection refused: connect`。

**原因分析**:
Docker 容器启动较慢，Java 程序启动时数据库端口尚未就绪。

**解决方案**:
1.  编写智能启动脚本 `run-sink.ps1`。
2.  **增加心跳检测**: 脚本中循环检测 8123 端口状态。
3.  **自动初始化**: 检测成功后自动执行 `CREATE DATABASE` SQL。

### 🔴 故障七：脚本与环境工具
**现象**:
- PowerShell 脚本中文乱码、SQL 括号解析错误。
- Maven 报错 `Unknown lifecycle phase` 或jar包被锁。

**解决方案**:
1.  优化 PowerShell 脚本，使用变量封装 SQL，去除特殊字符。
2.  配置 `exec-maven-plugin`，并使用 `taskkill` 清理僵尸进程。

---

## 3. ✅ 最终环境与架构状态

### Docker 组件状态
所有组件（ClickHouse, Hadoop, Kafka, Redis, Hive, Flink）已成功拉起。

### 关键端口映射表
| 组件 | 容器内端口 | 宿主机端口 | 用途 |
| :--- | :--- | :--- | :--- |
| **ClickHouse** | 8123 | **8123** | HTTP 查询 / JDBC |
| **ClickHouse** | 9000 | **9001** | TCP / Native 协议 (改端口避让) |
| **NameNode** | 9870 | **9870** | HDFS Web UI |
| **NameNode** | 9000 | **9000** | HDFS IPC |
| **Kafka** | 9092 | **9092** | Broker (External) |
| **HiveServer2**| 10000 | **10000**| JDBC 连接 |

### 业务链路状态
1.  **数据生产**: `MockDataGenerator` 正常并发生成模拟用户行为数据。
2.  **实时计算**: `ClickHouseSinkApp` (Flink Job) 成功启动，Checkpoint 机制正常。
3.  **数据落地**:
    *   **ClickHouse**: 查询 `globalinsight.behavior_olap` 表，数据量实时增长。
    *   **Hive/HDFS**: HDFS 路径 `/user/hive/warehouse/global_insight.db/behavior_dwd` 下成功生成 Parquet 文件。

---

## 4. 🚀 快速启动指南

以后启动环境，只需执行这两个脚本：

**步骤 1: 一键部署 (编译 + 重启 Docker + 初始化 DB)**
```powershell
.\run-upgrade.ps1
```

**步骤 2: 启动 Flink 任务 (带自动端口检测)**
```powershell
.\run-sink.ps1
```

**步骤 3: 启动离线归档 (Hive Sink)**
```powershell
cd Code
mvn exec:java -Dexec.mainClass="HiveSinkApp"
```

**步骤 4: 启动数据生成器 (新开窗口)**
```powershell
cd Code
mvn exec:java -Dexec.mainClass="MockDataGenerator"
```

