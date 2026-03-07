# GlobalInsight TKE 部署指南

本目录包含了将 GlobalInsight 项目部署到腾讯云 TKE (Kubernetes) 所需的最小配置清单。

## 1. 准备 TKE 集群
确保你有一个 Kubernetes 集群 (v1.26+)。
**最低配置建议**：
- 节点数量: 3
- 单节点规格: 8 vCPU, 16GB RAM
- 磁盘: 每节点 300GB SSD 数据盘

## 2. 自动化部署 (推荐)
运行以下脚本，自动应用所有 Kubernetes 配置文件：

```powershell
.\local-deploy-to-tke.ps1
```

这会自动部署：Zookeeper, Kafka, ElasticSearch, Redis, Flink Session Cluster, Prometheus, Grafana, 以及模拟数据生成器和监控大屏。

### 检查部署状态
```powershell
kubectl get pods -n global-insight
kubectl get svc -n global-insight
```
确保所有 Pod 状态为 `Running`。对于 StatefulSet (kafka, es, redis)，可能需要几分钟才能启动完成。

## 3. 构建镜像 (MockDataGenerator & DataMonitor)
如果你在 TKE 环境中运行，你需要构建镜像并上传到镜像仓库 (如 TCR)。

```powershell
# 1. 登录 TCR (根据腾讯云控制台指引)
docker login ccr.ccs.tencentyun.com

# 2. 构建并推送镜像 (替换为你自己的镜像仓库地址)
cd k8s-deploy
.\build-image.ps1 ccr.ccs.tencentyun.com/your-namespace/global-insight-app:latest
docker push ccr.ccs.tencentyun.com/your-namespace/global-insight-app:latest

# 3. 更新 YAML 文件
# 修改 k8s-deploy/06-data-generator.yaml 和 k8s-deploy/07-data-monitor.yaml 中的 image 字段为你推送的地址
```

## 4. 提交 Flink 任务
Flink 集群启动后，需要手动提交作业。你可以使用 `submit-jobs.ps1` 脚本自动化此过程。

```powershell
# 提交主要计算作业 (HotSearchRankingApp)
.\k8s-deploy\submit-jobs.ps1 HotSearchRankingApp

# (可选) 提交 GMV 计算作业
.\k8s-deploy\submit-jobs.ps1 RealtimeGMVApp
```

### 方法 B: Flink Web UI (由于安全组限制可能无法直接访问)
1. 将本地 8081 端口转发到 Flink JobManager：
   ```powershell
   kubectl port-forward svc/flink-jobmanager 8081:8081 -n global-insight
   ```
2. 打开浏览器访问 `http://localhost:8081`。
3. 在 "Submit New Job" 页面，上传你编译好的 Jar 包 (`Code/target/GlobalInsight-1.0.jar`)。
4. 填写 Entry Class 并提交。

## 5. 访问监控 & 大屏

### 普罗米修斯 & Grafana
- **Grafana**:
  ```powershell
  kubectl port-forward svc/grafana 3000:3000 -n global-insight
  ```
  访问 `http://localhost:3000` (默认 admin/admin)。
- 接入 `http://prometheus:9090` k8s内部数据

### 控制台监控大屏 (DataMonitorApp)
查看实时数据大屏输出：
```powershell
kubectl logs -f deployment/data-monitor-dashboard -n global-insight
```

