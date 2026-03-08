# 1. 重新编译打包
cd Code
mvn clean package -DskipTests

# 2. 构建并推送新镜像（每次改 Tag，比如 v2, v3... 以防止 K8s 缓存镜像）
docker build -t global-insight.tencentcloudcr.com/app/app:v2 .
docker push global-insight.tencentcloudcr.com/app/app:v2

# 3. 更新集群镜像
kubectl set image deployment/flink-jobmanager jobmanager=global-insight.tencentcloudcr.com/app/app:v2 -n global-insight
kubectl set image deployment/flink-taskmanager taskmanager=global-insight.tencentcloudcr.com/app/app:v2 -n global-insight

# 4. 再次提交任务
cd ..
.\k8s-deploy\submit-jobs.ps1 HotSearchRankingApp
