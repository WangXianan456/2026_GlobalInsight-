# 检查 kubectl 是否安装
if (-not (Get-Command "kubectl" -ErrorAction SilentlyContinue)) {
    Write-Host "Error: kubectl command not found. Please install kubectl first." -ForegroundColor Red
    exit 1
}

$deployDir = "k8s-deploy"

Write-Host ">>> 1. Creating Namespace and base resources..." -ForegroundColor Green
kubectl apply -f "$deployDir/00-base.yaml"

Write-Host ">>> 2. Deploying Zookeeper and Kafka..." -ForegroundColor Green
kubectl apply -f "$deployDir/01-kafka-zookeeper.yaml"

Write-Host ">>> 3. Deploying ElasticSearch and Redis..." -ForegroundColor Green
kubectl apply -f "$deployDir/02-es-redis.yaml"

Write-Host ">>> 4. Deploying Flink cluster (Session Mode)..." -ForegroundColor Green
kubectl apply -f "$deployDir/04-flink-session-cluster.yaml"

Write-Host ">>> 5. Deploying monitoring components (Prometheus and Grafana)..." -ForegroundColor Green
kubectl apply -f "$deployDir/05-monitoring.yaml"

Write-Host ">>> 6. Deploying data generator..." -ForegroundColor Green
kubectl apply -f "$deployDir/06-data-generator.yaml"

Write-Host ">>> 7. Deploying dashboard..." -ForegroundColor Green
kubectl apply -f "$deployDir/07-data-monitor.yaml"

Write-Host "`n>>> Deployment commands sent!" -ForegroundColor Cyan
Write-Host "Please wait 3-5 minutes for all Pods to reach 'Running' status."
Write-Host "Check progress with: kubectl get pods -n global-insight -w"
