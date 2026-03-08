Write-Host ">>> GlobalInsight 升级部署脚本启动..." -ForegroundColor Cyan

# 1. 编译项目
Write-Host "1.正在编译 Java 项目..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot\Code"
mvn clean package -DskipTests
if ($LASTEXITCODE -ne 0) {
    Write-Host "编译失败，请检查错误日志。" -ForegroundColor Red
    exit 1
}
Write-Host "编译成功！" -ForegroundColor Green

# 2. 启动 Docker 环境
Write-Host "2. 正在启动 Docker 环境..." -ForegroundColor Yellow
Set-Location "$PSScriptRoot\GlobalInsight-Env"
docker-compose down
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker 启动失败。" -ForegroundColor Red
    exit 1
}

# 3. 等待 ClickHouse 就绪
Write-Host "3. 等待 ClickHouse 服务就绪 (预计 15 秒)..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# 4. 初始化 ClickHouse 表结构
Write-Host "4. 初始化 ClickHouse 表结构..." -ForegroundColor Yellow
$sqlPath = "$PSScriptRoot\GlobalInsight-Env\clickhouse-init.sql"
docker cp $sqlPath clickhouse:/tmp/init.sql
docker exec clickhouse clickhouse-client --multiline --queries-file /tmp/init.sql

if ($LASTEXITCODE -eq 0) {
    Write-Host "ClickHouse 初始化成功！" -ForegroundColor Green
} else {
    Write-Host "ClickHouse 初始化失败，请手动检查。" -ForegroundColor Red
}

Write-Host ">>> 部署完成！" -ForegroundColor Cyan
Write-Host "接下来的操作："
Write-Host "1. 运行 MockDataGenerator (生成数据)"
Write-Host "2. 运行 ClickHouseSinkApp (实时处理)"
Write-Host "3. 运行 HiveSinkApp (离线处理)"

