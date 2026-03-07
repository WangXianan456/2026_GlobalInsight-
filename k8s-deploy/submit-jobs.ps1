# Usage: .\submit-jobs.ps1 [JobClassName]
# JobClassName defaults to "HotSearchRankingApp"
param (
    [string]$JobClass = "HotSearchRankingApp"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = "$scriptDir\.."
$jarPath = "$projectRoot\Code\target\GlobalInsight-1.0.jar"

if (-not (Test-Path $jarPath)) {
    Write-Host "Error: JAR file not found at $jarPath. Please run 'mvn package' first." -ForegroundColor Red
    exit 1
}

Write-Host "Finding JobManager Pod..." -ForegroundColor Cyan
$jmPod = kubectl get pod -n global-insight -l component=jobmanager -o jsonpath="{.items[0].metadata.name}"

if (-not $jmPod) {
    Write-Host "Error: JobManager pod not found. Is Flink cluster deployed?" -ForegroundColor Red
    exit 1
}

Write-Host "Found JobManager: $jmPod" -ForegroundColor Green

# Write-Host "Copying JAR to JobManager..." -ForegroundColor Cyan
# 使用更健壮的路径格式
# kubectl cp "$jarPath" "global-insight/${jmPod}:/opt/flink/usrlib/GlobalInsight.jar" -n global-insight

# if ($LASTEXITCODE -eq 0) {
#     Write-Host "JAR copied successfully." -ForegroundColor Green
# } else {
#     Write-Host "Failed to copy JAR." -ForegroundColor Red
#     exit 1
# }

Write-Host "Submitting Job: $JobClass ..." -ForegroundColor Cyan
# Run job in detached mode (-d)
# JAR is already in the image at /opt/flink/usrlib/GlobalInsight.jar
kubectl exec -it -n global-insight $jmPod -- /opt/flink/bin/flink run -d -c $JobClass /opt/flink/usrlib/GlobalInsight.jar

if ($LASTEXITCODE -eq 0) {
    Write-Host "Job Submitted Successfully!" -ForegroundColor Green
    Write-Host "Check Flink Dashboard to see running jobs."
} else {
    Write-Host "Job Submission Failed." -ForegroundColor Red
}

