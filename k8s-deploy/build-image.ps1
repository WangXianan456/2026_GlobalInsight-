# Usage: .\build-image.ps1 <registry-url>
# Example: .\build-image.ps1 ccr.ccs.tencentyun.com/my-namespace/global-insight-app:latest

param (
    [string]$imageName = "global-insight-app:latest"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = "$scriptDir\.."
$dockerFile = "$projectRoot\Code\Dockerfile"
$contextDir = "$projectRoot\Code"

if (-not (Test-Path $dockerFile)) {
    Write-Host "Error: Dockerfile not found at $dockerFile" -ForegroundColor Red
    exit 1
}

Write-Host "Building Docker Image: $imageName ..." -ForegroundColor Green
docker build -t $imageName -f $dockerFile $contextDir

if ($LASTEXITCODE -eq 0) {
    Write-Host "Build Success!" -ForegroundColor Green
    Write-Host "To push to registry, run: docker push $imageName" -ForegroundColor Cyan
} else {
    Write-Host "Build Failed!" -ForegroundColor Red
    exit 1
}

