# Set current directory to project root
$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Stopping and cleaning up all services..." -ForegroundColor Cyan

Write-Host "1. Stopping Airflow stack..." -ForegroundColor Yellow
docker compose -p airflow -f docker-compose.airflow.yml down

Write-Host "2. Stopping Kafka/Zookeeper stack..." -ForegroundColor Yellow
docker compose down

Write-Host "=========================================" -ForegroundColor Green
Write-Host "All background Docker services have been stopped!" -ForegroundColor Green
Write-Host "Please close any open PowerShell windows running the Producer, Spark Job, or Streamlit." -ForegroundColor Green
