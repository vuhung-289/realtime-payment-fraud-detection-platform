# Set current directory to project root
$ProjectRoot = Resolve-Path "$PSScriptRoot/.."
Set-Location $ProjectRoot

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "1. Starting Kafka stack (Docker)..." -ForegroundColor Cyan
docker compose up -d

Write-Host "Waiting 15 seconds for Kafka to fully boot up..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "2. Starting Airflow stack (Docker)..." -ForegroundColor Cyan
docker compose -p airflow -f docker-compose.airflow.yml up -d

Write-Host "Waiting 5 seconds for Airflow services..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "3. Starting Spark Streaming Job in new window..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe src/streaming/fraud_streaming_job.py"

Write-Host "Waiting 8 seconds for Spark session initialization..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "4. Starting Payment Event Producer in new window..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\python.exe src/producer/payment_event_producer.py"

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "5. Starting Streamlit Dashboard in new window..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectRoot'; .\.venv\Scripts\streamlit.exe run dashboard/app.py"

Write-Host "=========================================" -ForegroundColor Green
Write-Host "All processes launched! Check spawned windows for details." -ForegroundColor Green

