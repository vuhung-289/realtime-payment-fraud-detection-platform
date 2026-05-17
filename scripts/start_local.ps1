Write-Host "Starting Kafka stack..."
docker compose up -d

Write-Host "Starting producer in new shell..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot/..'; python src/producer/payment_event_producer.py"

Write-Host "Starting streaming job in new shell..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot/..'; python src/streaming/fraud_streaming_job.py"

Write-Host "Starting dashboard in new shell..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot/..'; streamlit run dashboard/app.py"

Write-Host "All processes launched."
