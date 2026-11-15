# Step 6.7 - Run all BigData_IL14 integration tests.
# Execute from the BigData_IL14 project root:
#   .\run_integration_tests.ps1

$ErrorActionPreference = 'Stop'

Write-Host 'Checking Kafka container...' -ForegroundColor Cyan
$running = docker inspect -f '{{.State.Running}}' il14-kafka 2>$null
if ($LASTEXITCODE -ne 0 -or $running -ne 'true') {
    Write-Host 'ERROR: il14-kafka is not running. Start Kafka first.' -ForegroundColor Red
    exit 1
}

Write-Host 'Kafka is running. Starting integration tests...' -ForegroundColor Green

if (Test-Path '.\.venv\Scripts\python.exe') {
    $python = '.\.venv\Scripts\python.exe'
} else {
    $python = 'python'
}

& $python -m pytest tests/integration/ -v -s
$testExitCode = $LASTEXITCODE

if ($testExitCode -eq 0) {
    Write-Host 'All BigData_IL14 integration tests PASSED.' -ForegroundColor Green
} else {
    Write-Host "Integration tests FAILED (exit code $testExitCode)." -ForegroundColor Red
}

exit $testExitCode
