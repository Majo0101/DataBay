param(
    [Parameter(Position = 0)]
    [ValidateSet("fast", "integration", "slow", "all")]
    [string]$Mode = "fast",

    [string]$Environment = "databay-0.3-test"
)

$pytestArgs = @("run", "-n", $Environment, "python", "-m", "pytest", "-ra", "-p", "no:cacheprovider")

switch ($Mode) {
    "fast"        { $pytestArgs += @("-m", "not integration") }
    "integration" { $pytestArgs += @("-m", "integration and not slow") }
    "slow"        { $pytestArgs += @("-m", "integration and slow") }
    "all"         { }
}

Write-Host "Running DataBay test mode '$Mode' in Conda environment '$Environment'..."
& conda @pytestArgs
exit $LASTEXITCODE
