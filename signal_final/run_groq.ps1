param(
    [string]$Model = "openai/gpt-oss-20b",
    [double]$Timeout = 120,
    [int]$Retries = 2
)

$savedKey = [Environment]::GetEnvironmentVariable("GROQ_API_KEY", "User")
if (-not $env:GROQ_API_KEY -and $savedKey) {
    $env:GROQ_API_KEY = $savedKey
}

if (-not $env:GROQ_API_KEY) {
    $secureKey = Read-Host "Paste your Groq API key" -AsSecureString
    $plainKey = [System.Net.NetworkCredential]::new("", $secureKey).Password
    if (-not $plainKey) {
        throw "No Groq API key was entered."
    }
    [Environment]::SetEnvironmentVariable("GROQ_API_KEY", $plainKey, "User")
    $env:GROQ_API_KEY = $plainKey
    Remove-Variable secureKey, plainKey
    Write-Host "Groq API key saved to your Windows user environment."
}

Push-Location $PSScriptRoot
try {
    & python agent_demo.py --provider groq --model $Model --timeout $Timeout --retries $Retries
    if ($LASTEXITCODE -ne 0) {
        throw "Groq demo failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}
