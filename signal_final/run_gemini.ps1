param(
    [string]$Model = "gemini-3.7-flash",
    [double]$Timeout = 300,
    [int]$Retries = 2
)

$savedKey = [Environment]::GetEnvironmentVariable("GEMINI_API_KEY", "User")
if (-not $env:GEMINI_API_KEY -and $savedKey) {
    $env:GEMINI_API_KEY = $savedKey
}

if (-not $env:GEMINI_API_KEY) {
    $secureKey = Read-Host "Paste your Google AI Studio Gemini API key" -AsSecureString
    $plainKey = [System.Net.NetworkCredential]::new("", $secureKey).Password
    if (-not $plainKey) {
        throw "No Gemini API key was entered."
    }
    [Environment]::SetEnvironmentVariable("GEMINI_API_KEY", $plainKey, "User")
    $env:GEMINI_API_KEY = $plainKey
    Remove-Variable secureKey, plainKey
    Write-Host "Gemini API key saved to your Windows user environment."
}

Push-Location $PSScriptRoot
try {
    & python agent_demo.py --provider gemini --model $Model --timeout $Timeout --retries $Retries
    if ($LASTEXITCODE -ne 0) {
        throw "Gemini demo failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}
