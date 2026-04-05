param (
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Join-Path $PSScriptRoot ".."
$envPath = Join-Path $repoRoot ".env.web"
$localEnvPath = Join-Path $repoRoot ".env"
$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"
$removedCompatibilityKeys = @(
    "API_KEY",
    "ASSET_ALLOCATION_API_KEY",
    "VITE_BACKTEST_API_BASE_URL"
)

if (-not (Test-Path $envPath)) {
    Write-Error "Error: env file not found at $envPath (create .env.web)."
    exit 1
}

function Parse-EnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $map = @{}
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) { continue }
        if ($line -notmatch "^([^=]+)=(.*)$") { continue }

        $key = $matches[1].Trim()
        $value = $matches[2].Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        $map[$key] = $value
    }
    return $map
}

function Test-LocalAddressValue {
    param(
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $false
    }

    try {
        $uri = [System.Uri]$Value
        if ($uri.IsAbsoluteUri -and ($uri.Host -in @("localhost", "127.0.0.1", "::1"))) {
            return $true
        }
    } catch {
        # Fall back to substring check for non-URI values.
    }

    return $Value -match "(?i)\blocalhost\b|127\.0\.0\.1|::1"
}

function Should-SkipParityKey {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][hashtable]$ContractMap
    )

    if (-not $ContractMap.ContainsKey($Key)) {
        return $true
    }

    $entry = $ContractMap[$Key]
    $githubStorage = (($entry.github_storage | Out-String).Trim()).ToLowerInvariant()
    return $githubStorage -notin @("secret", "var")
}

function Load-EnvContract {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path $Path)) {
        throw "Env contract not found at $Path"
    }

    $rows = Import-Csv -Path $Path
    $map = @{}
    foreach ($row in $rows) {
        $name = (($row.name | Out-String).Trim())
        if ([string]::IsNullOrWhiteSpace($name)) {
            continue
        }
        $map[$name] = $row
    }
    return $map
}

function Assert-NoRemovedCompatibilityKeys {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceName,
        [Parameter(Mandatory = $true)]
        [hashtable]$Map
    )

    $presentKeys = @($removedCompatibilityKeys | Where-Object { $Map.ContainsKey($_) })
    if ($presentKeys.Count -gt 0) {
        throw ("{0} contains removed compatibility keys that must stay out of GitHub sync: {1}" -f $SourceName, ($presentKeys -join ", "))
    }
}

# Check if gh CLI is installed
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Error "Error: GitHub CLI (gh) is not installed or not in PATH."
    exit 1
}

Write-Host "Reading local env from: $envPath"
if ($DryRun) { Write-Host "Running in DRY RUN mode (no changes will be made)..." -ForegroundColor Yellow }

$envMap = Parse-EnvFile -Path $envPath
$lines = Get-Content $envPath
$contractMap = Load-EnvContract -Path $contractPath
Assert-NoRemovedCompatibilityKeys -SourceName ".env.web" -Map $envMap
$unsupportedWebKeys = @($envMap.Keys | Where-Object { -not $contractMap.ContainsKey($_) } | Sort-Object -Unique)
if ($unsupportedWebKeys.Count -gt 0) {
    throw (".env.web contains undocumented keys. Update docs/ops/env-contract.csv or remove the stale entries: {0}" -f ($unsupportedWebKeys -join ", "))
}

if (Test-Path $localEnvPath) {
    $localMap = Parse-EnvFile -Path $localEnvPath
    Assert-NoRemovedCompatibilityKeys -SourceName ".env" -Map $localMap
    $missingInWeb = @()
    foreach ($key in $localMap.Keys) {
        if (Should-SkipParityKey -Key $key -ContractMap $contractMap) { continue }

        $localValue = $localMap[$key]
        if ([string]::IsNullOrWhiteSpace($localValue)) { continue }
        if (-not $envMap.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($envMap[$key])) {
            $missingInWeb += $key
        }
    }

    if ($missingInWeb.Count -gt 0) {
        $sortedMissing = $missingInWeb | Sort-Object -Unique
        Write-Error ("Error: .env.web is missing populated keys from .env: {0}" -f ($sortedMissing -join ", "))
        exit 1
    }
} else {
    Write-Warning "No local .env found; skipping key parity check against .env."
}

$webUrlKeys = @("ASSET_ALLOCATION_API_BASE_URL")
$localEndpointViolations = @()
foreach ($key in $webUrlKeys) {
    if (-not $envMap.ContainsKey($key)) { continue }
    $value = $envMap[$key]
    if (Test-LocalAddressValue -Value $value) {
        $localEndpointViolations += "$key=$value"
    }
}
if ($localEndpointViolations.Count -gt 0) {
    Write-Error ("Error: .env.web contains local endpoints for web sync: {0}" -f ($localEndpointViolations -join ", "))
    exit 1
}
$ExpectedSecrets = @()
$ExpectedVars = @()

# -------------------------------------------------------------------------
# 1. PARSE .ENV
# -------------------------------------------------------------------------
foreach ($line in $lines) {
    $line = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#")) { continue }

    if ($line -match "^([^=]+)=(.*)$") {
        $key = $matches[1].Trim()
        $value = $matches[2].Trim()

        # Remove surrounding quotes for value
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or 
            ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if (-not $contractMap.ContainsKey($key)) {
            continue
        }

        $contract = $contractMap[$key]
        $githubStorage = (($contract.github_storage | Out-String).Trim()).ToLowerInvariant()

        switch ($githubStorage) {
            "var" {
                if ([string]::IsNullOrWhiteSpace($value)) {
                    if ($DryRun) {
                        Write-Host "[DRY RUN] Would SKIP empty VARIABLE: $key" -ForegroundColor Yellow
                    } else {
                        Write-Host "Skipping VARIABLE with empty value: $key" -ForegroundColor Yellow
                    }
                } else {
                    if ($DryRun) {
                        Write-Host "[DRY RUN] Would set VARIABLE: $key" -ForegroundColor Cyan
                    } else {
                        Write-Host "Setting VARIABLE: $key" -NoNewline
                        try {
                            $value | gh variable set "$key"
                            Write-Host " [OK]" -ForegroundColor Green
                        } catch {
                            Write-Host " [FAILED]" -ForegroundColor Red
                            Write-Error $_
                        }
                    }
                }
                $ExpectedVars += $key
            }
            "secret" {
                if ([string]::IsNullOrWhiteSpace($value)) {
                    if ($DryRun) {
                        Write-Host "[DRY RUN] Would SKIP empty SECRET:   $key" -ForegroundColor Yellow
                    } else {
                        Write-Host "Skipping SECRET with empty value:   $key" -ForegroundColor Yellow
                    }
                } else {
                    if ($DryRun) {
                        Write-Host "[DRY RUN] Would set SECRET:   $key" -ForegroundColor Magenta
                    } else {
                        Write-Host "Setting SECRET:   $key" -NoNewline
                        try {
                            $value | gh secret set "$key"
                            Write-Host " [OK]" -ForegroundColor Green
                        } catch {
                            Write-Host " [FAILED]" -ForegroundColor Red
                            Write-Error $_
                        }
                    }
                }
                $ExpectedSecrets += $key
            }
            default {
                Write-Host "Ignoring non-GitHub env key per contract: $key" -ForegroundColor DarkGray
            }
        }
    }
}

# -------------------------------------------------------------------------
# 2. PRUNE SECRETS
# -------------------------------------------------------------------------
Write-Host "`n----------------------------------------"
Write-Host "Checking for unexpected SECRETS in GitHub..."
$remoteSecrets = gh secret list --json name --jq ".[].name" 2>$null
if (-not $remoteSecrets) { $remoteSecrets = @() }

$secretsToDelete = @()
foreach ($s in $remoteSecrets) {
    if ($ExpectedSecrets -notcontains $s) {
        $secretsToDelete += $s
    }
}

if ($secretsToDelete.Count -gt 0) {
    $secretsToDelete | ForEach-Object { Write-Host " - [UNEXPECTED SECRET] $_" -ForegroundColor Red }
    if ($DryRun) {
        Write-Host "[DRY RUN] Would delete these secrets." -ForegroundColor Cyan
    } else {
        $confirm = Read-Host "Delete these secrets? Type 'yes' to confirm"
        if ($confirm -eq "yes") {
            foreach ($s in $secretsToDelete) {
                Write-Host "Deleting secret: $s..." -NoNewline
                gh secret delete "$s"
                Write-Host " [OK]" -ForegroundColor Green
            }
        } else {
            Write-Host "Skipping secret deletions." -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "No unexpected secrets found." -ForegroundColor Green
}

# -------------------------------------------------------------------------
# 3. PRUNE VARIABLES
# -------------------------------------------------------------------------
Write-Host "`n----------------------------------------"
Write-Host "Checking for unexpected VARIABLES in GitHub..."
$remoteVars = gh variable list --json name --jq ".[].name" 2>$null
if (-not $remoteVars) { $remoteVars = @() }

$varsToDelete = @()
foreach ($v in $remoteVars) {
    if ($ExpectedVars -notcontains $v) {
        $varsToDelete += $v
    }
}

if ($varsToDelete.Count -gt 0) {
    $varsToDelete | ForEach-Object { Write-Host " - [UNEXPECTED VARIABLE] $_" -ForegroundColor Red }
    if ($DryRun) {
        Write-Host "[DRY RUN] Would delete these variables." -ForegroundColor Cyan
    } else {
        $confirm = Read-Host "Delete these variables? Type 'yes' to confirm"
        if ($confirm -eq "yes") {
            foreach ($v in $varsToDelete) {
                Write-Host "Deleting variable: $v..." -NoNewline
                gh variable delete "$v"
                Write-Host " [OK]" -ForegroundColor Green
            }
        } else {
            Write-Host "Skipping variable deletions." -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "No unexpected variables found." -ForegroundColor Green
}

Write-Host "`nSync complete." -ForegroundColor Green
