param(
  [string]$Dsn,
  [switch]$DryRun,
  [switch]$Force,
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..") -ErrorAction Stop).Path

function Is-CompleteDsn {
  param([string]$Value)
  return ($Value -match "^[a-zA-Z][a-zA-Z0-9+.-]*://")
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Path,
    [Parameter(Mandatory = $true)][string]$Key
  )

  if (-not (Test-Path $Path)) {
    return ""
  }

  $keyPattern = "^{0}\s*=" -f [regex]::Escape($Key)
  foreach ($line in Get-Content $Path) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
      continue
    }

    if ($trimmed -notmatch $keyPattern) {
      continue
    }

    $parts = $trimmed -split "=", 2
    if ($parts.Count -ne 2) {
      continue
    }

    $value = $parts[1].Trim()
    if ($value.StartsWith('"') -and $value.EndsWith('"') -and $value.Length -ge 2) {
      $value = $value.Substring(1, $value.Length - 2)
    }
    return $value
  }

  return ""
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
}

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  $preferDocker = $UseDockerPsql.IsPresent
  if (-not $preferDocker -and -not (Get-Command psql -ErrorAction SilentlyContinue)) {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
      Write-Host "Local psql is not installed; falling back to Dockerized psql."
      $preferDocker = $true
    }
    else {
      throw "Missing required command 'psql'. Install it or retry with Docker available."
    }
  }

  if ($preferDocker) {
    Assert-CommandExists -Name "docker"
    $cmd = @("run", "--rm", "postgres:16-alpine", "psql") + $Args
    $output = & docker @cmd 2>&1
    if ($LASTEXITCODE -ne 0) {
      throw "psql (docker) failed.`n$($output -join "`n")"
    }
    return @($output)
  }

  Assert-CommandExists -Name "psql"
  $output = & psql @Args 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "psql failed.`n$($output -join "`n")"
  }
  return @($output)
}

if (-not $Dsn) {
  $DsnFromEnv = Get-EnvValue -Path (Join-Path $RepoRoot ".env") -Key "POSTGRES_DSN"
  if (-not $DsnFromEnv) {
    $DsnFromEnv = $env:POSTGRES_DSN
    if (-not $DsnFromEnv) {
      $DsnFromEnv = Get-EnvValue -Path (Join-Path (Split-Path $PSScriptRoot) ".env") -Key "POSTGRES_DSN"
    }
  }
  if ($DsnFromEnv) {
    $Dsn = $DsnFromEnv
  }
}

if (-not $Dsn) {
  throw "POSTGRES_DSN is not configured. Set POSTGRES_DSN in `.env` or pass -Dsn."
}

if (-not (Is-CompleteDsn -Value $Dsn)) {
  throw "Invalid or incomplete POSTGRES_DSN: '$Dsn'. Expected full DSN format, e.g. postgresql://user:pass@host:5432/db?sslmode=require"
}

$listTablesSql = @'
SELECT tablename
FROM pg_catalog.pg_tables
WHERE schemaname = 'gold'
ORDER BY tablename;
'@

$tables = @(
  Invoke-Psql -Args @("-d", $Dsn, "-X", "-v", "ON_ERROR_STOP=1", "-At", "-c", $listTablesSql) |
    ForEach-Object { $_.ToString().Trim() } |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)

if (-not $tables) {
  Write-Host "No tables found in schema gold."
  return
}

Write-Host "Found $($tables.Count) gold table(s):"
foreach ($table in $tables) {
  Write-Host " - gold.$table"
}

if ($DryRun) {
  Write-Host "Dry run only. No tables were dropped."
  return
}

if (-not $Force) {
  Write-Host "WARNING: This will permanently drop every table in the gold schema."
  Write-Host "Dependent objects such as views may also be removed because tables are dropped with CASCADE."
  $response = Read-Host "Are you sure you want to continue? (y/N)"
  if ($response.Trim().ToLowerInvariant() -ne "y") {
    Write-Host "Aborted."
    return
  }
}

$dropTablesSql = @'
DO $$
DECLARE
  gold_table RECORD;
BEGIN
  FOR gold_table IN
    SELECT tablename
    FROM pg_catalog.pg_tables
    WHERE schemaname = 'gold'
    ORDER BY tablename
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS gold.%I CASCADE', gold_table.tablename);
  END LOOP;
END $$;
'@

Invoke-Psql -Args @("-d", $Dsn, "-X", "-v", "ON_ERROR_STOP=1", "-c", $dropTablesSql) | Out-Null
Write-Host "Dropped $($tables.Count) gold table(s) from schema gold."
