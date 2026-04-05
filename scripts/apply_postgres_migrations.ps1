param(
  [string]$Dsn,
  [string]$MigrationsDir,
  [switch]$UseDockerPsql
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..") -ErrorAction Stop).Path
if (-not $MigrationsDir) {
  $MigrationsDir = Join-Path $RepoRoot "deploy/sql/postgres/migrations"
}

function Is-CompleteDsn {
  param([string]$Value)
  return ($Value -match "^[a-zA-Z][a-zA-Z0-9+.-]*://")
}

function Assert-CommandExists {
  param([Parameter(Mandatory = $true)][string]$Name)
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required command '$Name'. Install it and retry."
  }
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

function Invoke-Psql {
  param(
    [Parameter(Mandatory = $true)][string[]]$Args
  )

  $preferDocker = $UseDockerPsql.IsPresent

  if ($preferDocker) {
    Assert-CommandExists -Name "docker"
    $dockerArgs = @()
    $dockerStdinPath = $null

    for ($i = 0; $i -lt $Args.Count; $i++) {
      $arg = $Args[$i]
      if ($arg -eq "-f") {
        if (($i + 1) -ge $Args.Count) {
          throw "psql -f requires a file path."
        }

        $fileArg = $Args[$i + 1]
        $dockerArgs += "-f"
        if ($fileArg -eq "-") {
          $dockerArgs += "-"
        }
        else {
          if ($dockerStdinPath) {
            throw "Dockerized psql supports only one file-based -f input per invocation."
          }
          $dockerStdinPath = (Resolve-Path $fileArg -ErrorAction Stop).Path
          $dockerArgs += "-"
        }

        $i++
        continue
      }

      $dockerArgs += $arg
    }

    $cmd = @("run", "--rm")
    if ($dockerStdinPath) {
      $cmd += "-i"
    }
    $cmd += @("postgres:16-alpine", "psql") + $dockerArgs

    if ($dockerStdinPath) {
      # Stream SQL over stdin because host file paths are not visible inside `docker run`.
      Get-Content -Path $dockerStdinPath -Raw -Encoding UTF8 | & docker @cmd
    }
    else {
      & docker @cmd
    }
    if (-not $?) { throw "psql (docker) failed." }
    return
  }

  Assert-CommandExists -Name "psql"
  & psql @Args
  if (-not $?) { throw "psql failed." }
}

function Add-CreateTableIfNotExistsGuards {
  param(
    [Parameter(Mandatory = $true)][string]$Sql
  )

  $pattern = '(?im)^(\s*CREATE\s+TABLE\s+)(?!IF\s+NOT\s+EXISTS\b)([^\s(]+)'
  $rewriteCount = ([regex]::Matches($Sql, $pattern)).Count
  $rewritten = [regex]::Replace($Sql, $pattern, '$1IF NOT EXISTS $2')

  return [pscustomobject]@{
    Sql          = $rewritten
    RewriteCount = $rewriteCount
  }
}

function New-PreparedMigrationFile {
  param(
    [Parameter(Mandatory = $true)][string]$SourcePath
  )

  $sql = Get-Content -Path $SourcePath -Raw -Encoding UTF8
  $guarded = Add-CreateTableIfNotExistsGuards -Sql $sql
  if ($guarded.RewriteCount -eq 0) {
    return [pscustomobject]@{
      Path         = $SourcePath
      Temporary    = $false
      RewriteCount = 0
    }
  }

  $tempPath = Join-Path ([System.IO.Path]::GetTempPath()) ("{0}.sql" -f [System.IO.Path]::GetRandomFileName())
  $utf8 = [System.Text.UTF8Encoding]::new($false)
  [System.IO.File]::WriteAllText($tempPath, $guarded.Sql, $utf8)

  return [pscustomobject]@{
    Path         = $tempPath
    Temporary    = $true
    RewriteCount = $guarded.RewriteCount
  }
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

$resolvedDir = (Resolve-Path $MigrationsDir -ErrorAction Stop).Path
Write-Host "Applying migrations from: $resolvedDir"

$files = Get-ChildItem -Path $resolvedDir -File -Filter "*.sql" | Sort-Object Name
if (-not $files) {
  throw "No migration files found in $resolvedDir"
}

foreach ($file in $files) {
  Write-Host "Applying: $($file.Name)"
  $prepared = New-PreparedMigrationFile -SourcePath $file.FullName
  try {
    if ($prepared.RewriteCount -gt 0) {
      Write-Host "  Guarded $($prepared.RewriteCount) CREATE TABLE statement(s) with IF NOT EXISTS."
    }
    Invoke-Psql -Args @("$Dsn", "-v", "ON_ERROR_STOP=1", "-f", $prepared.Path)
  }
  finally {
    if ($prepared.Temporary -and (Test-Path $prepared.Path)) {
      Remove-Item -Path $prepared.Path -Force -ErrorAction SilentlyContinue
    }
  }
}

Write-Host "Migrations applied successfully."
