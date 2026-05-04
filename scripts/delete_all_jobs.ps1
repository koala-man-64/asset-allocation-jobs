[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$ApiAppName = "",
  [string]$UiAppName = "",
  [string]$AcrName = "",
  [string]$DeployDir = "",
  [switch]$ManifestOnly,
  [switch]$SkipContainerApps,
  [switch]$SkipAcrRepositories
)

$repoRoot = Split-Path $PSScriptRoot -Parent
if (-not $DeployDir) {
  $DeployDir = Join-Path $repoRoot "deploy"
}

function ConvertTo-NameList {
  param([string]$Raw)

  if ([string]::IsNullOrWhiteSpace($Raw)) {
    return @()
  }

  return @(
    $Raw -split "\s+" |
      ForEach-Object { $_.Trim() } |
      Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
      Sort-Object -Unique
  )
}

function Get-ManifestJobNames {
  param([string]$ManifestDir)

  if (-not (Test-Path -LiteralPath $ManifestDir)) {
    Write-Host "Deploy manifest directory not found: $ManifestDir" -ForegroundColor Yellow
    return @()
  }

  $names = New-Object System.Collections.Generic.List[string]
  foreach ($manifest in Get-ChildItem -LiteralPath $ManifestDir -Filter "job_*.yaml" -File | Sort-Object Name) {
    $nameLine = Get-Content -LiteralPath $manifest.FullName -TotalCount 20 |
      Where-Object { $_ -match '^\s*name:\s*(.+?)\s*$' } |
      Select-Object -First 1
    if ($nameLine -match '^\s*name:\s*(.+?)\s*$') {
      $name = $Matches[1].Trim().Trim('"').Trim("'")
      if (-not [string]::IsNullOrWhiteSpace($name)) {
        $names.Add($name)
      }
    }
  }

  return @($names | Sort-Object -Unique)
}

function Get-LiveJobNames {
  param([string]$TargetResourceGroup)

  $raw = az containerapp job list `
    --resource-group $TargetResourceGroup `
    --query "[].name" `
    --output tsv `
    --only-show-errors 2>$null

  if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to enumerate ACA jobs in Resource Group '$TargetResourceGroup'." -ForegroundColor Yellow
    return @()
  }

  return ConvertTo-NameList -Raw $raw
}

function Resolve-JobNames {
  if (-not $ManifestOnly) {
    $liveJobs = Get-LiveJobNames -TargetResourceGroup $ResourceGroup
    if ($liveJobs.Count -gt 0) {
      Write-Host "Resolved $($liveJobs.Count) ACA jobs from live Resource Group '$ResourceGroup'."
      return $liveJobs
    }
  }

  $manifestJobs = Get-ManifestJobNames -ManifestDir $DeployDir
  if ($manifestJobs.Count -gt 0) {
    Write-Host "Resolved $($manifestJobs.Count) ACA jobs from manifests in '$DeployDir'."
  }
  return $manifestJobs
}

$resolvedApiAppName = $ApiAppName
if (-not $resolvedApiAppName) {
  $resolvedApiAppName = $env:API_APP_NAME
}
if (-not $resolvedApiAppName) {
  $resolvedApiAppName = "asset-allocation-api"
}

$resolvedUiAppName = $UiAppName
if (-not $resolvedUiAppName) {
  $resolvedUiAppName = $env:UI_APP_NAME
}

$resolvedAcrName = $AcrName
if (-not $resolvedAcrName) {
  $resolvedAcrName = $env:ACR_NAME
}
if (-not $resolvedAcrName) {
  $resolvedAcrName = $env:AZURE_ACR_NAME
}
if (-not $resolvedAcrName) {
  $resolvedAcrName = "assetallocationacr"
}

$containerApps = @($resolvedApiAppName)
if (-not [string]::IsNullOrWhiteSpace($resolvedUiAppName) -and $resolvedUiAppName -ne $resolvedApiAppName) {
  $containerApps += $resolvedUiAppName
}
$containerApps = @($containerApps | Sort-Object -Unique)
$jobs = @(Resolve-JobNames)

Write-Host "Deleting jobs in Resource Group: $ResourceGroup"

if ($jobs.Count -eq 0) {
  Write-Host "No ACA jobs resolved for deletion." -ForegroundColor Yellow
} else {
  foreach ($job in $jobs) {
    Write-Host "Deleting $job..."
    if (-not $PSCmdlet.ShouldProcess($job, "Delete ACA job")) {
      continue
    }
    az containerapp job delete --name $job --resource-group $ResourceGroup --yes --only-show-errors
    if ($LASTEXITCODE -eq 0) {
      Write-Host "Successfully deleted $job" -ForegroundColor Green
    } else {
      Write-Host "Failed to delete $job (it may not exist)" -ForegroundColor Yellow
    }
  }
}

if ($SkipContainerApps) {
  Write-Host "Skipping container app deletion."
} else {
  Write-Host "Deleting container apps in Resource Group: $ResourceGroup"

  foreach ($app in $containerApps) {
    Write-Host "Deleting $app..."
    if (-not $PSCmdlet.ShouldProcess($app, "Delete container app")) {
      continue
    }
    az containerapp delete --name $app --resource-group $ResourceGroup --yes --only-show-errors
    if ($LASTEXITCODE -eq 0) {
      Write-Host "Successfully deleted $app" -ForegroundColor Green
    } else {
      Write-Host "Failed to delete $app (it may not exist)" -ForegroundColor Yellow
    }
  }
}

if ($SkipAcrRepositories) {
  Write-Host "Skipping ACR repository deletion."
  Write-Host "Done."
  exit 0
}

Write-Host "Deleting container registry repositories in ACR: $resolvedAcrName"
$repositories = az acr repository list --name $resolvedAcrName --output tsv --only-show-errors 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Failed to enumerate repositories in ACR '$resolvedAcrName' (it may not exist or you may not have access)" -ForegroundColor Yellow
} elseif (-not $repositories) {
  Write-Host "No repositories found in ACR '$resolvedAcrName'" -ForegroundColor Yellow
} else {
  foreach ($repository in ($repositories -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
    Write-Host "Deleting repository $repository..."
    if (-not $PSCmdlet.ShouldProcess("$resolvedAcrName/$repository", "Delete ACR repository")) {
      continue
    }
    az acr repository delete --name $resolvedAcrName --repository $repository --yes --only-show-errors
    if ($LASTEXITCODE -eq 0) {
      Write-Host "Successfully deleted repository $repository" -ForegroundColor Green
    } else {
      Write-Host "Failed to delete repository $repository" -ForegroundColor Yellow
    }
  }
}

Write-Host "Done."
