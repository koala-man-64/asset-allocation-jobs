param(
  [string]$ResourceGroup = "",
  [string]$JobName = "",
  [string]$SubscriptionId = "",
  [string]$EnvFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Usage {
  @"
Usage: ensure_job_start_rbac.ps1 [-ResourceGroup <rg>] [-JobName <job>] [-SubscriptionId <sub>] [-EnvFile <path>]

Ensures Container App Job identities can start downstream jobs and wake
container apps by granting Contributor at the resource group scope.
"@
}

function Get-EnvValue {
  param(
    [Parameter(Mandatory = $true)][string]$Key,
    [string[]]$Lines
  )

  foreach ($line in $Lines) {
    $trimmed = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) { continue }
    if ($trimmed -match ("^" + [regex]::Escape($Key) + "=(.*)$")) {
      $value = $matches[1].Trim()
      if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
          ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
      }
      return $value
    }
  }
  return $null
}

$envPath = $EnvFile
if ([string]::IsNullOrWhiteSpace($envPath)) {
  $envPath = Join-Path (Join-Path $PSScriptRoot "..") ".env"
}

$envLines = @()
if (Test-Path $envPath) {
  $envLines = Get-Content $envPath
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValue -Key "AZURE_SUBSCRIPTION_ID" -Lines $envLines
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = Get-EnvValue -Key "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID" -Lines $envLines
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = $env:AZURE_SUBSCRIPTION_ID
  if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
    $SubscriptionId = $env:SUBSCRIPTION_ID
  }
}

if ([string]::IsNullOrWhiteSpace($SubscriptionId)) {
  $SubscriptionId = (az account show --query id -o tsv) -replace "`r", ""
}

$resolvedResourceGroup = $ResourceGroup
if ([string]::IsNullOrWhiteSpace($resolvedResourceGroup)) {
  $resolvedResourceGroup = Get-EnvValue -Key "SYSTEM_HEALTH_ARM_RESOURCE_GROUP" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($resolvedResourceGroup)) {
  $resolvedResourceGroup = Get-EnvValue -Key "RESOURCE_GROUP" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($resolvedResourceGroup)) {
  $resolvedResourceGroup = $env:SYSTEM_HEALTH_ARM_RESOURCE_GROUP
}
if ([string]::IsNullOrWhiteSpace($resolvedResourceGroup)) {
  $resolvedResourceGroup = $env:RESOURCE_GROUP
}

$resolvedJobName = $JobName
if ([string]::IsNullOrWhiteSpace($resolvedJobName)) {
  $resolvedJobName = Get-EnvValue -Key "TRIGGER_NEXT_JOB_NAME" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($resolvedJobName)) {
  $resolvedJobName = Get-EnvValue -Key "JOB_NAME" -Lines $envLines
}
if ([string]::IsNullOrWhiteSpace($resolvedJobName)) {
  $resolvedJobName = $env:TRIGGER_NEXT_JOB_NAME
}
if ([string]::IsNullOrWhiteSpace($resolvedJobName)) {
  $resolvedJobName = $env:JOB_NAME
}

if ([string]::IsNullOrWhiteSpace($resolvedResourceGroup)) {
  Write-Usage
  Write-Error "Missing required values. Provide -ResourceGroup or set SYSTEM_HEALTH_ARM_RESOURCE_GROUP in .env."
  exit 2
}

function Resolve-JobNames {
  param(
    [string]$ResourceGroup,
    [string]$SingleJobName,
    [string[]]$EnvLines
  )

  if (-not [string]::IsNullOrWhiteSpace($SingleJobName)) {
    return @($SingleJobName)
  }

  $allowlist = Get-EnvValue -Key "SYSTEM_HEALTH_ARM_JOBS" -Lines $EnvLines
  if (-not [string]::IsNullOrWhiteSpace($allowlist)) {
    $jobs = $allowlist.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    if ($jobs.Count -gt 0) {
      return @($jobs)
    }
  }

  try {
    $jobs = @(az containerapp job list --resource-group $ResourceGroup --query "[].name" -o tsv --only-show-errors)
    $jobs = $jobs | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ }
    return @($jobs)
  } catch {
    return @()
  }
}

$jobNames = Resolve-JobNames -ResourceGroup $resolvedResourceGroup -SingleJobName $resolvedJobName -EnvLines $envLines
if ($jobNames.Count -eq 0) {
  Write-Usage
  Write-Error "No job names found. Provide -JobName, set SYSTEM_HEALTH_ARM_JOBS, or ensure jobs exist in the resource group."
  exit 2
}

$rgScope = "/subscriptions/$SubscriptionId/resourceGroups/$resolvedResourceGroup"

$failed = 0

foreach ($job in $jobNames) {
  $jobName = $job.ToString().Trim()
  if ([string]::IsNullOrWhiteSpace($jobName)) { continue }

  $identityType = (az containerapp job show -g $resolvedResourceGroup -n $jobName --query "identity.type" -o tsv) -replace "`r", ""

  $principalId = ""
  $clientId = ""
  if ($identityType -like "*UserAssigned*") {
    $principalId = (az containerapp job show -g $resolvedResourceGroup -n $jobName --query "identity.userAssignedIdentities.*.principalId | [0]" -o tsv) -replace "`r", ""
    $clientId = (az containerapp job show -g $resolvedResourceGroup -n $jobName --query "identity.userAssignedIdentities.*.clientId | [0]" -o tsv) -replace "`r", ""
  } else {
    $principalId = (az containerapp job show -g $resolvedResourceGroup -n $jobName --query "identity.principalId" -o tsv) -replace "`r", ""
  }

  if ([string]::IsNullOrWhiteSpace($principalId) -or $principalId -eq "None") {
    Write-Warning "Could not resolve job identity principalId for '$jobName' in RG '$resolvedResourceGroup'. Identity type: '$identityType'."
    $failed += 1
    continue
  }

  if (-not [string]::IsNullOrWhiteSpace($clientId) -and $clientId -ne "None") {
    Write-Host "Resolved job identity: job=$jobName principalId=$principalId clientId=$clientId"
  } else {
    Write-Host "Resolved job identity: job=$jobName principalId=$principalId"
  }

  $existing = "0"
  try {
    $existing = (az role assignment list --assignee-object-id $principalId --scope $rgScope --query "[?roleDefinitionName=='Contributor'] | length(@)" -o tsv --only-show-errors) -replace "`r", ""
    if ([string]::IsNullOrWhiteSpace($existing)) { $existing = "0" }
  } catch {
    $existing = "0"
  }

  if ([int]$existing -eq 0) {
    try {
      az role assignment create `
        --assignee-object-id $principalId `
        --assignee-principal-type ServicePrincipal `
        --role "Contributor" `
        --scope $rgScope `
        --only-show-errors 1>$null
      Write-Host "Granted Contributor at RG scope to $jobName identity ($principalId)."
    } catch {
      Write-Warning "Failed to grant Contributor at RG scope for $jobName ($principalId): $($_.Exception.Message)"
      $failed += 1
    }
  } else {
    Write-Host "Contributor already present at RG scope for $jobName identity ($principalId)."
  }
}

if ($failed -gt 0) {
  exit 1
}
