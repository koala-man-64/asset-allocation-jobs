param(
  [string]$ResourceGroup = "AssetAllocationRG",
  [string]$ApiAppName = "",
  [string]$UiAppName = "",
  [string]$AcrName = ""
)

$jobs = @(
  "bronze-market-job",
  "bronze-finance-job",
  "bronze-price-target-job",
  "bronze-earnings-job",
  "silver-market-job",
  "silver-finance-job",
  "silver-price-target-job",
  "silver-earnings-job",
  "gold-market-job",
  "gold-finance-job",
  "gold-price-target-job",
  "gold-earnings-job",
  "gold-regime-job",
  "backtests-job"
)

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

Write-Host "Deleting jobs in Resource Group: $ResourceGroup"

foreach ($job in $jobs) {
  Write-Host "Deleting $job..."
  az containerapp job delete --name $job --resource-group $ResourceGroup --yes --only-show-errors
  if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully deleted $job" -ForegroundColor Green
  } else {
    Write-Host "Failed to delete $job (it may not exist)" -ForegroundColor Yellow
  }
}

Write-Host "Deleting container apps in Resource Group: $ResourceGroup"

foreach ($app in $containerApps) {
  Write-Host "Deleting $app..."
  az containerapp delete --name $app --resource-group $ResourceGroup --yes --only-show-errors
  if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully deleted $app" -ForegroundColor Green
  } else {
    Write-Host "Failed to delete $app (it may not exist)" -ForegroundColor Yellow
  }
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
    az acr repository delete --name $resolvedAcrName --repository $repository --yes --only-show-errors
    if ($LASTEXITCODE -eq 0) {
      Write-Host "Successfully deleted repository $repository" -ForegroundColor Green
    } else {
      Write-Host "Failed to delete repository $repository" -ForegroundColor Yellow
    }
  }
}

Write-Host "Done."
