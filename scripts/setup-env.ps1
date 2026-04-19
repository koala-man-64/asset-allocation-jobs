param(
    [string]$EnvFilePath = "",
    [switch]$DryRun,
    [string[]]$Set = @()
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $repoRoot ".env.web"
}

$contractPath = Join-Path $repoRoot "docs\ops\env-contract.csv"
$templatePath = Join-Path $repoRoot ".env.template"
$controlPlaneInternalAppName = "asset-allocation-api-vnet"
$controlPlaneInternalBaseUrl = "http://$controlPlaneInternalAppName"

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Invoke-TextCommand {
    param([Parameter(Mandatory = $true)][string]$FilePath, [Parameter(Mandatory = $true)][string[]]$ArgumentList)
    if (-not (Test-CommandAvailable -Name $FilePath)) { return "" }
    try {
        $result = & $FilePath @ArgumentList 2>$null
        return (($result | Out-String).Trim())
    } catch {
        return ""
    }
}

function Invoke-JsonCommand {
    param([Parameter(Mandatory = $true)][string]$FilePath, [Parameter(Mandatory = $true)][string[]]$ArgumentList)
    $text = Invoke-TextCommand -FilePath $FilePath -ArgumentList $ArgumentList
    if ([string]::IsNullOrWhiteSpace($text)) { return $null }
    try { return $text | ConvertFrom-Json } catch { return $null }
}

function Parse-EnvFile {
    param([Parameter(Mandatory = $true)][string]$Path)
    $map = @{}
    if (-not (Test-Path $Path)) { return $map }
    foreach ($rawLine in (Get-Content $Path)) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("#") -or $line -notmatch "^([^=]+)=(.*)$") { continue }
        $map[$matches[1].Trim()] = $matches[2]
    }
    return $map
}

function Load-ContractRows {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path $Path)) { throw "Env contract not found at $Path" }
    return @(Import-Csv -Path $Path | Where-Object { $_.template -eq "true" -and $_.github_storage -in @("var", "secret") })
}

function ConvertFrom-SecureStringPlain {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Secure)
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

function Normalize-EnvValue {
    param([AllowNull()][string]$Value)
    if ($null -eq $Value) { return "" }
    return $Value.Replace("`r", "").Replace("`n", "\n")
}

$overrideMap = @{}
foreach ($entry in $Set) {
    if ($entry -match "^([^=]+)=(.*)$") {
        $overrideMap[$matches[1].Trim()] = $matches[2]
    }
}

$existingMap = Parse-EnvFile -Path $EnvFilePath
$templateMap = Parse-EnvFile -Path $templatePath
$contractRows = Load-ContractRows -Path $contractPath

$script:AzureAccount = $null
$script:ResourceGroup = $null
$script:GitOwner = $null
$script:Identities = $null
$script:ContainerAppsEnv = $null
$script:ContainerAppJobs = $null
$script:StorageAccounts = $null

function Get-AzureAccount {
    if ($null -eq $script:AzureAccount) {
        $script:AzureAccount = Invoke-JsonCommand -FilePath "az" -ArgumentList @("account", "show", "-o", "json")
    }
    return $script:AzureAccount
}

function Get-ResourceGroupName {
    if ($null -eq $script:ResourceGroup) {
        $script:ResourceGroup = if ($templateMap.ContainsKey("RESOURCE_GROUP")) { $templateMap["RESOURCE_GROUP"] } else { "AssetAllocationRG" }
    }
    return $script:ResourceGroup
}

function Get-GitOwner {
    if ($null -eq $script:GitOwner) {
        $remote = Invoke-TextCommand -FilePath "git" -ArgumentList @("-C", $repoRoot, "config", "--get", "remote.origin.url")
        $owner = ""
        if ($remote -match "github\.com[:/](?<owner>[^/]+)/(?<repo>[^/.]+)(?:\.git)?$") {
            $owner = $matches["owner"]
        }
        $script:GitOwner = $owner
    }
    return $script:GitOwner
}

function Get-ItemsFromAzure {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    $items = Invoke-JsonCommand -FilePath "az" -ArgumentList $Arguments
    if ($null -eq $items) { return $null }
    return @($items)
}

function Get-UserAssignedIdentities {
    if ($null -eq $script:Identities) {
        $script:Identities = Get-ItemsFromAzure -Arguments @("identity", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:Identities
}

function Get-ContainerAppsEnvironments {
    if ($null -eq $script:ContainerAppsEnv) {
        $script:ContainerAppsEnv = Get-ItemsFromAzure -Arguments @("containerapp", "env", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:ContainerAppsEnv
}

function Get-ContainerAppJobs {
    if ($null -eq $script:ContainerAppJobs) {
        $script:ContainerAppJobs = Get-ItemsFromAzure -Arguments @("containerapp", "job", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:ContainerAppJobs
}

function Get-StorageAccounts {
    if ($null -eq $script:StorageAccounts) {
        $script:StorageAccounts = Get-ItemsFromAzure -Arguments @("storage", "account", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
    }
    return $script:StorageAccounts
}

function Select-PreferredName {
    param($Items, [Parameter(Mandatory = $true)][string]$Preferred, [string[]]$Contains = @())
    $list = @($Items)
    $namedItems = @(
        $list |
            Where-Object {
                $null -ne $_ -and
                $_.PSObject.Properties.Match("name").Count -gt 0 -and
                -not [string]::IsNullOrWhiteSpace([string]$_.name)
            }
    )
    if ($namedItems.Count -eq 0) { return "" }
    $exact = @($namedItems | Where-Object { $_.name -eq $Preferred } | Select-Object -First 1)
    if ($exact.Count -gt 0) { return $exact[0].name }
    foreach ($needle in $Contains) {
        $match = @($namedItems | Where-Object { $_.name -like "*$needle*" } | Select-Object -First 1)
        if ($match.Count -gt 0) { return $match[0].name }
    }
    return $Preferred
}

function Get-EntraAppClientId {
    param([Parameter(Mandatory = $true)][string]$DisplayName)
    $apps = Invoke-JsonCommand -FilePath "az" -ArgumentList @("ad", "app", "list", "--display-name", $DisplayName, "-o", "json")
    if ($apps -and @($apps).Count -gt 0) { return @($apps)[0].appId }
    return ""
}

function Get-RepoSlug {
    param([Parameter(Mandatory = $true)][string]$RepoName)
    $owner = Get-GitOwner
    if ($owner) { return "$owner/$RepoName" }
    return ""
}

function New-Resolution {
    param([AllowEmptyString()][string]$Value = "", [string]$Source = "default", [bool]$PromptRequired = $false)
    return @{ Value = (Normalize-EnvValue -Value $Value); Source = $Source; PromptRequired = $PromptRequired }
}

function Test-CanAutoDiscoverSecretValue {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $Name -in @(
        "ASSET_ALLOCATION_API_BASE_URL",
        "ASSET_ALLOCATION_API_SCOPE"
    )
}

function Resolve-DiscoveredValue {
    param([Parameter(Mandatory = $true)][string]$Key)
    switch ($Key) {
        "AZURE_TENANT_ID" {
            $account = Get-AzureAccount
            if ($account -and $account.tenantId) { return (New-Resolution -Value $account.tenantId -Source "azure") }
        }
        "AZURE_SUBSCRIPTION_ID" {
            $account = Get-AzureAccount
            if ($account -and $account.id) { return (New-Resolution -Value $account.id -Source "azure") }
        }
        "RESOURCE_GROUP" { return (New-Resolution -Value (Get-ResourceGroupName) -Source "azure") }
        "ACR_NAME" {
            $items = Get-ItemsFromAzure -Arguments @("acr", "list", "--resource-group", (Get-ResourceGroupName), "-o", "json")
            if (@($items).Count -gt 0) { return (New-Resolution -Value (Select-PreferredName -Items $items -Preferred "assetallocationacr" -Contains @("acr", "asset")) -Source "azure") }
        }
        "ACR_PULL_IDENTITY_NAME" { return (New-Resolution -Value (Select-PreferredName -Items (Get-UserAssignedIdentities) -Preferred "asset-allocation-acr-pull-mi" -Contains @("acr", "pull")) -Source "azure") }
        "SERVICE_ACCOUNT_NAME" { return (New-Resolution -Value (Select-PreferredName -Items (Get-UserAssignedIdentities) -Preferred "asset-allocation-sa" -Contains @("service", "sa")) -Source "azure") }
        "CONTAINER_APPS_ENVIRONMENT_NAME" { return (New-Resolution -Value (Select-PreferredName -Items (Get-ContainerAppsEnvironments) -Preferred "asset-allocation-env" -Contains @("asset", "env")) -Source "azure") }
        "AZURE_STORAGE_ACCOUNT_NAME" { return (New-Resolution -Value (Select-PreferredName -Items (Get-StorageAccounts) -Preferred "assetallocstorage001" -Contains @("asset", "storage")) -Source "azure") }
        "CONTRACTS_REPOSITORY" {
            $slug = Get-RepoSlug -RepoName "asset-allocation-contracts"
            if ($slug) { return (New-Resolution -Value $slug -Source "git") }
        }
        "CONTROL_PLANE_REPOSITORY" {
            $slug = Get-RepoSlug -RepoName "asset-allocation-control-plane"
            if ($slug) { return (New-Resolution -Value $slug -Source "git") }
        }
        "ASSET_ALLOCATION_API_BASE_URL" {
            return (New-Resolution -Value $controlPlaneInternalBaseUrl -Source "default")
        }
        "ASSET_ALLOCATION_API_SCOPE" {
            $appId = Get-EntraAppClientId -DisplayName "asset-allocation-api"
            if ($appId) { return (New-Resolution -Value "api://$appId/.default" -Source "azure") }
        }
        "JOB_STARTUP_API_CONTAINER_APPS" { return (New-Resolution -Value $controlPlaneInternalAppName -Source "default") }
        "AZURE_CLIENT_ID" {
            $identities = @(Get-UserAssignedIdentities)
            $candidate = @(
                $identities |
                    Where-Object {
                        $null -ne $_ -and
                        $_.PSObject.Properties.Match("name").Count -gt 0 -and
                        $_.PSObject.Properties.Match("clientId").Count -gt 0 -and
                        ($_.name -like "*job*" -or $_.name -like "*github*" -or $_.name -like "*gha*")
                    } |
                    Select-Object -First 1
            )
            if (@($candidate).Count -gt 0 -and $candidate[0].clientId) { return (New-Resolution -Value $candidate[0].clientId -Source "azure") }
        }
    }
    return (New-Resolution)
}

function Prompt-PlainValue {
    param([Parameter(Mandatory = $true)][string]$Name, [string]$Suggestion = "", [string]$Description = "")
    if ($Description) { Write-Host "# $Description" -ForegroundColor DarkGray }
    $input = Read-Host "$Name [$Suggestion]"
    if ([string]::IsNullOrWhiteSpace($input)) { return $Suggestion }
    return $input
}

function Prompt-SecretValue {
    param([Parameter(Mandatory = $true)][string]$Name, [string]$Description = "")
    if ($Description) { Write-Host "# $Description" -ForegroundColor DarkGray }
    $secure = Read-Host "$Name [secret]" -AsSecureString
    return (ConvertFrom-SecureStringPlain -Secure $secure)
}

$results = New-Object System.Collections.Generic.List[object]
foreach ($row in $contractRows) {
    $name = $row.name
    $description = (($row.notes | Out-String).Trim())
    $isSecret = $row.github_storage -eq "secret"
    $defaultValue = if ($templateMap.ContainsKey($name)) { Normalize-EnvValue -Value $templateMap[$name] } else { "" }

    if ($existingMap.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($existingMap[$name])) {
        $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValue -Value $existingMap[$name]); Source = "existing"; IsSecret = $isSecret; PromptRequired = $false })
        continue
    }
    if ($overrideMap.ContainsKey($name) -and -not [string]::IsNullOrWhiteSpace($overrideMap[$name])) {
        $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValue -Value $overrideMap[$name]); Source = "prompted"; IsSecret = $isSecret; PromptRequired = $false })
        continue
    }

    if ($isSecret -and (Test-CanAutoDiscoverSecretValue -Name $name)) {
        $discovered = Resolve-DiscoveredValue -Key $name
        if (-not [string]::IsNullOrWhiteSpace($discovered.Value)) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $discovered.Value; Source = $discovered.Source; IsSecret = $true; PromptRequired = $false })
            continue
        }
    }

    if (-not $isSecret) {
        $discovered = Resolve-DiscoveredValue -Key $name
        if (-not [string]::IsNullOrWhiteSpace($discovered.Value)) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $discovered.Value; Source = $discovered.Source; IsSecret = $false; PromptRequired = $false })
            continue
        }
        if (-not [string]::IsNullOrWhiteSpace($defaultValue)) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; Source = "default"; IsSecret = $false; PromptRequired = $false })
            continue
        }
        if ($DryRun) {
            $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; Source = "default"; IsSecret = $false; PromptRequired = $true })
            continue
        }
        $value = Prompt-PlainValue -Name $name -Suggestion $defaultValue -Description $description
        $source = if ([string]::IsNullOrWhiteSpace($value) -or $value -eq $defaultValue) { "default" } else { "prompted" }
        $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValue -Value $value); Source = $source; IsSecret = $false; PromptRequired = $false })
        continue
    }

    if (-not [string]::IsNullOrWhiteSpace($defaultValue)) {
        $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; Source = "default"; IsSecret = $true; PromptRequired = $false })
        continue
    }

    if ($DryRun) {
        $results.Add([pscustomobject]@{ Name = $name; Value = $defaultValue; Source = "default"; IsSecret = $true; PromptRequired = $true })
        continue
    }
    $secretValue = Prompt-SecretValue -Name $name -Description $description
    $secretSource = if ([string]::IsNullOrWhiteSpace($secretValue)) { "default" } else { "prompted" }
    $results.Add([pscustomobject]@{ Name = $name; Value = (Normalize-EnvValue -Value $secretValue); Source = $secretSource; IsSecret = $true; PromptRequired = $false })
}

$lines = foreach ($result in $results) { "{0}={1}" -f $result.Name, $result.Value }
Write-Host "Target env file: $EnvFilePath" -ForegroundColor Cyan
foreach ($result in $results) {
    $displayValue = if ($result.IsSecret -and -not [string]::IsNullOrWhiteSpace($result.Value)) { "<redacted>" } else { $result.Value }
    Write-Host ("{0}={1} [source={2}; prompt_required={3}]" -f $result.Name, $displayValue, $result.Source, $result.PromptRequired.ToString().ToLowerInvariant())
}
if ($DryRun) {
    Write-Host ""
    Write-Host "# Preview (.env.web)" -ForegroundColor Cyan
    foreach ($result in $results) {
        $displayValue = if ($result.IsSecret -and -not [string]::IsNullOrWhiteSpace($result.Value)) { "<redacted>" } else { $result.Value }
        Write-Host ("{0}={1}" -f $result.Name, $displayValue)
    }
    return
}
Set-Content -Path $EnvFilePath -Value $lines -Encoding utf8
Write-Host "Wrote $EnvFilePath" -ForegroundColor Green
