
# Script to add Federated Identity Credential for GitHub Actions (ag-main branch)
# Usage: ./add_federated_credential.ps1

# 1. Load Environment Variables from .env file (parent directory)
$envPath = Join-Path $PSScriptRoot "..\.env"
if (Test-Path $envPath) {
    Write-Host "Loading .env from $envPath" -ForegroundColor Cyan
    Get-Content $envPath | Where-Object { $_ -match '^\s*[^#]' } | ForEach-Object {
        $parts = $_ -split '=', 2
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim().Trim('"')
            [System.Environment]::SetEnvironmentVariable($key, $value, [System.EnvironmentVariableTarget]::Process)
        }
    }
} else {
    Write-Error ".env file not found at $envPath. Please verify path."
    exit 1
}

# 2. Get Config
$clientId = [System.Environment]::GetEnvironmentVariable("AZURE_CLIENT_ID")
$tenantId = [System.Environment]::GetEnvironmentVariable("AZURE_TENANT_ID")
$org = "koala-man-64"
$repo = "asset-allocation"
$branch = "ag-main"
$subject = "repo:${org}/${repo}:ref:refs/heads/${branch}"
$credentialName = "github-ag-main"

if (-not $clientId) {
    Write-Error "AZURE_CLIENT_ID not found in .env."
    exit 1
}

Write-Host "Configuration:" -ForegroundColor Cyan
Write-Host "  Client ID: $clientId"
Write-Host "  Repo:      $org/$repo"
Write-Host "  Branch:    $branch"
Write-Host "  Subject:   $subject"

# 3. Create Credential via Azure CLI
Write-Host "`nCreating Federated Credential '$credentialName'..." -ForegroundColor Yellow

# parameters.json approach is robust
$params = @{
    name = $credentialName
    issuer = "https://token.actions.githubusercontent.com"
    subject = $subject
    description = "GitHub Actions credential for branch $branch"
    audiences = @("api://AzureADTokenExchange")
} | ConvertTo-Json

# Save literal JSON to a temp file to avoid escaping hell
$jsonFile = Join-Path $env:TEMP "fed_cred.json"
$params | Set-Content -Path $jsonFile

try {
    # Check if we are logged in
    $account = az account show 2>$null
    if (-not $account) {
        Write-Warning "Not logged in to Azure CLI. Triggering login..."
        az login
    }

    # Create credential
    # Note: If it exists, this might fail unless we delete first or separate check.
    # The output from the user showed "No matching federated identity record found", 
    # so we assume it doesn't exist.
    
    az ad app federated-credential create --id $clientId --parameters $jsonFile
    
    Write-Host "`nSuccess! Federated credential created." -ForegroundColor Green
    Write-Host "You can now retry the GitHub Action."
} catch {
    Write-Error "Failed to create credential. $_"
} finally {
    if (Test-Path $jsonFile) { Remove-Item $jsonFile }
}
