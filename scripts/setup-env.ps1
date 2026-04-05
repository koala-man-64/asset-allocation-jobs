param(
    [string]$EnvFilePath = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# Check for psql and install if missing
if (-not (Get-Command "psql" -ErrorAction SilentlyContinue)) {
    Write-Host "psql not found. Installing PostgreSQL client via winget..."
    winget install PostgreSQL.PostgreSQL --accept-source-agreements --accept-package-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Winget installation may have failed or required interaction. Please verify psql is installed."
    }
    else {
        Write-Host "PostgreSQL installed. You may need to restart your terminal."
    }
}

# Determine the repo root (prefer parent of scripts/ when executed from this folder).
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = if ((Split-Path -Leaf $scriptDir) -eq "scripts") { Split-Path -Parent $scriptDir } else { (Get-Location).Path }

if ([string]::IsNullOrWhiteSpace($EnvFilePath)) {
    $EnvFilePath = Join-Path $rootDir ".env"
}

$EnvFileName = Split-Path -Leaf $EnvFilePath
$IsGitHubSyncTarget = $EnvFileName -ieq ".env.web"
$EnvContractPath = Join-Path $rootDir "docs\ops\env-contract.csv"

# Load existing variables (if any) to use as defaults.
$ExistingVars = @{}
if (Test-Path $EnvFilePath) {
    Write-Host "Loading existing values from $EnvFilePath as defaults..." -ForegroundColor Gray
    Get-Content $EnvFilePath | ForEach-Object {
        if ($_ -match "^\s*([^#\s]+)\s*=\s*(.*)$") {
            $ExistingVars[$Matches[1]] = $Matches[2].Trim()
        }
    }
}

Write-Host "`n--- AssetAllocation Environment Setup ---" -ForegroundColor Cyan
Write-Host "This writes plaintext values to .env. Do NOT commit .env to git." -ForegroundColor Yellow
Write-Host "Press [Enter] to accept the suggestion in [brackets].`n"

$DefaultSystemHealthArmJobs = "bronze-market-job,bronze-finance-job,bronze-price-target-job,bronze-earnings-job,silver-market-job,silver-finance-job,silver-price-target-job,silver-earnings-job,gold-market-job,gold-finance-job,gold-price-target-job,gold-earnings-job,gold-regime-job,platinum-rankings-job,backtests-job"
$DefaultAssetAllocationApiBaseUrl = if ($IsGitHubSyncTarget) { "http://asset-allocation-api" } else { "http://localhost:9000" }
$DefaultJobStartupApiContainerApps = if ($IsGitHubSyncTarget) { "asset-allocation-api" } else { "" }
$DefaultViteApiProxyTarget = if ($IsGitHubSyncTarget) { "http://asset-allocation-api" } else { "http://127.0.0.1:9000" }

function ConvertFrom-SecureStringPlain {
    param([Parameter(Mandatory = $true)][System.Security.SecureString]$Secure)
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

function Prompt-Var {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Suggestion = "",
        [string]$Description = "",
        [switch]$Secret
    )

    if ($ExistingVars.ContainsKey($Name)) {
        $Suggestion = $ExistingVars[$Name]
    }

    if ($Description) { Write-Host "# $Description" -ForegroundColor Gray }

    if ($Secret) {
        $hasDefault = -not [string]::IsNullOrWhiteSpace($Suggestion)
        $hint = if ($hasDefault) { "[stored]" } else { "" }
        $secure = Read-Host "$Name $hint" -AsSecureString
        $input = ConvertFrom-SecureStringPlain -Secure $secure
        if ([string]::IsNullOrWhiteSpace($input)) { return $Suggestion }
        return $input
    }

    $input = Read-Host "$Name [$Suggestion]"
    if ([string]::IsNullOrWhiteSpace($input)) { return $Suggestion }
    return $input
}

function Get-SupportedEnvKeys {
    param(
        [Parameter(Mandatory = $true)][string]$ContractPath
    )

    if (-not (Test-Path $ContractPath)) {
        throw "Env contract not found at $ContractPath"
    }

    $keys = New-Object System.Collections.Generic.HashSet[string] ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($row in (Import-Csv -Path $ContractPath)) {
        $name = (($row.name | Out-String).Trim())
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            [void]$keys.Add($name)
        }
    }
    return $keys
}

function Filter-ConfigToSupportedKeys {
    param(
        [Parameter(Mandatory = $true)][string[]]$Config,
        [Parameter(Mandatory = $true)]$SupportedKeys
    )

    $filtered = @()
    foreach ($line in $Config) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#") -or $trimmed -notmatch "^([^=]+)=(.*)$") {
            $filtered += $line
            continue
        }

        $key = $matches[1].Trim()
        if ($SupportedKeys.Contains($key)) {
            $filtered += $line
        }
    }

    return $filtered
}

$Config = @()

# -------------------------------------------------------------------------
# Local Development & Logging
# -------------------------------------------------------------------------
$Config += "# =========================================="
$Config += "# Local Development & Logging"
$Config += "# =========================================="
$Config += "DISABLE_DOTENV=" + (Prompt-Var "DISABLE_DOTENV" "false" "Set true to prevent python-dotenv from loading .env automatically.")
$Config += "LOG_FORMAT=" + (Prompt-Var "LOG_FORMAT" "JSON" "Options: JSON | TEXT")
$Config += "LOG_LEVEL=" + (Prompt-Var "LOG_LEVEL" "INFO" "Options: DEBUG | INFO | WARNING | ERROR")
$Config += "TEST_MODE=" + (Prompt-Var "TEST_MODE" "false" "Set true to disable network calls during some code paths.")
$Config += "ENABLE_ENV_DIAGNOSTICS=" + (Prompt-Var "ENABLE_ENV_DIAGNOSTICS" "false" "Set true to log additional (allowlisted) environment diagnostics.")
$Config += "DEBUG_SYMBOLS=" + (Prompt-Var "DEBUG_SYMBOLS" "" "Optional: comma-separated symbols for debug runs (e.g., AAPL,MSFT).")
$Config += "SYMBOLS_REFRESH_INTERVAL_HOURS=" + (Prompt-Var "SYMBOLS_REFRESH_INTERVAL_HOURS" "24" "Optional: refresh symbol universe from NASDAQ + (Alpha Vantage via API) when older than this many hours (0 disables).")
$Config += "DOMAIN_METADATA_MAX_SCANNED_BLOBS=" + (Prompt-Var "DOMAIN_METADATA_MAX_SCANNED_BLOBS" "200000" "Optional: upper bound for domain metadata scans (monitoring).")
$Config += "ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE=" + (Prompt-Var "ASSET_ALLOCATION_REQUIRE_AZURE_STORAGE" "" "Optional: set true to require Azure storage config at startup.")
$Config += "ALPHA_VANTAGE_BASE_URL=" + (Prompt-Var "ALPHA_VANTAGE_BASE_URL" "https://www.alphavantage.co" "Optional: local override for the upstream Alpha Vantage API base URL.")

# -------------------------------------------------------------------------
# Azure Identity (GitHub deploy + optional local auth)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Azure Identity (GitHub deploy + optional local auth)"
$Config += "# =========================================="
$Config += "AZURE_CLIENT_ID=" + (Prompt-Var "AZURE_CLIENT_ID" "" "GitHub Actions OIDC client/app ID (required for deploy).")
$Config += "AZURE_TENANT_ID=" + (Prompt-Var "AZURE_TENANT_ID" "" "Azure tenant ID (required for deploy).")
$Config += "AZURE_SUBSCRIPTION_ID=" + (Prompt-Var "AZURE_SUBSCRIPTION_ID" "" "Azure subscription ID (required for deploy).")
$Config += "ENTRA_OPERATOR_USER_OBJECT_ID=" + (Prompt-Var "ENTRA_OPERATOR_USER_OBJECT_ID" "" "Provisioning only: Entra user object ID to assign the AssetAllocation.Access app role.")
$Config += "AZURE_CLIENT_SECRET=" + (Prompt-Var "AZURE_CLIENT_SECRET" "" "Optional: Service Principal client secret (not required for GitHub OIDC)." -Secret)

# -------------------------------------------------------------------------
# Azure Storage (required for pipelines)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Azure Storage (required for pipelines)"
$Config += "# =========================================="
$Config += "AZURE_STORAGE_ACCOUNT_NAME=" + (Prompt-Var "AZURE_STORAGE_ACCOUNT_NAME" "" "Storage account name (also used by CI).")
$Config += "AZURE_STORAGE_CONNECTION_STRING=" + (Prompt-Var "AZURE_STORAGE_CONNECTION_STRING" "" "Storage connection string (recommended for local dev; required by CI)." -Secret)
$Config += "AZURE_STORAGE_ACCOUNT_KEY=" + (Prompt-Var "AZURE_STORAGE_ACCOUNT_KEY" "" "Optional: storage account key (alternative to connection string)." -Secret)
$Config += "AZURE_STORAGE_ACCESS_KEY=" + (Prompt-Var "AZURE_STORAGE_ACCESS_KEY" "" "Optional: storage access key (alias of account key)." -Secret)
$Config += "AZURE_STORAGE_SAS_TOKEN=" + (Prompt-Var "AZURE_STORAGE_SAS_TOKEN" "" "Optional: SAS token (alternative auth)." -Secret)

# -------------------------------------------------------------------------
# External Data APIs
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# External Data APIs"
$Config += "# =========================================="
$Config += "ALPHA_VANTAGE_API_KEY=" + (Prompt-Var "ALPHA_VANTAGE_API_KEY" "" "Alpha Vantage API key (required by the API gateway; ETL jobs should call the Asset Allocation API instead)." -Secret)
$Config += "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN=" + (Prompt-Var "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN" "300" "Requests per minute allowed for your tier.")
$Config += "ALPHA_VANTAGE_TIMEOUT_SECONDS=" + (Prompt-Var "ALPHA_VANTAGE_TIMEOUT_SECONDS" "15" "HTTP timeout per request (seconds).")
$Config += "ALPHA_VANTAGE_MAX_WORKERS=" + (Prompt-Var "ALPHA_VANTAGE_MAX_WORKERS" "32" "Max concurrent fetch workers.")
$Config += "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS=" + (Prompt-Var "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS" "7" "Skip re-fetching earnings newer than this many days.")
$Config += "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON=" + (Prompt-Var "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON" "12month" "Upcoming earnings calendar retention horizon: 3month, 6month, or 12month.")
$Config += "ALPHA_VANTAGE_FINANCE_FRESH_DAYS=" + (Prompt-Var "ALPHA_VANTAGE_FINANCE_FRESH_DAYS" "28" "Skip re-fetching fundamentals newer than this many days.")
$Config += "NASDAQ_API_KEY=" + (Prompt-Var "NASDAQ_API_KEY" "" "Nasdaq Data Link API key (required for price targets)." -Secret)

# -------------------------------------------------------------------------
# ETL -> API Gateway (Alpha Vantage via API)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# ETL -> API Gateway (Alpha Vantage via API)"
$Config += "# =========================================="
$Config += "ASSET_ALLOCATION_API_BASE_URL=" + (Prompt-Var "ASSET_ALLOCATION_API_BASE_URL" $DefaultAssetAllocationApiBaseUrl "Base URL for the Asset Allocation API (jobs call /api/providers/alpha-vantage/*).")
$Config += "API_CONTAINER_APP_NAME=" + (Prompt-Var "API_CONTAINER_APP_NAME" "asset-allocation-api" "Azure Container App resource name for API startup wake checks.")
$Config += "ASSET_ALLOCATION_API_SCOPE=" + (Prompt-Var "ASSET_ALLOCATION_API_SCOPE" "" "Managed-identity scope for ETL -> API calls (for example api://<api-app-client-id>/.default). Required for production bronze jobs.")
$Config += "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=" + (Prompt-Var "ASSET_ALLOCATION_API_TIMEOUT_SECONDS" "120" "HTTP timeout for ETL -> API requests (seconds).")
$Config += "JOB_STARTUP_API_CONTAINER_APPS=" + (Prompt-Var "JOB_STARTUP_API_CONTAINER_APPS" $DefaultJobStartupApiContainerApps "Optional: comma-separated container apps to start; defaults to API_CONTAINER_APP_NAME/base-url host.")
$Config += "JOB_STARTUP_API_HEALTH_PATH=" + (Prompt-Var "JOB_STARTUP_API_HEALTH_PATH" "/healthz" "Optional: health endpoint path used by startup preflight.")
$Config += "JOB_STARTUP_API_PROBE_ATTEMPTS=" + (Prompt-Var "JOB_STARTUP_API_PROBE_ATTEMPTS" "6" "Optional: startup health probe attempts.")
$Config += "JOB_STARTUP_API_PROBE_SLEEP_SECONDS=" + (Prompt-Var "JOB_STARTUP_API_PROBE_SLEEP_SECONDS" "10" "Optional: delay between startup health probes.")
$Config += "JOB_STARTUP_API_PROBE_TIMEOUT_SECONDS=" + (Prompt-Var "JOB_STARTUP_API_PROBE_TIMEOUT_SECONDS" "5" "Optional: timeout per startup health probe request.")
$Config += "JOB_STARTUP_API_START_ATTEMPTS=" + (Prompt-Var "JOB_STARTUP_API_START_ATTEMPTS" "3" "Optional: ARM container app start attempts during startup preflight.")
$Config += "JOB_STARTUP_API_START_BASE_SECONDS=" + (Prompt-Var "JOB_STARTUP_API_START_BASE_SECONDS" "1.0" "Optional: exponential backoff base seconds for startup ARM start attempts.")

# -------------------------------------------------------------------------
# Postgres (optional locally; used by API and some tasks)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Postgres"
$Config += "# =========================================="
$Config += "POSTGRES_DSN=" + (Prompt-Var "POSTGRES_DSN" "" "Postgres DSN (postgresql://user:pass@host:5432/db)." -Secret)

# -------------------------------------------------------------------------
# API Service
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# API Service"
$Config += "# =========================================="
$Config += "API_ROOT_PREFIX=" + (Prompt-Var "API_ROOT_PREFIX" "" "Optional: mount API under /{API_ROOT_PREFIX}/api/* (e.g. asset-allocation).")
$Config += "API_PORT=" + (Prompt-Var "API_PORT" "9000" "Local API port (used by core/config.py).")
$Config += "API_CSP=" + (Prompt-Var "API_CSP" "" "Optional: Content-Security-Policy header value.")
$Config += "API_CORS_ALLOW_ORIGINS=" + (Prompt-Var "API_CORS_ALLOW_ORIGINS" "" "Optional: comma-separated or JSON list of allowed origins.")

# OIDC (optional)
$Config += "API_OIDC_ISSUER=" + (Prompt-Var "API_OIDC_ISSUER" "" "Optional: OIDC issuer URL (required together with API_OIDC_AUDIENCE).")
$Config += "API_OIDC_AUDIENCE=" + (Prompt-Var "API_OIDC_AUDIENCE" "" "Optional: comma-separated audiences (required together with API_OIDC_ISSUER).")
$Config += "API_OIDC_JWKS_URL=" + (Prompt-Var "API_OIDC_JWKS_URL" "" "Optional: JWKS URL (if not discoverable).")
$Config += "API_OIDC_REQUIRED_SCOPES=" + (Prompt-Var "API_OIDC_REQUIRED_SCOPES" "" "Optional: comma-separated required scopes. Leave blank for role-based production auth.")
$Config += "API_OIDC_REQUIRED_ROLES=" + (Prompt-Var "API_OIDC_REQUIRED_ROLES" "" "Optional: comma-separated required roles (for example AssetAllocation.Access).")

# UI auth config served by API (optional locally, required for browser OIDC)
$Config += "UI_OIDC_CLIENT_ID=" + (Prompt-Var "UI_OIDC_CLIENT_ID" "" "Optional: UI OIDC client ID.")
$Config += "UI_OIDC_AUTHORITY=" + (Prompt-Var "UI_OIDC_AUTHORITY" "" "Optional: UI OIDC authority (defaults to API_OIDC_ISSUER).")
$Config += "UI_OIDC_SCOPES=" + (Prompt-Var "UI_OIDC_SCOPES" "" "Optional: UI OIDC scopes (for example api://<api-app-client-id>/user_impersonation openid profile offline_access).")
$Config += "UI_OIDC_REDIRECT_URI=" + (Prompt-Var "UI_OIDC_REDIRECT_URI" "" "Required absolute callback URI when browser OIDC is enabled.")
$Config += "UI_DIST_DIR=" + (Prompt-Var "UI_DIST_DIR" "" "Optional: local path to UI dist for serving static UI.")

# -------------------------------------------------------------------------
# System Health Monitoring (FastAPI: GET /api/system/health)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# System Health Monitoring (FastAPI: GET /api/system/health)"
$Config += "# =========================================="
$Config += "SYSTEM_HEALTH_TTL_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_TTL_SECONDS" "10" "Cache TTL for /api/system/health.")
$Config += "SYSTEM_HEALTH_MAX_AGE_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_MAX_AGE_SECONDS" "129600" "Max staleness before reporting stale.")
$Config += "SYSTEM_HEALTH_VERBOSE_IDS=" + (Prompt-Var "SYSTEM_HEALTH_VERBOSE_IDS" "" "Optional: include Azure resource IDs in response.")
$Config += "SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON=" + (Prompt-Var "SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON" "" "Optional: JSON object mapping logical names to extra health probe URLs.")
$Config += "SYSTEM_HEALTH_CONTAINERAPP_PING_TIMEOUT_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_CONTAINERAPP_PING_TIMEOUT_SECONDS" "5" "Optional: timeout in seconds for extra container-app health probes.")

# Optional: Azure control-plane probes (ARM) for Container Apps + Jobs
$Config += "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID=" + (Prompt-Var "SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID" "" "Optional: ARM subscription ID for probes / job start allowlist.")
$Config += "SYSTEM_HEALTH_ARM_RESOURCE_GROUP=" + (Prompt-Var "SYSTEM_HEALTH_ARM_RESOURCE_GROUP" "" "Optional: ARM resource group for probes / job start allowlist.")
$Config += "SYSTEM_HEALTH_ARM_CONTAINERAPPS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_CONTAINERAPPS" "" "Optional: comma-separated Container Apps names.")
$Config += "SYSTEM_HEALTH_ARM_JOBS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_JOBS" $DefaultSystemHealthArmJobs "Optional: comma-separated Job names (also used as job-start allowlist).")
$Config += "BACKTEST_ACA_JOB_NAME=" + (Prompt-Var "BACKTEST_ACA_JOB_NAME" "backtests-job" "Container Apps Job name used by API-triggered backtest runs.")
$Config += "REGIME_ACA_JOB_NAME=" + (Prompt-Var "REGIME_ACA_JOB_NAME" "gold-regime-job" "Container Apps Job name used by API-triggered regime recompute runs.")
$Config += "SYSTEM_HEALTH_ARM_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_ARM_API_VERSION" "" "Optional: ARM API version (required if ARM probes enabled).")
$Config += "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS" "5" "Optional: ARM timeout seconds.")
$Config += "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB=" + (Prompt-Var "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB" "3" "Optional: how many executions to return per job.")

# Optional: Azure Resource Health (runtime availability)
$Config += "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION" "" "Optional: Resource Health API version.")

# Optional: Azure Monitor Metrics (runtime telemetry)
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION" "" "Optional: Metrics API version.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES" "" "Optional: timespan minutes (e.g., 15).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL" "" "Optional: interval (e.g., PT1M).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION" "" "Optional: aggregation (e.g., Average).")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS" "" "Optional: comma-separated metric names.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS" "" "Optional: comma-separated metric names.")
$Config += "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON=" + (Prompt-Var "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON" "" "Optional: JSON thresholds object.")

# Optional: Azure Log Analytics (KQL aggregates + job execution log tails)
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID" "" "Optional: Log Analytics workspace ID.")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS" "5" "Optional: Log Analytics timeout seconds.")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES" "15" "Optional: timespan minutes (e.g., 15).")
$Config += "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON=" + (Prompt-Var "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON" "" "Optional: JSON array of query specs.")
$Config += "REALTIME_LOG_STREAM_POLL_SECONDS=" + (Prompt-Var "REALTIME_LOG_STREAM_POLL_SECONDS" "5" "Optional: poll interval in seconds for realtime job-log streaming.")
$Config += "REALTIME_LOG_STREAM_LOOKBACK_SECONDS=" + (Prompt-Var "REALTIME_LOG_STREAM_LOOKBACK_SECONDS" "30" "Optional: initial lookback window in seconds for realtime job-log streaming.")
$Config += "REALTIME_LOG_STREAM_BATCH_SIZE=" + (Prompt-Var "REALTIME_LOG_STREAM_BATCH_SIZE" "200" "Optional: max events fetched per realtime log-stream poll.")
$Config += "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_LOOKBACK_HOURS=" + (Prompt-Var "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_LOOKBACK_HOURS" "168" "Optional: lookback window in hours for Bronze symbol-count jump detection.")
$Config += "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON=" + (Prompt-Var "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON" "" "Optional: JSON object of Bronze symbol-count jump thresholds.")

# -------------------------------------------------------------------------
# Pipeline Controls
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Pipeline Controls"
$Config += "# =========================================="
$Config += "BACKFILL_START_DATE=" + (Prompt-Var "BACKFILL_START_DATE" "2016-01-01" "Global minimum date retained by reconciliation sweeps (YYYY-MM-DD).")
$Config += "TRIGGER_NEXT_JOB_NAME=" + (Prompt-Var "TRIGGER_NEXT_JOB_NAME" "" "Optional: if set, trigger next job when current finishes.")
$Config += "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS=" + (Prompt-Var "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS" "" "Optional: retries when triggering next job (default 3).")
$Config += "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS=" + (Prompt-Var "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS" "" "Optional: base backoff seconds (default 1.0).")

# -------------------------------------------------------------------------
# UI / CI Variables (GitHub Variables)
# -------------------------------------------------------------------------
$Config += ""
$Config += "# =========================================="
$Config += "# Local UI Build Fallbacks"
$Config += "# =========================================="
$Config += "VITE_PORT=" + (Prompt-Var "VITE_PORT" "5174" "Vite dev server port.")
$Config += "VITE_PROXY_CONFIG_JS=" + (Prompt-Var "VITE_PROXY_CONFIG_JS" "false" "UI dev only: when true, proxy /config.js to the API.")
$Config += "VITE_API_PROXY_TARGET=" + (Prompt-Var "VITE_API_PROXY_TARGET" $DefaultViteApiProxyTarget "UI dev only: Vite proxy target for /api (do not include /api).")
$Config += "VITE_API_BASE_URL=" + (Prompt-Var "VITE_API_BASE_URL" "/api" "UI build-time fallback for the API base URL.")
$Config += "VITE_OIDC_AUTHORITY=" + (Prompt-Var "VITE_OIDC_AUTHORITY" "" "UI build-time fallback OIDC authority.")
$Config += "VITE_OIDC_CLIENT_ID=" + (Prompt-Var "VITE_OIDC_CLIENT_ID" "" "UI build-time fallback OIDC client ID.")
$Config += "VITE_OIDC_SCOPES=" + (Prompt-Var "VITE_OIDC_SCOPES" "" "UI build-time fallback OIDC scopes.")

if ($IsGitHubSyncTarget) {
    $supportedKeys = Get-SupportedEnvKeys -ContractPath $EnvContractPath
    $Config = Filter-ConfigToSupportedKeys -Config $Config -SupportedKeys $supportedKeys
}

if ($DryRun) {
    Write-Host "`n[DRY RUN] Would write the following to ${EnvFilePath}:" -ForegroundColor Yellow
    $Config | ForEach-Object { Write-Host $_ }
    exit 0
}

$Config | Out-File -FilePath $EnvFilePath -Encoding utf8
Write-Host "`n[SUCCESS] Environment saved to $EnvFilePath" -ForegroundColor Green
