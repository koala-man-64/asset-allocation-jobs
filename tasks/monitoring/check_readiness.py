import logging
from asset_allocation_runtime_common.foundation import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.logging_config import configure_logging

# Configure logging (force JSON for machine readability during checks if needed, but respect env)
configure_logging()
logger = logging.getLogger(__name__)

def check_env_vars():
    """Validates that all required configuration variables are loaded."""
    logger.info("Checking configuration...")
    required_containers = [
        ("AZURE_FOLDER_MARKET", cfg.AZURE_FOLDER_MARKET),
        ("AZURE_FOLDER_FINANCE", cfg.AZURE_FOLDER_FINANCE),
        ("AZURE_FOLDER_EARNINGS", cfg.AZURE_FOLDER_EARNINGS),
        ("AZURE_FOLDER_TARGETS", cfg.AZURE_FOLDER_TARGETS),
        ("AZURE_CONTAINER_COMMON", cfg.AZURE_CONTAINER_COMMON),
        ("AZURE_CONTAINER_PLATINUM", cfg.AZURE_CONTAINER_PLATINUM),
    ]
    
    missing = []
    for name, value in required_containers:
        if not value:
            missing.append(name)
        else:
            logger.debug(f"Config OK: {name}={value}")
            
    if missing:
        logger.error(f"Missing required configuration: {', '.join(missing)}")
        return False
    
    logger.info("Configuration check passed.")
    return True

def check_storage_connectivity():
    """Validates connectivity to Azure Blob Storage."""
    logger.info("Checking Azure Storage connectivity...")
    
    # Try to list blobs in the COMMON container as a connectivity test
    client = mdc.get_storage_client(cfg.AZURE_CONTAINER_COMMON)
    if not client:
        logger.error("Failed to initialize Storage Client for COMMON container.")
        return False
        
    try:
        # Perform a lightweight operation
        exists = client.container_client.exists()
        if not exists:
            logger.error(f"Container '{cfg.AZURE_CONTAINER_COMMON}' does not exist.")
            return False
            
        logger.info(f"Successfully connected to container: {cfg.AZURE_CONTAINER_COMMON}")
        return True
    except Exception as e:
        logger.error(f"Storage connectivity check failed: {e}")
        return False

def main():
    logger.info("Starting Operational Readiness Check...")
    
    checks = [
        ("Configuration", check_env_vars),
        ("Storage Connectivity", check_storage_connectivity),
    ]
    
    failed = []
    
    for name, task in checks:
        try:
            success = task()
            if not success:
                failed.append(name)
        except Exception as e:
            logger.error(f"Check '{name}' crashed: {e}")
            failed.append(name)
            
    if failed:
        logger.critical(f"Readiness Check FAILED. Failing checks: {', '.join(failed)}")
        return 1
    
    logger.info("All readiness checks PASSED. System is go.")
    return 0

if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job

    raise SystemExit(
        run_logged_job(
            job_name="monitoring-readiness-check",
            run=main,
            log_info=logger.info,
            log_warning=logger.warning,
            log_error=logger.error,
            log_exception=logger.exception,
        )
    )
