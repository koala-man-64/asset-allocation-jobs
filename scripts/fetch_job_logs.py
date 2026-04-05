import os

from dotenv import load_dotenv

from monitoring.log_analytics import AzureLogAnalyticsClient, render_query


load_dotenv()

workspace_id = os.getenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
job_names = ["silver-earnings-job", "silver-market-job"]

# Query template for Container App Job console logs
query_template = """
ContainerAppConsoleLogs_CL
| where ContainerJobName_s == '{resourceName}'
| project TimeGenerated, Log_s
| order by TimeGenerated desc
| take 50
"""

def fetch_logs():
    if not workspace_id:
        print("Error: SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID not set")
        return

    client = AzureLogAnalyticsClient()
    try:
        for name in job_names:
            print(f"--- Logs for {name} ---")
            query = render_query(query_template, resource_name=name, resource_id=None)
            try:
                payload = client.query(workspace_id=workspace_id, query=query)
                tables = payload.get("tables", [])
                if not tables:
                    print("No tables returned")
                    continue
                
                rows = tables[0].get("rows", [])
                if not rows:
                    print("No logs found for this job.")
                else:
                    for row in rows:
                        print(f"[{row[0]}] {row[1]}")
            except Exception as e:
                print(f"Error querying logs for {name}: {e}")
            print("\n")
    finally:
        client.close()

if __name__ == "__main__":
    fetch_logs()
