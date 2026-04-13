$repo = "koala-man-64/asset-allocation-jobs"

# Dry run: show runs whose workflow no longer exists on GitHub
$active = gh workflow list -R $repo --all --json id --jq '.[].id' | ForEach-Object { [int64]$_ }
$pages = gh api --paginate --slurp "repos/$repo/actions/runs?per_page=100" | ConvertFrom-Json
$runs = @($pages | ForEach-Object { $_.workflow_runs })

$stale = $runs | Where-Object {
  $_.workflow_id -notin $active -and
  ($_.status -eq "completed" -or [datetime]$_.created_at -lt (Get-Date).AddDays(-14))
}

$stale | Select-Object id, workflow_id, path, name, status, conclusion


$repo = "koala-man-64/asset-allocation-jobs"

# Delete the stale runs found above
$active = gh workflow list -R $repo --all --json id --jq '.[].id' | ForEach-Object { [int64]$_ }
$pages = gh api --paginate --slurp "repos/$repo/actions/runs?per_page=100" | ConvertFrom-Json
$runs = @($pages | ForEach-Object { $_.workflow_runs })

$stale = $runs | Where-Object {
  $_.workflow_id -notin $active -and
  ($_.status -eq "completed" -or [datetime]$_.created_at -lt (Get-Date).AddDays(-14))
}

$stale | ForEach-Object {
  gh api -X DELETE "repos/$repo/actions/runs/$($_.id)"
}
