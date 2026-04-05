#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <job-name> <template-path>" >&2
  exit 64
fi

job_name="$1"
template_path="$2"

: "${RESOURCE_GROUP:?RESOURCE_GROUP is required}"
: "${ACR_PULL_IDENTITY_RESOURCE_ID:?ACR_PULL_IDENTITY_RESOURCE_ID is required}"
: "${JOB_IMAGE:?JOB_IMAGE is required}"

if [ ! -f "$template_path" ]; then
  echo "::error::Job manifest '$template_path' does not exist."
  exit 1
fi

image_name_and_registry="${JOB_IMAGE%:*}"
image_tag="${JOB_IMAGE##*:}"
image_repository="${image_name_and_registry#*/}"
registry_server="${image_name_and_registry%%/*}"
acr_name="${ACR_NAME:-${registry_server%%.*}}"

if [ -z "$image_repository" ] || [ -z "$image_tag" ] || [ "$image_name_and_registry" = "$JOB_IMAGE" ]; then
  echo "::error::JOB_IMAGE '$JOB_IMAGE' is not a valid tagged ACR image reference."
  exit 1
fi

if ! az acr repository show \
  --name "$acr_name" \
  --image "${image_repository}:${image_tag}" \
  --only-show-errors > /dev/null 2>&1; then
  echo "::error::Job image '$JOB_IMAGE' is not present in ACR '$acr_name'. Build and push the current SHA tag before creating or updating Container Apps jobs."
  exit 1
fi

tmp_dir="${RUNNER_TEMP:-/tmp}"
tmp_file="$(mktemp "${tmp_dir%/}/$(basename "${template_path%.yaml}").XXXXXX.yaml")"
trap 'rm -f "$tmp_file"' EXIT

envsubst < "$template_path" > "$tmp_file"

echo "Rendered YAML (secrets redacted):"
awk '
  {
    line=$0
    match(line, /^ */)
    indent=RLENGTH
    if (match(line, /^[[:space:]]*secrets:[[:space:]]*$/)) { in_secrets=1; print line; next }
    if (in_secrets && indent <= 2 && match(line, /^[[:space:]]*[A-Za-z0-9_-]+:/)) { in_secrets=0 }
    if (in_secrets && match(line, /^[[:space:]]*value:/)) { sub(/value:.*/, "value: ***REDACTED***", line) }
    print line
  }
' "$tmp_file"

if az containerapp job show \
  --name "$job_name" \
  --resource-group "$RESOURCE_GROUP" \
  --only-show-errors > /dev/null 2>&1; then
  echo "Updating job from YAML (image + identity + registry)..."
  az containerapp job update \
    --name "$job_name" \
    --resource-group "$RESOURCE_GROUP" \
    --yaml "$tmp_file" \
    --only-show-errors
else
  echo "Creating job from YAML (image + identity + registry)..."
  az containerapp job create \
    --name "$job_name" \
    --resource-group "$RESOURCE_GROUP" \
    --yaml "$tmp_file" \
    --only-show-errors
fi
