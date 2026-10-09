#!/usr/bin/env bash
# Fails when a role could reach the internet: download / package-install
# modules, image pulls by hand, or URLs other than localhost. Runs in CI and
# as a pre-commit hook. Container images are pulled by docker compose from
# the internal Nexus mirror set in the inventory, which is allowed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

patterns=(
  '\b(ansible\.builtin\.)?get_url:'
  '\b(ansible\.builtin\.)?uri:'
  '\b(ansible\.builtin\.)?(pip|dnf|yum|package|apt):'
  '\bcommunity\.general\.(npm|gem|pipx)\b'
  '\b(pip3?|dnf|yum) install\b'
  '\b(podman|docker) pull\b'
  'https?://(?!localhost[:/]|127\.0\.0\.1[:/]|0\.0\.0\.0[:/])'
)

status=0
for pattern in "${patterns[@]}"; do
  if grep -rnP --include='*.yml' --include='*.yaml' --include='*.j2' --include='*.py' "${pattern}" roles/; then
    echo "^^^ forbidden in roles/: ${pattern}" >&2
    status=1
  fi
done

if [[ "${status}" -eq 0 ]]; then
  echo "check-offline: roles/ do not reach the internet"
fi
exit "${status}"
