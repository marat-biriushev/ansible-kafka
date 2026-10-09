#!/usr/bin/env bash
# Run on the control node after unpacking the bundle into artifacts/.
# Checks every file pinned in artifacts.yml against its checksum, then the
# whole bundle against artifacts/SHA256SUMS, and that the collections from
# requirements.yml are present. Exit code 0 = ready to install.
# Needs: bash, sha256sum/sha512sum, python3 with PyYAML (comes with Ansible).
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out_dir="${repo_dir}/artifacts"
failed=0

entries="$(python3 - "${repo_dir}/artifacts.yml" "${repo_dir}/requirements.yml" <<'PY'
import sys, yaml
data = yaml.safe_load(open(sys.argv[1]))
for a in data["artifacts"]:
    algo = "sha512" if "sha512" in a else "sha256"
    print("\t".join(["file", a["file"], algo, a[algo].lower()]))
for a in data.get("connect_plugins") or []:
    print("\t".join(["file", "connect-plugins/" + a["file"], "sha256", a["sha256"].lower()]))
for c in yaml.safe_load(open(sys.argv[2]))["collections"]:
    ns, name = c["name"].split(".")
    print("\t".join(["collection", f"collections/{ns}-{name}-{c['version']}.tar.gz", "", ""]))
PY
)"

while IFS=$'\t' read -r kind file algo sum; do
  path="${out_dir}/${file}"
  if [[ ! -f "${path}" ]]; then
    echo "MISSING   ${file}"; failed=1; continue
  fi
  if [[ "${kind}" == file ]] && [[ "$("${algo}sum" "${path}" | cut -d' ' -f1)" != "${sum}" ]]; then
    echo "BAD SUM   ${file} (${algo} does not match artifacts.yml)"; failed=1; continue
  fi
  echo "ok        ${file}"
done <<< "${entries}"

if [[ -f "${out_dir}/SHA256SUMS" ]]; then
  if (cd "${out_dir}" && sha256sum --quiet -c SHA256SUMS); then
    echo "ok        SHA256SUMS ($(wc -l < "${out_dir}/SHA256SUMS") files)"
  else
    echo "BAD SUM   some files do not match artifacts/SHA256SUMS"; failed=1
  fi
else
  echo "MISSING   SHA256SUMS (was the bundle made by fetch-artifacts.sh?)"; failed=1
fi

if [[ "${failed}" -ne 0 ]]; then
  echo; echo "Artifacts are NOT complete, see README \"Offline artifacts\"." >&2
  exit 1
fi
echo; echo "All artifacts present. Next: ansible-galaxy collection install ${out_dir}/collections/*.tar.gz -p ./collections"
