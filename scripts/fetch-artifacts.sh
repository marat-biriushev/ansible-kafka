#!/usr/bin/env bash
# Run on a machine WITH internet access. Downloads everything listed in
# artifacts.yml into artifacts/, checks the pinned checksums, downloads the
# Ansible collections from requirements.yml, and packs it all into
# ansible-kafka-artifacts-<date>.tar.gz for transfer to the control node.
#
#   scripts/fetch-artifacts.sh                 # artifacts + collections
#   scripts/fetch-artifacts.sh --with-java     # + java-21-openjdk-headless RPMs (needs dnf, RHEL/Alma 10)
#
# Idempotent: a file already present with the right checksum is not fetched again.
# Needs: bash, curl, sha256sum/sha512sum, python3 with PyYAML, ansible-galaxy.
# Container images are not part of the bundle: the services VM pulls them
# from the Nexus docker-proxy.
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
manifest="${repo_dir}/artifacts.yml"
out_dir="${repo_dir}/artifacts"
with_java=false

for arg in "$@"; do
  case "${arg}" in
    --with-java) with_java=true ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown argument: ${arg}" >&2; exit 2 ;;
  esac
done

for tool in curl sha256sum sha512sum python3 ansible-galaxy tar; do
  command -v "${tool}" >/dev/null || { echo "missing tool: ${tool}" >&2; exit 1; }
done

mkdir -p "${out_dir}"

# name<TAB>file<TAB>url<TAB>algo<TAB>checksum, one line per artifact
entries="$(python3 - "${manifest}" <<'PY'
import sys, yaml
data = yaml.safe_load(open(sys.argv[1]))
for a in data["artifacts"]:
    algo = "sha512" if "sha512" in a else "sha256"
    print("\t".join([a["name"], a["file"], a["url"], algo, a[algo].lower()]))
for a in data.get("connect_plugins") or []:
    print("\t".join([a["name"], "connect-plugins/" + a["file"], a["url"], "sha256", a["sha256"].lower()]))
PY
)"

verify() { # file algo checksum
  [[ -f "$1" ]] && [[ "$("${2}sum" "$1" | cut -d' ' -f1)" == "$3" ]]
}

while IFS=$'\t' read -r name file url algo sum; do
  target="${out_dir}/${file}"
  if verify "${target}" "${algo}" "${sum}"; then
    echo "ok        ${file}"
    continue
  fi
  echo "fetching  ${file}"
  mkdir -p "$(dirname "${target}")"
  curl -fL --retry 3 -o "${target}.part" "${url}"
  if ! verify "${target}.part" "${algo}" "${sum}"; then
    echo "CHECKSUM MISMATCH for ${name} (${file}), expected ${algo} ${sum}" >&2
    rm -f "${target}.part"
    exit 1
  fi
  mv "${target}.part" "${target}"
done <<< "${entries}"

echo "fetching  collections from requirements.yml"
ansible-galaxy collection download -r "${repo_dir}/requirements.yml" -p "${out_dir}/collections"

if [[ "${with_java}" == true ]]; then
  command -v dnf >/dev/null || { echo "--with-java needs dnf (RHEL 10 / AlmaLinux 10)" >&2; exit 1; }
  mkdir -p "${out_dir}/rpms"
  echo "fetching  java-21-openjdk-headless with dependencies"
  dnf download --resolve --alldeps --destdir "${out_dir}/rpms" java-21-openjdk-headless
fi

# Checksums of everything in the bundle, for verify-artifacts.sh
sums="$(cd "${out_dir}" && find . -type f ! -name SHA256SUMS ! -name README.md ! -name '*.part' -print0 \
  | sort -z | xargs -0 sha256sum)"
printf '%s\n' "${sums}" > "${out_dir}/SHA256SUMS"

bundle="${repo_dir}/ansible-kafka-artifacts-$(date +%Y%m%d).tar.gz"
tar -C "${out_dir}" -czf "${bundle}" --exclude=README.md --exclude='*.part' .

echo
echo "Files:"
(cd "${out_dir}" && cat SHA256SUMS)
echo
echo "Bundle: ${bundle} ($(du -h "${bundle}" | cut -f1))"
echo "Copy it to the control node and run: tar -xzf $(basename "${bundle}") -C artifacts/ && scripts/verify-artifacts.sh"
