#!/usr/bin/env bash
# Self-test for lint-checks.sh: each case writes a fixture tree into a temp
# directory, runs the checks against it, and asserts the exit code and the
# reported rule. Strings the real scan would match are built by concatenation,
# so this file stays clean under that scan.
set -uo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
script="$script_dir/lint-checks.sh"
tmp_dirs=''
cleanup() {
	while IFS= read -r dir; do
		[ -n "$dir" ] && rm -rf "$dir"
	done <<EOF_DIRS
$tmp_dirs
EOF_DIRS
}
trap cleanup EXIT

id="2610""22.0011"
legacy="plans/0""10-foo.md"
pl="PLA""N-1"
tk="task ""#1"
divider="// ""----"" helpers ""----"
failures=0

run_case() {
	name=$1
	expected=$2
	header=$3
	file=$4
	content=$5
	tmp=$(mktemp -d) || { printf 'FAIL %s: mktemp failed\n' "$name"; failures=$((failures + 1)); return; }
	tmp_dirs="${tmp_dirs}${tmp_dirs:+
}$tmp"
	mkdir -p "$(dirname "$tmp/$file")"
	printf '%s\n' "$content" > "$tmp/$file"
	bash "$script" "$tmp" 2>"$tmp/stderr"
	actual=$?
	if [ "$actual" -ne "$expected" ] || { [ "$expected" -ne 0 ] && ! grep -qF "$header" "$tmp/stderr"; }; then
		printf 'FAIL %s: expected exit %s with header %s, got %s\n' "$name" "$expected" "$header" "$actual"
		failures=$((failures + 1))
	fi
}

run_case 'clean Go file' 0 '' clean.go 'package fixture'
run_case 'section divider' 1 'section-divider comments' divider.go "$divider"
run_case 'plan id in Go comment' 1 'plan or task references' reference.go "// see .agents/plans/$id.x.md"
run_case 'plan id in Makefile comment' 1 'plan or task references' Makefile "# see $id"
run_case 'plan id in YAML' 1 'plan or task references' config.yml "# see $id"
run_case 'legacy plan reference' 1 'plan or task references' notes.md "see $legacy"
run_case 'PLAN reference' 1 'plan or task references' reference.go "// $pl"
run_case 'task reference' 1 'plan or task references' reference.go "// $tk"
run_case 'backlog exemption' 0 '' .agents/plans/backlog/x.md "see $id"
run_case 'agent memory exemption' 0 '' .claude/agent-memory/m.md "see $id"
run_case 'version and date' 0 '' notes.md 'v1.0.10 released 2026-10-22'
run_case 'mock without assertion' 1 'no compile-time interface assertion' mock_test.go 'type mockSink struct{}'
run_case 'mock with assertion' 0 '' mock_test.go $'type mockSink struct{}\nvar _ io.Writer = (*mockSink)(nil)'

if [ "$failures" -ne 0 ]; then
	exit 1
fi
printf 'All 13 lint-check cases passed.\n'
