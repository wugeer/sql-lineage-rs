#!/usr/bin/env bash
# 生成测试覆盖率报告：-Cinstrument-coverage + llvm-profdata + llvm-cov
# 需要: rustup component add llvm-tools-preview
set -euo pipefail
cd "$(dirname "$0")/.."

SYSROOT="$(rustc --print sysroot)"
HOST="$(rustc -vV | awk '/^host:/{print $2}')"
LLVM_BIN="$SYSROOT/lib/rustlib/$HOST/bin"
if [[ ! -x "$LLVM_BIN/llvm-cov" || ! -x "$LLVM_BIN/llvm-profdata" ]]; then
    echo "llvm-tools 未安装，请先执行: rustup component add llvm-tools-preview" >&2
    exit 1
fi

COV_DIR="${COV_DIR:-/tmp/hive_lineage_cov}"
rm -rf "$COV_DIR"
mkdir -p "$COV_DIR"

# 清理旧的插桩测试二进制，避免 --object 指向过期文件。
# 测试目标二进制名为 "<crate/test 名>-<hash>"（无扩展名），按"可执行且无点号"匹配，
# 对任意新增测试文件名均适用。
find target/debug/deps -maxdepth 1 -type f -executable ! -name "*.*" -delete 2>/dev/null || true

echo "==> 编译插桩并运行测试"
RUSTFLAGS="-Cinstrument-coverage" \
LLVM_PROFILE_FILE="$COV_DIR/cov-%p-%m.profraw" \
    cargo test "$@"

echo "==> 合并 profile"
"$LLVM_BIN/llvm-profdata" merge -sparse "$COV_DIR"/cov-*.profraw -o "$COV_DIR/coverage.profdata"

objs=()
while IFS= read -r b; do
    objs+=(--object "$b")
done < <(find target/debug/deps -maxdepth 1 -type f -executable ! -name "*.*" | sort)

echo "==> 覆盖率报告 (src/)"
"$LLVM_BIN/llvm-cov" report \
    --instr-profile="$COV_DIR/coverage.profdata" \
    "${objs[@]}" \
    --sources src/ --summary-only

echo
echo "如需行级明细: llvm-cov show --instr-profile=$COV_DIR/coverage.profdata ${objs[*]} --sources src/analyzer.rs"
echo "如需 HTML: llvm-cov export --format=lcov --instr-profile=$COV_DIR/coverage.profdata ${objs[*]} --sources src/ > coverage.lcov"
