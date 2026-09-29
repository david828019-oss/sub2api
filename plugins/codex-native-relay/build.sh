#!/usr/bin/env bash
# 构建各平台运行时并打包为 .s2plugin。
#   ./build.sh                                   # 未签名包（仅本地调试，需 plugins.allow_unsigned: true）
#   ./build.sh -signing-key keys/x.private -key-id x-v1 -output dist/codex-native-relay.s2plugin
set -euo pipefail
cd "$(dirname "$0")"

VERSION="$(sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' manifest.source.json | head -n1)"
OUTPUT="dist/codex-native-relay-${VERSION}.s2plugin"
SIGN_ARGS=()
PLATFORMS="${PLATFORMS:-linux-amd64 linux-arm64 windows-amd64 darwin-amd64 darwin-arm64}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -signing-key|-key-id) SIGN_ARGS+=("$1" "$2"); shift 2 ;;
    -output) OUTPUT="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

rm -rf build/runtimes
RUNTIME_ARGS=()
for platform in $PLATFORMS; do
  goos="${platform%-*}"
  goarch="${platform#*-}"
  binary="build/runtimes/${platform}/plugin"
  [[ "$goos" == "windows" ]] && binary="${binary}.exe"
  echo "building ${platform}"
  CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath \
    -ldflags "-s -w -X github.com/Wei-Shaw/sub2api/plugins/codex-native-relay/internal/plugin.Version=${VERSION}" \
    -o "$binary" ./cmd/codex-native-relay
  RUNTIME_ARGS+=(-runtime "${platform}=${binary}")
done

go run ./tools/packager -source manifest.source.json -ui ui -version "$VERSION" \
  -output "$OUTPUT" "${RUNTIME_ARGS[@]}" "${SIGN_ARGS[@]}"
echo "packaged ${OUTPUT}"
