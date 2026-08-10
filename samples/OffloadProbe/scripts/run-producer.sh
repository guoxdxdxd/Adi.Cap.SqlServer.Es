#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJ="$ROOT/src/CapOffloadProbe.Producer"
if [[ ! -f "$PROJ/appsettings.Local.json" ]]; then
  echo "缺少 $PROJ/appsettings.Local.json"
  echo "请: cp $PROJ/appsettings.Local.example.json $PROJ/appsettings.Local.json 并填写密码"
  exit 1
fi
cd "$PROJ"
echo ">>> 启动 CapOffloadProbe.Producer（每轮: NORMAL + OVERSIZE）"
dotnet run -c Debug
