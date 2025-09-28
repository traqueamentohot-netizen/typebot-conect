#!/usr/bin/env bash
set -euo pipefail

# Vai para a pasta do Bridge
cd bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet

# Aponta para a pasta do bot
export BRIDGE_BOT_DIR="$PWD/bot_gesto"

# Logs de diagnóstico úteis
echo "PWD: $(pwd)"
echo "BRIDGE_BOT_DIR: ${BRIDGE_BOT_DIR}"
ls -la .
ls -la bot_gesto || true

# Sobe o Bridge (usa ${PORT} do Railway)
exec gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT} app_bridge:app