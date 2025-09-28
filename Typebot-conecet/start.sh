#!/usr/bin/env bash
set -euo pipefail

# Vai para a pasta onde está o app_bridge.py
cd bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet

# Aponta o Bridge para a pasta do bot_gesto (import dinâmico)
export BRIDGE_BOT_DIR="$PWD/bot_gesto"

# Sobe o FastAPI com Uvicorn/Gunicorn (Railway injeta ${PORT})
exec gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT} app_bridge:app