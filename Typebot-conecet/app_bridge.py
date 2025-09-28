# ---------- Imagem base ----------
FROM python:3.11-slim

# ---------- Dependências do sistema ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# ---------- Diretório de trabalho ----------
WORKDIR /app

# ---------- Dependências Python ----------
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Copiar todo o projeto ----------
COPY . /app

# ---------- Indicar ao Bridge onde está o bot_gesto ----------
# (caminho Linux equivalente ao do Windows que você passou)
ENV BRIDGE_BOT_DIR=/app/bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet/bot_gesto

# ---------- Definir onde está o app_bridge.py ----------
WORKDIR /app/bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet

# ---------- Comando para rodar o Bridge (FastAPI) ----------
# O Railway injeta $PORT automaticamente
CMD gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:${PORT} app_bridge:app