# =============================
# Dockerfile — Bridge + BotGestor + GeoIP + supervisord
# Base: Debian 12 (bookworm) com Python 3.11
# =============================
FROM debian:bookworm-slim

# ------------ Metadados (opcional)
LABEL org.opencontainers.image.title="Typebot Bridge + BotGestor" \
      org.opencontainers.image.source="railway" \
      org.opencontainers.image.description="Bridge FastAPI + Bot Gestor com supervisord e GeoIP"

# ------------ Env globais
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    PATH="/usr/local/bin:$PATH" \
    PYTHONPATH="/app" \
    GEOIP_PATH="/app/GeoLite2-City.mmdb"

# Caminhos LINUX das suas pastas (case-sensitive!)
# Bridge: contém app_bridge.py
ENV BRIDGE_DIR="/app/bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet"
# Bot: contém db.py, fb_google.py, bot.py
ENV BOT_DIR="${BRIDGE_DIR}/bot_gesto"
# Exporta para o app (o seu bridge já lê BRIDGE_BOT_DIR)
ENV BRIDGE_BOT_DIR="${BOT_DIR}"
# App espera esta env (mapeamos para GEOIP_PATH)
ENV GEOIP_DB_PATH="${GEOIP_PATH}"

WORKDIR /app

# ------------ 1) Sistema + Python + build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-venv python3-dev python3-pip \
    gcc g++ make libpq-dev \
    curl unzip ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ------------ 2) Dependências Python
# Copiamos primeiro os requirements para cache
# Se você não tiver os dois arquivos abaixo, una tudo em um único requirements.txt na raiz.
COPY requirements-bridge.txt /app/requirements-bridge.txt
# (Opcional) requirements do bot dentro da árvore informada
# -> ajuste o caminho abaixo se seu requirements do bot estiver em outro lugar,
#    ou comente estas duas linhas se você centralizou tudo no requirements-bridge.txt
COPY bot_gestor/bot_gestora/bot_gestao/bot_gestor/typebot_conection/Typebot-conecet/typebot-conect/Typebot-conecet/bot_gesto/requirements.txt /app/requirements-bot.txt

RUN pip3 install --break-system-packages --no-cache-dir -r /app/requirements-bridge.txt \
 && if [ -f /app/requirements-bot.txt ]; then pip3 install --break-system-packages --no-cache-dir -r /app/requirements-bot.txt; fi

# ------------ 3) supervisord
RUN pip3 install --break-system-packages --no-cache-dir supervisor

# ------------ 4) Código
COPY . /app

# Validações de caminho ainda no build (falha cedo se algo estiver diferente)
RUN test -f "${BRIDGE_DIR}/app_bridge.py" || (echo "❌ app_bridge.py não encontrado em ${BRIDGE_DIR}" && exit 1) \
 && test -d "${BOT_DIR}" || (echo "❌ Pasta do bot não encontrada em ${BOT_DIR}" && exit 1)

# ------------ 5) GeoIP (MaxMind community mirror)
RUN curl -L -o "${GEOIP_PATH}" \
    https://github.com/P3TERX/GeoLite.mmdb/releases/latest/download/GeoLite2-City.mmdb \
 || echo "⚠️  GeoIP não baixado; continuando sem GeoIP"

# ------------ 6) Healthcheck (consulta o /health do Bridge)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
 CMD curl -fsS "http://127.0.0.1:${PORT}/health" || exit 1

# ------------ 7) Expor portas
EXPOSE 8080

# ------------ 8) supervisord.conf inline
# Criamos o arquivo no build para não depender do repositório
RUN printf "%s\n" \
"[supervisord]" \
"nodaemon=true" \
"logfile=/dev/null" \
"pidfile=/tmp/supervisord.pid" \
"" \
"[program:bridge]" \
"directory=${BRIDGE_DIR}" \
"command=gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:%(ENV_PORT)s app_bridge:app" \
"autostart=true" \
"autorestart=true" \
"startretries=3" \
"stdout_logfile=/dev/stdout" \
"stdout_logfile_maxbytes=0" \
"stderr_logfile=/dev/stderr" \
"stderr_logfile_maxbytes=0" \
"environment=PYTHONUNBUFFERED=\"1\",PYTHONDONTWRITEBYTECODE=\"1\",BRIDGE_BOT_DIR=\"%(ENV_BRIDGE_BOT_DIR)s\",GEOIP_DB_PATH=\"%(ENV_GEOIP_DB_PATH)s\"" \
"" \
"[program:bot]" \
"directory=${BOT_DIR}" \
"command=python3 -u bot.py" \
"autostart=true" \
"autorestart=true" \
"startretries=3" \
"stdout_logfile=/dev/stdout" \
"stdout_logfile_maxbytes=0" \
"stderr_logfile=/dev/stderr" \
"stderr_logfile_maxbytes=0" \
> /app/supervisord.conf

# ------------ 9) Entrada
CMD ["supervisord", "-c", "/app/supervisord.conf"]