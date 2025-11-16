# 1. Comece com a imagem base oficial do 'uv'
FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim

# 2. Defina o diretório de trabalho dentro do container
WORKDIR /app

# 3. Copie os arquivos do seu projeto (ex: pyproject.toml) para o container
# Vamos copiar apenas o necessário para instalar as dependências primeiro
COPY pyproject.toml uv.lock* ./

# 4. Instale as ferramentas de build (usando apt-get para Debian)
RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN uv pip compile pyproject.toml -o requirements.txt


# 5. Instale as dependências (isso fica em cache se os arquivos não mudarem)
# RUN uv pip sync pyproject.toml --system
RUN uv pip sync requirements.txt --system
# 6. Copie o resto do código-fonte do seu projeto
COPY . .

# 7. (Opcional) Comando padrão para manter o container rodando
# Isso é útil se você quiser usar 'docker compose exec'
CMD ["tail", "-f", "/dev/null"]

