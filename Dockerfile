FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim
WORKDIR /app
COPY pyproject.toml uv.lock* ./

RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN uv pip compile pyproject.toml -o requirements.txt
RUN uv pip sync requirements.txt --system
COPY . .CMD ["tail", "-f", "/dev/null"]
