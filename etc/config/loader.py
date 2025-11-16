import logging
import os
import yaml

def get_logger() -> logging.Logger:
    # Configuração de logging para vermos o que está acontecendo
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    log = logging.getLogger(__name__)
    return log


log = get_logger()


def deep_merge_dicts(base_dict: dict, overlay_dict: dict) -> dict:
    """
    Mescla dois dicionários recursivamente (deep merge).

    O 'overlay_dict' (dicionário de sobreposição) tem prioridade,
    sobrescrevendo chaves no 'base_dict'.
    """
    # Começa com uma cópia do dicionário base para não modificar o original
    merged = base_dict.copy()

    for key, value in overlay_dict.items():
        if (
            key in merged
            and isinstance(merged.get(key), dict)
            and isinstance(value, dict)
        ):
            # Se a chave existe em ambos e ambos os valores são dicionários,
            # mescla recursivamente.
            merged[key] = deep_merge_dicts(merged[key], value)
        else:
            # Caso contrário, o valor do 'overlay' (mais recente) vence.
            merged[key] = value

    return merged


def load_env(file_paths: list[str]) -> dict:
    """
    Carrega uma lista de arquivos YAML em ordem de prioridade e os mescla.

    Arquivos lidos por último na lista têm prioridade maior e
    sobrescreverão chaves de arquivos anteriores.

    Args:
        file_paths: Uma lista de caminhos para os arquivos .yml.

    Returns:
        Um único dicionário com todas as configurações mescladas.
    """
    merged_config: dict = {}

    if not file_paths:
        log.warning("Nenhum arquivo de configuração fornecido.")
        return merged_config

    for path in file_paths:
        if not os.path.exists(path):
            log.warning(f"Arquivo de configuração não encontrado, pulando: {path}")
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                # Usamos safe_load para segurança
                data = yaml.safe_load(f)

            if data and isinstance(data, dict):
                log.info(f"Carregando e mesclando configuração de: {path}")
                # A mágica está aqui:
                # O config atual é a 'base' e o novo 'data' é o 'overlay'
                merged_config = deep_merge_dicts(merged_config, data)
            elif not data:
                log.info(f"Arquivo de configuração está vazio, pulando: {path}")
            else:
                log.warning(
                    f"Arquivo de configuração não é um dicionário, pulando: {path}"
                )

        except yaml.YAMLError as e:
            log.error(f"Erro ao processar o arquivo YAML {path}: {e}")
        except IOError as e:
            log.error(f"Erro ao ler o arquivo {path}: {e}")

    log.info("Mesclagem de configuração YAML concluída.")

    # --- BLOCO CORRIGIDO: SOBRESCREVER COM VARIÁVEIS DE AMBIENTE ---
    #
    # Lógica atualizada para corresponder ao seu 'root.local.yml'
    #

    log.info("Verificando e mesclando com variáveis de ambiente...")

    # --- BROKER (RabbitMQ) ---
    # Alvo: merged_config['broker']['host']
    # Alvo: merged_config['broker']['username']
    # Alvo: merged_config['broker']['password']
    broker_config = merged_config.setdefault("broker", {})
    if "RABBITMQ_HOST" in os.environ:
        log.info("Sobrescrevendo 'broker.host' com var de ambiente 'RABBITMQ_HOST'")
        broker_config["host"] = os.environ["RABBITMQ_HOST"]
    if "RABBITMQ_DEFAULT_USER" in os.environ:
        log.info(
            "Sobrescrevendo 'broker.username' com var de ambiente 'RABBITMQ_DEFAULT_USER'"
        )
        broker_config["username"] = os.environ["RABBITMQ_DEFAULT_USER"]
    if "RABBITMQ_DEFAULT_PASS" in os.environ:
        log.info(
            "Sobrescrevendo 'broker.password' com var de ambiente 'RABBITMQ_DEFAULT_PASS'"
        )
        broker_config["password"] = os.environ["RABBITMQ_DEFAULT_PASS"]

    # --- BUCKET (MinIO) ---
    # Alvo: merged_config['bucket']['endpoint']
    # Alvo: merged_config['bucket']['username']
    # Alvo: merged_config['bucket']['password']
    bucket_config = merged_config.setdefault("bucket", {})
    if "MINIO_HOST" in os.environ:
        log.info("Sobrescrevendo 'bucket.endpoint' com var de ambiente 'MINIO_HOST'")
        # O Docker Compose nos dá o HOST (ex: 'minio').
        # O YAML espera o ENDPOINT (ex: 'minio:9000').
        # Vamos construir o endpoint correto:
        bucket_config["endpoint"] = f"{os.environ['MINIO_HOST']}:9000"
    if "MINIO_ROOT_USER" in os.environ:
        log.info(
            "Sobrescrevendo 'bucket.username' com var de ambiente 'MINIO_ROOT_USER'"
        )
        bucket_config["username"] = os.environ["MINIO_ROOT_USER"]
    if "MINIO_ROOT_PASSWORD" in os.environ:
        log.info(
            "Sobrescrevendo 'bucket.password' com var de ambiente 'MINIO_ROOT_PASSWORD'"
        )
        bucket_config["password"] = os.environ["MINIO_ROOT_PASSWORD"]

    # --- FIM DO BLOCO CORRIGIDO ---

    log.info("Mesclagem de configuração final concluída.")
    return merged_config
