import logging
import threading
import time
from typing import Any, Dict, Optional

from etc.config import loader

from application import usecase, validator
from domain import port
from infrastructure import broker, bucket, repository, storage

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def get_dependencies() -> Dict[str, Any]:
    log.info("Carregando dependências...")
    env = loader.load_env(["./etc/config/root.local.yml"])
    return env


class Consumer:

    def __init__(self, env: Optional[Dict[str, Any]] = None):
        log.info("Inicializando Consumer...")
        self.env = env or get_dependencies()

        # Inicialização dos adapters
        self.bucket_adapter = bucket.BucketAdapter.from_minio_client(self.env["bucket"])
        self.storage_connection = storage.StorageConnectionAdapter.from_duckdb_memory(
            self.env["storage"]
        )

        # Inicialização dos repositórios
        self.schema_repository = repository.SchemaRegistry()
        self.move_registry = repository.MoveRegistry()

        # Inicialização do validador
        self.checker = validator.ValidatorFactory()

        # Inicialização do broker
        self.broker_adapter = broker.BrokerAdapter(self.env["broker"])

        log.info("Consumer inicializado com sucesso")

    def on_data_received(self, amqp: broker.AmqpDelivery) -> None:
        log.info(f"Processando mensagem recebida: {amqp.message}")

        try:
            usecase.avaliate_data(
                amqp.body(),
                self.storage_connection,
                self.schema_repository,
                self.bucket_adapter,
                self.checker,
                self.move_registry,
            )
            log.info("Mensagem processada com sucesso")
        except Exception as e:
            log.error(f"Erro ao processar mensagem: {e}")
            # Marca a mensagem como falha para reprocessamento
            amqp.failure()

    def on_max_retry_reached(self, amqp: broker.AmqpDelivery) -> None:

        log.warning(
            f"Mensagem excedeu número máximo de tentativas: {amqp.message}. "
            "Encaminhando para DLQ."
        )
        # Aqui você pode adicionar lógica adicional para tratamento de mensagens
        # que falharam repetidamente, como logging especial, notificações, etc.
        amqp.failure()


def start_consuming(
    env: Optional[Dict[str, Any]] = None, duration: Optional[int] = None
) -> None:
    log.info(f"Iniciando consumo de mensagens (duração: {duration}s)")

    try:
        consumer = Consumer(env)
        consumer.broker_adapter.consume_blocking(
            consumer.on_data_received, consumer.on_max_retry_reached, duration
        )
        log.info("Consumo de mensagens finalizado")
    except KeyboardInterrupt:
        log.info("Consumo interrompido pelo usuário")
    except Exception as e:
        log.error(f"Erro durante o consumo de mensagens: {e}")
        raise


def main() -> None:
    log.info("Iniciando aplicação consumidora")

    try:
        # Você pode passar parâmetros aqui se necessário
        # Exemplo: start_consuming(duration=3600) para 1 hora
        start_consuming()
    except Exception as e:
        log.error(f"Erro fatal na aplicação: {e}")
        raise


if __name__ == "__main__":
    main()
