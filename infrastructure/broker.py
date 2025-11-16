import json
from typing import Callable
import pika
from domain import port
import time

import logging
# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


from etc.config import loader
env_g = loader.load_env(['./etc/config/root.local.yml'])

class BrokerAdapter(port.IBrokerAdapter):
    def __init__(self, env: dict):
        # Configuração técnica pura (Infra)
        credentials = pika.PlainCredentials(env["username"], env["password"])
        parameters = pika.ConnectionParameters(
            host=env["host"],
            # port=env['port'],
            credentials=credentials,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.setup_infrastructure(env)

    def setup_infrastructure(self, env: dict):

        # Exchange principal
        self.exchange_name = env["exchange"]
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic", durable=True
        )

        # Exchange para DLQ
        self.dlq_exchange_name = f"{self.exchange_name}.dlx"
        self.channel.exchange_declare(
            exchange=self.dlq_exchange_name, exchange_type="topic", durable=True
        )

        # Filas
        self.main_queue = env["queue_name"]
        self.retry_queue = env["queue_retry"]
        self.dlq_queue = env["queue_dlq"]

        # Arguments para fila principal com DLQ
        queue_args = {
            "x-dead-letter-exchange": self.dlq_exchange_name,
            "x-dead-letter-routing-key": self.dlq_queue,
        }

        # Declarar filas
        self.channel.queue_declare(
            queue=self.main_queue, durable=True, arguments=queue_args
        )

        self.channel.queue_declare(
            queue=self.retry_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": self.exchange_name,
                "x-dead-letter-routing-key": self.main_queue,
                "x-message-ttl": env["queue_ttl_milliseconds"],
            },
        )

        self.channel.queue_declare(queue=self.dlq_queue, durable=True)

        # Bindings
        self.channel.queue_bind(
            queue=self.main_queue, exchange=self.exchange_name, routing_key="app.*"
        )

        self.channel.queue_bind(
            queue=self.retry_queue,
            exchange=self.dlq_exchange_name,
            routing_key=self.retry_queue,
        )

        self.channel.queue_bind(
            queue=self.dlq_queue,
            exchange=self.dlq_exchange_name,
            routing_key=self.dlq_queue,
        )

    def publish_message(self, routing_key: str, message: str, count: int = 0) -> None:
        if not isinstance(message, str):
            raise ValueError("incorrect type ", type(message))

        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type="application/json",
                    headers={"count": count},  # Adiciona header count
                ),
            )
        except Exception as e:
            log.info(f"Erro ao publicar no RabbitMQ: {e}")
            raise e

    def consume_sync(self, qtd: int) -> list:
        messages = []

        for _ in range(qtd):
            method_frame, header_frame, body = self.channel.basic_get(
                queue=self.main_queue, auto_ack=False
            )

            if method_frame:
                message = json.loads(body)
                count = (
                    header_frame.headers.get("count", 0) if header_frame.headers else 0
                )

                messages.append(
                    {
                        "message": message,
                        "count": count,
                        "delivery_tag": method_frame.delivery_tag,
                    }
                )
            else:
                break  # Não há mais mensagens

        return messages

    def consume_blocking(
        self,
        callback_default: Callable,
        callback_dlq: Callable,
        duration: int | None = None,
    ):
        def message_handler(ch, method, properties, body):
            try:
                message = json.loads(body)
                count = properties.headers.get("count", 0) if properties.headers else 0

                # Criar objeto de mensagem com métodos de acknowledge, Visitor Pattern
                message_wrapper = AmqpDelivery(
                    message=message,
                    count=count,
                    delivery_tag=method.delivery_tag,
                    channel=ch,
                    broker_adapter=self,
                )

                # Verificar se deve ir para DLQ
                if count >= 5:
                    callback_dlq(message_wrapper)
                else:
                    callback_default(message_wrapper)

            except Exception as e:
                log.info(f"Erro no processamento da mensagem: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(
            queue=self.main_queue, on_message_callback=message_handler, auto_ack=False
        )

        log.info("Iniciando consumo assíncrono...")
        self.channel.start_consuming()

    def acknowledge_message(self, delivery_tag: int):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def reject_message(
        self, delivery_tag: int, count: int, message: str, routing_key: str
    ):
        """Rejeita mensagem e envia para retry com count incrementado"""
        # Incrementa count e publica na fila de retry
        self.publish_message(
            routing_key=self.retry_queue, message=message, count=count + 1
        )
        # Confirma a mensagem original para removê-la da fila principal
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


class AmqpDelivery:
    def __init__(
        self,
        message: dict,
        count: int,
        delivery_tag: int,
        channel,
        broker_adapter: BrokerAdapter,
    ):
        self.message = message
        self.count = count
        self.delivery_tag = delivery_tag
        self.channel = channel
        self.broker_adapter = broker_adapter

    def body(self) -> bytes:
        return self.message

    def success(self):
        self.broker_adapter.acknowledge_message(self.delivery_tag)

    def failure(self):
        self.broker_adapter.reject_message(
            delivery_tag=self.delivery_tag,
            count=self.count,
            message=self.message,
            routing_key=env_g['app']['retry_router'],
        )
