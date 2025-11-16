import json
import time
from typing import Any, Dict, List
import pytest
import pydantic as pyd
from fastapi.testclient import TestClient
from fastapi import Response as FastapiResponse
from starlette.status import HTTP_201_CREATED, HTTP_200_OK

# Schema constante para reutilização
def schema_creator(namespace: str):
    return {
    "type": "record",
    "namespace": namespace,
    "name": "RegistroUsuario",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "salary", "type": "double"},
        {"name": "data_criacao", "type": "string"},
        {"name": "data_nascimento", "type": "string"},
        {"name": "hora_registro", "type": "string"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "codigo", "type": ["null", "int"], "default": None},
    ],
}

class Data(pyd.BaseModel):
    name: str
    age: int
    salary: float
    data_criacao: str
    data_nascimento: str
    hora_registro: str
    tags: list[str]
    codigo: int | None = None

def convert_to_dictionary(response: FastapiResponse) -> List[Dict[str, Any]]:
    return json.loads(response.text)

from infrastructure import broker

class TestSchemaValidation:

    def test_produce(self, test_client: TestClient, rm: broker.BrokerAdapter):
        test_client.post("/job/validate/namespace/rfb.json")
        test_client.post("/job/validate/namespace/rfb.csv")
        import time
        time.sleep(3)
        msg = rm.consume_sync(1)
        self.check_message(msg)
        assert 'namespace' in msg[0]['message']


    def check_message(self, consumed_message):
        from typing import Iterable
        assert isinstance(consumed_message, Iterable)
        assert isinstance(consumed_message, Iterable)
        assert len(consumed_message) == 1
        
        # Verificações
        assert consumed_message is not None
        assert 'message' in consumed_message[0]
        
    

    def test_message_production_and_consumption(self, rm: broker.BrokerAdapter) -> None:
        """Testa produção e consumo de mensagens de forma isolada"""
        
        # Configura infraestrutura se necessário
        # rm.setup_infrastructure()  # Descomente se necessário
        
        rm.consume_sync(10) # consumir qualquer mensagem.

        # Produz mensagem de teste
        test_message = {'namespace': 'rfb.json', 'test_id': '123'}
        test_message_str = json.dumps(test_message, ensure_ascii=False)
        rm.publish_message(routing_key="app.mauler", message=test_message_str, count=0)
        
        # Consome a mensagem
        consumed_message = rm.consume_sync(1)
        from typing import Iterable
        self.check_message(consumed_message=consumed_message)
        assert 'namespace' in consumed_message[0]['message']
        assert consumed_message[0]['message']['namespace'] == 'rfb.json'

