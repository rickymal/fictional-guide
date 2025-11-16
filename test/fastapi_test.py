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


class TestFastApi:

    def test_create_and_verify_schemas(self, test_client: TestClient, env: Dict[str, Any]) -> None:
        if not env:
            pytest.fail("Environment variables not loaded")
        
        # response = test_client.delete("schema/rfb.json")
        # assert response.status_code is 201
        import time


        # Cria primeiro schema
        response = test_client.put("schema", json=schema_creator('rfb.json'))
        
        assert response.status_code == HTTP_201_CREATED
        
        # Verifica primeiro schema
        response = test_client.get("schema/namespace/rfb.json")
        schemas = convert_to_dictionary(response)
        assert len(schemas) == 2
        
        # Cria segundo schema
        response = test_client.put("schema", json=schema_creator('rfb.json'))
        assert response.status_code == HTTP_201_CREATED
        
        # Verifica dois schemas
        response = test_client.get("schema/namespace/rfb.json")
        schemas = convert_to_dictionary(response)
        assert len(schemas) == 3
        
        # Cria terceiro schema
        response = test_client.put("schema", json=schema_creator('rfb.json'))
        assert response.status_code == HTTP_201_CREATED
        
        # Verifica três schemas
        response = test_client.get("schema/namespace/rfb.json")
        schemas = convert_to_dictionary(response)
        assert len(schemas) == 4
        assert response.status_code == HTTP_200_OK

    def test_schema_namespace_operations(self, test_client: TestClient) -> None:
        # Primeiro cria os schemas necessários para este teste
        response = test_client.delete("schema/rfb.csv")
        assert response.status_code is 201

        for _ in range(3):
            response = test_client.put("schema", json=schema_creator('rfb.csv'))
            assert response.status_code == HTTP_201_CREATED
        
        # Agora testa o namespace
        response = test_client.get("schema/namespace/rfb.csv")
        namespace_schemas = convert_to_dictionary(response)
        
        assert len(namespace_schemas) == 3

