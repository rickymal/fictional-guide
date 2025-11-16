import json
import pytest
from infrastructure import storage
from application import validator

# Schema constante para reutilização
SCHEMA = {
    "type": "record",
    "namespace": "rfb.json",
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

class TestValidation:
    def _run_validation_test(self, data, expected_errors=None, error_count=None):
        filename = 'sample.json'
        blob = json.dumps(data).encode('utf-8')
        
        validator_impl = validator.from_file(filename)
        converted_json = validator_impl.convert(blob)
        
        for i, item in enumerate(converted_json):
            summary = validator_impl.validate_data_against_avro(item, SCHEMA)
            
            if expected_errors is None:
                assert summary == []
            else:
                assert len(summary) == error_count
                for error in summary:
                    assert error['field'] in expected_errors

    def test_validate_correct_schema(self, storage: storage.StorageConnectionAdapter) -> None:
        valid_data = [
            {
                "name": "João Silva", "age": 30, "salary": 5000.50,
                "data_criacao": "2025-11-14", "data_nascimento": "1995-01-10",
                "hora_registro": "12:22:00", "tags": ["python", "avro", "teste"],
                "codigo": 123
            },
            {
                "name": "Maria Oliveira", "age": 42, "salary": 15000.75,
                "data_criacao": "2025-11-13", "data_nascimento": "1983-05-20",
                "hora_registro": "10:30:00", "tags": ["gestao", "relatorios"],
                "codigo": None
            }
        ]
        self._run_validation_test(valid_data)

    def test_validate_extra_field(self, storage: storage.StorageConnectionAdapter) -> None:
        data_with_extra_field = [
            {
                "name": "João Silva", "age": 30, "salary": 5000.50,
                "data_criacao": "2025-11-14", "data_nascimento": "1995-01-10",
                "hora_registro": "12:22:00", "tags": ["python", "avro", "teste"],
                "codigo": 123, "extra_field": 123
            }
        ]
        self._run_validation_test(data_with_extra_field, ["extra_field"], 1)

    def test_validate_missing_fields(self, storage: storage.StorageConnectionAdapter) -> None:
        data_missing_fields = [
            {
                "name": "João Silva", "age": 30, "salary": 5000.50,
                "tags": ["python", "avro", "teste"], "codigo": 123
            }
        ]
        self._run_validation_test(data_missing_fields, 
                                ["data_criacao", "data_nascimento", "hora_registro"], 3)

    def test_validate_wrong_type_age(self, storage: storage.StorageConnectionAdapter) -> None:
        data_wrong_age_type = [
            {
                "name": "João Silva", "age": "30", "salary": 5000.50,
                "data_criacao": "2025-11-14", "data_nascimento": "1995-01-10",
                "hora_registro": "12:22:00", "tags": ["python", "avro", "teste"],
                "codigo": 123
            }
        ]
        self._run_validation_test(data_wrong_age_type, ["age"], 1)

    def test_validate_wrong_type_name(self, storage: storage.StorageConnectionAdapter) -> None:
        data_wrong_name_type = [
            {
                "name": 22, "age": 30, "salary": 5000.50,
                "data_criacao": "2025-11-14", "data_nascimento": "1995-01-10",
                "hora_registro": "12:22:00", "tags": ["python", "avro", "teste"],
                "codigo": 123
            }
        ]
        self._run_validation_test(data_wrong_name_type, ["name"], 1)