from abc import ABC, abstractmethod
import json
from typing import Dict


import logging
# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def from_file(filename: str) -> 'IChecker':
    extension_mapping = {
        'json': JsonValidator()
    }
    return extension_mapping[filename.split('.')[-1]]        


class IChecker(ABC):

    @abstractmethod
    def convert(self, data: bytes):
        pass

    def validate_data_against_avro(
        self,
        data: dict[str, any], 
        schema: dict[str, any]
    ) -> list[dict[str, any]] | None:
        errors_report = []
        try:
            schema_fields = schema.get("fields", [])
            # Criar um mapa de 'nome_campo' -> 'definição_campo' para facilitar
            schema_fields_map = {field["name"]: field for field in schema_fields}
        except Exception as e:
            return [{
                "field": "schema",
                "message": f"Schema Avro inválido ou mal formatado: {e}",
                "expected": "Um schema Avro válido",
                "received": str(schema)[:200]
            }]

        if not isinstance(data, dict):
            raise TypeError("expected dict and received {}".format(type(data)))
        data_field_names = set(data.keys())
        schema_field_names = set(schema_fields_map.keys())
        
        extra_fields = data_field_names - schema_field_names
        for field_name in extra_fields:
            errors_report.append({
                "field": field_name,
                "message": "Campo extra não definido no schema",
                "expected": "Nenhum (não estar no schema)",
                "received": str(data.get(field_name))[:50]
            })

        # 3. Iterar e validar CADA campo definido no SCHEMA
        for field_name, field_def in schema_fields_map.items():
            value = data.get(field_name)
            field_type = field_def["type"]
            
            is_optional = (
                "default" in field_def or 
                (isinstance(field_type, list) and "null" in field_type)
            )

            if value is None and not is_optional:
                errors_report.append({
                    "field": field_name,
                    "message": "Campo obrigatório ausente",
                    "expected": str(field_type),
                    "received": "None"
                })
                continue # Próximo campo

            if value is None and is_optional:
                continue # Próximo campo

            types_to_check = field_type if isinstance(field_type, list) else [field_type]
            
            is_valid_type = False
            
            for avro_type in types_to_check:
                if avro_type == "null" and value is None:
                    is_valid_type = True
                    break
                if avro_type == "string" and isinstance(value, str):
                    is_valid_type = True
                    break
                if avro_type == "int" and isinstance(value, int):
                    is_valid_type = True
                    break
                # Avro 'double' aceita float ou int do Python
                if avro_type == "double" and isinstance(value, (int, float)):
                    is_valid_type = True
                    break
                # Validação simples de array
                if isinstance(avro_type, dict) and avro_type.get("type") == "array":
                    if not isinstance(value, list):
                        continue # O tipo não é lista, tenta o próximo tipo na união
                    
                    # Verifica os itens do array
                    item_type = avro_type.get("items")
                    if all(isinstance(item, str) for item in value) and item_type == "string":
                        is_valid_type = True
                        break
                    # (Adicionar mais validações de 'items' aqui se necessário)

            # Se nenhum tipo na união foi compatível
            if not is_valid_type:
                errors_report.append({
                    "field": field_name,
                    "message": "Tipo de dado incorreto",
                    "expected": str(field_type),
                    "received": f"{str(value)[:50]} (tipo: {type(value).__name__})"
                })
        return errors_report
        





class ValidatorFactory:
    def __init__(self):
        self._cache: Dict[str, IChecker] = {}
    
    def from_file_name(self, filename: str) -> IChecker:
        # Extrai a extensão para usar como chave do cache
        file_extension = filename.lower().split('.')[-1] if '.' in filename else ''
        
        # Verifica se já existe no cache
        if file_extension in self._cache:
            log.info(f"♻️  Retornando {file_extension.upper()}Validator do cache")
            return self._cache[file_extension]
        
        # Cria nova instância se não estiver em cache
        if file_extension == 'json':
            validator = JsonValidator()
        elif file_extension == 'csv':
            # validator = CsvValidator()
            pass
        elif file_extension == 'xml':
            # validator = XmlValidator()
            pass
        elif file_extension == 'avro':
            # validator = AvroValidator()
            pass
        else:
            raise ValueError(f"Tipo de arquivo não suportado: {file_extension}")
        
        # Armazena no cache
        self._cache[file_extension] = validator
        log.info(f"🆕 Criando novo {file_extension.upper()}Validator e armazenando no cache")
        
        return validator
    
    def clear_cache(self):
        """Limpa o cache (útil para testes)"""
        self._cache.clear()
        log.info("🧹 Cache limpo")
    
    def cache_size(self) -> int:
        """Retorna o tamanho atual do cache"""
        return len(self._cache)

class JsonValidator(IChecker):

    def convert(self, data: bytes) -> dict | list[dict]:
        import json
        return json.loads(data)
        