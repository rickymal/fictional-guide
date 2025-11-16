class StorageNotFound(Exception):
    pass

class StorageConnectionErr(Exception):
    pass

class SchemaValidationError(Exception):
    pass

class StorageNotFoundErr(Exception):
    pass    

class ProducerConnectionRefusedError(Exception):
    pass

class ProducerSendingError(Exception):
    pass

class BucketConnectionError(Exception):
    pass

# Domínio de Dados/Storage
class DataValidationError(Exception):
    """Erro quando os dados não seguem o schema esperado"""
    pass

class DataIntegrityError(Exception):
    """Violação de integridade dos dados"""
    pass

class DataSerializationError(Exception):
    """Erro ao serializar/desserializar dados"""
    pass

class DataTransformationError(Exception):
    """Erro durante transformação de dados"""
    pass

# Schema
class SchemaVersionConflictError(Exception):
    """Conflito de versão de schema"""
    pass

class SchemaCompatibilityError(Exception):
    """Schema incompatível com versões anteriores"""
    pass

class SchemaNotFound(Exception):
    pass

class FieldValidationError(Exception):
    """Erro específico em validação de campo"""
    pass

class RequiredFieldMissingError(Exception):
    """Campo obrigatório faltando"""
    pass


# Mensageria/Eventos
class ConsumerConnectionError(Exception):
    """Erro de conexão do consumer"""
    pass

class MessageProcessingError(Exception):
    """Erro durante processamento da mensagem"""
    pass

class MessageDeserializationError(Exception):
    """Erro ao desserializar mensagem"""
    pass

class EventPublishingError(Exception):
    """Erro ao publicar evento de domínio"""
    pass

class RetryExhaustedError(Exception):
    """Tentativas de retry esgotadas"""
    pass


# Bucket/Storage Específicos

class BucketOperationError(Exception):
    """Erro genérico em operações de bucket"""
    pass

class ObjectNotFoundError(Exception):
    """Objeto não encontrado no bucket"""
    pass

class ObjectPermissionError(Exception):
    """Problema de permissão no objeto"""
    pass

class BucketNotEmptyError(Exception):
    """Bucket não está vazio para operação"""
    pass

class PresignedUrlError(Exception):
    """Erro ao gerar URL pré-assinada"""
    pass

# infraestrutura
class ConfigurationError(Exception):
    """Erro de configuração da aplicação"""
    pass


class ResourceExhaustedError(Exception):
    """Recursos esgotados (memória, CPU, etc)"""
    pass

class TimeoutError(Exception):
    """Timeout em operação"""
    pass

class CircuitBreakerOpenError(Exception):
    """Circuit breaker aberto"""
    pass


# others
class InternalError(Exception):
    pass


class BusinessRuleViolationError(Exception):
    """Violação de regra de negócio"""
    pass

class WorkflowStateError(Exception):
    """Estado inválido no workflow"""
    pass

class ConcurrentModificationError(Exception):
    """Modificação concorrente detectada"""
    pass

class IdempotencyKeyError(Exception):
    """Erro com chave de idempotência"""
    pass


# API error
class APIValidationError(Exception):
    """Erro de validação na API"""
    pass

class RateLimitExceededError(Exception):
    """Limite de requisições excedido"""
    pass

class AuthenticationError(Exception):
    """Erro de autenticação"""
    pass

class AuthorizationError(Exception):
    """Erro de autorização"""
    pass


# para adicionar outros
# Sufixos comuns:
# - Error: Genérico
# - Exception: Alternativa a Error  
# - Failure: Quando algo falhou
# - Violation: Quando uma regra foi violada
# - Conflict: Quando há conflito
# - Timeout: Tempo esgotado

# Padrões:
# [Contexto][Operação]Error
# [Contexto][Problema]Error