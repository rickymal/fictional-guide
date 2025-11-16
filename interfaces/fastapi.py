import logging
from typing import Any, Dict
import fastapi
import uvicorn
from config import loader
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from application import usecase
from domain import dto, error, port
from infrastructure import repository
from infrastructure.broker import BrokerAdapter
from infrastructure.storage import StorageConnectionAdapter

from starlette.status import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_404_NOT_FOUND,
    HTTP_422_UNPROCESSABLE_CONTENT,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def get_dependencies() -> Dict[str, Any]:
    log.info("Carregando dependências...")
    env = loader.load_env(["./config/root.local.yml"])
    return env


class RouterBuilder:
    def __init__(self, env: Dict[str, Any] | None = None, prefix: str = ""):
        self.env = env or get_dependencies()
        self.prefix = prefix
        self.router = APIRouter(prefix=prefix)
        self._setup_dependencies()
        self._setup_routes()

    def _setup_dependencies(self) -> None:
        self.broker_service: port.IBrokerAdapter = BrokerAdapter(
            self.env.get("broker", None)
        )
        self.storage_connection: port.IStorageConnection = (
            StorageConnectionAdapter.from_duckdb_memory(self.env["storage"])
        )
        self.schema_repository = repository.SchemaRegistry()
        self.metric_repository = repository.MoveRegistry()

    def _setup_routes(self) -> None:
        self._setup_test_routes()
        self._setup_schema_routes()
        self._setup_job_routes()
        self._setup_metrics_routes()

    def _setup_test_routes(self) -> None:
        @self.router.get("/hello", summary="Endpoint de teste", tags=["Testes"])
        async def hello():
            log.info("Endpoint /hello acessado")
            return {"message": "Endpoint funcionando!"}

        @self.router.post("/echo", summary="Endpoint de eco", tags=["Testes"])
        async def echo(data: dict):
            log.info(f"Endpoint /echo acessado com dados: {data}")
            return data

    def _setup_schema_routes(self) -> None:
        @self.router.delete("/schema/all")
        def delete_all_schema():
            log.info("Recebida requisição para deletar todos os schemas")
            try:
                usecase.delete_all_schema(
                    self.storage_connection, self.schema_repository
                )
                log.info("Todos os schemas deletados com sucesso")
                return JSONResponse(
                    status_code=HTTP_201_CREATED,
                    content={"message": "Todos os schemas deletados com sucesso"},
                )
            except error.StorageConnectionErr as err:
                log.error(f"Erro de conexão com storage ao deletar schemas: {err}")
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Erro de conexão com storage: {str(err)}",
                )
            except error.SchemaValidationError as err:
                log.error(f"Erro de validação ao deletar schemas: {err}")
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"Schema inválido: {str(err)}",
                )

        @self.router.delete("/schema/{namespace}")
        def delete_schema_by_namespace(namespace: str):
            log.info(
                f"Recebida requisição para deletar schema do namespace: {namespace}"
            )
            try:
                usecase.delete_some_schema(
                    self.storage_connection, self.schema_repository, namespace=namespace
                )
                log.info(f"Schema do namespace {namespace} deletado com sucesso")
                return JSONResponse(
                    status_code=HTTP_201_CREATED,
                    content={
                        "message": f"Schema do namespace {namespace} deletado com sucesso"
                    },
                )
            except error.StorageConnectionErr as err:
                log.error(f"Erro de conexão ao deletar schema {namespace}: {err}")
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Erro de conexão com storage: {str(err)}",
                )
            except error.SchemaValidationError as err:
                log.error(f"Erro de validação ao deletar schema {namespace}: {err}")
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"Schema inválido: {str(err)}",
                )

        @self.router.put(
            "/schema",
            summary="Endpoint para definir schema",
            tags=["Schemas"],
            responses={
                201: {"description": "Schema criado com sucesso"},
                500: {"description": "Erro interno do servidor"},
                422: {"description": "Dados de schema inválidos"},
            },
        )
        async def create_schema(schema: dto.SchemaCreateDto):
            log.info(f"Recebida requisição para criar schema: {schema}")
            try:
                usecase.create_schema(
                    schema, self.storage_connection, self.schema_repository
                )
                log.info("Schema criado com sucesso")
                return JSONResponse(
                    status_code=HTTP_201_CREATED,
                    content={"message": "Schema criado com sucesso"},
                )
            except error.StorageConnectionErr as err:
                log.error(f"Erro de conexão ao criar schema: {err}")
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Erro de conexão com storage: {str(err)}",
                )
            except error.SchemaValidationError as err:
                log.error(f"Erro de validação ao criar schema: {err}")
                raise HTTPException(
                    status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"Schema inválido: {str(err)}",
                )

        @self.router.get(
            "/schema/all", summary="Lista todos os schemas", tags=["Schemas"]
        )
        async def get_all_schemas():
            log.info("Recebida requisição para listar todos os schemas")
            try:
                schemas = usecase.get_all_schemas(
                    self.storage_connection, self.schema_repository
                )
                log.info(f"Retornados {len(schemas)} schemas")
                return schemas
            except error.StorageConnectionErr as err:
                log.error(f"Erro de conexão ao listar schemas: {err}")
                raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR)
            except error.StorageNotFoundErr as err:
                log.error(f"Storage não encontrado ao listar schemas: {err}")
                raise HTTPException(status_code=HTTP_404_NOT_FOUND)

        @self.router.get(
            "/schema/namespace/{namespace}",
            summary="Busca schemas por namespace",
            tags=["Schemas"],
        )
        async def get_schemas_by_namespace(namespace: str):
            log.info(
                f"Recebida requisição para buscar schemas do namespace: {namespace}"
            )
            try:
                schemas = usecase.get_schemas_by_namespace(
                    namespace, self.storage_connection, self.schema_repository
                )
                log.info(
                    f"Retornados {len(schemas)} schemas para namespace {namespace}"
                )
                return schemas
            except error.StorageConnectionErr as err:
                log.error(
                    f"Erro de conexão ao buscar schemas por namespace {namespace}: {err}"
                )
                raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR)
            except error.StorageNotFoundErr as err:
                log.error(
                    f"Storage não encontrado ao buscar schemas por namespace {namespace}: {err}"
                )
                raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    def _setup_job_routes(self) -> None:
        @self.router.post(
            "/job/validate/namespace/{namespace}",
            summary="Agenda validação de schema para um bucket",
            tags=["Jobs"],
        )
        async def validate_schema_endpoint(namespace: str):
            log.info(
                f"Recebida requisição para validar schema do namespace: {namespace}"
            )
            try:
                message = usecase.schedule_schema_validation(
                    namespace, self.broker_service
                )
                log.info(f"Validação agendada para namespace {namespace}: {message}")
                return {"message": message}
            except error.ProducerConnectionRefusedError as err:
                log.error(
                    f"Erro de conexão do producer para namespace {namespace}: {err}"
                )
                raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR)
            except error.ProducerSendingError as err:
                log.error(
                    f"Erro ao enviar mensagem para namespace {namespace}: {err}"
                )
                raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    def _setup_metrics_routes(self) -> None:

        @self.router.get(
            "/metrics",
            summary="Extrai métricas de aprovados e reprovados",
            tags=["Métricas"],
        )
        async def get_metrics():
            """Extrai métricas dizendo quantos foram aprovados e quantos reprovados"""
            log.info("Recebida requisição para obter métricas")
            try:
                metrics = usecase.get_metrics(
                    self.storage_connection, self.metric_repository
                )
                log.info(f"Métricas obtidas: {metrics}")
                return metrics
            except error.StorageConnectionErr as err:
                log.error(f"Erro de conexão ao obter métricas: {err}")
                raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR)
            except error.StorageNotFoundErr as err:
                log.error(f"Storage não encontrado ao obter métricas: {err}")
                raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    def get_router(self) -> APIRouter:
        return self.router


def setup_router(env: Dict[str, Any] | None = None, prefix: str = "") -> APIRouter:
    log.info(f"Configurando router com prefixo: {prefix}")
    builder = RouterBuilder(env, prefix)
    return builder.get_router()


def setup_fastapi() -> fastapi.FastAPI:
    log.info("Inicializando aplicação FastAPI")
    api_router = setup_router()
    api = fastapi.FastAPI(
        title="API de Validação de Schema",
        description="API para gerenciamento e validação de schemas de dados",
        version="1.0.0",
    )
    api.include_router(api_router)
    log.info("Aplicação FastAPI configurada com sucesso")
    return api


if __name__ == "__main__":
    log.info("Iniciando servidor Uvicorn...")
    uvicorn.run(setup_fastapi(), host="127.0.0.1", port=8000, log_level="info")
