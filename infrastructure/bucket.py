import logging
from pathlib import Path
from typing import Iterator
from minio.error import S3Error
from domain import abc, error, port

log = logging.getLogger(__name__)

from etc.config import loader
env_g = loader.load_env(['./etc/config/root.local.yml'])


class BucketAdapter(port.IBucketAdapter):
    def __init__(self, client):
        super().__init__(client)
        self.client = client

    @classmethod
    def from_minio_client(cls, env: dict):
        from minio import Minio

        minio = Minio(
            endpoint=env["endpoint"],
            access_key=env["username"],
            secret_key=env["password"],
            secure=False,
        )

        return cls(minio)

    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        try:
            # Verifica se o bucket existe
            if not self.client.bucket_exists(bucket_name):
                log.warning(f"Bucket '{bucket_name}' não encontrado.")
                raise error.BucketOperationError(f"Bucket '{bucket_name}' não existe")

            # Remove o objeto
            self.client.remove_object(bucket_name, object_name)
            log.info(
                f"Objeto '{object_name}' removido com sucesso do bucket '{bucket_name}'."
            )
            return True

        except S3Error as exc:
            log.error(
                f"Erro ao remover objeto '{object_name}' do bucket '{bucket_name}': {exc}"
            )
            if "NoSuchKey" in str(exc) or "Not Found" in str(exc):
                raise error.BucketOperationError(
                    f"Objeto '{object_name}' não encontrado no bucket '{bucket_name}'"
                )
            else:
                raise error.BucketConnectionError(
                    f"Erro de conexão ao remover objeto: {exc}"
                )

    def create_bucket(self, bucket_name: str) -> bool:
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                log.info(f"Bucket '{bucket_name}' created.")
                return True
            else:
                log.info(f"Bucket '{bucket_name}' already exists.")
                return True

        except S3Error as exc:
            log.info(f"failed at creating bucket: {exc}")
            raise error.BucketConnectionError

    def put_object(
        self, bucket_name: str, object_name: str, data: bytes | str, content_type: str
    ):
        import io

        if isinstance(data, str):
            data = data.encode("utf-8")

        data_stream = io.BytesIO(data)

        result = self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(data_stream.getvalue()),
            content_type=content_type,
        )

    def iter_bucket_by_prefix_key(
        self, bucket_name: str, prefix: str
    ) -> Iterator[tuple[str, bytes]]:
        try:
            objs = self.client.list_objects(bucket_name, prefix, recursive=True)

            for obj in objs:
                if obj.is_dir:
                    continue

                blob_content = self.read_object(bucket_name, obj.object_name)
                if blob_content:
                    filename = Path(obj.object_name).name
                    yield (filename, blob_content)
        except S3Error as exc:
            log.error(f"Erro S3 ao listar objetos com prefixo '{prefix}': {exc}")
            raise error.BucketConnectionError

    def read_object(self, bucket_name: str, object_name: str) -> bytes:
        response = None
        try:
            response = self.client.get_object(bucket_name, object_name)
            data_bytes = response.read()
            log.info(
                f"Objeto '{object_name}' lido com sucesso (Tamanho: {len(data_bytes)} bytes)."
            )
            return data_bytes
        except S3Error as exc:
            response.close()
            response.release_conn()
            log.error(f"algo de errado não está certo: {exc}")
            raise error.BucketConnectionError

    def remove_bucket_if_exists(self, bucket_name: str) -> bool:
        try:
            # Verifica se o bucket existe
            if not self.client.bucket_exists(bucket_name):
                log.info(f"Bucket '{bucket_name}' não existe. Nada a remover.")
                return False

            # Primeiro remove todos os objetos do bucket
            try:
                objects = self.client.list_objects(bucket_name, recursive=True)
                for obj in objects:
                    self.client.remove_object(bucket_name, obj.object_name)
                    log.debug(
                        f"Objeto '{obj.object_name}' removido do bucket '{bucket_name}'"
                    )
            except S3Error as exc:
                log.warning(f"Erro ao limpar objetos do bucket '{bucket_name}': {exc}")
                # Continua tentando remover o bucket mesmo com erro nos objetos

            # Remove o bucket vazio
            self.client.remove_bucket(bucket_name)
            log.info(f"Bucket '{bucket_name}' removido com sucesso.")
            return True

        except S3Error as exc:
            log.error(f"Erro ao remover bucket '{bucket_name}': {exc}")
            if "BucketNotEmpty" in str(exc):
                raise error.BucketOperationError(
                    f"Bucket '{bucket_name}' não está vazio"
                )
            else:
                raise error.BucketConnectionError(
                    f"Erro de conexão ao remover bucket: {exc}"
                )
