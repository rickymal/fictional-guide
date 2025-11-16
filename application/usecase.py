from domain import dto
from domain.port import IBrokerAdapter, IBucketAdapter, IStorageConnection
from domain import error
from infrastructure import repository
import logging
from application import validator

log = logging.getLogger(__name__)

def setup_schema_infrastructure(rm: IBrokerAdapter) -> str:
    rm.setup_infrastructure()


def schedule_schema_validation(bucket_name: str, rm: IBrokerAdapter) -> str:
    import json

    namepsace = bucket_name.replace("/", ".")
    message_str = json.dumps({"namespace": namepsace}, ensure_ascii=False)
    rm.publish_message(routing_key="app.mauler", message=message_str)
    return f"Schema validation scheduled for bucket: {bucket_name}"


def create_schema(
    schema: dto.SchemaCreateDto, dm: IStorageConnection, ds: repository.SchemaRegistry
) -> str:
    schema_data = schema.model_dump(exclude_unset=True)
    with dm.connect() as conn:
        return ds.insert_schema(conn, schema_data)


def delete_all_schema(dm: IStorageConnection, ds: repository.SchemaRegistry) -> str:
    with dm.connect() as conn:
        return ds.delete_schema(conn)


def delete_some_schema(
    dm: IStorageConnection, ds: repository.SchemaRegistry, namespace: str
) -> str:
    with dm.connect() as conn:
        return ds.delete_schema(conn, namespace)


# namespace
def get_all_schemas(
    dm: IStorageConnection, ds: repository.SchemaRegistry
) -> list[dict[str, object]]:
    with dm.connect() as conn:
        return ds.get_all(conn=conn)


def get_schemas_by_namespace(
    namespace: str, dm: IStorageConnection, ds: repository.SchemaRegistry
) -> list[dict[str, object]]:
    with dm.connect() as conn:
        return ds.get_avro_schema_by_namespace(conn=conn, namespace=namespace)


def get_metrics(dm: IStorageConnection, ds: repository.MoveRegistry):
    with dm.connect() as conn:
        return ds.get_metrics(conn=conn)
    pass

def avaliate_data(
    data: dict,
    dm: IStorageConnection,
    ds: repository.SchemaRegistry,
    bm: IBucketAdapter,
    ic: validator.ValidatorFactory,
    mr: repository.MoveRegistry,
) -> bool:
    namespace = data["namespace"]
    path = namespace.replace(".", "/")
    for filename, blob in bm.iter_bucket_by_prefix_key("gold", path):
        try:
            validator = ic.from_file_name(filename)
            data_as_dict = validator.convert(blob)
            with dm.connect() as conn:
                avro = ds.get_avro_schema_by_namespace(conn, namespace)
                if len(avro) == 0:
                    raise error.SchemaNotFound()
        except Exception as err:
            log.error(err)
            raise error.InternalError(err)
        import json

        schema_dump = json.loads(avro[0]["schema_avro"])
        summary: list[object] = validator.validate_data_against_avro(
            data_as_dict, schema_dump
        )
        final_bucket = "validated" if not summary else "quarantine"

        bm.put_object(
            final_bucket, f"{path}/{filename}", blob, content_type="application/json"
        )
        bm.delete_object("gold", f"{path}/{filename}")
        with dm.connect() as conn:
            mr.insert_metric(
                conn,
                avro[0]["id"],
                "gold",
                final_bucket,
                namespace,
                json.dumps(summary, ensure_ascii=False),
            )
        pass
    pass
