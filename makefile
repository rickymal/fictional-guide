build:
	echo "Hello, World!"

test_all:
	python -m job.fixture_storage
	pytest test/validation_test.py
	pytest test/schema_val_test.py
	pytest test/all_flow_test.py
	pytest test/fastapi_test.py

abc:
	uv run --active python -m etc.job.py2abc --file_path "./infrastructure/storage.py" --class_name StorageConnectionAdapter --output_path "./domain/port/i_storage_connection_adapter.py"
	uv run --active python -m etc.job.py2abc --file_path "./infrastructure/repository.py" --class_name SchemaRegistry --output_path "./domain/port/i_schema_registry.py"
	uv run --active python -m etc.job.py2abc --file_path "./infrastructure/bucket.py" --class_name BucketAdapter --output_path "./domain/port/i_bucket_adapter.py"
	uv run --active python -m etc.job.py2abc --file_path "./infrastructure/broker.py" --class_name BrokerAdapter --output_path "./domain/port/i_broker_adapter.py"
