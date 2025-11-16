


# --- LÓGICA PARA ARQUIVOS JSON ---
# Encontra todos os arquivos .json na pasta mock

import json
import uuid
import glob

from etc.config import loader
from infrastructure import bucket, storage
# Inicializa o BucketManager
env = loader.load_env(['./etc/config/root.local.yml'])
json_files = glob.glob("./etc/mock/*.json")
bm = bucket.BucketAdapter.from_minio_client(env['bucket'])

import logging
# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

log.info(f"Encontrados {len(json_files)} arquivos JSON.")

for file_path in json_files:
    with open(file_path, "r", encoding="utf-8") as f:
        samples = json.load(f)
        
        # Garante que seja uma lista para iterar
        if not isinstance(samples, list):
            samples = [samples]

        for sample in samples:
            # Salva cada registro como um arquivo separado no bucket
            bm.put_object(
                env['app']['source_bucket'],
                f"rfb/json/sample_{uuid.uuid4()}.json",
                json.dumps(sample, indent=2, ensure_ascii=False),
                content_type="application/json"
            )

# --- LÓGICA PARA ARQUIVOS CSV ---
# Encontra todos os arquivos .csv na pasta mock
csv_files = glob.glob("./etc/mock/*.csv")

log.info(f"Encontrados {len(csv_files)} arquivos CSV.")

for file_path in csv_files:
    with open(file_path, "r", encoding="utf-8") as f:
        # DictReader converte cada linha do CSV em um dicionário Python
        # usando a primeira linha (header) como chaves
        reader = f.read()
        # Convertemos a linha do CSV para JSON para manter o padrão no bucket
        # (Ajuste o caminho 'rfb/csv/' se preferir separar dos jsons originais)
        bm.put_object(
            env['app']['source_bucket'],
            f"rfb/csv/sample_{uuid.uuid4()}.csv",
            reader,
            content_type="application/csv"
        )

