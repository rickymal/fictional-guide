import logging
import os
import signal
import subprocess
from typing import List, Optional, Tuple

from fastapi.testclient import TestClient

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Schema de dados
SCHEMA: dict[str, object] = {
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


def run_process(command: List[str], timeout: int = 3) -> Tuple[Optional[int], str, str]:
    """
    Executa um comando e seu grupo de processos, garantindo que tudo
    seja encerrado se estourar o timeout.
    Retorna (returncode, stdout, stderr)
    """
    log.info(f"Executando (PGID): {' '.join(command)}")

    process = None

    try:
        # Inicia o processo em um novo grupo de processos
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,  # Cria novo grupo de processos (Unix)
        )

        stdout, stderr = process.communicate(timeout=timeout)

        log.debug(f"Comando finalizado. Código de saída: {process.returncode}")
        if stderr:
            log.warning(f"Stderr do comando: {stderr}")
        return process.returncode, stdout, stderr

    except subprocess.TimeoutExpired as e:
        log.warning(
            f"Timeout: comando {command} excedeu {timeout}s. Encerrando grupo..."
        )

        # Encerra o grupo inteiro de processos
        try:
            os.killpg(process.pid, signal.SIGKILL)
            log.info(
                f"Grupo de processos (PGID: {process.pid}) encerrado com SIGKILL."
            )
        except ProcessLookupError:
            log.debug("Processo já estava morto, não foi preciso 'killpg'.")
        except Exception as kill_e:
            log.error(f"Erro ao tentar 'killpg' {process.pid}: {kill_e}")

        # Obtém saída parcial
        partial_stdout = e.stdout or ""
        partial_stderr = e.stderr or ""

        if partial_stderr:
            log.warning(f"Stderr parcial (antes do timeout): {partial_stderr}")

        return None, partial_stdout, partial_stderr

    except Exception as e:
        log.exception(f"Erro ao executar {command}: {e}")
        # Garante que o processo não fique zumbi
        if process and hasattr(process, "pid"):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except Exception:
                pass  # Ignora erros no cleanup
        return -1, "", str(e)


class TestAllFlow:
    """Teste do fluxo completo da aplicação"""

    def test_all_flow(self, test_client: TestClient, bm: object) -> None:
        """Testa o fluxo completo da aplicação"""

        self._prepare_infrastructure()
        self._setup_schema(test_client)
        self._execute_validation_flow(test_client, bm)
        self._verify_metrics(test_client)

    def _prepare_infrastructure(self) -> None:
        """Prepara a infraestrutura necessária para o teste"""
        # Preparar bucket
        log.info("Preparando bucket...")
        return_code, stdout, stderr = run_process(["make", "prepare_bucket"], 10)
        if return_code != 0:
            log.error(f"Falha no prepare_bucket. stdout: {stdout}, stderr: {stderr}")
            raise Exception(
                f"Falha ao preparar bucket. Código de retorno: {return_code}"
            )

        # Preparar storage
        log.info("Preparando storage...")
        return_code, stdout, stderr = run_process(["make", "prepare_storage"], 10)
        if return_code != 0:
            log.error(
                f"Falha no prepare_storage. stdout: {stdout}, stderr: {stderr}"
            )
            raise Exception(
                f"Falha ao preparar storage. Código de retorno: {return_code}"
            )

    def _setup_schema(self, test_client: TestClient) -> None:
        """Configura o schema no sistema"""
        log.info("Inserindo schema...")
        response = test_client.put("/schema", json=SCHEMA)
        assert response.status_code == 201
        log.info("Schema inserido com sucesso")

    def _execute_validation_flow(self, test_client: TestClient, bm: object) -> None:
        """Executa o fluxo de validação completo"""
        # Contagem inicial de arquivos gold
        qtd_gold = sum(1 for _ in bm.iter_bucket_by_prefix_key("gold", "rfb/json"))
        log.info(f"Quantidade inicial de arquivos gold: {qtd_gold}")

        # Disparar job de validação
        log.info("Iniciando job de validação...")
        response = test_client.post("/job/validate/namespace/rfb.json")
        assert response.status_code == 200

        # Executar consumer
        log.info("Executando consumer...")
        return_code, stdout, stderr = run_process(["make", "run_consumer"], 15)

        # Verificar resultados da validação
        self._verify_validation_results(bm, qtd_gold)

    def _verify_validation_results(self, bm: object, expected_total: int) -> None:
        """Verifica os resultados do processo de validação"""
        qtd_quarantine = sum(
            1 for _ in bm.iter_bucket_by_prefix_key("quarantine", "rfb/json")
        )
        qtd_validated = sum(
            1 for _ in bm.iter_bucket_by_prefix_key("validated", "rfb/json")
        )

        log.info(f"Arquivos quarantine: {qtd_quarantine}")
        log.info(f"Arquivos validated: {qtd_validated}")

        assert expected_total == qtd_quarantine + qtd_validated
        log.info("Validação de quantidade de arquivos bem-sucedida")

    def _verify_metrics(self, test_client: TestClient) -> None:
        """Verifica as métricas do sistema"""
        log.info("Verificando métricas...")
        response = test_client.get("/metrics")
        assert response.status_code == 200

        metrics = response.json()
        log.info(f"Métricas obtidas: {metrics}")

        assert isinstance(metrics, list)
        assert len(metrics) == 2
        assert {"new_bucket": "validated", "total": 18} in metrics
        assert {"new_bucket": "quarantine", "total": 20} in metrics
        log.info("Teste de métricas concluído com sucesso")
