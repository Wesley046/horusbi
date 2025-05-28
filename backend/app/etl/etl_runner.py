# backend/app/etl/etl_runner.py
import sys
import os
from pathlib import Path
import importlib # Para importação dinâmica
from sqlalchemy.orm import Session
from dotenv import load_dotenv
import logging

# --- Configuração do Nível de Logging ---
# Altere para logging.DEBUG para ver logs mais detalhados,
# especialmente os logs de debug que adicionamos em etl_cliente_1.py
LOG_LEVEL = logging.DEBUG  # <--- CERTIFIQUE-SE QUE ESTÁ DEBUG!
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s')
logger = logging.getLogger(__name__) # Logger específico para este módulo (etl_runner)

# --- Configuração de ambiente e path ---
env_file_path = Path(__file__).resolve().parent.parent.parent / '.env'
if not env_file_path.exists():
    logger.warning(f"Arquivo .env não encontrado em {env_file_path}. As variáveis de ambiente devem ser definidas externamente.")
load_dotenv(dotenv_path=env_file_path)

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if backend_dir not in sys.path:
    logger.info(f"Adicionando diretório ao sys.path: {backend_dir}")
    sys.path.append(backend_dir)

# Importações do projeto (após sys.path ser ajustado)
try:
    # É crucial que AppDeclarativeBase seja a ÚNICA instância de declarative_base() usada em todo o projeto
    # que interage com este engine/SessionLocal.
    from app.database import SessionLocal, Base as AppDeclarativeBase 
except ImportError as e:
    logger.critical(f"Falha ao importar SessionLocal ou Base de app.database: {e}. Verifique o PYTHONPATH e a estrutura do projeto (app/database.py).", exc_info=True)
    sys.exit(1) 

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL não configurada. ETL não pode ser executado.")
    sys.exit(1) 

# --- REGISTRO DE TAREFAS ETL POR CLIENTE ---
CLIENT_ETL_REGISTRY = {
    1: {
        "module_path": "app.etl.etl_cliente_1", 
        "function_name": "process_etl_cliente_1", 
        "schema_name": "cliente1",
        "description": "ETL para vendas do Cliente 1 (API Betel Tecnologia, paginada)"
    },
    2: {
        "module_path": "app.etl.etl_cliente_2", # Certifique-se que o nome do arquivo é etl_cliente_2.py
        "function_name": "process_etl_cliente_2", # Nome da função que definimos acima
        "schema_name": "cliente2", # Schema para o cliente 2
        "description": "ETL para Cliente 2 (API Posseidom, não paginada)"
    },
}

def run_all_etls():
    logger.info(f"Iniciando orquestração de ETLs (Nível de Log Global: {logging.getLevelName(logger.getEffectiveLevel())})...")
    db: Session = SessionLocal()
    if not db:
        logger.critical("Falha ao criar sessão do banco de dados (SessionLocal retornou None). Verifique app.database.py.")
        return # Não podemos continuar sem sessão de DB

    overall_success = True

    for client_id, task_info in CLIENT_ETL_REGISTRY.items():
        module_path = task_info["module_path"]
        function_name = task_info["function_name"]
        schema_name = task_info["schema_name"]
        description = task_info.get("description", f"Cliente ID {client_id}")

        logger.info(f"--- Iniciando ETL para: {description} (Schema: {schema_name}) ---")

        try:
            client_module = importlib.import_module(module_path)
            etl_function = getattr(client_module, function_name)

            # Passamos a sessão 'db', 'schema_name' e a 'AppDeclarativeBase'
            result = etl_function(db=db, schema_name=schema_name, sqlalchemy_base=AppDeclarativeBase)

            # Analisa o resultado esperado do dicionário retornado pela função ETL do cliente
            if isinstance(result, dict) and result.get("success"):
                logger.info(f"SUCESSO ETL para {description}. Mensagem: {result.get('message', 'Concluído sem mensagem detalhada.')}")
            elif isinstance(result, dict): # Se é um dict mas success não é True
                logger.error(f"FALHA ETL para {description} (conforme retornado pela função). Resultado: {result}")
                overall_success = False
            else: # Se a função não retornou um dicionário (inesperado)
                logger.warning(f"ETL para {description} retornou um resultado de tipo inesperado: {type(result)}. Esperado: dict. Resultado: {result}")
                overall_success = False

        except ModuleNotFoundError:
            logger.error(f"Módulo ETL não encontrado para {description}: '{module_path}'. Verifique o CLIENT_ETL_REGISTRY.", exc_info=True)
            overall_success = False
        except AttributeError:
            logger.error(f"Função ETL '{function_name}' não encontrada no módulo '{module_path}' para {description}. Verifique o CLIENT_ETL_REGISTRY.", exc_info=True)
            overall_success = False
        except Exception as e: # Captura outras exceções inesperadas da função ETL do cliente
            logger.critical(f"Erro CRÍTICO INESPERADO durante a execução do ETL para {description}: {e}", exc_info=True)
            overall_success = False
            try:
                # Tentar rollback pode ser útil, mas se a sessão estiver muito corrompida, pode falhar.
                db.rollback()
                logger.info(f"Sessão do banco de dados REVERTIDA (rollback) após erro crítico em {description}.")
            except Exception as rb_exc:
                logger.error(f"Erro adicional ao tentar reverter (rollback) a sessão do banco de dados após erro em {description}: {rb_exc}", exc_info=True)
        finally:
            logger.info(f"--- Tentativa de ETL para: {description} CONCLUÍDA ---")

    if db:
        try:
            db.close()
            logger.info("Sessão principal do banco de dados fechada.")
        except Exception as close_exc:
            logger.error(f"Erro ao fechar a sessão principal do banco de dados: {close_exc}", exc_info=True)


    if overall_success:
        logger.info("Orquestração de ETLs concluída. Todos os clientes processados (verifique logs para status individuais de sucesso/falha).")
    else:
        logger.warning("Orquestração de ETLs concluída, MAS um ou mais ETLs de clientes encontraram FALHAS ou erros críticos. Verifique os logs detalhadamente.")

if __name__ == "__main__":
    logger.info("Iniciando script etl_runner.py como __main__...")
    run_all_etls()
    logger.info("Script etl_runner.py FINALIZADO!")