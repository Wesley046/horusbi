# backend/app/etl/etl_cliente_2.py

import requests
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging
# Removido 'import xml.etree.ElementTree as ET' pois a API parece retornar JSON diretamente

try:
    from app.models import get_vendas_model
except ImportError as e:
    logging.critical(f"Falha CRÍTICA ao importar get_vendas_model de app.models em etl_cliente_2: {e}", exc_info=True)
    raise

logger = logging.getLogger(__name__)

# --- Configurações da API para Cliente 2 ---
API_BASE_URL_CLIENTE_2 = "https://posseidom.com/Posseidom"
API_RELATIVE_PATH_CLIENTE_2 = "/Sistema/DPSistemas.WS/v1/api.asmx/ObterVendasPeriodo"
API_FULL_URL_CLIENTE_2 = API_BASE_URL_CLIENTE_2 + API_RELATIVE_PATH_CLIENTE_2
API_BODY_CLIENTE_2_STR = "Token=aadb92a3-b861-4c45-9ec6-1c93e711df0d&EmpID=434&DataInicio=01/01/2020&DataFinal=01/01/2030"
API_HEADERS_CLIENTE_2 = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json" # Adicionado para solicitar JSON, se a API suportar. Remova se causar problemas.
}

def process_etl_cliente_2(db: Session, schema_name: str, sqlalchemy_base):
    logger.info(f"CLIENTE 2: Iniciando ETL para schema '{schema_name}'. API: {API_FULL_URL_CLIENTE_2}")

    VendasModelCliente2 = None
    try:
        VendasModelCliente2 = get_vendas_model(schema_name, sqlalchemy_base)
        logger.info(f"CLIENTE 2: Modelo dinâmico '{VendasModelCliente2.__name__}' obtido para tabela '{VendasModelCliente2.__table__.fullname}'")
    except Exception as e:
        logger.error(f"CLIENTE 2: Erro crítico ao obter modelo dinâmico para schema '{schema_name}': {e}", exc_info=True)
        return {"success": False, "message": f"Erro ao configurar modelo para {schema_name}.", "error": str(e)}

    total_registros_comitados_cliente = 0
    sucesso_geral_cliente = True

    try:
        logger.info(f"CLIENTE 2: Enviando requisição POST para API. Body (início): {API_BODY_CLIENTE_2_STR[:100]}...")
        response = requests.post(API_FULL_URL_CLIENTE_2, headers=API_HEADERS_CLIENTE_2, data=API_BODY_CLIENTE_2_STR, timeout=60)
        response.raise_for_status()
        logger.debug(f"CLIENTE 2: Resposta da API (status {response.status_code}). Conteúdo (início): {response.text[:1000]}")

        vendas_api_cliente_2 = []
        try:
            json_data = response.json()
            if isinstance(json_data, list):
                vendas_api_cliente_2 = json_data
                logger.info(f"CLIENTE 2: Resposta JSON da API é uma lista com {len(vendas_api_cliente_2)} itens.")
            else:
                # Se a API envolver a lista em uma chave, ex: {"dados_vendas": [...] }
                # Tente extrair de uma chave comum, se aplicável.
                # Exemplo:
                # chave_principal = "dados_vendas" # Substitua pela chave correta da sua API
                # if isinstance(json_data, dict) and chave_principal in json_data and isinstance(json_data[chave_principal], list):
                #     vendas_api_cliente_2 = json_data[chave_principal]
                #     logger.info(f"CLIENTE 2: Extraída lista de {len(vendas_api_cliente_2)} vendas da chave '{chave_principal}'.")
                # else:
                logger.error(f"CLIENTE 2: Resposta JSON da API não é uma lista como esperado (ou não está na chave esperada). Tipo recebido: {type(json_data)}")
                sucesso_geral_cliente = False
        
        except requests.exceptions.JSONDecodeError:
            logger.error(f"CLIENTE 2: Falha ao decodificar resposta como JSON. A API pode ter retornado XML ou texto não-JSON. Conteúdo: {response.text[:500]}", exc_info=True)
            sucesso_geral_cliente = False
        
        if not sucesso_geral_cliente: # Se o parse falhou
            # O return já está no final da função, refletindo sucesso_geral_cliente
            pass
        elif not vendas_api_cliente_2:
            logger.info(f"CLIENTE 2: Nenhuma venda encontrada na resposta da API (após parse).")
        else:
            # Como a API não é paginada, processamos todos os itens de uma vez.
            vendas_para_adicionar = []
            for i, item_venda_api in enumerate(vendas_api_cliente_2):
                # Usar um ID único do item da API para rastreamento, se disponível
                item_api_id_rastreio = item_venda_api.get("ID_Servico", item_venda_api.get("ID_Produto", f"item_{i+1}"))
                logger.debug(f"CLIENTE 2: Processando item_venda_api com ID de rastreio '{item_api_id_rastreio}'.")

                try:
                    # --- MAPEAMENTO DE DADOS PARA CLIENTE 2 (COM BASE NO JSON FORNECIDO) ---
                    
                    # Data da Venda
                    data_venda_str = item_venda_api.get("DataVenda")
                    data_venda_dt = None
                    if data_venda_str:
                        try:
                            data_venda_dt = datetime.strptime(data_venda_str, "%Y-%m-%d")
                        except ValueError:
                            logger.error(f"CLIENTE 2: Formato de data inválido '{data_venda_str}' para item '{item_api_id_rastreio}'. Será None.", exc_info=True)
                            sucesso_geral_cliente = False # Pode optar por pular o item ou marcar falha geral

                    # ID_Produto (API envia int, Mixin espera String)
                    id_produto_api = item_venda_api.get("ID_Produto")
                    id_produto_str = str(id_produto_api) if id_produto_api is not None else None
                    
                    # ID_Servico (API envia int, Mixin espera String)
                    id_servico_api = item_venda_api.get("ID_Servico")
                    id_servico_str = str(id_servico_api) if id_servico_api is not None else None

                    # Cálculo de valor_venda_real e lucro
                    api_valor_venda = float(item_venda_api.get("Valor_Venda") or 0) # Bruto da venda do item
                    api_valor_desconto = float(item_venda_api.get("Valor_Desconto") or 0)
                    api_valor_custo = float(item_venda_api.get("Valor_Custo") or 0)
                    
                    valor_venda_real_calc = api_valor_venda - api_valor_desconto
                    lucro_calc = valor_venda_real_calc - api_valor_custo

                    dados_venda_instancia = {
                        "user_id": 2, 
                        "company_name": "Empresa Cliente 2", 
                        "codigo_cliente": item_venda_api.get("Codigo_Cliente"),
                        "nome_cliente": item_venda_api.get("Nome_Cliente"),
                        "data_venda": data_venda_dt,
                        "codigo_vendedor": item_venda_api.get("Codigo_Vendedor"),
                        "nome_vendedor": item_venda_api.get("NomeVendedor"),
                        "codigo_fornecedor": item_venda_api.get("Codigo_Fornecedor"),
                        "fornecedor": item_venda_api.get("Fornecedor"),
                        "id_produto": id_produto_str,
                        "codigo_produto": item_venda_api.get("Codigo_Produto"),
                        "codigo_grupo_produto": item_venda_api.get("Codigo_Grupo_Produto"),
                        "descricao_grupo_produto": item_venda_api.get("Descricao_Grupo_Produto"),
                        "nome_produto": item_venda_api.get("Nome_Produto"),
                        "codigo_unidade": item_venda_api.get("Codigo_Unidade"),
                        "quantidade": float(item_venda_api.get("Quantidade") or 0),
                        "preco_venda": float(item_venda_api.get("Preco_Venda") or 0),
                        "valor_desconto": api_valor_desconto,
                        "valor_venda": api_valor_venda, # Valor total do item (Qtd * Preco_Venda) ou valor bruto da API
                        "valor_custo": api_valor_custo,
                        "valor_venda_real": valor_venda_real_calc,
                        "lucro": lucro_calc,
                        "id_servico": id_servico_str,
                        "flag_devolucao": None, # API não parece fornecer, VendasBaseMixin permite NULL
                        "valor_troca": None     # API não parece fornecer, VendasBaseMixin permite NULL
                    }
                    logger.debug(f"CLIENTE 2 (Item '{item_api_id_rastreio}'): Dados mapeados: {dados_venda_instancia}")
                    
                    venda_instancia = VendasModelCliente2(**dados_venda_instancia)
                    vendas_para_adicionar.append(venda_instancia)
                    logger.debug(f"CLIENTE 2 (Item '{item_api_id_rastreio}'): Instância adicionada. Total na lista: {len(vendas_para_adicionar)}")

                except Exception as e_map:
                    logger.error(f"CLIENTE 2 (Item '{item_api_id_rastreio}'): Erro ao mapear/instanciar. Detalhes do item API: {item_venda_api}. Erro: {e_map}", exc_info=True)
                    sucesso_geral_cliente = False 

            logger.info(f"CLIENTE 2: Após processar {len(vendas_api_cliente_2)} itens da API, {len(vendas_para_adicionar)} instâncias preparadas para adição.")

            if not vendas_para_adicionar:
                if len(vendas_api_cliente_2) > 0: # A API retornou dados, mas nenhum foi mapeado com sucesso
                     logger.warning(f"CLIENTE 2: API retornou {len(vendas_api_cliente_2)} itens, mas NENHUM foi mapeado com sucesso para adição. Nenhum commit será tentado.")
            else: 
                try:
                    db.add_all(vendas_para_adicionar)
                    db.commit() 
                    logger.info(f"CLIENTE 2: {len(vendas_para_adicionar)} registros COMITADOS com sucesso para schema '{schema_name}'.")
                    total_registros_comitados_cliente += len(vendas_para_adicionar)
                except SQLAlchemyError as e_db:
                    logger.error(f"CLIENTE 2: Erro de BANCO DE DADOS ao commitar dados para schema '{schema_name}': {e_db}", exc_info=True)
                    db.rollback()
                    sucesso_geral_cliente = False
                    original_exception = getattr(e_db, 'orig', None)
                    if original_exception:
                        original_exception_type_name = str(type(original_exception).__name__)
                        if "UndefinedTable" in original_exception_type_name or "InvalidSchemaName" in original_exception_type_name:
                            logger.error(f"CLIENTE 2: TABELA '{schema_name}.vendas' NÃO EXISTE ou schema é inválido. Verifique o banco e o SQL de criação.")
                except Exception as e_commit:
                    logger.error(f"CLIENTE 2: Erro INESPERADO no commit para schema '{schema_name}': {e_commit}", exc_info=True)
                    db.rollback()
                    sucesso_geral_cliente = False
        
    except requests.exceptions.HTTPError as e_http:
        logger.error(f"CLIENTE 2: Erro HTTP da API: {e_http.response.status_code} - {e_http.response.text}", exc_info=True)
        sucesso_geral_cliente = False
    except requests.exceptions.RequestException as e_req:
        logger.error(f"CLIENTE 2: Erro de requisição da API: {e_req}", exc_info=True)
        sucesso_geral_cliente = False
    except Exception as e_proc: 
        logger.error(f"CLIENTE 2: Erro INESPERADO no processamento para schema '{schema_name}': {e_proc}", exc_info=True)
        sucesso_geral_cliente = False

    logger.info(f"CLIENTE 2: ETL para schema '{schema_name}' concluído. Total de registros efetivamente comitados: {total_registros_comitados_cliente}.")
    return {
        "success": sucesso_geral_cliente,
        "message": f"Cliente 2 ETL para schema '{schema_name}' finalizado. Registros comitados: {total_registros_comitados_cliente}.",
        "total_registros_comitados": total_registros_comitados_cliente
    }