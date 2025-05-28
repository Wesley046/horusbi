# backend/app/etl/etl_cliente_1.py

import requests
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging

try:
    from app.models import get_vendas_model
except ImportError as e:
    logging.critical(f"Falha CRÍTICA ao importar get_vendas_model de app.models em etl_cliente_1: {e}", exc_info=True)
    raise 

logger = logging.getLogger(__name__)

API_URL_CLIENTE_1 = "https://api.beteltecnologia.com/vendas"
API_HEADERS_CLIENTE_1 = {
    "Content-Type": "application/json",
    "access-token": "7f52ee8bb0d8595199f6f67520e6f8670db275dc",
    "secret-access-token": "ff3256b5d64878eae7fd618e2a23974024f2fb7e"
}
PARAMS_BASE_CLIENTE_1 = {
    "data_inicio": "2023-01-01",
    "data_fim": "2026-01-01",
    "loja_id": "353900"
}

def process_etl_cliente_1(db: Session, schema_name: str, sqlalchemy_base):
    logger.info(f"CLIENTE 1: Iniciando ETL para schema '{schema_name}'. API: {API_URL_CLIENTE_1}")
    
    VendasModelCliente1 = None
    try:
        VendasModelCliente1 = get_vendas_model(schema_name, sqlalchemy_base)
        logger.info(f"CLIENTE 1: Modelo dinâmico '{VendasModelCliente1.__name__}' obtido para tabela '{VendasModelCliente1.__table__.fullname}'")
    except Exception as e:
        logger.error(f"CLIENTE 1: Erro crítico ao obter modelo dinâmico para schema '{schema_name}': {e}", exc_info=True)
        return {"success": False, "message": f"Erro ao configurar modelo para {schema_name}.", "error": str(e)}

    pagina_atual = 1
    total_registros_comitados_cliente = 0 # Nome mais claro
    continuar_paginacao = True
    sucesso_geral_cliente = True

    while continuar_paginacao:
        logger.info(f"CLIENTE 1: Buscando dados da API - Página {pagina_atual}")
        params_api = {**PARAMS_BASE_CLIENTE_1, "pagina": pagina_atual}

        try:
            response = requests.get(API_URL_CLIENTE_1, headers=API_HEADERS_CLIENTE_1, params=params_api, timeout=30)
            response.raise_for_status()
            json_data = response.json()
            vendas_api_pagina = json_data.get("data", [])

            if not vendas_api_pagina:
                logger.info(f"CLIENTE 1: Nenhuma venda encontrada na API na página {pagina_atual}. Fim da paginação.")
                continuar_paginacao = False
                continue

            logger.info(f"CLIENTE 1: {len(vendas_api_pagina)} vendas encontradas na API na página {pagina_atual}.")
            
            vendas_para_adicionar_nesta_pagina = []
            for i, venda_api_item in enumerate(vendas_api_pagina):
                venda_api_id = venda_api_item.get("id_venda", venda_api_item.get("id", f"item_{i+1}_sem_id"))
                produtos = venda_api_item.get("produtos", [])
                
                logger.debug(f"CLIENTE 1 (Pág {pagina_atual}): Processando venda_api_item ID '{venda_api_id}'. Encontrados {len(produtos)} produto(s).")

                if not produtos:
                    logger.warning(f"CLIENTE 1 (Pág {pagina_atual}): Venda API Item ID '{venda_api_id}' não possui produtos. Pulando esta venda_api_item.")
                    continue

                for j, prod_item in enumerate(produtos):
                    produto_detalhe = prod_item.get("produto", {})
                    produto_api_id = produto_detalhe.get("produto_id", f"prod_{j+1}_sem_id")
                    logger.debug(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}'): Processando produto_detalhe ID '{produto_api_id}'.")
                    
                    try:
                        # --- INÍCIO DO MAPEAMENTO ---
                        api_flag_devolucao_str = venda_api_item.get("flag_devolucao_da_api_aqui") # Substitua pela chave correta da API
                        flag_devolucao_bool = None
                        if api_flag_devolucao_str is not None:
                            if isinstance(api_flag_devolucao_str, str):
                                if api_flag_devolucao_str.lower() in ['true', 's', '1', 'sim']:
                                    flag_devolucao_bool = True
                                elif api_flag_devolucao_str.lower() in ['false', 'n', '0', 'nao', 'não']:
                                    flag_devolucao_bool = False
                                else:
                                    logger.warning(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}'): Valor não reconhecido para flag_devolucao da API: '{api_flag_devolucao_str}'. Será None.")
                            elif isinstance(api_flag_devolucao_str, bool): # Se a API já envia booleano
                                flag_devolucao_bool = api_flag_devolucao_str
                            else: # Se for número, por exemplo
                                try:
                                    flag_devolucao_bool = bool(int(api_flag_devolucao_str))
                                except ValueError:
                                     logger.warning(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}'): Valor numérico não booleano para flag_devolucao da API: '{api_flag_devolucao_str}'. Será None.")
                        
                        # Se id_servico vem da API e pode ser numérico mas está como string, converta se necessário
                        # ou garanta que seu VendasBaseMixin.id_servico seja String se a API sempre envia String.
                        id_servico_api = produto_detalhe.get("id_servico_da_api") # Substitua pela chave correta
                        # Se id_servico no Mixin é String, não precisa converter para int.
                        # Se id_servico no Mixin fosse Integer:
                        # id_servico_val = None
                        # if id_servico_api:
                        # try:
                        # id_servico_val = int(id_servico_api)
                        # except ValueError:
                        # logger.warning(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}'): id_servico '{id_servico_api}' não é um inteiro válido.")


                        dados_venda_instancia = {
                            "user_id": 1,
                            "company_name": "Bicortex",
                            "codigo_cliente": venda_api_item.get("cliente_id"),
                            "nome_cliente": venda_api_item.get("nome_cliente"),
                            "data_venda": datetime.strptime(venda_api_item.get("data"), "%Y-%m-%d") if venda_api_item.get("data") else None,
                            "codigo_vendedor": venda_api_item.get("vendedor_id"),
                            "nome_vendedor": venda_api_item.get("nome_vendedor"),
                            "codigo_fornecedor": venda_api_item.get("codigo_fornecedor_api"), # Substitua se existir
                            "fornecedor": venda_api_item.get("fornecedor_api"), # Substitua se existir
                            "id_produto": produto_detalhe.get("produto_id"), 
                            "codigo_produto": produto_detalhe.get("codigo_produto_api"), # Substitua se existir
                            "codigo_grupo_produto": produto_detalhe.get("codigo_grupo_produto_api"), # Substitua se existir
                            "descricao_grupo_produto": produto_detalhe.get("nome_tipo_valor"),
                            "nome_produto": produto_detalhe.get("nome_produto"),
                            "codigo_unidade": produto_detalhe.get("codigo_unidade_api"), # Substitua se existir
                            "quantidade": float(produto_detalhe.get("quantidade") or 0),
                            "preco_venda": float(produto_detalhe.get("valor_venda") or 0),
                            "valor_desconto": float(produto_detalhe.get("desconto_valor") or 0),
                            "valor_venda": 0,
                            "valor_custo": float(produto_detalhe.get("valor_custo") or 0),
                            "valor_venda_real": float(produto_detalhe.get("valor_total") or 0),
                            "lucro": (float(produto_detalhe.get("valor_total") or 0) - float(produto_detalhe.get("valor_custo") or 0)),
                            "id_servico": id_servico_api, # Se VendasBaseMixin.id_servico é String
                            "flag_devolucao": flag_devolucao_bool, # Mapeado para Boolean ou None
                            "valor_troca": produto_detalhe.get("valor_troca_api") # Substitua se existir, converta para float
                        }
                        # --- FIM DO MAPEAMENTO ---
                                                
                        logger.debug(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}', Produto '{produto_api_id}'): Dados mapeados para instância: {dados_venda_instancia}")
                        
                        venda_instancia = VendasModelCliente1(**dados_venda_instancia)
                        vendas_para_adicionar_nesta_pagina.append(venda_instancia)
                        logger.debug(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}', Produto '{produto_api_id}'): Instância adicionada à lista da página. Total de itens na lista desta página agora: {len(vendas_para_adicionar_nesta_pagina)}")

                    except Exception as e_map:
                        logger.error(f"CLIENTE 1 (Pág {pagina_atual}, Venda '{venda_api_id}', Produto '{produto_api_id}'): Erro ao mapear/instanciar dados da API. Detalhes do produto: {produto_detalhe}. Erro: {e_map}", exc_info=True)
                        sucesso_geral_cliente = False 

            logger.info(f"CLIENTE 1 (Pág {pagina_atual}): Após processar {len(vendas_api_pagina)} itens da API, {len(vendas_para_adicionar_nesta_pagina)} instâncias foram preparadas para adição ao banco.")

            if not vendas_para_adicionar_nesta_pagina:
                if len(vendas_api_pagina) > 0:
                     logger.warning(f"CLIENTE 1 (Pág {pagina_atual}): A API retornou {len(vendas_api_pagina)} itens, mas NENHUM foi mapeado com sucesso para adição. Nenhum commit será tentado para esta página.")
            else: 
                try:
                    db.add_all(vendas_para_adicionar_nesta_pagina)
                    db.commit() 
                    logger.info(f"CLIENTE 1 (Pág {pagina_atual}): {len(vendas_para_adicionar_nesta_pagina)} registros COMITADOS com sucesso para schema '{schema_name}'.")
                    total_registros_comitados_cliente += len(vendas_para_adicionar_nesta_pagina)
                except SQLAlchemyError as e_db:
                    logger.error(f"CLIENTE 1 (Pág {pagina_atual}): Erro de BANCO DE DADOS ao commitar dados para schema '{schema_name}': {e_db}", exc_info=True)
                    db.rollback()
                    sucesso_geral_cliente = False
                    if hasattr(e_db, 'orig') and "UndefinedTable" in str(type(e_db.orig).__name__):
                        logger.error(f"CLIENTE 1 (Pág {pagina_atual}): A TABELA '{schema_name}.vendas' PODE NÃO EXISTIR ou é inacessível. Verifique o banco de dados. Interrompendo paginação para este cliente.")
                        continuar_paginacao = False 
                except Exception as e_commit:
                    logger.error(f"CLIENTE 1 (Pág {pagina_atual}): Erro INESPERADO ao commitar dados para schema '{schema_name}': {e_commit}", exc_info=True)
                    db.rollback()
                    sucesso_geral_cliente = False
            
            if continuar_paginacao:
                pagina_atual += 1
                if pagina_atual > 200: 
                    logger.warning(f"CLIENTE 1: Limite de paginação (200) atingido para schema '{schema_name}'. Interrompendo.")
                    continuar_paginacao = False

        except requests.exceptions.HTTPError as e_http:
            logger.error(f"CLIENTE 1 (Pág {pagina_atual}): Erro HTTP da API: {e_http.response.status_code} - {e_http.response.text}", exc_info=True)
            sucesso_geral_cliente = False
            continuar_paginacao = False 
        except requests.exceptions.RequestException as e_req:
            logger.error(f"CLIENTE 1 (Pág {pagina_atual}): Erro de requisição da API: {e_req}", exc_info=True)
            sucesso_geral_cliente = False
            continuar_paginacao = False 
        except Exception as e_page_proc: 
            logger.error(f"CLIENTE 1 (Pág {pagina_atual}): Erro INESPERADO no processamento geral da página para schema '{schema_name}': {e_page_proc}", exc_info=True)
            sucesso_geral_cliente = False
            continuar_paginacao = False

    logger.info(f"CLIENTE 1: ETL para schema '{schema_name}' concluído. Total de registros efetivamente comitados: {total_registros_comitados_cliente}.")
    return {
        "success": sucesso_geral_cliente,
        "message": f"Cliente 1 ETL para schema '{schema_name}' finalizado. Registros comitados: {total_registros_comitados_cliente}.",
        "total_registros_comitados": total_registros_comitados_cliente
    }