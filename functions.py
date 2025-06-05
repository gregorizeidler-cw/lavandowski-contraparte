import datetime
import pandas as pd
from google.cloud import bigquery
from gpt_utils import get_chatgpt_response
import json
import decimal
import logging
import os
import re
from dotenv import load_dotenv

# Importar BDC-UTILS se disponível
try:
    from bdc_utils import analyze_document
    BDC_AVAILABLE = True
except ImportError:
    BDC_AVAILABLE = False
    logging.warning("BDC-UTILS não disponível. Análise de contrapartes será desabilitada.")

load_dotenv()
logging.basicConfig(level=logging.INFO)

class CustomJSONEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, decimal.Decimal):
      return float(obj)
    elif isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
      return obj.isoformat()
    else:
      return super().default(obj)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 40)
pd.set_option('display.min_rows', 40)

project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
location = os.getenv("LOCATION")
client = bigquery.Client(project=project_id, location=location)

def format_date_portuguese(date_str: str) -> str:
  """Formata uma string de data para o formato em português."""
  if date_str is None:
    return 'Not available.'
  month_names = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
  date = datetime.datetime.strptime(date_str, '%d-%m-%Y')
  return f"{date.day} de {month_names[date.month]} de {date.year}"

def format_cpf(cpf: str) -> str:
  """Formata uma string de CPF."""
  if cpf is None:
    return None
  cpf = cpf.replace('.', '').replace('-', '')
  if len(cpf) == 11:
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
  else:
    return cpf

def execute_query(query):
  """Executa uma query no BigQuery e retorna um DataFrame."""
  try:
    df = client.query(query).result().to_dataframe()
    return df
  except Exception as e:
    logging.error(f"Error executing query: {e}")
    return pd.DataFrame()

def fetch_lawsuit_data(user_id: int) -> pd.DataFrame:
  """Busca dados de processos para o user_id informado."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_lawsuits_data`
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_business_data(user_id: int) -> pd.DataFrame:
  """Busca dados de relacionamento empresarial para o user_id informado."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_business_relationships_data`
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_sanctions_history(user_id: int) -> pd.DataFrame:
  """Busca dados de sanções para o user_id informado."""
  query = f"""
  SELECT * FROM infinitepay-production.metrics_amlft.sanctions_history
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_denied_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações negadas para o user_id (merchant_id)."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_transactions_data`
  WHERE merchant_id = {user_id} ORDER BY card_number
  """
  return execute_query(query)

def fetch_denied_pix_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações PIX negadas para o user_id."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_pix_transfers_data`
  WHERE debitor_user_id = '{user_id}' ORDER BY str_pix_transfer_id DESC
  """
  return execute_query(query)

def fetch_prison_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações no presídio para o user_id informado."""
  query = f"""
  SELECT * EXCEPT(user_id) FROM infinitepay-production.metrics_amlft.prison_transactions
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_bets_pix_transfers(user_id: int) -> pd.DataFrame:
  """Busca transações de apostas via PIX para o user_id informado."""
  query = f"""
    SELECT
  transfer_type,
  pix_status,
  user_id,
  user_name,
  gateway,
  gateway_document_number,
  gateway_pix_key,
  gateway_name,
  SUM(transfer_amount) total_amount,
  COUNT(pix_transfer_id) count_transactions
FROM `infinitepay-production.metrics_amlft.bets_pix_transfers`
WHERE user_id = {user_id}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  """
  return execute_query(query)

def convert_decimals(data):
  """Converte recursivamente objetos Decimal em float."""
  if isinstance(data, list):
    return [{k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v for k, v in item.items()} for item in data]
  elif isinstance(data, dict):
    return {k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v for k, v in data.items()}
  else:
    return data

def analyze_counterparties(cash_in_list: list, cash_out_list: list, user_id: int) -> dict:
  """
  Analisa as contrapartes (top 3 cash in e top 3 cash out) usando BDC-UTILS.
  Foca especificamente em processos judiciais e sanções.
  
  Args:
      cash_in_list (list): Lista de transações cash in
      cash_out_list (list): Lista de transações cash out
      user_id (int): ID do usuário para logging
      
  Returns:
      dict: Resultado da análise das contrapartes com foco em processos e sanções
  """
  counterparty_analysis = {
    "top_cash_in_analysis": [],
    "top_cash_out_analysis": [],
    "analysis_enabled": BDC_AVAILABLE,
    "summary": {
      "total_counterparties_analyzed": 0,
      "counterparties_with_processes": 0,
      "counterparties_with_sanctions": 0,
      "high_risk_counterparties": 0
    }
  }
  
  if not BDC_AVAILABLE:
    logging.warning(f"BDC-UTILS não disponível para análise do usuário {user_id}")
    return counterparty_analysis
  
  def extract_document_from_transaction(transaction):
    """Extrai documento da transação, tentando vários campos possíveis"""
    possible_fields = [
      'party_document_number',  # Campo correto para PIX concentration
      'gateway_document_number', 'document_number', 'cpf', 'cnpj', 
      'counterparty_document', 'payer_document', 'receiver_document',
      'origin_document', 'destination_document'
    ]
    
    for field in possible_fields:
      if field in transaction and transaction[field]:
        return str(transaction[field]).strip()
    return None
  
  def extract_processes_and_sanctions(bdc_result):
    """Extrai especificamente processos judiciais e sanções do resultado BDC"""
    analysis = {
      "document": None,
      "name": None,
      "processes": [],
      "sanctions": [],
      "has_processes": False,
      "has_sanctions": False,
      "risk_level": "BAIXO"
    }
    
    if not bdc_result or 'Result' not in bdc_result:
      logging.warning(f"BDC result inválido ou sem campo 'Result': {bdc_result}")
      return analysis
    
    results = bdc_result.get('Result', [])
    if not results:
      logging.warning(f"Campo 'Result' vazio no BDC: {bdc_result}")
      return analysis
    
    person_data = results[0] if results else {}
    logging.info(f"Dados da pessoa encontrados: {list(person_data.keys())}")
    
    # DEBUG: Log completo dos dados recebidos
    logging.info(f"DEBUG - Dados completos recebidos do BDC: {json.dumps(person_data, indent=2, default=str)}")
    
    # Extrair dados básicos - CORRIGIDO: BasicData ao invés de basic_data
    basic_data = person_data.get('BasicData', {})
    if basic_data:
      analysis["document"] = basic_data.get('TaxIdNumber', '')
      analysis["name"] = basic_data.get('Name', '')
      logging.info(f"Dados básicos extraídos: {analysis['name']} - {analysis['document']}")
    
    # Extrair processos judiciais - CORRIGIDO: Processes ao invés de processes
    processes_data = person_data.get('Processes', {})
    logging.info(f"DEBUG - Processes data completa: {json.dumps(processes_data, indent=2, default=str)}")
    
    processes = processes_data.get('Lawsuits', []) if processes_data else []
    logging.info(f"DEBUG - Lista de Lawsuits: {json.dumps(processes, indent=2, default=str)}")
    logging.info(f"Processos encontrados: {len(processes)}")
    logging.info(f"DEBUG - CONTAGEM EXATA DE PROCESSOS PARA {analysis.get('name', 'NOME_NAO_ENCONTRADO')}: {len(processes)} processos")
    
    if processes:
      analysis["has_processes"] = True
      # IMPORTANTE: Contar quantos processos realmente vamos adicionar
      processes_to_add = processes[:10]  # Limitar a 10 processos mais relevantes
      logging.info(f"DEBUG - PROCESSOS QUE SERÃO ADICIONADOS PARA {analysis.get('name', 'NOME_NAO_ENCONTRADO')}: {len(processes_to_add)} de {len(processes)} total")
      
      for i, process in enumerate(processes_to_add):
        process_info = {
          "process_number": process.get('Number', ''),
          "court": process.get('CourtName', ''),
          "subject": process.get('MainSubject', ''),
          "type": process.get('Type', ''),
          "court_level": process.get('CourtLevel', ''),
          "court_type": process.get('CourtType', ''),
          "district": process.get('CourtDistrict', '')
        }
        analysis["processes"].append(process_info)
        logging.info(f"Processo {i+1}/{len(processes_to_add)} adicionado para {analysis.get('name', 'NOME_NAO_ENCONTRADO')}: {process_info['process_number']} - {process_info['subject']}")
      
      logging.info(f"DEBUG - TOTAL FINAL DE PROCESSOS ADICIONADOS PARA {analysis.get('name', 'NOME_NAO_ENCONTRADO')}: {len(analysis['processes'])}")
    else:
      logging.info(f"DEBUG - NENHUM PROCESSO ENCONTRADO PARA {analysis.get('name', 'NOME_NAO_ENCONTRADO')}")
    
    # Extrair sanções (KYC) - CORRIGIDO: KycData ao invés de kyc
    kyc_data = person_data.get('KycData', {})
    logging.info(f"DEBUG - KycData completa: {json.dumps(kyc_data, indent=2, default=str)}")
    
    sanctions = []
    
    # Verificar PEP History
    pep_history = kyc_data.get('PEPHistory', []) if kyc_data else []
    logging.info(f"DEBUG - PEPHistory: {json.dumps(pep_history, indent=2, default=str)}")
    if pep_history:
      for pep in pep_history:
        sanctions.append({
          "type": "PEP",
          "description": pep.get('Description', ''),
          "source": "PEP Database"
        })
        logging.info(f"DEBUG - PEP adicionado: {pep}")
    
    # Verificar Sanctions History
    sanctions_history = kyc_data.get('SanctionsHistory', []) if kyc_data else []
    logging.info(f"DEBUG - SanctionsHistory: {json.dumps(sanctions_history, indent=2, default=str)}")
    if sanctions_history:
      for sanction in sanctions_history:
        # FILTRO CRÍTICO: Só aceitar sanções com MatchRate = 100
        match_rate = sanction.get('MatchRate', 0)
        logging.info(f"DEBUG - Sanção encontrada: {sanction.get('Type', '')} - MatchRate: {match_rate}")
        
        if match_rate == 100:
          sanctions.append({
            "type": sanction.get('Type', ''),
            "standardized_type": sanction.get('StandardizedSanctionType', ''),
            "source": sanction.get('Source', ''),
            "description": sanction.get('Details', {}).get('WarrantDescription', '') if sanction.get('Details') else '',
            "match_rate": match_rate
          })
          logging.info(f"DEBUG - Sanção VÁLIDA adicionada (MatchRate=100): {sanction}")
        else:
          logging.warning(f"DEBUG - Sanção REJEITADA (MatchRate={match_rate}): {sanction.get('Type', '')} - {sanction.get('Source', '')}")
          logging.warning(f"DEBUG - Detalhes da sanção rejeitada: Nome original='{sanction.get('Details', {}).get('OriginalName', '')}', Nome sanção='{sanction.get('Details', {}).get('SanctionName', '')}'")
    
    # Verificar flags de sanções atuais
    if kyc_data:
      is_currently_pep = kyc_data.get('IsCurrentlyPEP', False)
      is_currently_sanctioned = kyc_data.get('IsCurrentlySanctioned', False)
      logging.info(f"DEBUG - IsCurrentlyPEP: {is_currently_pep}, IsCurrentlySanctioned: {is_currently_sanctioned}")
      
      if is_currently_pep:
        sanctions.append({
          "type": "Current PEP",
          "description": "Currently a Politically Exposed Person",
          "source": "PEP Database"
        })
        logging.info(f"DEBUG - Current PEP flag adicionado")
      
      if is_currently_sanctioned:
        sanctions.append({
          "type": "Current Sanction",
          "description": "Currently under sanctions",
          "source": "Sanctions Database"
        })
        logging.info(f"DEBUG - Current Sanction flag adicionado")
    
    logging.info(f"DEBUG - Total de sanções coletadas: {len(sanctions)}")
    logging.info(f"DEBUG - Lista final de sanções: {json.dumps(sanctions, indent=2, default=str)}")
    
    if sanctions:
      analysis["has_sanctions"] = True
      analysis["sanctions"] = sanctions
      for sanction in sanctions:
        logging.info(f"Sanção adicionada: {sanction['type']} - {sanction['source']}")
    
    # Determinar nível de risco
    if analysis["has_sanctions"]:
      analysis["risk_level"] = "ALTO"
    elif analysis["has_processes"] and len(analysis["processes"]) > 3:
      analysis["risk_level"] = "MÉDIO"
    elif analysis["has_processes"]:
      analysis["risk_level"] = "BAIXO-MÉDIO"
    
    logging.info(f"Análise final: processos={analysis['has_processes']}, sanções={analysis['has_sanctions']}, risco={analysis['risk_level']}")
    return analysis
  
  # Analisar top 3 cash in
  top_cash_in = sorted(cash_in_list, key=lambda x: float(x.get('pix_amount', 0)), reverse=True)[:3]
  logging.info(f"Analisando {len(top_cash_in)} contrapartes Cash In para usuário {user_id}")
  
  for i, transaction in enumerate(top_cash_in):
    document = extract_document_from_transaction(transaction)
    logging.info(f"Cash In {i+1}: Documento extraído: {document}, Valor: {transaction.get('pix_amount', 0)}")
    
    if document:
      try:
        logging.info(f"Consultando BDC para documento: {document}")
        bdc_result = analyze_document(document)
        logging.info(f"Resultado BDC recebido para {document}: {bool(bdc_result)}")
        
        analysis = extract_processes_and_sanctions(bdc_result)
        logging.info(f"Análise processada para {document}: processos={analysis['has_processes']}, sanções={analysis['has_sanctions']}")
        logging.info(f"DEBUG - ANÁLISE COMPLETA PARA CONTRAPARTE {analysis.get('name', 'NOME_NAO_ENCONTRADO')} (DOC: {document}): {len(analysis.get('processes', []))} processos, {len(analysis.get('sanctions', []))} sanções, risco={analysis.get('risk_level', 'N/A')}")
        
        analysis["transaction_amount"] = transaction.get('pix_amount', 0)
        analysis["transaction_date"] = transaction.get('created_at', '')  # Manter created_at se existir
        analysis["transaction_type"] = "CASH_IN"
        analysis["party_name"] = transaction.get('party', '')  # Adicionar nome da contraparte
        counterparty_analysis["top_cash_in_analysis"].append(analysis)
        
        # Atualizar sumário
        counterparty_analysis["summary"]["total_counterparties_analyzed"] += 1
        if analysis["has_processes"]:
          counterparty_analysis["summary"]["counterparties_with_processes"] += 1
        if analysis["has_sanctions"]:
          counterparty_analysis["summary"]["counterparties_with_sanctions"] += 1
        if analysis["risk_level"] in ["ALTO", "MÉDIO"]:
          counterparty_analysis["summary"]["high_risk_counterparties"] += 1
          
      except Exception as e:
        logging.error(f"Erro ao analisar contraparte cash in {document}: {str(e)}")
    else:
      logging.warning(f"Documento não encontrado para transação Cash In {i+1}: {transaction}")
  
  # Analisar top 3 cash out
  top_cash_out = sorted(cash_out_list, key=lambda x: float(x.get('pix_amount', 0)), reverse=True)[:3]
  logging.info(f"Analisando {len(top_cash_out)} contrapartes Cash Out para usuário {user_id}")
  
  for i, transaction in enumerate(top_cash_out):
    document = extract_document_from_transaction(transaction)
    logging.info(f"Cash Out {i+1}: Documento extraído: {document}, Valor: {transaction.get('pix_amount', 0)}")
    
    if document:
      try:
        logging.info(f"Consultando BDC para documento: {document}")
        bdc_result = analyze_document(document)
        logging.info(f"Resultado BDC recebido para {document}: {bool(bdc_result)}")
        
        analysis = extract_processes_and_sanctions(bdc_result)
        logging.info(f"Análise processada para {document}: processos={analysis['has_processes']}, sanções={analysis['has_sanctions']}")
        logging.info(f"DEBUG - ANÁLISE COMPLETA PARA CONTRAPARTE {analysis.get('name', 'NOME_NAO_ENCONTRADO')} (DOC: {document}): {len(analysis.get('processes', []))} processos, {len(analysis.get('sanctions', []))} sanções, risco={analysis.get('risk_level', 'N/A')}")
        
        analysis["transaction_amount"] = transaction.get('pix_amount', 0)
        analysis["transaction_date"] = transaction.get('created_at', '')  # Manter created_at se existir
        analysis["transaction_type"] = "CASH_OUT"
        analysis["party_name"] = transaction.get('party', '')  # Adicionar nome da contraparte
        counterparty_analysis["top_cash_out_analysis"].append(analysis)
        
        # Atualizar sumário
        counterparty_analysis["summary"]["total_counterparties_analyzed"] += 1
        if analysis["has_processes"]:
          counterparty_analysis["summary"]["counterparties_with_processes"] += 1
        if analysis["has_sanctions"]:
          counterparty_analysis["summary"]["counterparties_with_sanctions"] += 1
        if analysis["risk_level"] in ["ALTO", "MÉDIO"]:
          counterparty_analysis["summary"]["high_risk_counterparties"] += 1
          
      except Exception as e:
        logging.error(f"Erro ao analisar contraparte cash out {document}: {str(e)}")
    else:
      logging.warning(f"Documento não encontrado para transação Cash Out {i+1}: {transaction}")
  
  logging.info(f"Análise de contrapartes concluída para usuário {user_id}: {counterparty_analysis['summary']}")
  return counterparty_analysis

def merchant_report(user_id: int, alert_type: str, pep_data=None) -> dict:
  """Gera um relatório para merchant."""
  query_merchants = f"""
  SELECT * FROM metrics_amlft.merchant_report WHERE user_id = {user_id} LIMIT 1
  """
  query_issuing_concentration = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_issuing_payments_data`
  WHERE user_id = {user_id}
  """
  query_pix_concentration = f"""
  SELECT * FROM metrics_amlft.pix_concentration WHERE user_id = {user_id}
  """
  query_transaction_concentration = f"""
  SELECT * EXCEPT(merchant_id) FROM `infinitepay-production.metrics_amlft.cardholder_concentration`
  WHERE merchant_id = {user_id} ORDER BY total_approved_by_ch DESC
  """
  query_offense_history = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis_data`
  WHERE user_id = {user_id} ORDER BY id DESC
  """
  products_online_store = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_online_store_data`
  WHERE user_id = {user_id}
  """
  contacts_query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_phonecast_data`
  WHERE user_id = {user_id}
  """
  devices_query = f"""
  SELECT * EXCEPT(user_id) FROM metrics_amlft.user_device WHERE user_id = {user_id}
  """
  merchant_info = execute_query(query_merchants)
  issuing_concentration = execute_query(query_issuing_concentration)
  pix_concentration = execute_query(query_pix_concentration)
  transaction_concentration = execute_query(query_transaction_concentration)
  offense_history = execute_query(query_offense_history)
  products_online = execute_query(products_online_store)
  contacts = execute_query(contacts_query)
  devices = execute_query(devices_query)
  cash_in = pd.DataFrame()
  cash_out = pd.DataFrame()
  total_cash_in_pix = 0.0
  total_cash_out_pix = 0.0
  total_cash_in_pix_atypical_hours = 0.0
  total_cash_out_pix_atypical_hours = 0.0
  if not pix_concentration.empty:
    cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
    cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
    total_cash_in_pix = cash_in['pix_amount'].sum()
    total_cash_out_pix = cash_out['pix_amount'].sum()
    total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
    total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()
  merchant_info_dict = merchant_info.to_dict(orient='records')[0] if not merchant_info.empty else {}
  issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
  transaction_concentration_list = transaction_concentration.to_dict(orient='records') if not transaction_concentration.empty else []
  cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
  cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
  offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
  products_online_list = products_online.to_dict(orient='records') if not products_online.empty else []
  contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
  devices_list = devices.to_dict(orient='records') if not devices.empty else []
  merchant_info_dict = convert_decimals(merchant_info_dict)
  issuing_concentration_list = convert_decimals(issuing_concentration_list)
  transaction_concentration_list = convert_decimals(transaction_concentration_list)
  cash_in_list = convert_decimals(cash_in_list)
  cash_out_list = convert_decimals(cash_out_list)
  offense_history_list = convert_decimals(offense_history_list)
  products_online_list = convert_decimals(products_online_list)
  contacts_list = convert_decimals(contacts_list)
  devices_list = convert_decimals(devices_list)
  lawsuit_data = fetch_lawsuit_data(user_id)
  lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []
  denied_transactions_df = fetch_denied_transactions(user_id)
  denied_transactions_list = denied_transactions_df.to_dict(orient='records') if not denied_transactions_df.empty else []
  business_data = fetch_business_data(user_id)
  business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []
  prison_transactions_df = fetch_prison_transactions(user_id)
  prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []
  prison_transactions_list = convert_decimals(prison_transactions_list)
  sanctions_history_df = fetch_sanctions_history(user_id)
  sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []
  sanctions_history_list = convert_decimals(sanctions_history_list)
  denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
  denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []
  denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)
  bets_pix_transfers_df = fetch_bets_pix_transfers(user_id)
  bets_pix_transfers_list = bets_pix_transfers_df.to_dict(orient='records') if not bets_pix_transfers_df.empty else []
  bets_pix_transfers_list = convert_decimals(bets_pix_transfers_list)
  counterparty_analysis = analyze_counterparties(cash_in_list, cash_out_list, user_id)
  report = {
    "merchant_info": merchant_info_dict,
    "total_cash_in_pix": total_cash_in_pix,
    "total_cash_out_pix": total_cash_out_pix,
    "total_cash_in_pix_atypical_hours": total_cash_in_pix_atypical_hours,
    "total_cash_out_pix_atypical_hours": total_cash_out_pix_atypical_hours,
    "issuing_concentration": issuing_concentration_list,
    "transaction_concentration": transaction_concentration_list,
    "pix_cash_in": cash_in_list,
    "pix_cash_out": cash_out_list,
    "offense_history": offense_history_list,
    "products_online": products_online_list,
    "contacts": contacts_list,
    "devices": devices_list,
    "lawsuit_data": lawsuit_data,
    "denied_transactions": denied_transactions_list,
    "business_data": business_data_list,
    "prison_transactions": prison_transactions_list,
    "sanctions_history": sanctions_history_list,
    "denied_pix_transactions": denied_pix_transactions_list,
    "bets_pix_transfers": bets_pix_transfers_list,
    "counterparty_analysis": counterparty_analysis
  }
  return report

def cardholder_report(user_id: int, alert_type: str, pep_data=None) -> dict:
  """Gera um relatório para cardholders."""
  query_cardholders = f"""
  SELECT * FROM metrics_amlft.cardholder_report WHERE user_id = {user_id} LIMIT 1
  """
  query_issuing_concentration = f"""
  SELECT * EXCEPT(user_id) FROM metrics_amlft.issuing_concentration WHERE user_id = {user_id}
  """
  query_pix_concentration = f"""
  SELECT * FROM metrics_amlft.pix_concentration WHERE user_id = {user_id}
  """
  query_offense_history = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis_data`
  WHERE user_id = {user_id} ORDER BY id DESC
  """
  contacts_query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_phonecast_data` WHERE user_id = {user_id}
  """
  devices_query = f"""
  SELECT * EXCEPT(user_id) FROM metrics_amlft.user_device WHERE user_id = {user_id}
  """
  cardholder_info = execute_query(query_cardholders)
  issuing_concentration = execute_query(query_issuing_concentration)
  pix_concentration = execute_query(query_pix_concentration)
  offense_history = execute_query(query_offense_history)
  contacts = execute_query(contacts_query)
  devices = execute_query(devices_query)
  cash_in = pd.DataFrame()
  cash_out = pd.DataFrame()
  total_cash_in_pix = 0.0
  total_cash_out_pix = 0.0
  total_cash_in_pix_atypical_hours = 0.0
  total_cash_out_pix_atypical_hours = 0.0
  if not pix_concentration.empty:
    cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
    cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
    total_cash_in_pix = cash_in['pix_amount'].sum()
    total_cash_out_pix = cash_out['pix_amount'].sum()
    total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
    total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()
  cardholder_info_dict = cardholder_info.to_dict(orient='records')[0] if not cardholder_info.empty else {}
  issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
  cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
  cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
  offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
  contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
  devices_list = devices.to_dict(orient='records') if not devices.empty else []
  cardholder_info_dict = convert_decimals(cardholder_info_dict)
  issuing_concentration_list = convert_decimals(issuing_concentration_list)
  cash_in_list = convert_decimals(cash_in_list)
  cash_out_list = convert_decimals(cash_out_list)
  offense_history_list = convert_decimals(offense_history_list)
  contacts_list = convert_decimals(contacts_list)
  devices_list = convert_decimals(devices_list)
  lawsuit_data = fetch_lawsuit_data(user_id)
  lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []
  business_data = fetch_business_data(user_id)
  business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []
  prison_transactions_df = fetch_prison_transactions(user_id)
  prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []
  prison_transactions_list = convert_decimals(prison_transactions_list)
  sanctions_history_df = fetch_sanctions_history(user_id)
  sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []
  sanctions_history_list = convert_decimals(sanctions_history_list)
  denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
  denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []
  denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)
  bets_pix_transfers_df = fetch_bets_pix_transfers(user_id)
  bets_pix_transfers_list = bets_pix_transfers_df.to_dict(orient='records') if not bets_pix_transfers_df.empty else []
  bets_pix_transfers_list = convert_decimals(bets_pix_transfers_list)
  counterparty_analysis = analyze_counterparties(cash_in_list, cash_out_list, user_id)
  report = {
    "cardholder_info": cardholder_info_dict,
    "total_cash_in_pix": total_cash_in_pix,
    "total_cash_out_pix": total_cash_out_pix,
    "total_cash_in_pix_atypical_hours": total_cash_in_pix_atypical_hours,
    "total_cash_out_pix_atypical_hours": total_cash_out_pix_atypical_hours,
    "issuing_concentration": issuing_concentration_list,
    "pix_cash_in": cash_in_list,
    "pix_cash_out": cash_out_list,
    "offense_history": offense_history_list,
    "contacts": contacts_list,
    "devices": devices_list,
    "lawsuit_data": lawsuit_data,
    "business_data": business_data_list,
    "prison_transactions": prison_transactions_list,
    "sanctions_history": sanctions_history_list,
    "denied_pix_transactions": denied_pix_transactions_list,
    "bets_pix_transfers": bets_pix_transfers_list,
    "counterparty_analysis": counterparty_analysis
  }
  return report

def generate_prompt(report_data: dict, user_type: str, alert_type: str, betting_houses: pd.DataFrame = None, pep_data: pd.DataFrame = None, features: str = None) -> str:
  """Gera o prompt para o GPT com base no relatório."""
  import json
  user_info_key = f"{user_type.lower()}_info"
  user_info_json = json.dumps(report_data[user_info_key], ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  issuing_concentration_json = json.dumps(report_data.get('issuing_concentration', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  pix_cash_in_json = json.dumps(report_data.get('pix_cash_in', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  pix_cash_out_json = json.dumps(report_data.get('pix_cash_out', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  offense_history_json = json.dumps(report_data.get('offense_history', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  contacts_json = json.dumps(report_data.get('contacts', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  devices_json = json.dumps(report_data.get('devices', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  lawsuit_data_json = json.dumps(report_data.get('lawsuit_data', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  denied_transactions_json = json.dumps(report_data.get('denied_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  business_data_json = json.dumps(report_data.get('business_data', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  prison_transactions_json = json.dumps(report_data.get('prison_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  sanctions_history_json = json.dumps(report_data.get('sanctions_history', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  denied_pix_transactions_json = json.dumps(report_data.get('denied_pix_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  bets_pix_transfers_json = json.dumps(report_data.get('bets_pix_transfers', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  counterparty_analysis_json = json.dumps(report_data.get('counterparty_analysis', {}), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
  prompt = f"""
Por favor, analise o caso abaixo.

Considere os seguintes níveis de risco:
1 - Baixo;
2 - Médio (possível ligação com PEPs);
3 - Alto (PEP, indivíduos ou empresas com histórico em listas de sanções, etc.)

Tipo de Alerta: {alert_type}

Informação do {user_type}:
{user_info_json}
"""
  if user_type == 'Merchant':
    transaction_concentration_json = json.dumps(report_data.get('transaction_concentration', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    products_online_json = json.dumps(report_data.get('products_online', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    prompt += f"""
Total de Transações PIX:
- Cash In: R${report_data['total_cash_in_pix']:,.2f}
- Cash Out: R${report_data['total_cash_out_pix']:,.2f}

Transações em Horários Atípicos:
- Cash In PIX: R${report_data['total_cash_in_pix_atypical_hours']:,.2f}
- Cash Out PIX: R${report_data['total_cash_out_pix_atypical_hours']:,.2f}

Concentração de Transações por Portador de Cartão:
{transaction_concentration_json}

Concentração de Issuing:
{issuing_concentration_json}

Transações Negadas:
{denied_transactions_json}

Histórico Profissional:
{business_data_json}

Transações Confirmadamente Executadas Dentro do Presídio (Atenção especial às colunas status e transaction_type. Transações negadas ou com errors também devem ser consideradas):
{prison_transactions_json}

Contatos:
{contacts_json}

Dispositivos Utilizados:
{devices_json}

Produtos na Loja InfinitePay:
{products_online_json}

Sanções Judiciais (Dê detalhes sobre o caso durante a análise. Pensão alimentícia ou casos de família podem ser desconsiderados):
{sanctions_history_json}

Transação PIX Negadas e motivo (coluna risk_check):
{denied_pix_transactions_json}

Concentrações PIX:
Cash In:
{pix_cash_in_json}
Cash Out:
{pix_cash_out_json}

Informações sobre processos judiciais:
{lawsuit_data_json}

Histórico de Offenses:
{offense_history_json}

Transações de Apostas via PIX:
{bets_pix_transfers_json}

Análise de Contrapartes (Top 3 Cash In e Cash Out):
ATENÇÃO ESPECIAL: Esta seção contém análise das principais contrapartes do cliente no Big Data Corp, verificando processos judiciais e sanções.
FOQUE ESPECIFICAMENTE EM:
- Contrapartes com PROCESSOS JUDICIAIS (campo "has_processes": true)
- Contrapartes com SANÇÕES (campo "has_sanctions": true) 
- Nível de risco das contrapartes (campo "risk_level")
- Detalhes dos processos: número, tribunal, assunto, status
- Detalhes das sanções: tipo, fonte, descrição
- Valores transacionados com contrapartes de alto risco

INSTRUÇÕES PARA ANÁLISE:
1. Identifique quantas contrapartes têm processos judiciais
2. Identifique quantas contrapartes têm sanções
3. Calcule o valor total transacionado com contrapartes de risco ALTO ou MÉDIO
4. Detalhe os tipos de processos e sanções encontrados
5. Avalie o impacto no risco geral do cliente

{counterparty_analysis_json}
"""
  else:
    prompt += f"""
Total de Transações PIX:
- Cash In: R${report_data['total_cash_in_pix']:,.2f}
- Cash Out: R${report_data['total_cash_out_pix']:,.2f}

Transações em Horários Atípicos:
- Cash In PIX: R${report_data['total_cash_in_pix_atypical_hours']:,.2f}
- Cash Out PIX: R${report_data['total_cash_out_pix_atypical_hours']:,.2f}

Concentração de Issuing:
{issuing_concentration_json}

Análise Adicional para Concentração de Issuing:
- Verifique se há repetição de merchant_name ou padrões de valores anômalos em total_amount.
- Utilize os campos total_amount e percentage_of_total para identificar picos ou discrepâncias.
- Considere analisar se os códigos MCC (message__card_acceptor_mcc) indicam setores de risco elevado.

Contatos (Atenção para contatos com status 'blocked'):
{contacts_json}

Dispositivos Utilizados (atenção para número elevado de dispositivos):
{devices_json}

Sanções Judiciais (Dê detalhes sobre o caso durante a análise. Pensão alimentícia ou casos de família podem ser desconsiderados):
{sanctions_history_json}

Transação PIX Negadas e motivo (coluna risk_check):
{denied_pix_transactions_json}

Concentrações PIX:
Cash In:
{pix_cash_in_json}
Cash Out:
{pix_cash_out_json}

Histórico Profissional:
{business_data_json}

Informações sobre processos judiciais:
{lawsuit_data_json}

Transações Confirmadamente Executadas Dentro do Presídio (Atenção especial às colunas status e transaction_type. Transações negadas ou com errors também devem ser consideradas):
{prison_transactions_json}

Histórico de Offenses:
{offense_history_json}

Transações de Apostas via PIX:
{bets_pix_transfers_json}

Análise de Contrapartes (Top 3 Cash In e Cash Out):
ATENÇÃO ESPECIAL: Esta seção contém análise das principais contrapartes do cliente no Big Data Corp, verificando processos judiciais e sanções.
FOQUE ESPECIFICAMENTE EM:
- Contrapartes com PROCESSOS JUDICIAIS (campo "has_processes": true)
- Contrapartes com SANÇÕES (campo "has_sanctions": true) 
- Nível de risco das contrapartes (campo "risk_level")
- Detalhes dos processos: número, tribunal, assunto, status
- Detalhes das sanções: tipo, fonte, descrição
- Valores transacionados com contrapartes de alto risco

INSTRUÇÕES PARA ANÁLISE:
1. Identifique quantas contrapartes têm processos judiciais
2. Identifique quantas contrapartes têm sanções
3. Calcule o valor total transacionado com contrapartes de risco ALTO ou MÉDIO
4. Detalhe os tipos de processos e sanções encontrados
5. Avalie o impacto no risco geral do cliente

{counterparty_analysis_json}
"""
  if alert_type == 'betting_houses_alert [BR]' and betting_houses is not None:
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com casas de apostas."

Atenção especial para transações com as casas de apostas abaixo:
{betting_houses.to_json(orient='records', force_ascii=False, indent=2)}

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Verificar se o nome da parte ou o CNPJ corresponde a alguma das casas de apostas listadas acima.
2. Se houver correspondência, calcular:
 a) A soma total de valores transacionados com essa casa de apostas específica.
 b) A porcentagem que essa soma representa do valor TOTAL de Cash In ou Cash Out (conforme aplicável).

Na sua análise, descreva:
- A soma total de Cash In e Cash Out para cada casa de apostas correspondente.
- A porcentagem que esses valores representam do total de Cash In e Cash Out.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações, independentemente do tipo de alerta.
"""
  elif alert_type == 'Goverment_Corporate_Cards_Alert':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com cartões corporativos governamentais."

Atenção especial para transações com BINs de cartões de crédito que começam com os seguintes prefixos:
- 409869
- 467481
- 498409

Para CADA transação, você DEVE:
1. Verificar se o BIN (os primeiros 6 dígitos do número do cartão) corresponde a algum dos prefixos listados acima.
2. Se houver correspondência, calcular:
 a) A soma total de valores transacionados com esses BINs específicos.
 b) A porcentagem que essa soma representa do valor de TPV TOTAL (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para cada prefixo BIN correspondente.
- A porcentagem que esses valores representam do total de Cash In e Cash Out.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações de cartões de crédito relacionadas a este alerta.
Se não houver correspondências com os BINs listados, informe explicitamente na sua análise.
"""
  elif alert_type == 'ch_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente com possíveis anomalias em PIX."

Atenção especial para Transações PIX:

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Analisar os valores de Cash In e Cash Out para identificar quaisquer anomalias ou padrões suspeitos.
2. Comparar os valores com transações típicas para determinar se há desvios significativos.

Na sua análise, descreva:
- Quaisquer transações de Cash In ou Cash Out que apresentam valores anormais.
- Padrões ou tendências observadas nas transações PIX.
- Recomendação sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações PIX relacionadas a este alerta.
Se não houver anomalias detectadas, informe explicitamente na sua análise.

Além disso, você deve verificar se o usuário pode ser estrangeiro, quando nome não soar Brasileiro, ou a data de criação do CPF for muito recente.
"""
  elif alert_type == 'pix_merchant_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente Merchant com possíveis anomalias em PIX Cash In."
Atenção especial para Transações PIX Cash-In e Cash-Out:

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Analisar os valores de Cash In para identificar quaisquer anomalias ou padrões suspeitos.
2. Revisar os valores de Cash Out para detectar valores atípicos ou incomuns.

Na sua análise, descreva:
- Quaisquer transações de Cash In que apresentam valores anormais.
- Quaisquer transações de Cash Out que apresentam valores atípicos ou incomuns.
- Padrões ou tendências observadas nas transações PIX Cash-In e Cash-Out.
- Recomendação sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações PIX relacionadas a este alerta.
Se não houver anomalias ou valores atípicos detectados, informe explicitamente na sua análise.
"""
  elif alert_type == 'international_cards_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com cartões internacionais."
Atenção especial para Transações com Issuer Não Brasileiro:

Para CADA transação, você DEVE:
1. Verificar se o nome do emissor (issuer_name) da transação não é de uma instituição financeira brasileira.
2. Se o emissor não for do Brasil, calcular:
 a) A soma total de valores transacionados com esse emissor específico.
 b) A porcentagem que essa soma representa do TPV Total (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para cada emissor não brasileiro correspondente.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver correspondências com emissores não brasileiros, informe explicitamente na sua análise.
"""
  elif alert_type == 'bank_slips_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente com possíveis anomalias envolvendo boletos bancários."

Atenção especial para Transações com Método de Captura 'bank_slip':

Para CADA transação, você DEVE:
1. Verificar se o método de captura (capture_method) da transação é 'bank_slip'.
2. Se for 'bank_slip', analisar:
 a) A soma total de valores transacionados com este método.
 b) A porcentagem que essa soma representa do valor do TPV TOTAL (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para transações capturadas via 'bank_slip'.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver transações com método de captura 'bank_slip', informe explicitamente na sua análise.
"""
  elif alert_type == 'gafi_alert [US]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com países proibidos do GAFI."

Atenção especial para Transações cujo issuer seja emitido em algum dos países abaixo:

'Bulgaria', 'Burkina Faso', 'Cameroon', 'Croatia', 'Haiti', 'Jamaica', 'Kenya', 'Mali', 'Mozambique',
'Myanmar', 'Namibia', 'Nigeria', 'Philippines', 'Senegal', 'South Africa', 'Tanzania', 'Vietnam', 'Congo, Dem. Rep.',
'Syrian Arab Republic', 'Turkey', 'Yemen, Rep.', 'Yemen Democratic', 'Iran, Islamic Rep.', 'Korea, Dem. Rep.' ,'Venezuela'

Para CADA transação, você DEVE:
1. Verificar se o nome do emissor (issuer_name) da transação não é de alguma instituição financeira com oriens em algum dos países acima.
2. Se positivo, calcular:
 a) A soma total de valores transacionados com esse emissor específico.
 b) A porcentagem que essa soma representa do TPV Total (conforme aplicável).
 c) Nomear o país de origem.

Na sua análise, descreva:
- A soma total de valores para cada emissor com origens nos países acima, restritos pelo GAFI.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver correspondências com emissores não brasileiros, informe explicitamente na sua análise.
"""
  elif alert_type == 'Pep_Pix Alert' and pep_data is not None:
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente transacionando com Pessoas Politicamente Expostas (PEP)."

Atenção especial para as transações identificadas abaixo:
{pep_data.to_json(orient='records', force_ascii=False, indent=2)}

Você DEVE:
1. Para cada PEP na lista, informar:
 - Nome completo do PEP (pep_name)
 - Documento do PEP (pep_document_number).
 - Cargo do PEP (job_description).
 - Órgão de trabalho (agencies).
 - Soma total dos valores transacionados com cada PEP (DEBIT + CREDIT).
 - A porcentagem que essa soma representa do total de Cash In e/ou Cash Out transacionado com outros indivíduos.
2. Analisar se os valores e frequências das transações com PEP são atípicos ou suspeitos.

Na sua análise, descreva:
- Detalhes das transações com cada PEP identificado.
- Qualquer padrão ou anomalia observada nessas transações.
- Recomendações sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações de Cash In e Cash Out relacionadas a este alerta.
"""
  elif alert_type == 'AI Alert' and features:
    prompt += f"""
Atenção especial às anomalias identificadas pelo modelo de AI:
{features}

Por favor, descreva os padrões ou comportamentos anômalos identificados com base nas características acima.
Você também deve analisar os demais dados disponíveis, como transações, contatos, dispositivos, issuing, produtos, para confirmar ou ajustar a suspeita de fraude.
"""
  elif alert_type == 'Issuing Transactions Alert':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando altos valores via Issuing."

Atenção especial para a tabela de Issuing e as seguintes informações:
- Coluna total_amount
- mcc e mcc_description
- card_acceptor_country_code

Na sua análise, descreva:
- merchant_name com total_amount e percentage_of_total elevados.
- Se mcc e mcc_description fazem parte de negócios de alto risco.
- Se o país em card_acceptor_country_code é considerado um país de alto risco.
"""
  prompt += """

Importante - Ao final da sua análise, você DEVE incluir uma classificação de risco de lavagem de dinheiro em uma escala de 1 a 10, seguindo estas diretrizes:

- 1 a 5: Baixo risco (Normal - não exige ação adicional)
- 6: Médio risco (Normal com aviso de monitoramento)
- 7 a 8: Médio-Alto risco (requer verificação)
- 9: Alto risco (requer Business Validation urgente - BV)
- 10: Risco extremo (requer descredenciamento e reporte ao COAF)

Fatores para considerar na classificação de risco:
- Volume e frequência de transações
- Presença em listas restritivas ou processos
- Conexões com PEPs
- Transações em horários atípicos
- Transações com países de alto risco
- Compatibilidade entre perfil declarado e comportamento transacional

Formato: "Risco de Lavagem de Dinheiro: X/10" (onde X é o número de 1 a 10)
"""
  return prompt

def get_gpt_analysis(prompt: str) -> str:
  """Retorna a análise do GPT para o prompt fornecido."""
  return get_chatgpt_response(prompt)

def format_export_payload(user_id, description, business_validation):
  """
  Formata o payload para exportação conforme o padrão:
  {
      "user_id": user_id,
      "description": clean_description,
      "analysis_type": "manual",
      "conclusion": conclusion,
      "priority": "high",
      "automatic_pipeline": True,
      "offense_group": "illegal_activity",
      "offense_name": "money_laundering",
      "related_analyses": []
  }
  Remove caracteres de formatação Markdown do campo description e determina a conclusão
  a partir do texto da análise.
  """
  clean_description = re.sub(r'[#\*\_]', '', description)
  
  # Verifica se há mensagens de erro na descrição
  error_indicators = [
    "Não consigo tankar este caso", 
    "An error occurred",
    "muitas transações",
    "context_length_exceeded",
    "token limit",
    "chame um analista humano"
  ]
  
  has_error = any(indicator.lower() in clean_description.lower() for indicator in error_indicators)
  
  if has_error:
    # Se houver erro, deixa a conclusão vazia para não enviar nem "suspicious" nem "normal"
    conclusion = ""
    priority = "high"
  else:
    risk_score = 0
    # Regex mais robusto que aceita variações na formatação
    risk_score_match = re.search(r'(?:[Rr]isco\s+(?:de\s+[Ll]avagem\s+(?:de\s+)?[Dd]inheiro)?|[Cc]lassificação\s+(?:de\s+)?[Rr]isco):?\s*(\d+)(?:/|\s*de\s*)10', clean_description)
    if risk_score_match:
      risk_score = int(risk_score_match.group(1))
    else:
      # Tenta encontrar padrões alternativos como "Score: X/10"
      alt_match = re.search(r'[Ss]core:?\s*(\d+)(?:/|\s*de\s*)10', clean_description)
      if alt_match:
        risk_score = int(alt_match.group(1))
    
    # Nova lógica de classificação baseada no score
    if risk_score <= 5:
      # Baixo risco (1-5): normal
      conclusion = "normal"
      priority = "high"
    elif risk_score <= 6:
      # Médio risco (6): normal com aviso
      conclusion = "normal"
      priority = "high"
      # Adicionar texto de aviso ao final da descrição
      if not "Caso de médio risco" in clean_description:
        clean_description += "\n\nOBS: Caso de médio risco que requer monitoramento contínuo."
    elif risk_score <= 8:
      # Risco médio-alto (7-8): suspicious mid
      conclusion = "suspicious"
      priority = "mid"
      if not "Caso de risco médio-alto" in clean_description:
        clean_description += "\n\nOBS: Caso de risco médio-alto que requer validação do negócio, sem necessidade de bloqueio temporário."
    elif risk_score <= 9:
      # Alto risco (9): suspicious high
      conclusion = "suspicious"
      priority = "high"
    else:
      # Risco extremo (10): offense high
      conclusion = "offense"
      priority = "high"
    
    # Se explicitamente mencionar normalizar o caso, mantem como normal
    if "normalizar o caso" in clean_description.lower() and conclusion != "offense":
      conclusion = "normal"
  
  payload = {
    "user_id": user_id,
    "description": clean_description,
    "analysis_type": "manual",
    "conclusion": conclusion,
    "priority": priority,
    "automatic_pipeline": True,
    "offense_group": "illegal_activity",
    "offense_name": "money_laundering",
    "related_analyses": []
  }
  return payload 
