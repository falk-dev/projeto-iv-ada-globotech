# relatorios.py
import pandas as pd
import os

def executar_relatorio_sql(query_sql: str, engine, nome_csv: str = None, exibir_linhas: int = 10) -> pd.DataFrame:
   """
   Executa uma query SQL no banco de dados e opcionalmente exporta o resultado para CSV.

   Parâmetros:
      query_sql (str): Consulta SQL a ser executada.
      engine (sqlalchemy.Engine): Conexão com o banco.
      nome_csv (str, opcional): Caminho do arquivo CSV para salvar os resultados.
      exibir_linhas (int): Número de linhas para exibir no console (default: 10).

   Retorna:
      pd.DataFrame: Resultado da consulta SQL.
   """
   try:
      df = pd.read_sql(query_sql, con=engine)
   except Exception as e:
      print(f"❌ Erro ao executar a consulta: {e}")
      return pd.DataFrame()

   if df.empty:
      print("⚠️ A consulta retornou nenhum resultado.")
   else:
      print(f"✅ Consulta executada com sucesso. Exibindo as {min(exibir_linhas, len(df))} primeiras linhas:\n")
      print(df.head(exibir_linhas))

   if nome_csv:
      if not nome_csv.endswith(".csv"):
         nome_csv += ".csv"
      
      # Adiciona a pasta 'data' ao caminho do arquivo
      caminho_completo = os.path.join("data", nome_csv)
      
      # Cria a pasta 'data' se não existir
      os.makedirs(os.path.dirname(caminho_completo), exist_ok=True)
      
      # Salva o CSV dentro da pasta 'data'
      df.to_csv(caminho_completo, index=False)
      print(f"\n📁 Resultado exportado para: {caminho_completo}")


   return df
