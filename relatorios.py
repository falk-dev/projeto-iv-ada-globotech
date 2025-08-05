# relatorios.py
import pandas as pd
import os

def executar_relatorio_sql(query_sql: str, engine, nome_csv: str = None, exibir_linhas: int = 5) -> pd.DataFrame:
   try:
      # Configurações do pandas para melhor visualização
      pd.set_option('display.max_columns', None)
      pd.set_option('display.width', 1000)
      pd.set_option('display.colheader_justify', 'left')
      
      df = pd.read_sql(query_sql, con=engine)
      
      if df.empty:
         print("ℹ️ A consulta não retornou resultados.")
         return df
         
      # Corrige a formatação da data se existir
      if 'criado_em_formatado' in df.columns:
         df['criado_em_formatado'] = df['criado_em_formatado'].str.replace('%%', '%', regex=False)
         
      print(f"\n🔍 Resultados ({len(df)} linhas):")
      print(df.head(exibir_linhas).to_string(index=False))
      
      if nome_csv:
         os.makedirs(os.path.dirname(nome_csv), exist_ok=True)
         df.to_csv(nome_csv, index=False)
         print(f"\n💾 Salvo em: {nome_csv}")
         
      return df
      
   except Exception as e:
      print(f"\n❌ Erro ao executar consulta: {str(e)}")
      return pd.DataFrame()