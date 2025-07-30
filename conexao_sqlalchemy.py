# conexao_sqlalchemy.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

import os
from dotenv import load_dotenv
load_dotenv()

# ====================
# Conexão com MySQL
# ====================
# Exemplo de URL: mysql+mysqlconnector://<usuario>:<senha>@<host>/<banco>

def criar_engine_mysql(usuario, senha, host="localhost", banco="globo_tech"):
   usuario = os.environ.get("DB_USER")
   senha = os.environ.get("DB_PASS")
   host = os.environ.get("DB_HOST", "localhost")
   banco = os.environ.get("DB_NAME", "globo_tech")
   url = f"mysql+mysqlconnector://{usuario}:{senha}@{host}/{banco}"
   return create_engine(url, echo=True)



# ======================
# Criar sessão (comum)
# ======================

def criar_sessao(engine):
   Session = sessionmaker(bind=engine)
   return Session()


# ======================
# Carregar CSV para MySQL
# ======================

def carregar_csv_para_mysql(caminho_csv, tabela_destino, engine):
   df = pd.read_csv(caminho_csv)
   df.to_sql(name=tabela_destino, con=engine, if_exists='append', index=False)
   print(f"Dados inseridos na tabela '{tabela_destino}' com sucesso.")


# ======================
# Executar Relatório com Pandas
# ======================

def executar_relatorio_sql(query, engine, nome_csv=None):
   df = pd.read_sql_query(query, engine)
   print("\nResultado do Relatório:")
   print(df.head(10))
   
   if nome_csv:
      pasta_destino = "data"
      os.makedirs(pasta_destino, exist_ok=True)  
      caminho_completo = os.path.join(pasta_destino, nome_csv)
      df.to_csv(caminho_completo, index=False)
      print(f"Relatório exportado como {caminho_completo}")
   
   return df


# ======================
# Lista de Relatórios
# ======================

consultas = {
   "ranking_conteudos_consumidos": """
      SELECT 
         c.id_conteudo, 
         c.nome_conteudo, 
         c.tipo_conteudo, 
         SEC_TO_TIME(SUM(i.watch_duration_seconds)) AS total_consumo
      FROM conteudo c
      JOIN interacao i ON c.id_conteudo = i.id_conteudo
      GROUP BY c.id_conteudo, c.nome_conteudo, c.tipo_conteudo
      ORDER BY SUM(i.watch_duration_seconds) DESC
      LIMIT 10;
   """,
   "plataforma_maior_engajamento": """
      SELECT p.nome, COUNT(*) AS total_engajamento
      FROM interacao i
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      WHERE i.tipo_interacao IN ('like', 'share', 'comment')
      GROUP BY p.nome
      ORDER BY total_engajamento DESC
      LIMIT 10;
   """,
   "total_de_engajamentos_por_plataforma": """
      SELECT
         p.nome AS nome_plataforma,
         COUNT(*) AS total_engajamento,
         SUM(CASE WHEN i.tipo_interacao = 'like' THEN 1 ELSE 0 END) AS total_like,
         SUM(CASE WHEN i.tipo_interacao = 'comment' THEN 1 ELSE 0 END) AS total_comment,
         SUM(CASE WHEN i.tipo_interacao = 'share' THEN 1 ELSE 0 END) AS total_share,
         SUM(CASE WHEN i.tipo_interacao = 'view' THEN 1 ELSE 0 END) AS total_view
      FROM interacao i
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      WHERE i.tipo_interacao IN ('like', 'comment', 'share', 'view')
      GROUP BY p.nome
      ORDER BY total_engajamento DESC;

   """,
   "conteudos_mais_comentados": """
      SELECT c.id_conteudo, c.nome_conteudo, COUNT(*) AS total_comentarios
      FROM interacao i
      JOIN conteudo c ON i.id_conteudo = c.id_conteudo
      WHERE i.tipo_interacao = 'comment'
      GROUP BY c.id_conteudo, c.nome_conteudo
      ORDER BY total_comentarios DESC
      LIMIT 10;
   """,
   "interacoes_por_tipo_conteudo": """
      SELECT c.tipo_conteudo, COUNT(*) AS total_interacoes
      FROM conteudo c
      JOIN interacao i ON c.id_conteudo = i.id_conteudo
      GROUP BY c.tipo_conteudo
      ORDER BY total_interacoes DESC;
   """,
   "tempo_medio_por_plataforma": """
      SELECT p.nome, SEC_TO_TIME(AVG(i.watch_duration_seconds)) AS tempo_medio
      FROM interacao i
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      GROUP BY p.nome
      ORDER BY tempo_medio DESC;
   """,
   "comentarios_por_conteudo": """
      SELECT c.id_conteudo, c.nome_conteudo, COUNT(*) AS quantidade_comentarios
      FROM conteudo c
      JOIN interacao i ON c.id_conteudo = i.id_conteudo
      WHERE i.tipo_interacao = 'comment'
      GROUP BY c.id_conteudo, c.nome_conteudo
      ORDER BY quantidade_comentarios DESC;
   """,
   "conteudos_mais_assistidos_por_plataforma": """
      SELECT p.nome AS plataforma, c.nome_conteudo, COUNT(i.id_interacao) AS total_assistidos
      FROM interacao i
      JOIN conteudo c ON i.id_conteudo = c.id_conteudo
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      WHERE i.tipo_interacao = 'view'
      GROUP BY p.nome, c.nome_conteudo
      ORDER BY total_assistidos DESC
      LIMIT 10;
   """
}

# Exemplo de uso
if __name__ == "__main__":
   engine_mysql = criar_engine_mysql("root", "sua_senha")

   # Executar um dos relatórios disponíveis
   nome_relatorio = "conteudos_mais_comentados"
   df_resultado = executar_relatorio_sql(consultas[nome_relatorio], engine_mysql, nome_csv=f"{nome_relatorio}.csv")
   print(f"Relatório '{nome_relatorio}' executado e salvo como {nome_relatorio}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_plataforma = "plataforma_maior_engajamento"
   df_plataforma = executar_relatorio_sql(consultas[relatorio_plataforma], engine_mysql, nome_csv=f"{relatorio_plataforma}.csv")
   print(f"Relatório '{relatorio_plataforma}' executado e salvo como {relatorio_plataforma}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_ranking_conteudos_consumidos = "ranking_conteudos_consumidos"
   df_ranking_conteudos_consumidos = executar_relatorio_sql(consultas[relatorio_ranking_conteudos_consumidos], engine_mysql, nome_csv=f"{relatorio_ranking_conteudos_consumidos}.csv")
   print(f"Relatório '{relatorio_ranking_conteudos_consumidos}' executado e salvo como {relatorio_ranking_conteudos_consumidos}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_interacoes_por_tipo_conteudo = "interacoes_por_tipo_conteudo"
   df_interacoes_por_tipo_conteudo = executar_relatorio_sql(consultas[relatorio_interacoes_por_tipo_conteudo], engine_mysql, nome_csv=f"{relatorio_interacoes_por_tipo_conteudo}.csv")
   print(f"Relatório '{relatorio_interacoes_por_tipo_conteudo}' executado e salvo como {relatorio_interacoes_por_tipo_conteudo}.csv")
   print("------------------------------------------------------------")  
   print("\n")
   
   relatorio_tempo_medio_por_plataforma = "tempo_medio_por_plataforma"
   df_tempo_medio_por_plataforma = executar_relatorio_sql(consultas[relatorio_tempo_medio_por_plataforma], engine_mysql, nome_csv=f"{relatorio_tempo_medio_por_plataforma}.csv")
   print(f"Relatório '{relatorio_tempo_medio_por_plataforma}' executado e salvo como {relatorio_tempo_medio_por_plataforma}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_comentarios_por_conteudo = "comentarios_por_conteudo"
   df_comentarios_por_conteudo = executar_relatorio_sql(consultas[relatorio_comentarios_por_conteudo], engine_mysql, nome_csv=f"{relatorio_comentarios_por_conteudo}.csv")
   print(f"Relatório '{relatorio_comentarios_por_conteudo}' executado e salvo como {relatorio_comentarios_por_conteudo}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_conteudos_mais_assistidos_por_plataforma = "conteudos_mais_assistidos_por_plataforma"
   df_conteudos_mais_assistidos_por_plataforma = executar_relatorio_sql(consultas[relatorio_conteudos_mais_assistidos_por_plataforma], engine_mysql, nome_csv=f"{relatorio_conteudos_mais_assistidos_por_plataforma}.csv")
   print(f"Relatório '{relatorio_conteudos_mais_assistidos_por_plataforma}' executado e salvo como {relatorio_conteudos_mais_assistidos_por_plataforma}.csv")
   print("------------------------------------------------------------")
   print("\n")
   
   relatorio_total_engajamentos_por_plataforma = "total_de_engajamentos_por_plataforma"
   df_total_engajamentos_por_plataforma = executar_relatorio_sql(consultas[relatorio_total_engajamentos_por_plataforma], engine_mysql, nome_csv=f"{relatorio_total_engajamentos_por_plataforma}.csv")
   print(f"Relatório '{relatorio_total_engajamentos_por_plataforma}' executado e salvo como {relatorio_total_engajamentos_por_plataforma}.csv")