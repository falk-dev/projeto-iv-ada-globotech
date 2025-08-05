from conexao_sqlalchemy import criar_engine_mysql, consultas
from sqlalchemy import text
from dotenv import load_dotenv
import os

def inserir_relatorios(engine):
   with engine.begin() as conn:
      for nome, query in consultas.items():
         conn.execute(text("""
            INSERT INTO relatorios_sql (nome, descricao, query_sql)
            VALUES (:nome, :descricao, :query_sql)
            ON DUPLICATE KEY UPDATE
               descricao = VALUES(descricao),
               query_sql = VALUES(query_sql)
         """), {
            "nome": nome,
            "descricao": nome.replace('_', ' ').capitalize(),
            "query_sql": query.strip()
         })
   print("✅ Relatórios inseridos com sucesso no banco.")

if __name__ == "__main__":
   load_dotenv()

   DB_USER = os.getenv("DB_USER")
   DB_PASS = os.getenv("DB_PASS")
   DB_HOST = os.getenv("DB_HOST", "localhost")
   DB_NAME = os.getenv("DB_NAME", "globo_tech")

   engine = criar_engine_mysql(DB_USER, DB_PASS, DB_HOST, DB_NAME)
   inserir_relatorios(engine)
