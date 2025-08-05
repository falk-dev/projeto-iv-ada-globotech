from conexao_sqlalchemy import criar_engine_mysql, consultas, RelatoriosSQL
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
import sys

def verificar_e_inserir_relatorios(engine):
   Session = sessionmaker(bind=engine)
   session = Session()
   
   try:
      # Verifica se já existem relatórios cadastrados
      existentes = session.query(RelatoriosSQL).count()
      
      if existentes == 0:
         print("🔍 Nenhum relatório encontrado. Iniciando inserção...")
         for nome, query in consultas.items():
               novo_relatorio = RelatoriosSQL(
                  nome=nome,
                  descricao=nome.replace('_', ' ').capitalize(),
                  query_sql=query.strip()
               )
               session.add(novo_relatorio)
         
         session.commit()
         print(f"✅ {len(consultas)} relatórios inseridos com sucesso.")
      else:
         print(f"ℹ️ {existentes} relatórios já existentes no banco.")
         print("Atualizando consultas existentes...")
         
         for nome, query in consultas.items():
               session.query(RelatoriosSQL).filter_by(nome=nome).update({
                  "query_sql": query.strip(),
                  "descricao": nome.replace('_', ' ').capitalize()
               })
         
         session.commit()
         print("✅ Consultas atualizadas com sucesso.")
         
   except Exception as e:
      session.rollback()
      print(f"❌ Erro ao inserir relatórios: {e}")
      sys.exit(1)
   finally:
      session.close()

if __name__ == "__main__":
   load_dotenv()
   
   DB_USER = os.getenv("DB_USER")
   DB_PASS = os.getenv("DB_PASS")
   DB_HOST = os.getenv("DB_HOST", "localhost")
   DB_NAME = os.getenv("DB_NAME", "globo_tech")
   
   try:
      engine = criar_engine_mysql(DB_USER, DB_PASS, DB_HOST, DB_NAME)
      verificar_e_inserir_relatorios(engine)
      
      # Verificação final
      with engine.connect() as conn:
         result = conn.execute(text("SELECT nome FROM relatorios_sql"))
         print("\nRelatórios disponíveis no banco:")
         for row in result:
               print(f"- {row.nome}")
               
   except Exception as e:
      print(f"❌ Falha na conexão com o banco: {e}")
      sys.exit(1)