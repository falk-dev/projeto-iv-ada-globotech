# carga_dados.py
import pandas as pd
import os
from sqlalchemy import text
from conexao_sqlalchemy import criar_engine_mysql
from utils import validar_dataframe

def carregar_dados(csv_path: str):
   df = pd.read_csv(csv_path)
   df = validar_dataframe(df)
   df["tipo_interacao"] = df["tipo_interacao"].replace({"view_start": "view"})

   engine = criar_engine_mysql()

   # Plataformas
   plataformas = df["plataforma"].unique()
   with engine.begin() as conn:
      for nome in plataformas:
         conn.execute(text("""
               INSERT INTO plataforma (nome) VALUES (:nome)
               ON DUPLICATE KEY UPDATE nome = VALUES(nome)
         """), {"nome": nome})

   # Usuários
   usuarios = df["id_usuario"].unique()
   with engine.begin() as conn:
      for uid in usuarios:
         conn.execute(text("""
               INSERT INTO usuario (id_usuario) VALUES (:id_usuario)
               ON DUPLICATE KEY UPDATE id_usuario = VALUES(id_usuario)
         """), {"id_usuario": int(uid)})

   # Mapear plataforma
   df_plataformas = pd.read_sql("SELECT id_plataforma, nome FROM plataforma", con=engine)
   mapa_plataformas = dict(zip(df_plataformas["nome"], df_plataformas["id_plataforma"]))
   df["id_plataforma"] = df["plataforma"].map(mapa_plataformas)

   # Conteúdos
   conteudos = df[["id_conteudo", "nome_conteudo", "id_plataforma"]].drop_duplicates()
   with engine.begin() as conn:
      for _, row in conteudos.iterrows():
         conn.execute(text("""
               INSERT INTO conteudo (id_conteudo, nome_conteudo, tipo_conteudo, id_plataforma)
               VALUES (:id_conteudo, :nome_conteudo, 'Vídeo', :id_plataforma)
               ON DUPLICATE KEY UPDATE nome_conteudo = VALUES(nome_conteudo), id_plataforma = VALUES(id_plataforma)
         """), {
               "id_conteudo": int(row["id_conteudo"]),
               "nome_conteudo": row["nome_conteudo"],
               "id_plataforma": int(row["id_plataforma"])
         })
         
   # relatorios_sql
   df_relatorios = pd.read_sql("SELECT * FROM relatorios_sql", con=engine)
   with engine.begin() as conn:
      for _, row in df_relatorios.iterrows():
         conn.execute(text("""
               INSERT INTO relatorios_sql (nome, descricao, query_sql)
               VALUES (:nome, :descricao, :query_sql)
               ON DUPLICATE KEY UPDATE descricao = VALUES(descricao), query_sql = VALUES(query_sql)
         """), {
               "nome": row["nome"],
               "descricao": row["descricao"],
               "query_sql": row["query_sql"]
         })
      
   # Preparar dados para inserção na tabela 'interacao'
   df.rename(columns={"timestamp_interacao": "data_interacao"}, inplace=True)
   df["data_interacao"] = pd.to_datetime(df["data_interacao"])
   df_final = df[["id_usuario", "id_conteudo", "id_plataforma", "tipo_interacao", "data_interacao", "watch_duration_seconds"]]

   print("🔍 Visualização dos dados a serem inseridos:")
   print(df_final.head())
   print("📊 Tipos de dados:")
   print(df_final.dtypes)
   print("❓ Valores nulos por coluna:")
   print(df_final.isnull().sum())

   # Inserção na tabela interacao
   df_final.to_sql(name="interacao", con=engine, if_exists="append", index=False)
   print("✅ Dados inseridos com sucesso na tabela 'interacao'.")

   # Verificação: total de registros
   with engine.connect() as conn:
      result = conn.execute(text("SELECT COUNT(*) FROM interacao"))
      print(f"📈 Total de registros na tabela 'interacao': {result.scalar()}")


if __name__ == "__main__":
   csv_path = "interacoes_globo.csv"
   print(f"Arquivo CSV existe? {os.path.exists(csv_path)}")
   print(f"Caminho absoluto: {os.path.abspath(csv_path)}")
   carregar_dados(csv_path)
