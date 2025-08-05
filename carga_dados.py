import pandas as pd
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

   # Conteúdos
   conteudos = df[["id_conteudo", "nome_conteudo"]].drop_duplicates()
   with engine.begin() as conn:
      for _, row in conteudos.iterrows():
         conn.execute(text("""
               INSERT INTO conteudo (id_conteudo, nome_conteudo, tipo_conteudo, id_plataforma)
               VALUES (:id_conteudo, :nome_conteudo, 'Vídeo', 1)
               ON DUPLICATE KEY UPDATE nome_conteudo = VALUES(nome_conteudo)
         """), {
               "id_conteudo": int(row["id_conteudo"]),
               "nome_conteudo": row["nome_conteudo"]
         })

   # Mapear plataforma
   df_plataformas = pd.read_sql("SELECT id_plataforma, nome FROM plataforma", con=engine)
   mapa_plataformas = dict(zip(df_plataformas["nome"], df_plataformas["id_plataforma"]))
   df["id_plataforma"] = df["plataforma"].map(mapa_plataformas)

   df.rename(columns={"timestamp_interacao": "data_interacao"}, inplace=True)
   df["data_interacao"] = pd.to_datetime(df["data_interacao"])
   df_final = df[["id_usuario", "id_conteudo", "id_plataforma", "tipo_interacao", "data_interacao", "watch_duration_seconds"]]

   df_final.to_sql(name="interacao", con=engine, if_exists="append", index=False)
   print("✅ Dados inseridos com sucesso na tabela 'interacao'.")
