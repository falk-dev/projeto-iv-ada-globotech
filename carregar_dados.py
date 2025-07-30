import pandas as pd
from sqlalchemy import text
from conexao_sqlalchemy import criar_engine_mysql

# Carrega o CSV
df = pd.read_csv("interacoes_globo.csv")

# Corrige o tipo_interacao
df["tipo_interacao"] = df["tipo_interacao"].replace({"view_start": "view"})

# Conecta ao MySQL
engine = criar_engine_mysql("root", "sua_senha")

# ======= Inserir plataformas (evitando duplicatas) =======
plataformas = df["plataforma"].unique()
with engine.begin() as conn:
   for nome in plataformas:
      conn.execute(text("""
         INSERT IGNORE INTO plataforma (nome) VALUES (:nome)
      """), {"nome": nome})

# ======= Inserir usuários (evitando duplicatas) =======
usuarios = df["id_usuario"].unique()
with engine.begin() as conn:
   for id_usuario in usuarios:
      conn.execute(text("""
         INSERT IGNORE INTO usuario (id_usuario) VALUES (:id_usuario)
      """), {"id_usuario": int(id_usuario)})

# ======= Inserir conteúdos (evitando duplicatas) =======
conteudos = df[["id_conteudo", "nome_conteudo"]].drop_duplicates()
with engine.begin() as conn:
   for _, row in conteudos.iterrows():
      conn.execute(text("""
         INSERT IGNORE INTO conteudo (id_conteudo, nome_conteudo, tipo_conteudo, id_plataforma)
         VALUES (:id_conteudo, :nome_conteudo, 'Vídeo', 1) -- Ajuste tipo e id_plataforma se necessário
      """), {
         "id_conteudo": int(row["id_conteudo"]),
         "nome_conteudo": row["nome_conteudo"]
      })

# ======= Obter mapeamento plataforma => id_plataforma =======
df_plataformas = pd.read_sql("SELECT id_plataforma, nome FROM plataforma", con=engine)
mapa_plataformas = dict(zip(df_plataformas["nome"], df_plataformas["id_plataforma"]))

# Mapeia os nomes das plataformas no DataFrame
df["id_plataforma"] = df["plataforma"].map(mapa_plataformas)

# Renomeia a coluna de timestamp
df.rename(columns={"timestamp_interacao": "data_interacao"}, inplace=True)

# Seleciona apenas as colunas que existem na tabela interacao
df_final = df[["id_usuario", "id_conteudo", "id_plataforma", "tipo_interacao", "data_interacao", "watch_duration_seconds"]]

# Converte a data para datetime
df_final["data_interacao"] = pd.to_datetime(df_final["data_interacao"])

# ======= Inserir na tabela interacao =======
df_final.to_sql(name="interacao", con=engine, if_exists="append", index=False)
print("✅ Dados inseridos com sucesso na tabela 'interacao'.")
