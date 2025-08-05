import pandas as pd
from sqlalchemy import create_engine, text

# Carrega o CSV
df = pd.read_csv("interacoes_globo.csv")

# Corrige o tipo_interacao
df["tipo_interacao"] = df["tipo_interacao"].replace({"view_start": "view"})

# Conecta ao SQLite (arquivo local)
engine = create_engine("sqlite:///globo_tech.db", echo=True)

# Cria tabelas no SQLite se não existirem
with engine.begin() as conn:
    # Tabela de plataformas
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS plataforma (
            id_plataforma INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE
        );
    """))
    # Tabela de usuários
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS usuario (
            id_usuario INTEGER PRIMARY KEY
        );
    """))
    # Tabela de conteúdos
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS conteudo (
            id_conteudo INTEGER PRIMARY KEY,
            nome_conteudo TEXT,
            tipo_conteudo TEXT NOT NULL,
            id_plataforma INTEGER NOT NULL,
            FOREIGN KEY(id_plataforma) REFERENCES plataforma(id_plataforma)
        );
    """))
    # Tabela de interações
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS interacao (
            id_interacao INTEGER PRIMARY KEY AUTOINCREMENT,
            id_usuario INTEGER NOT NULL,
            id_conteudo INTEGER NOT NULL,
            id_plataforma INTEGER NOT NULL,
            tipo_interacao TEXT NOT NULL,
            data_interacao DATETIME NOT NULL,
            watch_duration_seconds INTEGER DEFAULT 0,
            FOREIGN KEY(id_usuario) REFERENCES usuario(id_usuario),
            FOREIGN KEY(id_conteudo) REFERENCES conteudo(id_conteudo),
            FOREIGN KEY(id_plataforma) REFERENCES plataforma(id_plataforma)
        );
    """))

# ======= Inserir plataformas (evitando duplicatas) =======
plataformas = df["plataforma"].unique()
with engine.begin() as conn:
   for nome in plataformas:
      conn.execute(text("""
         INSERT OR IGNORE INTO plataforma (nome) VALUES (:nome)
      """), {"nome": nome})

# ======= Inserir usuários (evitando duplicatas) =======
usuarios = df["id_usuario"].unique()
with engine.begin() as conn:
   for id_usuario in usuarios:
      conn.execute(text("""
         INSERT OR IGNORE INTO usuario (id_usuario) VALUES (:id_usuario)
      """), {"id_usuario": int(id_usuario)})

# ======= Obter mapeamento plataforma => id_plataforma =======
df_plataformas = pd.read_sql("SELECT id_plataforma, nome FROM plataforma", con=engine)
mapa_plataformas = dict(zip(df_plataformas["nome"], df_plataformas["id_plataforma"]))

# ======= Inserir conteúdos (evitando duplicatas) =======
conteudos = df[["id_conteudo", "nome_conteudo", "plataforma"]].drop_duplicates()
with engine.begin() as conn:
   for _, row in conteudos.iterrows():
      # obtém o id da plataforma a partir do nome
      id_plat = mapa_plataformas[row["plataforma"]]
      conn.execute(text("""
         INSERT OR IGNORE INTO conteudo 
            (id_conteudo, nome_conteudo, tipo_conteudo, id_plataforma)
         VALUES 
            (:id_conteudo, :nome_conteudo, :tipo_conteudo, :id_plataforma)
      """), {
         "id_conteudo": int(row["id_conteudo"]),
         "nome_conteudo": row["nome_conteudo"],
         "tipo_conteudo": "Vídeo",  # ou derive de row, se tiver outras categorias
         "id_plataforma": int(id_plat)
      })

# ======= Inserir na tabela interacao ======= => id_plataforma =======
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
