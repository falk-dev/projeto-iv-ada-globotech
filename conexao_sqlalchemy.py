# conexao_sqlalchemy.py
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Enum, Text, CheckConstraint, TIMESTAMP, text
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import os
from dotenv import load_dotenv
import logging


# Configuração de logging
logging.basicConfig()
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)

load_dotenv()

Base = declarative_base()

class Plataforma(Base):
   __tablename__ = 'plataforma'
   id_plataforma = Column(Integer, primary_key=True, autoincrement=True)
   nome = Column(String(100), nullable=False, unique=True)
   conteudos = relationship('Conteudo', back_populates='plataforma')
   interacoes = relationship('Interacao', back_populates='plataforma')

class Usuario(Base):
   __tablename__ = 'usuario'
   id_usuario = Column(Integer, primary_key=True)
   interacoes = relationship('Interacao', back_populates='usuario')

class Conteudo(Base):
   __tablename__ = 'conteudo'
   id_conteudo = Column(Integer, primary_key=True)
   nome_conteudo = Column(String(255))
   tipo_conteudo = Column(String(50), nullable=False)
   id_plataforma = Column(Integer, ForeignKey('plataforma.id_plataforma'), nullable=False)

   plataforma = relationship('Plataforma', back_populates='conteudos')
   interacoes = relationship('Interacao', back_populates='conteudo')

class Interacao(Base):
   __tablename__ = 'interacao'
   id_interacao = Column(Integer, primary_key=True, autoincrement=True)
   id_usuario = Column(Integer, ForeignKey('usuario.id_usuario'), nullable=False)
   id_conteudo = Column(Integer, ForeignKey('conteudo.id_conteudo'), nullable=False)
   id_plataforma = Column(Integer, ForeignKey('plataforma.id_plataforma'), nullable=False)
   tipo_interacao = Column(Enum('view', 'like', 'share', 'comment'), nullable=False)
   data_interacao = Column(DateTime, nullable=False)
   watch_duration_seconds = Column(Integer, default=0)

   __table_args__ = (
      CheckConstraint('watch_duration_seconds >= 0', name='check_watch_duration_positive'),
   )

   usuario = relationship('Usuario', back_populates='interacoes')
   conteudo = relationship('Conteudo', back_populates='interacoes')
   plataforma = relationship('Plataforma', back_populates='interacoes')

class RelatoriosSQL(Base):
    __tablename__ = 'relatorios_sql'
    id = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(100), nullable=False, unique=True)
    descricao = Column(Text)
    query_sql = Column(Text, nullable=False)
    criado_em = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

def criar_database_if_not_exists(usuario, senha, host, nome_banco):
   url_sem_banco = f"mysql+mysqlconnector://{usuario}:{senha}@{host}/"
   engine = create_engine(url_sem_banco)
   with engine.connect() as conn:
      conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {nome_banco}"))
   engine.dispose()

def criar_engine_mysql(usuario=None, senha=None, host=None, banco=None):
   usuario = usuario or os.getenv("DB_USER")
   senha = senha or os.getenv("DB_PASS")
   host = host or os.getenv("DB_HOST", "localhost")
   banco = banco or os.getenv("DB_NAME", "globo_tech")

   criar_database_if_not_exists(usuario, senha, host, banco)

   url = f"mysql+mysqlconnector://{usuario}:{senha}@{host}/{banco}"
   engine = create_engine(url, echo=True)
   
   # Verifica se a tabela de relatórios existe e está populada
   with engine.connect() as conn:
      tabela_existe = conn.execute(text(
         "SELECT COUNT(*) FROM information_schema.tables "
         "WHERE table_schema = :banco AND table_name = 'relatorios_sql'"
      ), {"banco": banco}).scalar()
      
      if tabela_existe:
         registros = conn.execute(text("SELECT COUNT(*) FROM relatorios_sql")).scalar()
         if registros == 0:
               print("⚠️ Tabela relatorios_sql existe mas está vazia. Execute inserir_relatorios.py")

   # Cria as tabelas automaticamente
   Base.metadata.create_all(engine)

   return engine

def criar_sessao(engine):
   Session = sessionmaker(bind=engine)
   return Session()

def carregar_csv_para_mysql(caminho_csv, tabela_destino, engine):
   df = pd.read_csv(caminho_csv)
   df.to_sql(name=tabela_destino, con=engine, if_exists='append', index=False)
   print(f"Dados inseridos na tabela '{tabela_destino}' com sucesso.")

# Dicionário com consultas SQL para relatórios
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
      SELECT 
         p.nome, 
         COUNT(*) AS total_engajamento
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
      SELECT 
         c.id_conteudo, 
         c.nome_conteudo, 
         COUNT(*) AS total_comentarios
      FROM interacao i
      JOIN conteudo c ON i.id_conteudo = c.id_conteudo
      WHERE i.tipo_interacao = 'comment'
      GROUP BY c.id_conteudo, c.nome_conteudo
      ORDER BY total_comentarios DESC
      LIMIT 10;
   """,
   "interacoes_por_tipo_conteudo": """
      SELECT 
         c.tipo_conteudo, 
         COUNT(*) AS total_interacoes
      FROM conteudo c
      JOIN interacao i ON c.id_conteudo = i.id_conteudo
      GROUP BY c.tipo_conteudo
      ORDER BY total_interacoes DESC;
   """,
   "tempo_medio_por_plataforma": """
      SELECT 
         p.nome, 
         SEC_TO_TIME(AVG(i.watch_duration_seconds)) AS tempo_medio
      FROM interacao i
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      GROUP BY p.nome
      ORDER BY tempo_medio DESC;
   """,
   "comentarios_por_conteudo": """
      SELECT 
         c.id_conteudo, 
         c.nome_conteudo, 
         COUNT(*) AS quantidade_comentarios
      FROM conteudo c
      JOIN interacao i ON c.id_conteudo = i.id_conteudo
      WHERE i.tipo_interacao = 'comment'
      GROUP BY c.id_conteudo, c.nome_conteudo
      ORDER BY quantidade_comentarios DESC;
   """,
   "conteudos_mais_assistidos_por_plataforma": """
      SELECT 
         p.nome AS plataforma, 
         c.nome_conteudo, 
         COUNT(i.id_interacao) AS total_assistidos
      FROM interacao i
      JOIN conteudo c ON i.id_conteudo = c.id_conteudo
      JOIN plataforma p ON i.id_plataforma = p.id_plataforma
      WHERE i.tipo_interacao = 'view'
      GROUP BY p.nome, c.nome_conteudo
      ORDER BY total_assistidos DESC
      LIMIT 10;
   """,
   "relatorios_sql": """
      SELECT 
         id, 
         nome, 
         descricao, 
         criado_em
      FROM relatorios_sql
      ORDER BY criado_em DESC;
   """
}

# Exemplo de uso
if __name__ == "__main__":
   engine_mysql = criar_engine_mysql("root", "sua_senha")

   nome_relatorio = "conteudos_mais_comentados"
   df_resultado = executar_relatorio_sql(consultas[nome_relatorio], engine_mysql, nome_csv=f"data/{nome_relatorio}.csv")
   print(f"Relatório '{nome_relatorio}' executado e salvo como {nome_relatorio}.csv")
   print("------------------------------------------------------------\n")

   relatorio_plataforma = "plataforma_maior_engajamento"
   df_plataforma = executar_relatorio_sql(consultas[relatorio_plataforma], engine_mysql, nome_csv=f"{relatorio_plataforma}.csv")
   print(f"Relatório '{relatorio_plataforma}' executado e salvo como {relatorio_plataforma}.csv")
   print("------------------------------------------------------------\n")

   relatorio_ranking_conteudos_consumidos = "ranking_conteudos_consumidos"
   df_ranking_conteudos_consumidos = executar_relatorio_sql(consultas[relatorio_ranking_conteudos_consumidos], engine_mysql, nome_csv=f"{relatorio_ranking_conteudos_consumidos}.csv")
   print(f"Relatório '{relatorio_ranking_conteudos_consumidos}' executado e salvo como {relatorio_ranking_conteudos_consumidos}.csv")
   print("------------------------------------------------------------\n")

   relatorio_interacoes_por_tipo_conteudo = "interacoes_por_tipo_conteudo"
   df_interacoes_por_tipo_conteudo = executar_relatorio_sql(consultas[relatorio_interacoes_por_tipo_conteudo], engine_mysql, nome_csv=f"{relatorio_interacoes_por_tipo_conteudo}.csv")
   print(f"Relatório '{relatorio_interacoes_por_tipo_conteudo}' executado e salvo como {relatorio_interacoes_por_tipo_conteudo}.csv")
   print("------------------------------------------------------------\n")

   relatorio_tempo_medio_por_plataforma = "tempo_medio_por_plataforma"
   df_tempo_medio_por_plataforma = executar_relatorio_sql(consultas[relatorio_tempo_medio_por_plataforma], engine_mysql, nome_csv=f"{relatorio_tempo_medio_por_plataforma}.csv")
   print(f"Relatório '{relatorio_tempo_medio_por_plataforma}' executado e salvo como {relatorio_tempo_medio_por_plataforma}.csv")
   print("------------------------------------------------------------\n")

   relatorio_comentarios_por_conteudo = "comentarios_por_conteudo"
   df_comentarios_por_conteudo = executar_relatorio_sql(consultas[relatorio_comentarios_por_conteudo], engine_mysql, nome_csv=f"{relatorio_comentarios_por_conteudo}.csv")
   print(f"Relatório '{relatorio_comentarios_por_conteudo}' executado e salvo como {relatorio_comentarios_por_conteudo}.csv")
   print("------------------------------------------------------------\n")

   relatorio_conteudos_mais_assistidos_por_plataforma = "conteudos_mais_assistidos_por_plataforma"
   df_conteudos_mais_assistidos_por_plataforma = executar_relatorio_sql(consultas[relatorio_conteudos_mais_assistidos_por_plataforma], engine_mysql, nome_csv=f"{relatorio_conteudos_mais_assistidos_por_plataforma}.csv")
   print(f"Relatório '{relatorio_conteudos_mais_assistidos_por_plataforma}' executado e salvo como {relatorio_conteudos_mais_assistidos_por_plataforma}.csv")
   print("------------------------------------------------------------\n")

   relatorio_total_engajamentos_por_plataforma = "total_de_engajamentos_por_plataforma"
   df_total_engajamentos_por_plataforma = executar_relatorio_sql(consultas[relatorio_total_engajamentos_por_plataforma], engine_mysql, nome_csv=f"{relatorio_total_engajamentos_por_plataforma}.csv")
   print(f"Relatório '{relatorio_total_engajamentos_por_plataforma}' executado e salvo como {relatorio_total_engajamentos_por_plataforma}.csv")