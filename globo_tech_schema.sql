-- schema.sql - Criação do banco de dados globo_tech 
-- globo_tech_schema.sql

-- 1. Criação do banco
CREATE DATABASE IF NOT EXISTS globo_tech; -- Cria o banco de dados se não existir
USE globo_tech; -- Seleciona o banco para uso

-- 2. Tabela de Plataformas
CREATE TABLE IF NOT EXISTS plataforma (
    id_plataforma INT AUTO_INCREMENT PRIMARY KEY, -- Identificador único da plataforma
    nome VARCHAR(100) NOT NULL UNIQUE -- Nome da plataforma, não pode repetir
);

-- 3. Tabela de Usuários
CREATE TABLE IF NOT EXISTS usuario (
    id_usuario INT PRIMARY KEY -- Identificador único do usuário
);

-- 4. Tabela de Conteúdo
CREATE TABLE IF NOT EXISTS conteudo (
    id_conteudo INT PRIMARY KEY, -- Identificador único do conteúdo
    nome_conteudo VARCHAR(255), -- Título do conteúdo
    tipo_conteudo VARCHAR(50) NOT NULL, -- Tipo do conteúdo (Vídeo, Podcast, Artigo)
    id_plataforma INT NOT NULL, -- Plataforma relacionada ao conteúdo
    FOREIGN KEY (id_plataforma) 
    REFERENCES plataforma(id_plataforma) -- Chave estrangeira para plataforma
);

-- 5. Tabela de Interação
CREATE TABLE IF NOT EXISTS interacao (
    id_interacao INT AUTO_INCREMENT PRIMARY KEY, -- Identificador único da interação
    id_usuario INT NOT NULL, -- Usuário que realizou a interação
    id_conteudo INT NOT NULL, -- Conteúdo relacionado à interação
    id_plataforma INT NOT NULL, -- Plataforma onde ocorreu a interação
    tipo_interacao ENUM('view', 'like', 'share', 'comment') 
    NOT NULL, -- Tipo da interação
    data_interacao DATETIME NOT NULL, -- Data e hora da interação
    watch_duration_seconds INT DEFAULT 0 CHECK(watch_duration_seconds >= 0), -- Duração assistida (em segundos)

    FOREIGN KEY (id_usuario) REFERENCES usuario(id_usuario), -- Chave estrangeira para usuário
    FOREIGN KEY (id_conteudo) REFERENCES conteudo(id_conteudo), -- Chave estrangeira para conteúdo
    FOREIGN KEY (id_plataforma) REFERENCES plataforma(id_plataforma) -- Chave estrangeira para plataforma
);

-- ==========================
-- Consultas utilizadas
-- ==========================

-- 1. Ranking de conteúdos mais consumidos (por tempo total)
-- SUM: soma total do tempo assistido
-- SEC_TO_TIME: converte segundos para HH:MM:SS
-- JOIN: une conteúdo e interações
-- GROUP BY: agrupa por conteúdo
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

-- 2. Plataforma com maior engajamento (like, share, comment)
-- COUNT(): conta o total de interações
-- WHERE IN: filtra pelos tipos de engajamento
SELECT p.nome, COUNT(*) AS total_engajamento
FROM interacao i
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
WHERE i.tipo_interacao IN ('like', 'share', 'comment')
GROUP BY p.nome
ORDER BY total_engajamento DESC
LIMIT 10;

-- 3. Total de engajamento por tipo e por plataforma
-- CASE WHEN: cria contagens separadas por tipo
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

SELECT 
    c.id_conteudo, c.nome_conteudo, 
    COUNT(*) AS total_comentarios
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
WHERE i.tipo_interacao = 'comment'
GROUP BY c.id_conteudo, c.nome_conteudo
ORDER BY total_comentarios DESC
LIMIT 10;

-- 5. Interações por tipo de conteúdo
-- Agrupamento por tipo de conteúdo
SELECT c.tipo_conteudo, COUNT(*) AS total_interacoes
FROM conteudo c
JOIN interacao i ON c.id_conteudo = i.id_conteudo
GROUP BY c.tipo_conteudo
ORDER BY total_interacoes DESC;

-- 6. Tempo médio de visualização por plataforma
-- AVG: calcula a média de tempo assistido
SELECT 
    p.nome, 
    SEC_TO_TIME(AVG(i.watch_duration_seconds)) AS tempo_medio
FROM interacao i
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY p.nome
ORDER BY tempo_medio DESC;

-- 7. Comentários por conteúdo
-- Mesma estrutura da consulta 4
SELECT 
    c.id_conteudo, 
    c.nome_conteudo, COUNT(*) AS quantidade_comentarios
FROM conteudo c
JOIN interacao i ON c.id_conteudo = i.id_conteudo
WHERE i.tipo_interacao = 'comment'
GROUP BY c.id_conteudo, c.nome_conteudo
ORDER BY quantidade_comentarios DESC;

-- 8. Conteúdos mais assistidos por plataforma
-- COUNT: total de views por conteúdo por plataforma
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

-- Tabela relatorios_sql
CREATE TABLE IF NOT EXISTS relatorios_sql (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE,
    descricao TEXT,
    query_sql TEXT NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. Usuários mais ativos por plataforma
-- Agrupamento duplo por usuário e plataforma
SELECT 
    u.id_usuario, 
    COUNT(i.id_interacao) AS total_interacoes, 
    p.nome AS plataforma
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY u.id_usuario, p.nome
ORDER BY total_interacoes DESC
LIMIT 10;

-- 10. Conteúdos mais compartilhados
SELECT 
    c.id_conteudo, 
    c.nome_conteudo, 
    COUNT(i.id_interacao) AS total_compartilhados
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
WHERE i.tipo_interacao = 'share'
GROUP BY c.id_conteudo, c.nome_conteudo
ORDER BY total_compartilhados DESC
LIMIT 10;

-- 11. Usuários com mais likes
SELECT 
    u.id_usuario, 
    COUNT(i.id_interacao) AS total_likes
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'like'
GROUP BY u.id_usuario
ORDER BY total_likes DESC
LIMIT 10;

-- 12. Conteúdos mais assistidos por usuário
SELECT 
    u.id_usuario, 
    c.nome_conteudo, 
    COUNT(i.id_interacao) AS total_assistidos
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'view'
GROUP BY u.id_usuario, c.nome_conteudo
ORDER BY total_assistidos DESC
LIMIT 10;

-- 13. Conteúdos mais comentados por usuário
SELECT 
    u.id_usuario, 
    c.nome_conteudo, 
    COUNT(i.id_interacao) AS total_comentarios
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'comment'
GROUP BY u.id_usuario, c.nome_conteudo
ORDER BY total_comentarios DESC
LIMIT 10;

-- 14. Conteúdos mais compartilhados por usuário
SELECT 
    u.id_usuario, 
    c.nome_conteudo, 
    COUNT(i.id_interacao) AS total_compartilhados
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'share'
GROUP BY u.id_usuario, c.nome_conteudo
ORDER BY total_compartilhados DESC
LIMIT 10;

-- 15. Usuários com mais interações por plataforma
SELECT 
    u.id_usuario, 
    COUNT(i.id_interacao) AS total_interacoes, p.nome AS plataforma
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY u.id_usuario, p.nome
ORDER BY total_interacoes DESC
LIMIT 10;

-- 6. Tabela para armazenar relatórios SQL
CREATE TABLE IF NOT EXISTS relatorios_sql (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL UNIQUE,
    descricao TEXT,
    query_sql TEXT NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);