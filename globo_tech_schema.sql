-- schema.sql - Criação do banco de dados globo_tech 

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
    titulo VARCHAR(255), -- Título do conteúdo
    tipo_conteudo VARCHAR(50) NOT NULL, -- Tipo do conteúdo (Vídeo, Podcast, Artigo)
    id_plataforma INT NOT NULL, -- Plataforma relacionada ao conteúdo
    FOREIGN KEY (id_plataforma) REFERENCES plataforma(id_plataforma) -- Chave estrangeira para plataforma
);

-- 5. Tabela de Interação
CREATE TABLE IF NOT EXISTS interacao (
    id_interacao INT AUTO_INCREMENT PRIMARY KEY, -- Identificador único da interação
    id_usuario INT NOT NULL, -- Usuário que realizou a interação
    id_conteudo INT NOT NULL, -- Conteúdo relacionado à interação
    id_plataforma INT NOT NULL, -- Plataforma onde ocorreu a interação
    tipo_interacao ENUM('view', 'like', 'share', 'comment') NOT NULL, -- Tipo da interação
    data_interacao DATETIME NOT NULL, -- Data e hora da interação
    watch_duration_seconds INT DEFAULT 0 CHECK (watch_duration_seconds >= 0), -- Duração assistida (em segundos)

    FOREIGN KEY (id_usuario) REFERENCES usuario(id_usuario), -- Chave estrangeira para usuário
    FOREIGN KEY (id_conteudo) REFERENCES conteudo(id_conteudo), -- Chave estrangeira para conteúdo
    FOREIGN KEY (id_plataforma) REFERENCES plataforma(id_plataforma) -- Chave estrangeira para plataforma
);

-- Relatórios (Consultas SQL)

-- 1. Ranking de conteúdos mais consumidos (tempo total de consumo)
-- Lista os 10 conteúdos com maior tempo total de consumo
SELECT c.id_conteudo, c.titulo, c.tipo_conteudo, SUM(i.watch_duration_seconds) AS total_consumo
FROM conteudo c
JOIN interacao i ON c.id_conteudo = i.id_conteudo
GROUP BY c.id_conteudo, c.titulo, c.tipo_conteudo
ORDER BY total_consumo DESC
LIMIT 10;

-- 2. Plataforma com maior engajamento (interações like, share, comment)
-- Mostra as plataformas com maior número de interações de engajamento
SELECT p.nome, COUNT(*) AS total_engajamento
FROM interacao i
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
WHERE i.tipo_interacao IN ('like', 'share', 'comment')
GROUP BY p.nome
ORDER BY total_engajamento DESC
LIMIT 10;

-- 3. Conteúdos mais comentados
-- Lista os conteúdos com maior número de comentários
SELECT c.id_conteudo, c.titulo, COUNT(*) AS total_comentarios
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
WHERE i.tipo_interacao = 'comment'
GROUP BY c.id_conteudo, c.titulo
ORDER BY total_comentarios DESC
LIMIT 10;

-- 4. Total de interações por tipo de conteúdo
-- Mostra o total de interações agrupado pelo tipo de conteúdo
SELECT c.tipo_conteudo, COUNT(*) AS total_interacoes
FROM conteudo c
JOIN interacao i ON c.id_conteudo = i.id_conteudo
GROUP BY c.tipo_conteudo
ORDER BY total_interacoes DESC;

-- 5. Tempo médio de consumo por plataforma
-- Exibe o tempo médio de consumo (em formato de tempo) por plataforma
SELECT p.nome, 
SEC_TO_TIME(AVG(i.watch_duration_seconds)) AS tempo_medio
FROM interacao i
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY p.nome
ORDER BY tempo_medio DESC;

-- 6. Quantidade de comentários registrados por conteúdo
-- Mostra a quantidade de comentários para cada conteúdo
SELECT c.id_conteudo, c.titulo, COUNT(*) AS quantidade_comentarios
FROM conteudo c
JOIN interacao i ON c.id_conteudo = i.id_conteudo
WHERE i.tipo_interacao = 'comment'
GROUP BY c.id_conteudo, c.titulo
ORDER BY quantidade_comentarios DESC;

-- 7. Conteúdos mais assistidos por plataforma
-- Lista os conteúdos mais assistidos em cada plataforma
SELECT p.nome AS plataforma, c.titulo, COUNT(i.id_interacao) AS total_assistidos
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
WHERE i.tipo_interacao = 'view'
GROUP BY p.nome, c.titulo
ORDER BY total_assistidos DESC
LIMIT 10;

-- 8. Usuários mais ativos por plataforma
-- Mostra os usuários com maior número de interações em cada plataforma
SELECT u.id_usuario, COUNT(i.id_interacao) AS total_interacoes, p.nome AS
plataforma
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY u.id_usuario, p.nome
ORDER BY total_interacoes DESC
LIMIT 10;

-- 9. Conteúdos mais compartilhados
-- Lista os conteúdos mais compartilhados
SELECT c.id_conteudo, c.titulo, COUNT(i.id_interacao) AS total_compartilhados
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
WHERE i.tipo_interacao = 'share'
GROUP BY c.id_conteudo, c.titulo
ORDER BY total_compartilhados DESC
LIMIT 10;

-- 10. Usuários com mais likes
-- Mostra os usuários que mais deram likes
SELECT u.id_usuario, COUNT(i.id_interacao) AS total_likes
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'like'
GROUP BY u.id_usuario
ORDER BY total_likes DESC
LIMIT 10;

-- 11. Conteúdos mais assistidos por usuário
-- Lista os conteúdos mais assistidos por cada usuário
SELECT u.id_usuario, c.titulo, COUNT(i.id_interacao) AS total_assistidos
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'view'
GROUP BY u.id_usuario, c.titulo
ORDER BY total_assistidos DESC
LIMIT 10;

-- 12. Conteúdos mais comentados por usuário
-- Mostra os conteúdos mais comentados por cada usuário
SELECT u.id_usuario, c.titulo, COUNT(i.id_interacao) AS total_comentarios
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'comment'
GROUP BY u.id_usuario, c.titulo
ORDER BY total_comentarios DESC
LIMIT 10;

-- 13. Conteúdos mais compartilhados por usuário
-- Lista os conteúdos mais compartilhados por cada usuário
SELECT u.id_usuario, c.titulo, COUNT(i.id_interacao) AS total_compartilhados
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN usuario u ON i.id_usuario = u.id_usuario
WHERE i.tipo_interacao = 'share'
GROUP BY u.id_usuario, c.titulo
ORDER BY total_compartilhados DESC
LIMIT 10;

-- 14. Conteúdos mais assistidos por plataforma
-- Mostra os conteúdos mais assistidos em cada plataforma
SELECT p.nome AS plataforma, c.titulo, COUNT(i.id_interacao) AS total_assistidos
FROM interacao i
JOIN conteudo c ON i.id_conteudo = c.id_conteudo
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
WHERE i.tipo_interacao = 'view'
GROUP BY p.nome, c.titulo
ORDER BY total_assistidos DESC
LIMIT 10;

-- 15. Usuários com mais interações por plataforma
-- Lista os usuários com maior número de interações em cada plataforma
SELECT u.id_usuario, COUNT(i.id_interacao) AS total_interacoes, p.nome AS plataforma
FROM interacao i
JOIN usuario u ON i.id_usuario = u.id_usuario
JOIN plataforma p ON i.id_plataforma = p.id_plataforma
GROUP BY u.id_usuario, p.nome
ORDER BY total_interacoes DESC
LIMIT 10;