# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster

print("Conectando a Cassandra...")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

print("Creando keyspace 'musica'...")
session.execute("""
CREATE KEYSPACE IF NOT EXISTS musica 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('musica')

print("Creando tablas básicas...")
# Tablas principales
session.execute("""
CREATE TABLE IF NOT EXISTS usuarios (
    usuario_id INT PRIMARY KEY,
    nombre TEXT,
    ciudad TEXT,
    pais TEXT
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS canciones (
    cancion_id INT PRIMARY KEY,
    titulo TEXT,
    artista TEXT,
    genero TEXT
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS escuchas (
    usuario_id INT,
    fecha_escucha DATE,
    cancion_id INT,
    PRIMARY KEY (usuario_id, fecha_escucha, cancion_id)
) WITH CLUSTERING ORDER BY (fecha_escucha DESC)
""")

print("Creando tablas de análisis (con COUNTER)...")

# ✅ SOLO columnas COUNTER en estas tablas
session.execute("""
CREATE TABLE IF NOT EXISTS escuchas_por_genero (
    genero TEXT,
    cancion_id INT,
    total_escuchas COUNTER,
    PRIMARY KEY (genero, cancion_id)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS reporte_por_genero (
    genero TEXT,
    mes TEXT,
    total_escuchas COUNTER,
    PRIMARY KEY (genero, mes)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS reporte_por_pais (
    pais TEXT,
    mes TEXT,
    total_escuchas COUNTER,
    PRIMARY KEY (pais, mes)
)
""")

print("✅ ¡Esquema creado exitosamente!")

