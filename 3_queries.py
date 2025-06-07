# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('musica')

# 1. Recomendación por género
def recomendar_canciones_por_genero(genero):
    print(f"\n🎧 Top canciones del género {genero}:\n")
    rows = session.execute("""
        SELECT cancion_id, total_escuchas 
        FROM escuchas_por_genero 
        WHERE genero = %s 
        LIMIT 5
    """, [genero])
    for row in rows:
        cancion = session.execute("SELECT titulo, artista FROM canciones WHERE cancion_id = %s", [row.cancion_id]).one()
        print(f"- {cancion.titulo} - {cancion.artista} ({row.total_escuchas} escuchas)")

# 2. Reporte por género y mes
def reporte_genero_mes():
    print("\n📊 Escuchas por género y mes:\n")
    rows = session.execute("SELECT genero, mes, total_escuchas FROM reporte_por_genero")
    for row in rows:
        print(f"- {row.genero} ({row.mes}): {row.total_escuchas} escuchas")

# 3. Reporte por país
def reporte_pais_mes():
    print("\n🌍 Escuchas por país y mes:\n")
    rows = session.execute("SELECT pais, mes, total_escuchas FROM reporte_por_pais")
    for row in rows:
        print(f"- {row.pais} ({row.mes}): {row.total_escuchas} escuchas")

# Ejecutar todo
recomendar_canciones_por_genero("Pop")
reporte_genero_mes()
reporte_pais_mes()
