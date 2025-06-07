# -*- coding: utf-8 -*-
from cassandra.cluster import Cluster
import csv
from datetime import datetime

import os


print(" Conectando a Cassandra...")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('musica')

# Extraer mes de una fecha (YYYY-MM-DD)
def obtener_mes(fecha_str):
    return fecha_str[:7]

print(" Insertando usuarios...")
with open('data/usuarios.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        session.execute("""
            INSERT INTO usuarios (usuario_id, nombre, ciudad, pais)
            VALUES (%s, %s, %s, %s)
        """, (int(row[0]), row[1], row[3], row[2]))  # ciudad está en la columna 3

print(" Insertando canciones...")
with open('data/canciones.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=',', quotechar='"')
    next(reader)
    for row in reader:
        print("DEBUG:", row)
        if len(row) != 4:
            print("❌ Fila con columnas incorrectas:", row)
            continue  # Saltar fila incorrecta
        session.execute("""
            INSERT INTO canciones (cancion_id, titulo, artista, genero)
            VALUES (%s, %s, %s, %s)
        """, (int(row[0]), row[1], row[2], row[3]))



print(" Insertando escuchas...")
with open('data/escuchas.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        usuario_id = int(row[0])
        cancion_id = int(row[1])
        fecha = datetime.strptime(row[2], "%Y-%m-%d").date()
        session.execute("""
            INSERT INTO escuchas (usuario_id, fecha_escucha, cancion_id)
            VALUES (%s, %s, %s)
        """, (usuario_id, fecha, cancion_id))

print(" Actualizando tablas de análisis...")

# 1. escuchas_por_genero (solo UPDATE con COUNTER)
with open('data/escuchas.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        cancion_id = int(row[1])
        result = session.execute(
            "SELECT genero FROM canciones WHERE cancion_id = %s", [cancion_id]
        ).one()
        if result:
            genero = result.genero
            session.execute("""
                UPDATE escuchas_por_genero 
                SET total_escuchas = total_escuchas + 1
                WHERE genero = %s AND cancion_id = %s
            """, (genero, cancion_id))


# 2. reporte_por_genero y reporte_por_pais
with open('data/escuchas.csv', 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        usuario_id = int(row[0])
        cancion_id = int(row[1])
        mes = obtener_mes(row[2])

        result_cancion = session.execute("SELECT genero FROM canciones WHERE cancion_id = %s", [cancion_id]).one()
        result_usuario = session.execute("SELECT pais FROM usuarios WHERE usuario_id = %s", [usuario_id]).one()

        if result_cancion and result_usuario:
            genero = result_cancion.genero
            pais = result_usuario.pais

            session.execute("""
                UPDATE reporte_por_genero SET total_escuchas = total_escuchas + 1
                WHERE genero = %s AND mes = %s
            """, (genero, mes))

            session.execute("""
                UPDATE reporte_por_pais SET total_escuchas = total_escuchas + 1
                WHERE pais = %s AND mes = %s
            """, (pais, mes))

print(" ¡Datos y análisis OLAP cargados!")
