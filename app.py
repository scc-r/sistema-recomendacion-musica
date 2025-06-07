from flask import Flask, render_template_string, request
from cassandra.cluster import Cluster
from datetime import datetime
import re

app = Flask(__name__)
app.secret_key = 'supersecretkey'
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('musica')

TEMPLATE = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Sistema de Recomendación Musical</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { font-family: Arial; padding: 40px; background: #f9f9f9; }
    h1, h2, h3 { color: #333; }
    canvas { margin: 20px 0; }
    select, button, textarea, input { font-size: 16px; margin-top: 10px; }
  </style>
</head>
<body>
  <h1>🎶 Sistema de Recomendación Musical</h1>

  <hr>
  <h2>🎧 Recomendaciones</h2>
  <form method="get">
    <label>Selecciona un género:</label>
    <select name="genero">
      {% for g in generos %}
        <option value="{{ g }}" {% if g == genero %}selected{% endif %}>{{ g }}</option>
      {% endfor %}
    </select>
    <button type="submit">Ver Top 5</button>
  </form>
  <canvas id="cancionesChart"></canvas>

  <hr>
  <h2>📊 Consultas OLAP</h2>
  <form method="get">
    <label>Filtrar por mes (YYYY-MM):</label>
    <input type="text" name="mes" value="{{ mes }}" placeholder="2024-01">
    <button type="submit">Filtrar</button>
  </form>

  <h3>Escuchas por género y mes</h3>
  <canvas id="generoChart"></canvas>

  <h3>Escuchas por país</h3>
  <canvas id="paisChart"></canvas>

  <hr>
  <h2>🛠 Ejecutar consulta manual</h2>
  <form method="post">
    <textarea name="consulta" rows="4" cols="80" placeholder="Escribe tu consulta CQL aquí..."></textarea><br>
    <button type="submit">Ejecutar</button>
  </form>

  <div style="color: green">{{ resultado|safe }}</div>
  <p style="color: red">{{ error }}</p>

<script>
const cancionesLabels = {{ canciones_nombres|safe }};
const cancionesData = {{ canciones_totales|safe }};

new Chart(document.getElementById('cancionesChart'), {
    type: 'bar',
    data: {
        labels: cancionesLabels,
        datasets: [{
            label: 'Total escuchas',
            data: cancionesData,
            backgroundColor: 'rgba(54, 162, 235, 0.6)'
        }]
    }
});

const generoLabels = {{ genero_meses|safe }};
const generoData = {{ genero_totales|safe }};

new Chart(document.getElementById('generoChart'), {
    type: 'bar',
    data: {
        labels: generoLabels,
        datasets: [{
            label: 'Escuchas por género',
            data: generoData,
            backgroundColor: 'rgba(255, 159, 64, 0.6)'
        }]
    }
});

const paisLabels = {{ paises|safe }};
const paisData = {{ pais_totales|safe }};

new Chart(document.getElementById('paisChart'), {
    type: 'pie',
    data: {
        labels: paisLabels,
        datasets: [{
            label: 'Escuchas por país',
            data: paisData,
            backgroundColor: [
                'rgba(255, 99, 132, 0.6)',
                'rgba(54, 162, 235, 0.6)',
                'rgba(255, 206, 86, 0.6)',
                'rgba(75, 192, 192, 0.6)',
                'rgba(153, 102, 255, 0.6)'
            ]
        }]
    }
});
</script>
</body>
</html>
"""

@app.route("/", methods=["GET", "POST"])
def index():
    genero = request.args.get("genero", "Pop")
    mes = request.args.get("mes", "")
    resultado = ""
    error = ""

    if request.method == "POST":
        consulta = request.form.get("consulta")
        try:
            if consulta.strip().lower().startswith("select"):
                rows = session.execute(consulta)
                resultado = """
                <style>
                .result-table {
                border-collapse: collapse;
                width: 100%;
                margin-top: 20px;
                font-family: Arial, sans-serif;
                }
                .result-table th, .result-table td {
                border: 1px solid #ccc;
                padding: 8px 12px;
                text-align: left;
                }
                .result-table th {
                background-color: #f2f2f2;
                color: #333;
                }
                </style>
                <table class="result-table">
                <tr>
                """
                rows = list(session.execute(consulta))
                if rows:
                    for col in rows[0]._fields:
                        resultado += f"<th>{col}</th>"
                    resultado += "</tr>"
                    for row in rows:
                        resultado += "<tr>" + "".join(f"<td>{getattr(row, col)}</td>" for col in row._fields) + "</tr>"
                    resultado += "</table>"
                else:
                    resultado = "🔍 Consulta ejecutada, pero no se encontraron resultados."
            else:
                session.execute(consulta)
                resultado = "✅ Consulta ejecutada correctamente."

                if consulta.strip().lower().startswith("insert into escuchas"):
                    match = re.search(r"\((\d+),\s*'([\d-]+)',\s*(\d+)\)", consulta)
                    if match:
                        usuario_id = int(match.group(1))
                        fecha_str = match.group(2)
                        cancion_id = int(match.group(3))
                        mes_texto = fecha_str[:7]

                        genero_row = session.execute("SELECT genero FROM canciones WHERE cancion_id = %s", [cancion_id]).one()
                        pais_row = session.execute("SELECT pais FROM usuarios WHERE usuario_id = %s", [usuario_id]).one()

                        if genero_row and pais_row:
                            genero_val = genero_row.genero
                            pais_val = pais_row.pais

                            session.execute("""
                                UPDATE escuchas_por_genero SET total_escuchas = total_escuchas + 1
                                WHERE genero = %s AND cancion_id = %s
                            """, (genero_val, cancion_id))

                            session.execute("""
                                UPDATE reporte_por_genero SET total_escuchas = total_escuchas + 1
                                WHERE genero = %s AND mes = %s
                            """, (genero_val, mes_texto))

                            session.execute("""
                                UPDATE reporte_por_pais SET total_escuchas = total_escuchas + 1
                                WHERE pais = %s AND mes = %s
                            """, (pais_val, mes_texto))

                            resultado += "<br>✅ Actualización OLAP completada."
                        else:
                            error = "❌ No se encontró género o país."

        except Exception as e:
            error = f"❌ Error: {e}"

    generos_set = set()
    for row in session.execute("SELECT genero FROM canciones"):
        generos_set.add(row.genero)
    generos = sorted(list(generos_set))

    canciones_nombres = []
    canciones_totales = []
    rows = session.execute("""
        SELECT cancion_id, total_escuchas FROM escuchas_por_genero WHERE genero = %s LIMIT 5
    """, [genero])
    for row in rows:
        c = session.execute("SELECT titulo FROM canciones WHERE cancion_id = %s", [row.cancion_id]).one()
        if c:
            canciones_nombres.append(c.titulo)
            canciones_totales.append(row.total_escuchas)

    genero_meses = []
    genero_totales = []
    rows = session.execute("SELECT genero, mes, total_escuchas FROM reporte_por_genero")
    for row in rows:
        if mes and row.mes != mes:
            continue
        genero_meses.append(f"{row.genero} ({row.mes})")
        genero_totales.append(row.total_escuchas)

    paises = []
    pais_totales = []
    for row in session.execute("SELECT pais, mes, total_escuchas FROM reporte_por_pais"):
        paises.append(f"{row.pais} ({row.mes})")
        pais_totales.append(row.total_escuchas)

    return render_template_string(
        TEMPLATE,
        genero=genero,
        generos=generos,
        canciones_nombres=canciones_nombres,
        canciones_totales=canciones_totales,
        genero_meses=genero_meses,
        genero_totales=genero_totales,
        paises=paises,
        pais_totales=pais_totales,
        mes=mes,
        resultado=resultado,
        error=error
    )

if __name__ == "__main__":
    app.run(debug=True)


