"""
guillo_api.py — Flask API para skills de Guillo
"""
import os
import json
from flask import Flask, request, jsonify

app = Flask(__name__)

def get_conn():
    import psycopg2
    return psycopg2.connect(os.environ["DATABASE_URL"])

@app.route("/")
def root():
    return jsonify({"status": "Guillo Skills API activa", "version": "1.0.0"})

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok", "db": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "db": str(e)}), 500

@app.route("/ejecutar", methods=["POST"])
def ejecutar_skill():
    data = request.get_json()
    skill_name = data.get("skill", "")
    params = data.get("params", {})
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except:
            params = {}
    telefono = data.get("telefono")
    if telefono and "telefono" not in params:
        params["telefono"] = telefono
    from guillo_skills import ejecutar_skill as _ejecutar
    result = _ejecutar(skill_name, params)
    return jsonify(result)

@app.route("/historial", methods=["POST"])
def obtener_historial():
    data = request.get_json()
    from guillo_skills import skill_obtener_historial
    return jsonify(skill_obtener_historial(
        data.get("telefono", ""), data.get("limite", 20)
    ))

@app.route("/guardar_mensaje", methods=["POST"])
def guardar_mensaje():
    data = request.get_json()
    from guillo_skills import skill_guardar_mensaje
    return jsonify(skill_guardar_mensaje(
        data.get("telefono", ""), data.get("rol", ""),
        data.get("mensaje", ""), data.get("lead_id")
    ))

@app.route("/leads")
def listar_leads():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nombre, empresa, telefono, ciudad,
               urgencia, score, potencial_facturacion, estado, creado
        FROM leads ORDER BY score DESC, creado DESC LIMIT 50
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in rows:
        if r.get("creado"): r["creado"] = str(r["creado"])
    conn.close()
    return jsonify({"leads": rows, "total": len(rows)})

@app.route("/leads/<int:lead_id>/reporte")
def reporte_lead(lead_id):
    from guillo_skills import skill_generar_reporte
    return jsonify(skill_generar_reporte(lead_id))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
