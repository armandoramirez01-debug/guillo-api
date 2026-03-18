"""
guillo_api.py
=============
API FastAPI para las skills de Guillo.
Se despliega en Render.com de forma gratuita.
n8n llama a este endpoint para ejecutar las skills.
"""
import os
import json
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any

# Importar skills
import sys
sys.path.insert(0, os.path.dirname(__file__))

app = FastAPI(title="Guillo Skills API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Modelos ────────────────────────────────────────────────────────────────

class SkillRequest(BaseModel):
    skill: str
    params: dict = {}
    telefono: Optional[str] = None

class MensajeRequest(BaseModel):
    telefono: str
    rol: str
    mensaje: str
    lead_id: Optional[int] = None

class HistorialRequest(BaseModel):
    telefono: str
    limite: Optional[int] = 20

# ── DB Connection ──────────────────────────────────────────────────────────

def get_conn():
    import psycopg2
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "Guillo Skills API activa", "version": "1.0.0"}

@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": str(e)}

@app.post("/ejecutar")
def ejecutar_skill(req: SkillRequest):
    """Ejecuta cualquier skill de Guillo."""
    from guillo_skills import ejecutar_skill as _ejecutar
    # Agregar teléfono a params si viene
    params = req.params.copy()
    if req.telefono and "telefono" not in params:
        params["telefono"] = req.telefono
    result = _ejecutar(req.skill, params)
    return result

@app.post("/historial")
def obtener_historial(req: HistorialRequest):
    """Obtiene historial de conversación por teléfono."""
    from guillo_skills import skill_obtener_historial
    return skill_obtener_historial(req.telefono, req.limite)

@app.post("/guardar_mensaje")
def guardar_mensaje(req: MensajeRequest):
    """Guarda un mensaje en el historial."""
    from guillo_skills import skill_guardar_mensaje
    return skill_guardar_mensaje(req.telefono, req.rol, req.mensaje, req.lead_id)

@app.get("/leads")
def listar_leads():
    """Lista todos los leads activos."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, nombre, empresa, telefono, ciudad,
               urgencia, score, potencial_facturacion,
               estado, creado
        FROM leads
        ORDER BY score DESC, creado DESC
        LIMIT 50
    """)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    # Convert datetime to string
    for r in rows:
        if r.get("creado"):
            r["creado"] = str(r["creado"])
    return {"leads": rows, "total": len(rows)}

@app.get("/leads/{lead_id}/reporte")
def reporte_lead(lead_id: int):
    """Genera el reporte completo de un lead."""
    from guillo_skills import skill_generar_reporte
    return skill_generar_reporte(lead_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
