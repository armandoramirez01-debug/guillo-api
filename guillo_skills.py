"""
guillo_skills.py
================
API de skills para el agente Guillo.
n8n llama a este endpoint con las herramientas que Guillo necesita.

Desplegable como script independiente o integrable en la app Streamlit.
"""
import os
import json
import hashlib
from datetime import datetime
from typing import Optional

# ── Base de datos ─────────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")

def get_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 1 — COBERTURA Y TARIFA 99MIN
# ══════════════════════════════════════════════════════════════════════════════

def skill_cobertura_99min(cp: str = None, ciudad: str = None,
                           peso_kg: float = 5.0) -> dict:
    """
    Dado un código postal o ciudad, retorna cobertura y tarifa de 99 Minutos Sprint.
    """
    conn = get_conn()
    cur  = conn.cursor()

    if cp:
        cp_clean = str(cp).strip().zfill(5)
        cur.execute("""
            SELECT cp, estado, municipio, zona,
                   tarifa_1_5kg, tarifa_6_10kg, tarifa_11_15kg, kg_adicional,
                   sameday, recoleccion
            FROM tarifas_sprint_99min
            WHERE cp = %s
        """, (cp_clean,))
        row = cur.fetchone()

        if not row:
            conn.close()
            return {"cobertura": False, "mensaje": f"CP {cp_clean} sin cobertura de 99 Minutos Sprint"}

        cp_db, estado, municipio, zona, t15, t610, t1115, tadd, sameday, rec = row

        # Determinar tarifa según peso
        if peso_kg <= 5:
            tarifa = t15
        elif peso_kg <= 10:
            tarifa = t610
        elif peso_kg <= 15:
            tarifa = t1115
        else:
            tarifa_base = t1115
            kg_extra = peso_kg - 15
            tarifa = tarifa_base + (kg_extra * tadd)

        conn.close()
        return {
            "cobertura": True,
            "cp": cp_clean,
            "estado": estado,
            "municipio": municipio,
            "zona": zona,
            "tarifa_estimada": round(tarifa, 2),
            "peso_kg": peso_kg,
            "sameday": sameday,
            "recoleccion_disponible": rec,
            "retorno_pct": 50,
            "proveedor": "99 Minutos Sprint",
            "mensaje": f"✅ Cobertura disponible en {municipio}, {estado} — Zona {zona}"
        }

    elif ciudad:
        ciudad_clean = ciudad.strip().upper()
        cur.execute("""
            SELECT COUNT(*) as total,
                   AVG(tarifa_1_5kg) as avg_15,
                   AVG(tarifa_6_10kg) as avg_610,
                   zona,
                   MAX(estado) as estado
            FROM tarifas_sprint_99min
            WHERE UPPER(municipio) LIKE %s OR UPPER(estado) LIKE %s
            GROUP BY zona
            ORDER BY total DESC
            LIMIT 1
        """, (f"%{ciudad_clean}%", f"%{ciudad_clean}%"))
        row = cur.fetchone()
        conn.close()

        if not row:
            return {"cobertura": False, "mensaje": f"Sin cobertura en {ciudad}"}

        total, avg15, avg610, zona, estado = row
        return {
            "cobertura": True,
            "ciudad": ciudad,
            "zona": zona,
            "cps_cubiertos": total,
            "tarifa_aprox_1_5kg": round(avg15, 2),
            "tarifa_aprox_6_10kg": round(avg610, 2),
            "proveedor": "99 Minutos Sprint",
            "mensaje": f"✅ Cobertura en {ciudad} — Zona {zona} ({total} CPs cubiertos)"
        }

    conn.close()
    return {"error": "Proporciona cp o ciudad"}


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 2 — COSTOS OPERATIVOS
# ══════════════════════════════════════════════════════════════════════════════

def skill_calcular_costos(ciudad: str, m2: int,
                           pedidos_mes: int,
                           margen: float = 0.30) -> dict:
    """
    Calcula costo interno estimado y precio de venta con margen para una operación.
    """
    conn = get_conn()
    cur  = conn.cursor()

    # Buscar sucursales en esa ciudad
    cur.execute("""
        SELECT zona, nombre, metros_bodega, picker_mes, luz_mes,
               renta_mes, otros_fijos_mes, costo_fijo_total,
               precio_total, precio_x_orden, precio_x_m2
        FROM costos_operativos_zona
        WHERE UPPER(nombre) LIKE %s OR UPPER(ciudad) LIKE %s
        ORDER BY ABS(metros_bodega - %s)
        LIMIT 3
    """, (f"%{ciudad.upper()}%", f"%{ciudad.upper()}%", m2))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        # Usar promedios globales si no hay data específica
        cur2 = get_conn().cursor()
        cur2.execute("""
            SELECT AVG(picker_mes), AVG(luz_mes), AVG(renta_mes),
                   AVG(otros_fijos_mes), AVG(precio_x_m2), AVG(precio_x_orden)
            FROM costos_operativos_zona
        """)
        avgs = cur2.fetchone()
        cur2.connection.close()

        picker = avgs[0] or 14665
        luz_por_m2 = (avgs[1] or 10370) / 250
        renta_por_m2 = (avgs[2] or 65000) / 250
        otros_por_m2 = (avgs[3] or 15000) / 250
    else:
        # Promediar las sucursales encontradas
        r = rows[0]
        metros_ref = r[2] or 250
        picker = r[3] or 14665
        luz_por_m2 = (r[4] or 0) / metros_ref
        renta_por_m2 = (r[5] or 0) / metros_ref
        otros_por_m2 = (r[6] or 0) / metros_ref

    # Calcular para los m² del lead
    costo_picker  = picker
    costo_luz     = luz_por_m2 * m2
    costo_renta   = renta_por_m2 * m2
    costo_otros   = otros_por_m2 * m2
    costo_total   = costo_picker + costo_luz + costo_renta + costo_otros

    # Precio de venta con margen
    precio_venta  = costo_total / (1 - margen)
    ganancia_est  = precio_venta - costo_total

    # Costo y precio por pedido
    costo_x_pedido  = costo_total / pedidos_mes if pedidos_mes > 0 else 0
    precio_x_pedido = precio_venta / pedidos_mes if pedidos_mes > 0 else 0

    # OTE estimado (inversión inicial ~3 meses de operación + adecuaciones)
    ote_estimado = costo_total * 2.5

    return {
        "ciudad": ciudad,
        "m2": m2,
        "pedidos_mes": pedidos_mes,
        "costo_interno": {
            "picker_mes":   round(costo_picker, 2),
            "luz_mes":      round(costo_luz, 2),
            "renta_mes":    round(costo_renta, 2),
            "otros_mes":    round(costo_otros, 2),
            "total_mes":    round(costo_total, 2),
        },
        "precio_venta": {
            "mensual":      round(precio_venta, 2),
            "por_pedido":   round(precio_x_pedido, 2),
            "por_m2":       round(precio_venta / m2, 2),
        },
        "gross_margin_pct": margen * 100,
        "ganancia_estimada": round(ganancia_est, 2),
        "ote_estimado": round(ote_estimado, 2),
        "costo_x_pedido_interno": round(costo_x_pedido, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 3 — MODELOS OPERATIVOS
# ══════════════════════════════════════════════════════════════════════════════

def skill_modelos_operativos(m2: int = 0, pedidos_dia: int = 0,
                              ciudades: int = 1,
                              requiere_frio: bool = False) -> dict:
    """
    Retorna los modelos operativos aplicables según el perfil del lead.
    NO incluye precios — solo descripción y ventajas.
    """
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT codigo, nombre, descripcion, ventajas, requisitos,
               m2_minimo, pedidos_minimos, ciudades
        FROM modelos_operativos
        WHERE activo = TRUE
        ORDER BY m2_minimo
    """)
    todos = cur.fetchall()
    conn.close()

    pedidos_mes = pedidos_dia * 26  # ~26 días hábiles
    aplicables  = []
    posibles    = []

    for row in todos:
        codigo, nombre, desc, ventajas, req, m2_min, ped_min, ciudades_mod = row

        cumple_m2  = m2 >= m2_min
        cumple_ped = pedidos_mes >= ped_min
        cumple_ciu = ciudades >= 2 if codigo == "HIBRIDO_REGIONAL" else True

        if cumple_m2 and cumple_ped and cumple_ciu:
            aplicables.append({
                "codigo": codigo,
                "nombre": nombre,
                "descripcion": desc,
                "ventajas": ventajas,
                "fit": "✅ Ideal para tu operación"
            })
        elif (m2 >= m2_min * 0.7) and (pedidos_mes >= ped_min * 0.7):
            posibles.append({
                "codigo": codigo,
                "nombre": nombre,
                "descripcion": desc,
                "fit": "⚡ Posible con ajustes"
            })

    return {
        "modelos_recomendados": aplicables[:3],
        "modelos_posibles": posibles[:2],
        "total_encontrados": len(aplicables),
        "perfil_analizado": {
            "m2": m2,
            "pedidos_dia": pedidos_dia,
            "pedidos_mes": pedidos_mes,
            "ciudades": ciudades,
            "requiere_frio": requiere_frio
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 4 — GUARDAR/ACTUALIZAR LEAD
# ══════════════════════════════════════════════════════════════════════════════

def skill_guardar_lead(datos: dict) -> dict:
    """
    Guarda o actualiza un lead en la base de datos.
    """
    conn = get_conn()
    cur  = conn.cursor()

    telefono = datos.get("telefono", "").strip()
    if not telefono:
        conn.close()
        return {"error": "telefono requerido"}

    # Calcular score de urgencia
    score = 0
    urgencia = datos.get("urgencia", "normal")
    if urgencia == "inmediata":   score += 40
    elif urgencia == "1_mes":     score += 30
    elif urgencia == "3_meses":   score += 20

    pedidos = datos.get("pedidos_mes", 0) or 0
    if pedidos > 1000:   score += 30
    elif pedidos > 500:  score += 20
    elif pedidos > 100:  score += 10

    if datos.get("email"):      score += 10
    if datos.get("empresa"):    score += 10

    # Calcular potencial de facturación estimado
    m2 = datos.get("m2_requeridos", 50) or 50
    pot_facturacion = None
    if pedidos and m2:
        # Estimación rápida: precio por m² promedio
        pot_facturacion = (m2 * 800) + (pedidos * 6)

    cur.execute("""
        INSERT INTO leads
        (nombre, empresa, sector, telefono, email, ciudad,
         ciudades_operacion, m2_requeridos, pedidos_mes,
         tipo_operacion, requiere_frio, urgencia, score,
         potencial_facturacion, notas)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (telefono) DO UPDATE SET
            nombre=COALESCE(EXCLUDED.nombre, leads.nombre),
            empresa=COALESCE(EXCLUDED.empresa, leads.empresa),
            sector=COALESCE(EXCLUDED.sector, leads.sector),
            email=COALESCE(EXCLUDED.email, leads.email),
            ciudad=COALESCE(EXCLUDED.ciudad, leads.ciudad),
            ciudades_operacion=COALESCE(EXCLUDED.ciudades_operacion, leads.ciudades_operacion),
            m2_requeridos=COALESCE(EXCLUDED.m2_requeridos, leads.m2_requeridos),
            pedidos_mes=COALESCE(EXCLUDED.pedidos_mes, leads.pedidos_mes),
            urgencia=COALESCE(EXCLUDED.urgencia, leads.urgencia),
            score=EXCLUDED.score,
            potencial_facturacion=COALESCE(EXCLUDED.potencial_facturacion, leads.potencial_facturacion),
            actualizado=NOW()
        RETURNING id
    """, (
        datos.get("nombre"), datos.get("empresa"), datos.get("sector"),
        telefono, datos.get("email"), datos.get("ciudad"),
        datos.get("ciudades_operacion"), datos.get("m2_requeridos"),
        pedidos, datos.get("tipo_operacion"), datos.get("requiere_frio", False),
        urgencia, score, pot_facturacion, datos.get("notas"),
    ))

    lead_id = cur.fetchone()[0]
    conn.commit()
    conn.close()

    return {
        "lead_id": lead_id,
        "score": score,
        "potencial_facturacion": pot_facturacion,
        "mensaje": f"Lead guardado correctamente (ID: {lead_id})"
    }


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 5 — GUARDAR MENSAJE EN CONVERSACIÓN
# ══════════════════════════════════════════════════════════════════════════════

def skill_guardar_mensaje(telefono: str, rol: str,
                           mensaje: str, lead_id: int = None) -> dict:
    """
    Guarda un mensaje en el historial de conversación de Guillo.
    rol: 'guillo' | 'lead'
    """
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        INSERT INTO conversaciones_guillo (lead_id, telefono, rol, mensaje)
        VALUES (%s, %s, %s, %s)
    """, (lead_id, telefono, rol, mensaje))

    conn.commit()
    conn.close()
    return {"guardado": True}


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 6 — OBTENER HISTORIAL DE CONVERSACIÓN
# ══════════════════════════════════════════════════════════════════════════════

def skill_obtener_historial(telefono: str, limite: int = 20) -> dict:
    """
    Obtiene el historial de conversación para mantener contexto.
    """
    conn = get_conn()
    cur  = conn.cursor()

    # Obtener lead_id
    cur.execute("SELECT id FROM leads WHERE telefono = %s", (telefono,))
    lead_row = cur.fetchone()
    lead_id  = lead_row[0] if lead_row else None

    cur.execute("""
        SELECT rol, mensaje, timestamp
        FROM conversaciones_guillo
        WHERE telefono = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """, (telefono, limite))

    mensajes = [{"rol": r[0], "mensaje": r[1],
                 "timestamp": str(r[2])} for r in cur.fetchall()]
    mensajes.reverse()  # Orden cronológico

    conn.close()
    return {
        "lead_id": lead_id,
        "telefono": telefono,
        "mensajes": mensajes,
        "total": len(mensajes)
    }


# ══════════════════════════════════════════════════════════════════════════════
# SKILL 7 — GENERAR REPORTE PARA COMERCIAL
# ══════════════════════════════════════════════════════════════════════════════

def skill_generar_reporte(lead_id: int) -> dict:
    """
    Genera el reporte completo del lead para el equipo comercial.
    Incluye análisis, costos estimados y pre-propuesta.
    """
    conn = get_conn()
    cur  = conn.cursor()

    # Obtener datos del lead
    cur.execute("SELECT * FROM leads WHERE id = %s", (lead_id,))
    cols = [d[0] for d in cur.description]
    lead_row = cur.fetchone()
    if not lead_row:
        conn.close()
        return {"error": "Lead no encontrado"}

    lead = dict(zip(cols, lead_row))

    # Obtener conversación
    cur.execute("""
        SELECT rol, mensaje FROM conversaciones_guillo
        WHERE lead_id = %s ORDER BY timestamp
    """, (lead_id,))
    conv = [f"[{r[0].upper()}]: {r[1]}" for r in cur.fetchall()]
    conn.close()

    # Calcular propuesta
    m2  = lead.get("m2_requeridos") or 50
    ped = lead.get("pedidos_mes") or 100
    ciu = lead.get("ciudad") or "CDMX"

    costos = skill_calcular_costos(ciu, m2, ped)
    modelos = skill_modelos_operativos(m2, ped // 26)

    reporte = {
        "lead": {
            "id": lead_id,
            "nombre": lead.get("nombre"),
            "empresa": lead.get("empresa"),
            "sector": lead.get("sector"),
            "telefono": lead.get("telefono"),
            "email": lead.get("email"),
            "ciudad": lead.get("ciudad"),
            "m2_requeridos": m2,
            "pedidos_mes": ped,
            "urgencia": lead.get("urgencia"),
            "score": lead.get("score"),
        },
        "analisis_financiero": {
            "costo_interno_mensual": costos["costo_interno"]["total_mes"],
            "precio_venta_mensual":  costos["precio_venta"]["mensual"],
            "gross_margin_30pct":    costos["ganancia_estimada"],
            "precio_por_pedido":     costos["precio_venta"]["por_pedido"],
            "inversion_inicial_est": costos["ote_estimado"],
            "potencial_anual":       costos["precio_venta"]["mensual"] * 12,
        },
        "modelos_recomendados": [m["nombre"] for m in modelos["modelos_recomendados"]],
        "resumen_conversacion": "\n".join(conv[-20:]),
        "fecha_generacion": datetime.now().isoformat(),
        "siguiente_paso": "Contactar al lead en menos de 2 horas con propuesta personalizada",
    }

    return reporte


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT PRINCIPAL — llamado desde n8n
# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
# SKILL 8 — CALCULAR COSTO DE RUTA (casetas + combustible + operador)
# ══════════════════════════════════════════════════════════════════════════════

def skill_calcular_ruta(origen: str, destino: str,
                         tipo_vehiculo: str = "Tortón 10 Ton") -> dict:
    """
    Calcula el costo total de una ruta incluyendo casetas, combustible y operador.
    """
    conn = get_conn()
    cur  = conn.cursor()

    # Buscar corredor
    cur.execute("""
        SELECT id, nombre, distancia_km, num_casetas, costo_auto,
               costo_carga_56, costo_carga_79
        FROM corredores_carreteros
        WHERE UPPER(origen) LIKE %s AND UPPER(destino) LIKE %s
           OR UPPER(nombre) LIKE %s
        LIMIT 1
    """, (
        f"%{origen.upper()}%", f"%{destino.upper()}%",
        f"%{origen.upper()}%{destino.upper()}%"
    ))
    corredor = cur.fetchone()

    # Parámetros del vehículo
    cur.execute("""
        SELECT rendimiento_kml, costo_diesel_l, velocidad_prom, costo_operador_dia, ejes
        FROM parametros_transporte
        WHERE UPPER(tipo_vehiculo) LIKE %s
        LIMIT 1
    """, (f"%{tipo_vehiculo.upper()}%",))
    vehiculo = cur.fetchone()
    conn.close()

    if not vehiculo:
        # Defaults para tortón
        rendimiento, diesel, velocidad, op_dia, ejes = 4.0, 21.79, 70, 1400, "6"
    else:
        rendimiento, diesel, velocidad, op_dia, ejes = vehiculo

    if not corredor:
        return {
            "error": f"No se encontró ruta de {origen} a {destino}",
            "sugerencia": "Rutas disponibles: CDMX-Tijuana, CDMX-Monterrey, CDMX-Cancún, MTY-Juárez"
        }

    corr_id, nombre_corr, distancia, num_casetas, costo_auto, costo_56, costo_79 = corredor

    # Determinar costo de casetas según ejes
    ejes_num = int(ejes.split("-")[0]) if "-" in str(ejes) else int(ejes)
    if ejes_num <= 4:
        costo_casetas = costo_auto or 0
    elif ejes_num <= 6:
        costo_casetas = costo_56 or (costo_auto * 3 if costo_auto else 0)
    else:
        costo_casetas = costo_79 or (costo_auto * 4 if costo_auto else 0)

    # Calcular combustible
    litros_necesarios = distancia / rendimiento
    costo_combustible  = litros_necesarios * diesel

    # Calcular tiempo y operador
    horas_viaje = distancia / velocidad
    dias_viaje  = max(1, round(horas_viaje / 8))
    costo_operador = dias_viaje * op_dia

    # Total
    costo_total = costo_casetas + costo_combustible + costo_operador

    return {
        "ruta": nombre_corr,
        "origen": origen,
        "destino": destino,
        "vehiculo": tipo_vehiculo,
        "distancia_km": distancia,
        "num_casetas": num_casetas,
        "desglose": {
            "casetas": round(costo_casetas, 2),
            "combustible": round(costo_combustible, 2),
            "operador": round(costo_operador, 2),
        },
        "costo_total": round(costo_total, 2),
        "tiempo_estimado": f"{horas_viaje:.1f} horas ({dias_viaje} días)",
        "litros_diesel": round(litros_necesarios, 1),
        "nota": "TAG obligatorio desde 2026 para deducibilidad fiscal"
    }

def ejecutar_skill(skill_name: str, params: dict) -> dict:
    """
    Router principal de skills. n8n llama a este método.
    """
    skills = {
        "cobertura_99min":      skill_cobertura_99min,
        "calcular_costos":      skill_calcular_costos,
        "modelos_operativos":   skill_modelos_operativos,
        "guardar_lead":         skill_guardar_lead,
        "guardar_mensaje":      skill_guardar_mensaje,
        "obtener_historial":    skill_obtener_historial,
        "generar_reporte":      skill_generar_reporte,
        "calcular_ruta":        skill_calcular_ruta,
    }

    if skill_name not in skills:
        return {"error": f"Skill '{skill_name}' no encontrada"}

    try:
        return skills[skill_name](**params)
    except Exception as e:
        return {"error": str(e), "skill": skill_name}


if __name__ == "__main__":
    # Test rápido
    print("Probando skill_cobertura_99min para CP 06600 (Juárez, CDMX)...")
    result = skill_cobertura_99min(cp="06600", peso_kg=3)
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("\nProbando skill_calcular_costos para CDMX 100m² 500 pedidos/mes...")
    result2 = skill_calcular_costos("CDMX", 100, 500)
    print(json.dumps(result2, ensure_ascii=False, indent=2))
