import os
import re
import json
import unicodedata
import requests
from flask import Flask, request, jsonify
from datetime import datetime
import pytz
from difflib import SequenceMatcher
from urllib.parse import quote

app = Flask(__name__)

# =========================
# Config (usar variables de entorno)
# =========================
EVO_BASE_URL = "https://evo-integracao-api.w12app.com.br/api/v1"
EVO_BASE_URL_V2 = "https://evo-integracao-api.w12app.com.br/api/v2"

EVO_USER = os.getenv("EVO_USER")              # antes estaba hardcodeado
EVO_PASS = os.getenv("EVO_PASS")              # antes estaba hardcodeado
BSALE_TOKEN = os.getenv("BSALE_TOKEN")        # antes estaba hardcodeado

# Tipos de documento:
# - NOM: documento nominativo (cuando SÍ hay clientId)
# - NN : documento NO nominativo (cuando NO hay clientId) -> evita “inicial demo”
DOCUMENT_TYPE_ID_NOM = int(os.getenv("DOCUMENT_TYPE_ID_NOM", "1"))   # ajusta según tu cuenta
DOCUMENT_TYPE_ID_NN  = int(os.getenv("DOCUMENT_TYPE_ID_NN",  "28"))  # ajusta según tu cuenta

PRICE_LIST_ID = int(os.getenv("PRICE_LIST_ID", "2"))

# Sucursales (EVO -> Bsale)  | Ajusta según tu mapeo real
SUCURSALES_EVO = [424231, 424286, 424232, 424287]
SUCURSALES_BSALE = {424231: 1, 424286: 2, 424232: 1, 424287: 3}

CHILE_TZ = pytz.timezone("America/Santiago")

# Persistencia simple local (si no usás Sheets)
TX_FILE = "ventas_emitidas.json"
if not os.path.exists(TX_FILE):
    with open(TX_FILE, "w", encoding="utf-8") as f:
        json.dump([], f)

# Productos / variantes
VARIANT_ID_OTHERS = int(os.getenv("VARIANT_ID_OTHERS", "1244"))
VARIANT_MAP = {}  # si usás archivo, cargarlo aquí

# Excluir clientes Bsale por ID (típicamente 1 = cliente por defecto/inicial demo)
BSALE_EXCLUDE_CLIENT_IDS = {int(x.strip()) for x in os.getenv("BSALE_EXCLUDE_CLIENT_IDS", "1").split(",") if x.strip().isdigit()}

# =========================
# Helpers
# =========================
def normalizar_nombre(nombre: str) -> str:
    if not nombre:
        return ''
    nombre = nombre.lower().strip()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    return ' '.join(nombre.split())

def normalizar_rut_chile(rut: str | None) -> str | None:
    if not rut:
        return None
    rut = rut.strip().upper().replace(".", "").replace(" ", "")
    rut = re.sub(r"[^0-9K\-]", "", rut)
    if "-" not in rut and len(rut) > 1:
        cuerpo, dv = rut[:-1], rut[-1]
        rut = f"{cuerpo}-{dv}"
    return rut

def similitud(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, normalizar_nombre(a), normalizar_nombre(b)).ratio()

def rango_hoy():
    ahora = datetime.now(CHILE_TZ)
    return ahora.replace(hour=0, minute=0, second=0, microsecond=0), ahora.replace(hour=23, minute=59, second=59, microsecond=0)

def cargar_ventas_emitidas() -> set:
    with open(TX_FILE, "r", encoding="utf-8") as f:
        try:
            return set(json.load(f))
        except Exception:
            return set()

def guardar_ventas_emitidas(ids: set):
    with open(TX_FILE, "w", encoding="utf-8") as f:
        json.dump(list(ids), f, ensure_ascii=False, indent=2)

# =========================
# EVO
# =========================
def obtener_receivables(id_branch, inicio, fin):
    resultados, skip = [], 0
    while True:
        params = {
            "saleDateStart": inicio.strftime("%Y-%m-%dT%H:%M"),
            "saleDateEnd": fin.strftime("%Y-%m-%dT%H:%M"),
            "idBranchMember": id_branch,
            "status": 2,
            "take": 50,
            "skip": skip
        }
        res = requests.get(f"{EVO_BASE_URL}/receivables", auth=(EVO_USER, EVO_PASS), params=params, timeout=20)
        res.raise_for_status()
        data = res.json()
        lote = data if isinstance(data, list) else data.get("receivables", [])
        if not lote:
            break
        resultados.extend(lote)
        if len(lote) < 50:
            break
        skip += 50
    return resultados

def obtener_detalle_venta(id_sale):
    url = f"{EVO_BASE_URL}/sales/{id_sale}"
    res = requests.get(url, auth=(EVO_USER, EVO_PASS), timeout=15)
    res.raise_for_status()
    return res.json().get("saleItens", [])

def obtener_nombre_y_documento_de_sale(id_sale):
    url_sale = f"{EVO_BASE_URL}/sales/{id_sale}"
    res_sale = requests.get(url_sale, auth=(EVO_USER, EVO_PASS), timeout=15)
    res_sale.raise_for_status()
    sale = res_sale.json()
    id_member = sale.get("idMember")
    if not id_member:
        return None, None, None
    url_member = f"{EVO_BASE_URL_V2}/members/{id_member}"
    res_member = requests.get(url_member, auth=(EVO_USER, EVO_PASS), timeout=15)
    res_member.raise_for_status()
    member = res_member.json()
    nombre = f"{member.get('firstName', '').strip()} {member.get('lastName', '').strip()}".strip() or "Cliente EVO"
    documento = normalizar_rut_chile(member.get('document'))
    email = member.get('email')
    return nombre, documento, email

# =========================
# Bsale: clientes (solo búsqueda; NO crea)
# =========================
def _es_demo(nombre: str | None) -> bool:
    if not nombre:
        return False
    n = normalizar_nombre(nombre)
    return n.startswith("inicial") or "demo" in n or "cliente por defecto" in n

def _rut_match_item(item: dict, rut_norm: str) -> bool:
    tax = normalizar_rut_chile(item.get("taxNumber"))
    cod = normalizar_rut_chile(item.get("code"))
    return (tax == rut_norm) or (cod == rut_norm)

def buscar_cliente_por_rut(rut: str | None):
    rut_norm = normalizar_rut_chile(rut)
    if not rut_norm:
        return None
    headers = {"access_token": BSALE_TOKEN}
    try:
        # Primero: taxnumber directo
        url = f"https://api.bsale.io/v1/clients.json?taxnumber={quote(rut_norm)}&limit=100"
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        items = r.json().get("items", []) or []
        candidatos = [it for it in items if _rut_match_item(it, rut_norm) and it.get("id") not in BSALE_EXCLUDE_CLIENT_IDS and not _es_demo(it.get("name"))]
        if candidatos:
            elegido = sorted(candidatos, key=lambda x: x.get("id", 0), reverse=True)[0]
            return elegido["id"]
        # Fallback: búsqueda general por q
        url_q = f"https://api.bsale.io/v1/clients.json?q={quote(rut_norm)}&limit=100"
        r2 = requests.get(url_q, headers=headers, timeout=20)
        r2.raise_for_status()
        items2 = r2.json().get("items", []) or []
        candidatos = [it for it in items2 if _rut_match_item(it, rut_norm) and it.get("id") not in BSALE_EXCLUDE_CLIENT_IDS and not _es_demo(it.get("name"))]
        if candidatos:
            elegido = sorted(candidatos, key=lambda x: x.get("id", 0), reverse=True)[0]
            return elegido["id"]
    except Exception:
        return None
    return None

def buscar_cliente_por_nombre(nombre: str, umbral=0.92):
    if not nombre:
        return None
    headers = {"access_token": BSALE_TOKEN}
    try:
        best = None  # (score, item)
        offset, limit = 0, 100
        while True:
            url = f"https://api.bsale.io/v1/clients.json?q={quote(nombre)}&limit={limit}&offset={offset}"
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            items = r.json().get("items", []) or []
            if not items:
                break
            for it in items:
                if it.get("id") in BSALE_EXCLUDE_CLIENT_IDS or _es_demo(it.get("name")):
                    continue
                score = similitud(nombre, it.get("name", ""))
                if (best is None) or (score > best[0]):
                    best = (score, it)
            if len(items) < limit:
                break
            offset += limit
        if best and best[0] >= umbral:
            return best[1]["id"]
    except Exception:
        return None
    return None

def obtener_cliente_id_bsale(nombre_evo: str | None, rut_evo: str | None) -> int | None:
    # 1) RUT exacto
    cid = buscar_cliente_por_rut(rut_evo)
    if cid:
        return cid
    # 2) Fuzzy por nombre (umbral alto) con exclusiones
    return buscar_cliente_por_nombre(nombre_evo, umbral=0.92)

# =========================
# Variantes / Detalles
# =========================
def buscar_variant_id(nombre):
    nombre_normalizado = normalizar_nombre(nombre)
    if nombre_normalizado in VARIANT_MAP:
        return VARIANT_MAP[nombre_normalizado]
    for clave, vid in VARIANT_MAP.items():
        if nombre_normalizado in clave or clave in nombre_normalizado:
            return vid
    return None

def construir_detalles(items_evo, rec):
    detalles = []
    for item in items_evo:
        variant_id = buscar_variant_id(item["nombre"]) or VARIANT_ID_OTHERS
        valor_neto = round(item["precio"] / 1.19)
        if valor_neto <= 0:
            continue
        detalles.append({
            "quantity": item.get("cantidad", 1),
            "variantId": variant_id,
            "netUnitValue": valor_neto
        })
    if not detalles:
        # fallback: 1 ítem genérico con monto pagado
        detalles.append({
            "quantity": 1,
            "variantId": VARIANT_ID_OTHERS,
            "netUnitValue": round(rec.get("ammountPaid", 0) / 1.19)
        })
    return detalles

# =========================
# Construcción de documento Bsale
# =========================
def construir_boleta(rec, id_branch):
    # Datos cliente de EVO
    nombre, documento, email = obtener_nombre_y_documento_de_sale(rec['idSale'])
    if not nombre:
        nombre = rec.get("payerName")
    if not documento:
        documento = normalizar_rut_chile(rec.get("payerDocument"))

    # Buscar cliente EXISTENTE en Bsale
    client_id = obtener_cliente_id_bsale(nombre, documento)

    # Items de la venta
    sale_items = obtener_detalle_venta(rec["idSale"])
    items_evo = [
        {"nombre": it.get("description", "").strip(), "precio": it.get("itemValue", 0), "cantidad": it.get("quantity", 1)}
        for it in sale_items if it.get("description")
    ]
    if not items_evo:
        items_evo.append({"nombre": "Otros EVO", "precio": rec.get("ammountPaid", 0), "cantidad": 1})

    detalles = construir_detalles(items_evo, rec)

    # Selección del tipo de documento
    if client_id:
        chosen_doc_type = DOCUMENT_TYPE_ID_NOM
    else:
        chosen_doc_type = DOCUMENT_TYPE_ID_NN

    data = {
        "emissionDate": int(datetime.now().timestamp()),
        "documentTypeId": chosen_doc_type,
        "priceListId": PRICE_LIST_ID,
        "officeId": SUCURSALES_BSALE[id_branch],
        "details": detalles
    }
    if client_id:
        data["clientId"] = client_id  # SOLO si hay match real
    return data

def emitir_boleta_bsale(data):
    headers = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}
    res = requests.post("https://api.bsale.io/v1/documents.json", headers=headers, json=data, timeout=30)
    if res.status_code not in [200, 201]:
        try:
            err = res.json()
        except Exception:
            err = res.text
        return None, err
    return res.json().get("id"), None

# =========================
# Rutas
# =========================
@app.route("/sincronizar")
def sincronizar():
    modo = request.args.get("modo", "test")
    ventas_emitidas = cargar_ventas_emitidas()
    inicio, fin = rango_hoy()
    hoy = datetime.now(CHILE_TZ).strftime("%Y-%m-%d")

    respuesta = [f"<h3>Modo: {modo.upper()} | Rango: {inicio} a {fin}</h3>"]

    for id_branch in SUCURSALES_EVO:
        respuesta.append(f"<b>Sucursal EVO {id_branch}</b><br>")
        try:
            receivables = obtener_receivables(id_branch, inicio, fin)
        except Exception as e:
            respuesta.append(f"Error conexión EVO: {str(e)}<br>")
            continue

        for rec in receivables:
            # Filtrar estrictamente por fecha de hoy
            sale_date_str = rec.get("saleDate")
            if not sale_date_str:
                continue
            if sale_date_str.split("T")[0] != hoy:
                continue

            rec_id = f"receivable-{rec.get('idReceivable')}"
            if rec_id in ventas_emitidas:
                if modo == "test":
                    respuesta.append(f"Saltado {rec_id} (duplicado)<br>")
                continue

            try:
                data = construir_boleta(rec, id_branch)
                if modo == "prod":
                    boleta_id, error = emitir_boleta_bsale(data)
                    if boleta_id:
                        respuesta.append(f"✔ Boleta generada ID {boleta_id} para {rec.get('payerName')}<br>")
                    else:
                        respuesta.append(f"❌ Error generando boleta para {rec.get('payerName')}: {error}<br>")
                else:
                    respuesta.append(f"SIMULADO: {rec_id} Cliente {rec.get('payerName')}<br>")
                ventas_emitidas.add(rec_id)
            except Exception as e:
                respuesta.append(f"❌ Error {rec_id}: {str(e)}<br>")

    guardar_ventas_emitidas(ventas_emitidas)
    return "".join(respuesta)

@app.route("/health")
def health():
    return jsonify({"ok": True}), 200

if __name__ == "__main__":
    # Para Render: utiliza PORT si está definido
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)
