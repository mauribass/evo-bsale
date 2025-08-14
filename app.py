import re
import os
import json
import logging
import requests
from flask import Flask, request, jsonify
from datetime import datetime
import pytz
import unicodedata
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from difflib import SequenceMatcher
from urllib.parse import quote

# ============================================
# CONFIGURACIÓN
# ============================================
load_dotenv()

EVO_BASE_URL = "https://evo-integracao-api.w12app.com.br/api/v1"
EVO_BASE_URL_V2 = "https://evo-integracao-api.w12app.com.br/api/v2"
EVO_USER = os.getenv("EVO_USER")
EVO_PASS = os.getenv("EVO_PASS")
BSALE_TOKEN = os.getenv("BSALE_TOKEN")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "supersecreto")
CALLBACK_URL = os.getenv("CALLBACK_URL")

if not EVO_USER or not EVO_PASS or not BSALE_TOKEN:
    raise RuntimeError("Faltan credenciales en variables de entorno.")

# >>> CAMBIO CLAVE: 2 tipos de documento, no 1 <<<
# - NOM: cuando SÍ hay clientId (documento nominativo)
# - NN : cuando NO hay clientId (documento NO nominativo) → evita “inicial demo”
DOCUMENT_TYPE_ID_NOM = int(os.getenv("DOCUMENT_TYPE_ID_NOM", "1"))  # ajusta a tu cuenta
DOCUMENT_TYPE_ID_NN  = int(os.getenv("DOCUMENT_TYPE_ID_NN",  "28"))  # ajusta a tu cuenta

PRICE_LIST_ID = int(os.getenv("PRICE_LIST_ID", "2"))
VARIANT_MAP_FILE = "variant_map.json"
VARIANT_ID_OTHERS = int(os.getenv("VARIANT_ID_OTHERS", "1244"))  # Reemplazar por el ID real en Bsale

SUCURSALES_EVO = [1, 3, 4]
SUCURSALES_BSALE = {
    1: 1,
    3: 2,
    4: 3
}

CHILE_TZ = pytz.timezone("America/Santiago")

# ============================================
# LOGGING
# ============================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ============================================
# SESIÓN CON REINTENTOS PARA EVO Y BSALE
# ============================================
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

# ============================================
# GOOGLE SHEETS
# ============================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credentials.json")
SHEET_NAME = os.getenv("SHEET_NAME", "Ventas EVO-Bsale")

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).sheet1

def venta_ya_registrada_en_sheets(rec_key, registros_existentes):
    """Chequea en memoria para evitar re-leer la planilla."""
    return rec_key in registros_existentes

def registrar_en_google_sheet(id_evo, id_bsale, cliente, monto, estado):
    """Append directo. El control de duplicados se hace antes (en memoria)."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append_row([id_evo, id_bsale, cliente, monto, estado, fecha])

# ============================================
# CARGAR VARIANT MAP
# ============================================
if os.path.exists(VARIANT_MAP_FILE):
    with open(VARIANT_MAP_FILE, "r", encoding="utf-8") as f:
        VARIANT_MAP = json.load(f)
else:
    VARIANT_MAP = {}

# ============================================
# AUXILIARES (normalización / similitud)
# ============================================
def normalizar_rut_chile(rut: str | None) -> str | None:
    if not rut:
        return None
    rut = rut.strip().upper()
    rut = rut.replace(".", "").replace(" ", "")
    # Mantener sólo dígitos y K, y un guión si viene
    rut = re.sub(r"[^0-9K\-]", "", rut)
    if "-" not in rut:
        # Si viene sin guión, separar DV
        cuerpo, dv = rut[:-1], rut[-1]
        rut = f"{cuerpo}-{dv}"
    return rut

def normalizar_nombre(nombre: str) -> str:
    if not nombre:
        return ''
    nombre = nombre.lower().strip()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    return ' '.join(nombre.split())

def _similitud(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, normalizar_nombre(a), normalizar_nombre(b)).ratio()

def rango_hoy():
    ahora = datetime.now(CHILE_TZ)
    return ahora.replace(hour=0, minute=0, second=0), ahora.replace(hour=23, minute=59, second=59)

# ============================================
# API EVO
# ============================================
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
        res = session.get(f"{EVO_BASE_URL}/receivables", auth=(EVO_USER, EVO_PASS), params=params, timeout=20)
        res.raise_for_status()
        data = res.json()
        lote = data if isinstance(data, list) else data.get("receivables", [])
        if not lote:
            break
        resultados.extend(lote)

        if len(si) < 50:
            break
        skip += 50
    return resultados

def obtener_detalle_venta(id_sale):
    url = f"{EVO_BASE_URL}/sales/{id_sale}"
    res = session.get(url, auth=(EVO_USER, EVO_PASS), timeout=20)
    res.raise_for_status()
    return res.json().get("saleItens", [])

def obtener_nombre_y_documento_de_sale(id_sale):
    url_sale = f"{EVO_BASE_URL}/sales/{id_sale}"
    res_sale = session.get(url_sale, auth=(EVO_USER, EVO_PASS), timeout=20)
    res_sale.raise_for_status()
    sale = res_sale.json()
    id_member = sale.get("idMember")
    if not id_member:
        return "Cliente EVO", None, None
    try:
        url_member = f"{EVO_BASE_URL_V2}/members/{id_member}"
        res_member = session.get(url_member, auth=(EVO_USER, EVO_PASS), timeout=20)
        res_member.raise_for_status()
        member = res_member.json()
        nombre = f"{member.get('firstName', '').strip()} {member.get('lastName', '').strip()}".strip()
        documento = member.get('document')
        email = member.get('email')
        return nombre or "Cliente EVO", documento, email
    except Exception as e:
        logger.error(f"Error obteniendo datos del miembro {id_member}: {e}")
        return "Cliente EVO", None, None

# ============================================
# MAPEADOR DE VARIANTES POR NOMBRE
# ============================================
def buscar_variant_id(nombre):
    nombre_normalizado = normalizar_nombre(nombre)
    if nombre_normalizado in VARIANT_MAP:
        return VARIANT_MAP[nombre_normalizado]
    for clave, vid in VARIANT_MAP.items():
        if nombre_normalizado in clave or clave in nombre_normalizado:
            return vid
    # Si no existe, agregar automáticamente con ID genérico
    VARIANT_MAP[nombre_normalizado] = VARIANT_ID_OTHERS
    logger.warning(f"Producto no mapeado: {nombre} → agregado con ID genérico {VARIANT_ID_OTHERS}")
    with open(VARIANT_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(VARIANT_MAP, f, indent=2, ensure_ascii=False)
    return VARIANT_ID_OTHERS

def construir_detalles(items_evo, rec):
    detalles = []
    for item in items_evo:
        variant_id = buscar_variant_id(item["nombre"])
        valor_neto = round(item["precio"] / 1.19)
        if valor_neto > 0:
            detalles.append({
                "quantity": item.get("cantidad", 1),
                "variantId": variant_id,
                "netUnitValue": valor_neto
            })
    if not detalles:
        detalles.append({
            "quantity": 1,
            "variantId": VARIANT_ID_OTHERS,
            "netUnitValue": round(rec.get("ammountPaid", 0) / 1.19)
        })
        logger.warning(f"No había detalles válidos. Usando producto genérico para venta {rec['idSale']}")
    return detalles

# ============================================
# CLIENTES EN BSALE (match por RUT o por NOMBRE, sin crear/actualizar)
# ============================================
def _buscar_cliente_bsale_por_rut(rut: str | None):
    """Busca cliente EXISTENTE por RUT (taxNumber/code). No crea ni actualiza."""
    rut_norm = normalizar_rut_chile(rut)
    if not rut_norm:
        return None
    headers = {"access_token": BSALE_TOKEN}
    try:
        # 1) Búsqueda directa por taxnumber
        url = f"https://api.bsale.io/v1/clients.json?taxnumber={quote(rut_norm)}"
        r = session.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        items = r.json().get("items", [])
        if items:
            cid = items[0]["id"]
            logger.info(f"Cliente encontrado por RUT {rut_norm}: ID {cid}")
            return cid

        # 2) Fallback: búsqueda general por q, por si el formato difiere en cuenta
        url_q = f"https://api.bsale.io/v1/clients.json?q={quote(rut_norm)}&limit=50"
        r2 = session.get(url_q, headers=headers, timeout=20)
        r2.raise_for_status()
        for c in r2.json().get("items", []):
            tax = normalizar_rut_chile(c.get("taxNumber"))
            cod = normalizar_rut_chile(c.get("code"))
            if tax == rut_norm or cod == rut_norm:
                logger.info(f"Cliente encontrado por RUT (fallback q) {rut_norm}: ID {c['id']}")
                return c["id"]
    except Exception as e:
        logger.warning(f"No se pudo buscar cliente por RUT {rut}: {e}")
    return None

def _buscar_cliente_bsale_por_nombre(nombre: str, umbral=0.92):
    """Busca mejor coincidencia por nombre normalizado y devuelve ID si score>=umbral."""
    if not nombre:
        return None
    headers = {"access_token": BSALE_TOKEN}
    try:
        best_id, best_score = None, 0.0
        offset, limit = 0, 100
        while True:
            url = f"https://api.bsale.io/v1/clients.json?q={quote(nombre)}&limit={limit}&offset={offset}"
            r = session.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            items = r.json().get("items", [])
            if not items:
                break
            for c in items:
                score = _similitud(nombre, c.get("name", ""))
                if score > best_score:
                    best_score = score
                    best_id = c.get("id")
            if len(items) < limit:
                break
            offset += limit

        if best_id and best_score >= umbral:
            logger.info(f"Cliente encontrado por NOMBRE '{nombre}' → ID {best_id} (score={best_score:.2f})")
            return best_id
        logger.info(f"Sin coincidencia suficiente por NOMBRE '{nombre}' (mejor score={best_score:.2f})")
    except Exception as e:
        logger.warning(f"No se pudo buscar cliente por nombre '{nombre}': {e}")
    return None

def obtener_cliente_id_bsale(nombre_evo: str, rut_evo: str | None) -> int | None:
    """1) intenta por RUT normalizado; 2) fallback por nombre; nunca crea clientes."""
    cid = _buscar_cliente_bsale_por_rut(rut_evo)
    if cid:
        return cid
    return _buscar_cliente_bsale_por_nombre(nombre_evo, umbral=0.92)

# ============================================
# CONSTRUCCIÓN DE BOLETA
# ============================================
def construir_boleta(rec, id_branch):
    nombre, documento, email = obtener_nombre_y_documento_de_sale(rec['idSale'])

    client_id = obtener_cliente_id_bsale(nombre, documento)

    sale_items = obtener_detalle_venta(rec["idSale"])
    items_evo = [
        {"nombre": it.get("description", "").strip(), "precio": it.get("itemValue", 0), "cantidad": it.get("quantity", 1)}
        for it in sale_items if it.get("description")
    ]
    if not items_evo:
        items_evo.append({"nombre": "Otros EVO", "precio": rec.get("ammountPaid", 0), "cantidad": 1})

    detalles = construir_detalles(items_evo, rec)

    # Elegir tipo de documento según haya cliente o no
    if client_id:
        chosen_doc_type = DOCUMENT_TYPE_ID_NOM
        logger.info(f"Documento NOMINATIVO con clientId={client_id} y documentTypeId={chosen_doc_type}.")
    else:
        chosen_doc_type = DOCUMENT_TYPE_ID_NN
        logger.info(f"Documento NO NOMINATIVO sin clientId, documentTypeId={chosen_doc_type}.")

    data = {
        "emissionDate": int(datetime.now().timestamp()),
        "documentTypeId": chosen_doc_type,
        "priceListId": PRICE_LIST_ID,
        "officeId": SUCURSALES_BSALE[id_branch],
        "details": detalles
    }
    if client_id:
        data["clientId"] = client_id  # sólo cuando hay match real

    return data

def emitir_boleta_bsale(data):
    headers = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}
    res = session.post("https://api.bsale.io/v1/documents.json", headers=headers, json=data, timeout=20)
    if res.status_code not in [200, 201]:
        try:
            error_msg = res.json().get("error", res.text)
        except Exception:
            error_msg = res.text
        return None, error_msg
    return res.json().get("id"), None

# ============================================
# FLASK APP (Google Sheets + Filtro Fecha + Evita duplicados)
# ============================================
app = Flask(__name__)

@app.route("/sincronizar")
def sincronizar():
    if os.environ.get("MODO_PAUSA") == "1":
        logger.info("Sistema en pausa, sincronización ignorada.")
        return "<h3>Modo pausa activo: no se procesan ventas.</h3>"

    modo = request.args.get("modo", "test")
    inicio, fin = rango_hoy()
    hoy = datetime.now(CHILE_TZ).strftime("%Y-%m-%d")
    respuesta = [f"<h3>Modo: {modo.upper()} | Rango: {inicio} a {fin}</h3>"]

    # Lee todas las filas de Sheets SOLO UNA VEZ
    filas = sheet.get_all_values()
    registros_existentes = set(fila[0] for fila in filas if fila and fila[0])

    for id_branch in SUCURSALES_EVO:
        respuesta.append(f"<b>Sucursal EVO {id_branch}</b><br>")
        try:
            receivables = obtener_receivables(id_branch, inicio, fin)
        except Exception as e:
            logger.error(f"Error conexión EVO: {e}")
            respuesta.append(f"Error conexión EVO: {str(e)}<br>")
            continue

        for rec in receivables:
            sale_date_str = rec.get("saleDate")
            if not sale_date_str:
                continue
            sale_date = sale_date_str.split("T")[0]
            if sale_date != hoy:
                continue

            rec_id = rec.get("idReceivable")
            rec_key = f"receivable-{rec_id}"

            # Chequea en memoria, NO en Sheets de nuevo
            if venta_ya_registrada_en_sheets(rec_key, registros_existentes):
                continue

            try:
                registrar_en_google_sheet(rec_key, "-", rec.get('payerName'), rec.get('ammountPaid'), "PENDIENTE")
                registros_existentes.add(rec_key)  # Importante: agregar al set después de registrar

                data = construir_boleta(rec, id_branch)
                if modo == "prod":
                    boleta_id, error = emitir_boleta_bsale(data)
                    if boleta_id:
                        respuesta.append(f"✔ Boleta generada ID {boleta_id} para {rec.get('payerName')}<br>")
                    else:
                        respuesta.append(f"❌ Error generando boleta: {error}<br>")
                else:
                    respuesta.append(f"SIMULADO: receivable-{rec.get('idReceivable')} Cliente {rec.get('payerName')}<br>")
            except Exception as e:
                respuesta.append(f"❌ Error receivable-{rec.get('idReceivable')}: {str(e)}<br>")

    return "".join(respuesta)

@app.route("/evo-webhook", methods=["POST"])
def evo_webhook():
    # Pausa por bandera de entorno
    if os.environ.get("MODO_PAUSA") == "1":
        logger.info("Sistema en pausa, ignorando venta recibida por webhook.")
        return jsonify({"status": "paused"}), 200

    # Autenticación básica del webhook
    auth_header = request.headers.get("X-Webhook-Secret")
    if auth_header != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.get_json(silent=True) or {}
    logger.info(f"Webhook recibido: {data}")

    # Lee registros solo una vez
    filas = sheet.get_all_values()
    registros_existentes = set(fila[0] for fila in filas if fila and fila[0])

    if data.get("EventType") == "NewSale":
        id_sale = data.get("IdRecord")
        id_branch = data.get("IdBranch")
        rec_id = f"receivable-{id_sale}"

        if venta_ya_registrada_en_sheets(rec_id, registros_existentes):
            return jsonify({"status": "duplicated", "message": "Venta ya registrada"}), 200

        try:
            registrar_en_google_sheet(rec_id, "-", "Desconocido", 0, "PENDIENTE")
            registros_existentes.add(rec_id)

            rec = {"idSale": id_sale, "idReceivable": id_sale, "payerName": None, "ammountPaid": 0}
            data_boleta = construir_boleta(rec, id_branch)
            boleta_id, error = emitir_boleta_bsale(data_boleta)
            if boleta_id:
                return jsonify({"status": "success", "boleta_id": boleta_id}), 200
            else:
                return jsonify({"status": "error", "message": error}), 500
        except Exception as e:
            logger.error(f"Error procesando venta {id_sale}: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "ignored"}), 200

# ============================================
# RUN
# ============================================
app = app  # para gunicorn `app:app`

if __name__ == "__main__":
    # Compatible con Render: usa el puerto asignado por la plataforma
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

