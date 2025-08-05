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

DOCUMENT_TYPE_ID = 1
PRICE_LIST_ID = 2
VARIANT_MAP_FILE = "variant_map.json"
VARIANT_ID_OTHERS = 289  # Reemplazar por el ID real en Bsale


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

def registrar_en_google_sheet(id_evo, id_bsale, cliente, monto, estado):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sheet.append_row([id_evo, id_bsale, cliente, monto, estado, fecha])

# ============================================
# CARGAR O CREAR VARIANT MAP
# ============================================
if os.path.exists(VARIANT_MAP_FILE):
    with open(VARIANT_MAP_FILE, "r", encoding="utf-8") as f:
        VARIANT_MAP = json.load(f)
else:
    VARIANT_MAP = {}

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def normalizar_nombre(nombre):
    if not nombre:
        return ''
    nombre = nombre.lower().strip()
    nombre = ''.join(c for c in unicodedata.normalize('NFD', nombre) if unicodedata.category(c) != 'Mn')
    return ' '.join(nombre.split())

def normalizar_rut(rut):
    if not rut:
        return None
    rut = rut.replace('.', '').replace('-', '').replace(' ', '').strip().upper()
    if len(rut) < 2:
        return None
    cuerpo, dv = rut[:-1], rut[-1]
    try:
        cuerpo_int = int(cuerpo)
    except ValueError:
        return None
    return f"{cuerpo_int}-{dv}"

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
    res = requests.get(url, auth=(EVO_USER, EVO_PASS), timeout=10)
    res.raise_for_status()
    return res.json().get("saleItens", [])

def obtener_nombre_y_documento_de_sale(id_sale):
    url_sale = f"{EVO_BASE_URL}/sales/{id_sale}"
    res_sale = requests.get(url_sale, auth=(EVO_USER, EVO_PASS), timeout=10)
    res_sale.raise_for_status()
    sale = res_sale.json()
    id_member = sale.get("idMember")
    if not id_member:
        return None, None, None
    url_member = f"{EVO_BASE_URL_V2}/members/{id_member}"
    res_member = requests.get(url_member, auth=(EVO_USER, EVO_PASS), timeout=10)
    res_member.raise_for_status()
    member = res_member.json()
    nombre = f"{member.get('firstName', '').strip()} {member.get('lastName', '').strip()}".strip()
    documento = normalizar_rut(member.get('document'))
    email = member.get('email')
    return nombre or "Cliente EVO", documento, email

# ============================================
# API BSALE
# ============================================
def buscar_o_crear_cliente(nombre, rut=None, email=None):
    headers = {"access_token": BSALE_TOKEN}
    nombre_normalizado = normalizar_nombre(nombre)
    rut_normalizado = normalizar_rut(rut)

    if rut_normalizado:
        offset = 0
        while True:
            url_rut = f"https://api.bsale.io/v1/clients.json?q={rut_normalizado}&limit=25&offset={offset}"
            res_rut = requests.get(url_rut, headers=headers)
            res_rut.raise_for_status()
            data_rut = res_rut.json()
            for cliente in data_rut.get("items", []):
                if normalizar_rut(cliente.get("taxNumber")) == rut_normalizado or normalizar_rut(cliente.get("code")) == rut_normalizado:
                    return cliente["id"]
            offset += 25
            if offset >= data_rut.get("count", 0):
                break

    url_name = f"https://api.bsale.io/v1/clients.json?q={nombre}"
    res = requests.get(url_name, headers=headers)
    res.raise_for_status()
    for cliente in res.json().get("items", []):
        if normalizar_nombre(cliente.get("name", "")) == nombre_normalizado:
            return cliente["id"]

    if not rut_normalizado:
        rut_normalizado = "99999999-9"
    if not email:
        email = f"sin-email-{int(datetime.now().timestamp())}@noemail.com"

    payload = {
        "name": nombre,
        "municipality": "Providencia",
        "city": "Santiago",
        "countryId": 1,
        "taxNumber": rut_normalizado,
        "code": rut_normalizado,
        "email": email
    }
    res = requests.post("https://api.bsale.io/v1/clients.json", headers=headers, json=payload)
    res.raise_for_status()
    return res.json()["id"]

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

def construir_boleta(rec, id_branch):
    nombre, documento, email = obtener_nombre_y_documento_de_sale(rec['idSale'])
    if not nombre:
        nombre = rec.get("payerName", "Cliente EVO")
    if not documento:
        documento = normalizar_rut(rec.get("payerDocument"))
    if not email:
        email = f"sin-email-{rec.get('idReceivable')}@noemail.com"
    client_id = buscar_o_crear_cliente(nombre, documento, email)

    sale_items = obtener_detalle_venta(rec["idSale"])
    items_evo = [
        {"nombre": it.get("description", "").strip(), "precio": it.get("itemValue", 0), "cantidad": it.get("quantity", 1)}
        for it in sale_items if it.get("description")
    ]
    if not items_evo:
        items_evo.append({"nombre": "Otros EVO", "precio": rec.get("ammountPaid", 0), "cantidad": 1})
    detalles = construir_detalles(items_evo, rec)

    return {
        "emissionDate": int(datetime.now().timestamp()),
        "documentTypeId": DOCUMENT_TYPE_ID,
        "priceListId": PRICE_LIST_ID,
        "officeId": SUCURSALES_BSALE[id_branch],
        "clientId": client_id,
        "details": detalles
    }

def emitir_boleta_bsale(data):
    headers = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}
    res = requests.post("https://api.bsale.io/v1/documents.json", headers=headers, json=data)
    if res.status_code not in [200, 201]:
        try:
            error_msg = res.json().get("error", res.text)
        except:
            error_msg = res.text
        return None, error_msg
    return res.json().get("id"), None

# ============================================
# FLASK APP
# ============================================
app = Flask(__name__)

@app.route("/sincronizar")
def sincronizar():
    modo = request.args.get("modo", "test")
    inicio, fin = rango_hoy()
    respuesta = [f"<h3>Modo: {modo.upper()} | Rango: {inicio} a {fin}</h3>"]

    ventas_procesadas = set()  # Evitar duplicados entre sucursales

    for id_branch in SUCURSALES_EVO:
        respuesta.append(f"<b>Sucursal EVO {id_branch}</b><br>")
        try:
            receivables = obtener_receivables(id_branch, inicio, fin)
        except Exception as e:
            logger.error(f"Error conexión EVO: {e}")
            respuesta.append(f"Error conexión EVO: {str(e)}<br>")
            continue

        for rec in receivables:
            rec_id = rec.get("idReceivable")
            if rec_id in ventas_procesadas:
                continue  # Saltar duplicados
            ventas_procesadas.add(rec_id)

            rec_key = f"receivable-{rec_id}"
            try:
                data = construir_boleta(rec, id_branch)
                if modo == "prod":
                    boleta_id, error = emitir_boleta_bsale(data)
                    if boleta_id:
                        respuesta.append(f"✔ Boleta generada ID {boleta_id} para {rec.get('payerName')}<br>")
                        registrar_en_google_sheet(rec_key, boleta_id, rec.get('payerName'), rec.get('ammountPaid'), "OK")
                    else:
                        respuesta.append(f"❌ Error generando boleta: {error}<br>")
                        registrar_en_google_sheet(rec_key, "-", rec.get('payerName'), rec.get('ammountPaid'), f"ERROR: {error}")
                else:
                    respuesta.append(f"SIMULADO: {rec_key} Cliente {rec.get('payerName')}<br>")
            except Exception as e:
                respuesta.append(f"❌ Error {rec_key}: {str(e)}<br>")
                registrar_en_google_sheet(rec_key, "-", rec.get('payerName'), rec.get('ammountPaid'), f"ERROR: {str(e)}")

    return "".join(respuesta)

@app.route("/evo-webhook", methods=["POST"])
def evo_webhook():
    auth_header = request.headers.get("X-Webhook-Secret")
    if auth_header != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    logger.info(f"Webhook recibido: {data}")

    if data.get("EventType") == "NewSale":
        id_sale = data.get("IdRecord")
        id_branch = data.get("IdBranch")
        rec_id = f"receivable-{id_sale}"
        try:
            rec = {"idSale": id_sale, "idReceivable": id_sale, "payerName": None, "ammountPaid": 0}
            data_boleta = construir_boleta(rec, id_branch)
            boleta_id, error = emitir_boleta_bsale(data_boleta)
            if boleta_id:
                registrar_en_google_sheet(rec_id, boleta_id, rec.get('payerName', 'Desconocido'), 0, "OK")
                return jsonify({"status": "success", "boleta_id": boleta_id}), 200
            else:
                registrar_en_google_sheet(rec_id, "-", rec.get('payerName', 'Desconocido'), 0, f"ERROR: {error}")
                return jsonify({"status": "error", "message": error}), 500
        except Exception as e:
            logger.error(f"Error procesando venta {id_sale}: {e}")
            registrar_en_google_sheet(rec_id, "-", "Desconocido", 0, f"ERROR: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return jsonify({"status": "ignored"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

