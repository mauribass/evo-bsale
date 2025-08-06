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

# Configurar sesión con reintentos
session = requests.Session()
retry_strategy = Retry(
    total=3,  # máximo 3 intentos
    backoff_factor=2,  # espera exponencial: 1s, 2s, 4s
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)


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

def registrar_en_google_sheet(id_evo, id_bsale, cliente, monto, estado):
    filas = sheet.get_all_values()
    for fila in filas:
        if fila and fila[0] == id_evo:  # evitar duplicados
            logger.info(f"Registro ya existe en Google Sheets para {id_evo}")
            return
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
# FUNCIONES AUXILIARES
# ============================================
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
        if len(lote) < 50:
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
# API BSALE
# ============================================
def buscar_o_crear_cliente(nombre, rut=None, email=None):
    headers = {"access_token": BSALE_TOKEN}

    # Validaciones básicas
    if not nombre:
        nombre = "Cliente EVO"
    if not rut:
        rut = "99999999-9"
    if not email or "@" not in email:
        email = f"sin-email-{int(datetime.now().timestamp())}@noemail.com"

    payload = {
        "name": nombre,
        "municipality": "Providencia",
        "city": "Santiago",
        "countryId": 1,
        "taxNumber": rut,
        "code": rut,
        "email": email
    }

    try:
        res = session.post("https://api.bsale.io/v1/clients.json", headers=headers, json=payload, timeout=20)
        if res.status_code in [200, 201]:
            return res.json()["id"]
        elif res.status_code == 400:
            logger.error(f"Error al crear cliente en Bsale: {res.text} | Payload: {payload}")
            return None
        res.raise_for_status()
    except Exception as e:
        logger.error(f"Excepción creando cliente en Bsale: {e}")
    return None

# ============================================
# VARIANT MAP POR ID DE EVO
# ============================================
def buscar_variant_id(clave):
    if clave in VARIANT_MAP:
        return VARIANT_MAP[clave]

    # Agregar automáticamente con ID genérico
    VARIANT_MAP[clave] = VARIANT_ID_OTHERS
    logger.warning(f"Producto no mapeado: {clave} → agregado con ID genérico {VARIANT_ID_OTHERS}")
    with open(VARIANT_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(VARIANT_MAP, f, indent=2, ensure_ascii=False)
    return VARIANT_ID_OTHERS

def construir_detalles(items_evo, rec):
    detalles = []
    for item in items_evo:
        # Determinar la clave basada en IDs EVO
        clave = None
        if item.get("idProduct"):
            clave = f"product:{item['idProduct']}"
        elif item.get("idService"):
            clave = f"service:{item['idService']}"
        elif item.get("idMembership"):
            clave = f"membership:{item['idMembership']}"
        else:
            clave = "unknown:0"

        variant_id = buscar_variant_id(clave)
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
    client_id = buscar_o_crear_cliente(nombre, documento, email)

    sale_items = obtener_detalle_venta(rec["idSale"])
    items_evo = [
        {
            "nombre": it.get("description", "").strip(),
            "precio": it.get("itemValue", 0),
            "cantidad": it.get("quantity", 1),
            "idProduct": it.get("idProduct"),
            "idService": it.get("idService"),
            "idMembership": it.get("idMembership")
        }
        for it in sale_items if it.get("description")
    ]
    if not items_evo:
        items_evo.append({"nombre": "Otros EVO", "precio": rec.get("ammountPaid", 0), "cantidad": 1,
                          "idProduct": None, "idService": None, "idMembership": None})
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
    res = session.post("https://api.bsale.io/v1/documents.json", headers=headers, json=data, timeout=20)
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

    ventas_procesadas = set()

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
                continue
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

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)




