# Integración EVO → Bsale con Webhooks y Google Sheets

## ✅ Descripción
Este proyecto integra EVO con Bsale para emitir boletas automáticamente y registrar los resultados en **Google Sheets** para fácil control del cliente.

---

## ✅ Requisitos
- Cuenta EVO con credenciales API.
- Cuenta Bsale con token API.
- Cuenta Google + Google Sheets.
- Crear un **Service Account** en Google Cloud y descargar `credentials.json`.

---

## ✅ Archivos incluidos
- **app.py**: Código principal.
- **requirements.txt**: Dependencias.
- **Dockerfile**: Para desplegar en Render o cualquier servicio Docker.
- **variant_map.json**: Mapeo entre nombres de productos EVO y variantId de Bsale.

---

## ✅ Configuración de Google Sheets
1. Ir a [Google Cloud Console](https://console.cloud.google.com/).
2. Crear un proyecto y habilitar **Google Sheets API**.
3. Crear una **Service Account** y descargar el archivo `credentials.json`.
4. Compartir la hoja de cálculo con el email de la Service Account.
5. En la hoja, agregar encabezados:
   ```
   ID EVO | ID Bsale | Cliente | Monto | Estado | Fecha
   ```

---

## ✅ Variables de entorno (.env)
```
EVO_USER=usuario_evo
EVO_PASS=password_evo
BSALE_TOKEN=token_bsale
WEBHOOK_SECRET=token_unico
CALLBACK_URL=https://<tu-dominio>/evo-webhook
GOOGLE_CREDS_FILE=credentials.json
SHEET_NAME=Ventas EVO-Bsale
```

---

## ✅ Endpoints
- `/sincronizar?modo=test` → Simula emisión manual.
- `/sincronizar?modo=prod` → Emite boletas reales.
- `/evo-webhook` → Recibe notificaciones en tiempo real desde EVO.

---

## ✅ Ejecución
### Opción A: Local
```bash
pip install -r requirements.txt
python app.py
```

### Opción B: Docker
```bash
docker build -t evo-bsale-sync .
docker run -d -p 5000:5000 --env-file .env evo-bsale-sync
```

### Opción C: Render
- Subir a GitHub y conectar con Render.
- Configurar variables de entorno en Render.

---

## ✅ Cómo registrar el Webhook en EVO
Desde Python:
```python
from app import registrar_webhook
registrar_webhook()
```

Esto enviará la URL y el token secreto para que EVO dispare eventos `NewSale`.
