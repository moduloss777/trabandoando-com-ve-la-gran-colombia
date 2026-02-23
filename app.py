from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import requests
import logging
from datetime import datetime
import threading
import time
import re
import json
import os
import uuid
from functools import wraps

# Importar sistemas robustos
from database import db
from operators import router
from sender import SMSSender
from monitor import monitor
from rate_limiter import rate_limiter_global
from traffilink_service import traffilink_service

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sms_marketing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuración
URL_ACORTADOR = os.environ.get('URL_ACORTADOR', 'http://localhost:5001')

# Sistema de procesamiento en background
proceso_activo = {
    "activo": False,
    "total": 0,
    "procesados": 0,
    "enviados": 0,
    "fallidos": 0,
    "inicio": None,
    "detalles": [],
    "campana_id": None
}
proceso_lock = threading.Lock()

# Almacenamiento de estadísticas mejorado
stats = {
    "total_enviados": 0,
    "total_entregados": 0,
    "total_fallidos": 0,
    "ultimo_envio": None,
    "historial": []
}
stats_lock = threading.Lock()

def validar_numero_colombiano(numero):
    """Valida que el número sea colombiano (10 dígitos, inicia con 3)"""
    numero = str(numero).strip()
    numero = re.sub(r'\D', '', numero)

    if len(numero) == 10 and numero.startswith('3'):
        return numero
    elif len(numero) == 12 and numero.startswith('57'):
        return numero[2:]
    else:
        return None

def obtener_link_dinamico(campana_id):
    """Obtener link dinámico del acortador"""
    try:
        response = requests.get(
            f'{URL_ACORTADOR}/api/link-dinamico/{campana_id}',
            timeout=5
        )
        data = response.json()

        if data.get('status') == 'ok':
            return data['url_corta']
        else:
            logger.warning(f"Error obteniendo link: {data.get('message')}")
            return None
    except Exception as e:
        logger.warning(f"Error conectando con acortador: {e}")
        return None

@app.route("/", methods=["GET"])
def index():
    """Página principal"""
    return render_template("index.html")

@app.route("/stats", methods=["GET"])
def obtener_stats():
    """Endpoint para obtener estadísticas"""
    with stats_lock:
        return jsonify(stats)

@app.route("/progreso", methods=["GET"])
def obtener_progreso():
    """Endpoint para obtener el progreso del envío actual"""
    with proceso_lock:
        return jsonify(proceso_activo)

@app.route("/enviar", methods=["POST"])
def enviar():
    """Inicia el procesamiento de SMS en background con sistema robusto"""
    archivo = request.files.get("archivo")
    mensaje = request.form.get("mensaje", "")
    campana_id = request.form.get("campana_id")

    # Verificar si ya hay un proceso activo
    with proceso_lock:
        if proceso_activo["activo"]:
            return jsonify({
                "status": "error",
                "message": "Ya hay un envío en proceso. Espera a que termine."
            })

    if not archivo:
        return jsonify({"status": "error", "message": "No se recibió ningún archivo."})

    if not mensaje:
        return jsonify({"status": "error", "message": "El mensaje no puede estar vacío."})

    filename = archivo.filename.lower()

    try:
        # Leer archivo
        if filename.endswith(".csv"):
            df = pd.read_csv(archivo, dtype=str)
        elif filename.endswith(".txt"):
            df = pd.read_csv(archivo, sep="\t|,", engine="python", dtype=str)
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(archivo, dtype=str)
        else:
            return jsonify({
                "status": "error",
                "message": "Formato no soportado. Use CSV, TXT o Excel."
            })

        # Verificar que exista la columna "numero"
        if "numero" not in df.columns:
            columnas_disponibles = ", ".join(df.columns.tolist())
            return jsonify({
                "status": "error",
                "message": f"Falta la columna 'numero'. Columnas encontradas: {columnas_disponibles}"
            })

        # Limpiar dataframe
        df = df.fillna("")

        # Detectar variables en el mensaje
        variables_mensaje = re.findall(r'\{(\w+)\}', mensaje)
        columnas_faltantes = [var for var in variables_mensaje if var not in df.columns and var != "numero" and var != "link"]

        if columnas_faltantes:
            return jsonify({
                "status": "warning",
                "message": f"Advertencia: Variables {columnas_faltantes} no encontradas en el archivo.",
                "continuar": True
            })

        # Agregar todos los SMS a la cola de persistencia
        queue_ids = []
        for idx, row in df.iterrows():
            numero = validar_numero_colombiano(row["numero"])
            if numero:
                queue_id = db.agregar_a_cola(
                    numero,
                    mensaje,
                    campana_id=campana_id,
                    metadata={'fila': idx + 1, 'datos': row.to_dict()}
                )
                if queue_id:
                    queue_ids.append(queue_id)

        total_encolados = len(queue_ids)

        # Inicializar proceso
        with proceso_lock:
            proceso_activo["activo"] = True
            proceso_activo["total"] = total_encolados
            proceso_activo["procesados"] = 0
            proceso_activo["enviados"] = 0
            proceso_activo["fallidos"] = 0
            proceso_activo["inicio"] = datetime.now().isoformat()
            proceso_activo["detalles"] = []
            proceso_activo["campana_id"] = campana_id

        logger.info(f"Iniciada campaña con {total_encolados} SMS en cola")

        # Función para procesar en background
        def procesar_en_background():
            try:
                contador = 0
                link_actual = None
                usar_link = '{link}' in mensaje and campana_id

                while True:
                    # Obtener SMS pendientes
                    pendientes = db.obtener_pendientes(limit=20)

                    if not pendientes:
                        break

                    for item in pendientes:
                        queue_id = item['id']
                        numero = item['numero']
                        msg = item['mensaje']

                        # Obtener link dinámico si necesario
                        link_dinamico = None
                        if usar_link:
                            if contador % 15 == 0 or link_actual is None:
                                link_dinamico = obtener_link_dinamico(campana_id)
                                if link_dinamico:
                                    link_actual = link_dinamico
                            else:
                                link_dinamico = link_actual

                        # Seleccionar operador
                        operador = router.obtener_operador_siguiente(0)

                        # Enviar SMS
                        resultado = SMSSender.enviar_sms_ahora(
                            queue_id,
                            numero,
                            msg,
                            operador.operador,
                            link_dinamico=link_dinamico
                        )

                        contador += 1

                        # Actualizar progreso
                        with proceso_lock:
                            proceso_activo["procesados"] += 1
                            if resultado['success']:
                                proceso_activo["enviados"] += 1
                            else:
                                proceso_activo["fallidos"] += 1
                            proceso_activo["detalles"].append(resultado)

                        # Rate limit global
                        rate_limiter_global.esperar()

                    time.sleep(0.1)

                logger.info(f"Campaña completada: {contador} SMS procesados")

                # Finalizar proceso
                with proceso_lock:
                    proceso_activo["activo"] = False

            except Exception as e:
                logger.error(f"Error en procesamiento background: {e}")
                with proceso_lock:
                    proceso_activo["activo"] = False

        # Iniciar thread en background
        thread = threading.Thread(target=procesar_en_background, daemon=True)
        thread.start()

        # Tiempo estimado
        tiempo_estimado_min = round((total_encolados * 0.1) / 60, 1)

        # Respuesta inmediata
        return jsonify({
            "status": "processing",
            "message": "Envío iniciado en segundo plano con persistencia",
            "total": total_encolados,
            "estimado_minutos": tiempo_estimado_min,
            "info_velocidad": "Velocidad adaptativa con reintentos automáticos"
        })

    except Exception as e:
        logger.error(f"Error al procesar archivo: {e}")
        with proceso_lock:
            proceso_activo["activo"] = False
        return jsonify({
            "status": "error",
            "message": f"Error al procesar: {str(e)}"
        })

@app.route("/test", methods=["POST"])
def test_sms():
    """Enviar SMS de prueba"""
    data = request.get_json()
    numero = validar_numero_colombiano(data.get("numero", ""))
    mensaje = data.get("mensaje", "")

    if not numero:
        return jsonify({"status": "error", "message": "Número inválido"})

    if not mensaje:
        return jsonify({"status": "error", "message": "Mensaje vacío"})

    # Agregar a cola
    queue_id = db.agregar_a_cola(numero, mensaje, metadata={'prueba': True})

    if not queue_id:
        return jsonify({"status": "error", "message": "SMS duplicado reciente"})

    # Enviar inmediatamente
    operador = router.obtener_operador_siguiente(0)
    resultado = SMSSender.enviar_sms_ahora(queue_id, numero, mensaje, operador.operador)

    if resultado['success']:
        return jsonify({"status": "ok", "message": "SMS enviado correctamente"})
    else:
        return jsonify({"status": "error", "message": f"Error: {resultado.get('error', 'Desconocido')}"})

@app.route("/descargar-plantilla", methods=["GET"])
def descargar_plantilla():
    """Descarga archivo de ejemplo"""
    plantilla = pd.DataFrame({
        "numero": ["3001234567", "3109876543", "3201122334"],
        "nombre": ["Juan Pérez", "María López", "Carlos Ruiz"],
        "deuda": ["150000", "250000", "180000"],
        "ciudad": ["Bogotá", "Medellín", "Cali"]
    })

    archivo_path = "/tmp/plantilla_sms.csv"
    plantilla.to_csv(archivo_path, index=False)

    return send_file(archivo_path, as_attachment=True, download_name="plantilla_sms.csv")

@app.route("/campanas-acortador", methods=["GET"])
def obtener_campanas_acortador():
    """Obtener campañas disponibles del acortador"""
    try:
        response = requests.get(f'{URL_ACORTADOR}/api/campanas', timeout=5)
        campanas = response.json()
        return jsonify({"status": "ok", "campanas": campanas})
    except Exception as e:
        logger.warning(f"Error obteniendo campañas: {e}")
        return jsonify({"status": "error", "campanas": []})

# ============ ENDPOINTS DE MONITOREO ============

@app.route("/monitor/salud", methods=["GET"])
def monitor_salud():
    """Verifica salud general del sistema"""
    salud = monitor.verificar_salud_sistema()
    return jsonify(salud)

@app.route("/monitor/dashboard", methods=["GET"])
def monitor_dashboard():
    """Datos para el dashboard en tiempo real"""
    datos = monitor.obtener_dashboard_datos()
    return jsonify(datos)

@app.route("/monitor/reporte", methods=["GET"])
def monitor_reporte():
    """Genera reporte de actividad"""
    reporte = monitor.generar_reporte()
    return jsonify(reporte)

@app.route("/monitor/operadores", methods=["GET"])
def monitor_operadores():
    """Obtiene estadísticas detalladas de operadores"""
    stats = router.obtener_stats_operadores()
    return jsonify({"operadores": stats})

# ============ ENDPOINTS DE WEBHOOK (confirmación de entrega) ============

@app.route("/webhook/traffilink-report", methods=["POST"])
def webhook_traffilink():
    """
    Webhook para recibir reportes de entrega de TraffiLink
    POST /webhook/traffilink-report

    Esperado:
    {
        "id": "123456",           # ID TraffiLink del SMS
        "deliverystatus": "1",    # Estado de entrega (1=entregado, 2=fallido, etc)
        "timestamp": "2025-02-23 10:30:45"
    }
    """
    try:
        datos = request.get_json()

        if not datos:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        # Procesar webhook
        resultado = traffilink_service.procesar_webhook_reporte(datos)

        if resultado['exito']:
            traffilink_id = resultado.get('traffilink_id')
            estado = resultado.get('estado')

            # Mapear estado de TraffiLink a nuestro sistema
            estado_nuestro = estado

            logger.info(f"[TraffiLink Webhook] ID: {traffilink_id}, Estado: {estado_nuestro}")

            # TODO: Aquí puedes buscar el SMS por traffilink_id y actualizar su estado
            # Por ahora solo registramos

            return jsonify({
                "status": "ok",
                "message": "Report received",
                "traffilink_id": traffilink_id,
                "estado": estado_nuestro,
                "timestamp": datetime.now().isoformat()
            }), 200
        else:
            error = resultado.get('error', 'Unknown error')
            logger.warning(f"[TraffiLink Webhook] Error: {error}")
            return jsonify({
                "status": "error",
                "message": error
            }), 400

    except Exception as e:
        logger.error(f"Error procesando webhook TraffiLink: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route("/webhook/entregado", methods=["POST"])
def webhook_entregado():
    """
    Webhook para confirmación de entrega del operador
    Esperado: {"numero": "3001234567", "codigo": "0", "timestamp": "..."}
    """
    try:
        data = request.get_json()
        numero = data.get('numero', '').strip()
        codigo_error = data.get('codigo')

        if not numero:
            return jsonify({"status": "error", "message": "Número requerido"}), 400

        # Confirmar entrega
        confirmado = db.confirmar_entrega(numero, codigo_error)

        logger.info(f"Webhook recibido: {numero} - Confirmado: {confirmado}")

        return jsonify({
            "status": "ok",
            "confirmado": confirmado,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error en webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ============ ENDPOINTS DE CONFIGURACIÓN ============

@app.route("/config/operadores", methods=["GET"])
def config_operadores():
    """Obtiene configuración de operadores"""
    operadores = router.listar_operadores()
    return jsonify({"operadores": operadores})

@app.route("/config/operador/<nombre>/habilitar", methods=["POST"])
def habilitar_operador(nombre):
    """Habilita un operador"""
    data = request.get_json()
    habilitado = data.get('habilitado', True)

    if router.habilitar_operador(nombre, habilitado):
        return jsonify({
            "status": "ok",
            "operador": nombre,
            "habilitado": habilitado
        })
    else:
        return jsonify({"status": "error", "message": "Operador no encontrado"}), 404

# ============ ENDPOINTS DE REINTENTOS ============

@app.route("/procesar-cola", methods=["POST"])
def procesar_cola_manual():
    """Procesa manualmente la cola de SMS pendientes"""
    try:
        from sender import SMSSender
        SMSSender.procesar_cola()
        return jsonify({
            "status": "ok",
            "message": "Cola procesada"
        })
    except Exception as e:
        logger.error(f"Error procesando cola: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/reintentar-fallidos", methods=["POST"])
def reintentar_fallidos():
    """Reintenta SMS que fallaron"""
    try:
        from sender import SMSSender
        SMSSender.reintentar_fallidos()
        return jsonify({
            "status": "ok",
            "message": "Reintentos iniciados"
        })
    except Exception as e:
        logger.error(f"Error reintentando: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ============ ENDPOINT DE ESTADO ============

@app.route("/sistema/estado", methods=["GET"])
def sistema_estado():
    """Estado general del sistema"""
    estado = db.obtener_estado_general()
    salud = monitor.verificar_salud_sistema()

    return jsonify({
        "timestamp": datetime.now().isoformat(),
        "estado": estado,
        "alertas": salud['alertas'],
        "hay_problemas": salud['hay_alertas']
    })

# ============ WORKER THREAD PARA PROCESAMIENTO EN BACKGROUND ============

def worker_procesar_sms():
    """Worker thread que procesa SMS continuamente"""
    import time
    from sender import SMSSender

    logger.info("Worker de procesamiento iniciado")

    while True:
        try:
            # Procesar SMS pendientes
            SMSSender.procesar_cola()

            # Procesar reintentos
            SMSSender.reintentar_fallidos()

            # Dormir 5 segundos antes de siguiente ciclo
            time.sleep(5)

        except Exception as e:
            logger.error(f"Error en worker: {e}")
            time.sleep(10)

# Iniciar worker en background
worker_thread = threading.Thread(target=worker_procesar_sms, daemon=True)
worker_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"🚀 SMS Marketing API iniciando en puerto {port}")
    logger.info(f"📊 Usando base de datos: sms_marketing.db")
    logger.info(f"📡 Operadores disponibles: {[op['operador'] for op in router.listar_operadores()]}")
    app.run(debug=False, host="0.0.0.0", port=port)