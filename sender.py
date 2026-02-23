"""
Sistema de Envío SMS con Reintentos Inteligentes
Implementa backoff exponencial, multi-operador y rate limiting adaptativo
"""

import requests
import logging
import time
from datetime import datetime
from typing import Dict, Optional, Tuple
from database import db
from operators import router
from rate_limiter import RateLimiter
from traffilink_service import traffilink_service

logger = logging.getLogger(__name__)

# Rate limiters por operador
rate_limiters = {}


def acortar_url_tinyurl(url_larga: str) -> str:
    """
    Acorta una URL usando TinyURL (gratis, sin API key requerida)

    Args:
        url_larga: URL original a acortar

    Returns:
        URL acortada o URL original si falla
    """
    try:
        if not url_larga or len(url_larga) < 10:
            return url_larga

        # TinyURL API - gratis y sin autenticación
        response = requests.get(
            f'http://tinyurl.com/api-create.php?url={url_larga}',
            timeout=5
        )

        if response.status_code == 200:
            url_corta = response.text.strip()
            if url_corta.startswith('https://'):
                logger.info(f"✓ URL acortada: {url_larga[:50]}... → {url_corta}")
                return url_corta

        logger.warning(f"TinyURL no respondió correctamente para: {url_larga[:50]}")
        return url_larga

    except requests.Timeout:
        logger.warning(f"Timeout acortando URL: {url_larga[:50]}")
        return url_larga
    except Exception as e:
        logger.warning(f"Error acortando URL con TinyURL: {e}")
        return url_larga


def acortar_url_bitly(url_larga: str, api_token: Optional[str] = None) -> str:
    """
    Acorta una URL usando Bitly (requiere API token)

    Args:
        url_larga: URL original a acortar
        api_token: Token de Bitly (si no se proporciona, intenta obtener de variables)

    Returns:
        URL acortada o URL original si falla
    """
    try:
        import os
        token = api_token or os.environ.get('BITLY_TOKEN')

        if not token:
            logger.debug("BITLY_TOKEN no configurado, usando TinyURL como fallback")
            return acortar_url_tinyurl(url_larga)

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        data = {"long_url": url_larga}

        response = requests.post(
            'https://api-ssl.bitly.com/v4/shorten',
            headers=headers,
            json=data,
            timeout=5
        )

        if response.status_code == 200 or response.status_code == 201:
            url_corta = response.json()['link']
            logger.info(f"✓ URL acortada (Bitly): {url_larga[:50]}... → {url_corta}")
            return url_corta
        else:
            logger.warning(f"Bitly error {response.status_code}, usando TinyURL")
            return acortar_url_tinyurl(url_larga)

    except Exception as e:
        logger.warning(f"Error con Bitly: {e}, usando TinyURL")
        return acortar_url_tinyurl(url_larga)


def acortar_url(url_larga: str) -> str:
    """
    Acorta URL intentando primero Bitly, luego TinyURL

    Esta es la función principal que deberías usar
    """
    # Intentar primero con Bitly si está configurado
    import os
    if os.environ.get('BITLY_TOKEN'):
        url_corta = acortar_url_bitly(url_larga)
        if url_corta != url_larga:  # Si se acortó exitosamente
            return url_corta

    # Fallback a TinyURL
    return acortar_url_tinyurl(url_larga)


class SMSSender:
    """Gestor de envío de SMS con reintentos y validación"""

    # Configuración de backoff exponencial (segundos)
    BACKOFF_DELAYS = [1, 5, 30, 300, 1800]  # 1s, 5s, 30s, 5min, 30min

    @staticmethod
    def preparar_mensaje(mensaje: str, numero: str, row_data: Optional[Dict] = None, link_dinamico: Optional[str] = None) -> str:
        """
        Prepara el mensaje reemplazando variables y acortando URLs

        Variables soportadas:
        - {columna_csv}: Reemplazado con valor del CSV
        - {link}: Reemplazado con URL acortada
        """
        mensaje_final = mensaje

        # Reemplazar variables de datos
        if row_data:
            for columna, valor in row_data.items():
                placeholder = "{" + columna + "}"
                if placeholder in mensaje_final:
                    mensaje_final = mensaje_final.replace(placeholder, str(valor))

        # Reemplazar link dinámico CON MEJOR FORMATO PARA PASAR FILTRO ✨
        if link_dinamico and '{link}' in mensaje_final:
            # CLAVE: Agregar saltos de línea alrededor del link
            # Esto ayuda a que el operador no lo bloquee
            link_con_formato = f"\n{link_dinamico}\n"
            mensaje_final = mensaje_final.replace('{link}', link_con_formato)
            logger.info(f"✓ Link incluido en mensaje para {numero}: {link_dinamico[:50]}")

        return mensaje_final

    @staticmethod
    def enviar_sms_ahora(queue_id: int, numero: str, mensaje: str,
                         operador_nombre: str, row_data: Optional[Dict] = None,
                         link_dinamico: Optional[str] = None) -> Dict:
        """
        Envía un SMS inmediatamente a través del operador especificado

        Soporta operadores estándar y TraffiLink (con lógica especial)

        Retorna:
        {
            "success": bool,
            "numero": str,
            "operador": str,
            "respuesta": dict,
            "tiempo_ms": int
        }
        """
        # ============ SOPORTE ESPECIAL PARA TRAFFILINK ============
        if operador_nombre == 'traffilink':
            try:
                # Preparar mensaje
                mensaje_procesado = SMSSender.preparar_mensaje(mensaje, numero, row_data, link_dinamico)

                # Validar
                if not mensaje_procesado:
                    logger.error(f"Mensaje vacío para {numero}")
                    db.actualizar_intento(queue_id, 'traffilink', 'error',
                                        error="Mensaje vacío después de procesar variables")
                    return {
                        "success": False,
                        "numero": numero,
                        "operador": 'traffilink',
                        "error": "Mensaje vacío"
                    }

                # Rate limiting
                if 'traffilink' not in rate_limiters:
                    operador_config = router.obtener_operador('traffilink')
                    rate_limiters['traffilink'] = RateLimiter(operador_config.max_por_minuto if operador_config else 100)

                rate_limiters['traffilink'].esperar()

                inicio = time.time()

                # Enviar a través de TraffiLink
                resultado_traffilink = traffilink_service.enviar_sms(
                    numero,
                    mensaje_procesado,
                    str(queue_id)
                )

                tiempo_ms = int((time.time() - inicio) * 1000)

                if resultado_traffilink['exito']:
                    traffilink_id = resultado_traffilink.get('traffilink_id')

                    # Actualizar BD con ID de TraffiLink
                    db.actualizar_intento(
                        queue_id,
                        'traffilink',
                        'enviado',
                        respuesta_api=f"TraffiLink ID: {traffilink_id}",
                        tiempo_ms=tiempo_ms
                    )

                    logger.info(f"[TraffiLink] ✓ SMS enviado a {numero} (ID: {traffilink_id})")

                    return {
                        "success": True,
                        "numero": numero,
                        "operador": 'traffilink',
                        "respuesta": {
                            "traffilink_id": traffilink_id,
                            "status": "enviado"
                        },
                        "tiempo_ms": tiempo_ms
                    }
                else:
                    error_msg = resultado_traffilink.get('error', 'Error desconocido')

                    db.actualizar_intento(
                        queue_id,
                        'traffilink',
                        'error',
                        error=error_msg
                    )

                    logger.error(f"[TraffiLink] ✗ Error enviando a {numero}: {error_msg}")

                    return {
                        "success": False,
                        "numero": numero,
                        "operador": 'traffilink',
                        "error": error_msg
                    }

            except Exception as e:
                logger.error(f"[TraffiLink] Error inesperado: {e}")
                db.actualizar_intento(queue_id, 'traffilink', 'error', error=str(e))
                return {
                    "success": False,
                    "numero": numero,
                    "operador": 'traffilink',
                    "error": str(e)
                }

        # ============ OPERADORES ESTÁNDAR ============
        operador = router.obtener_operador(operador_nombre)

        if not operador:
            logger.error(f"Operador no encontrado: {operador_nombre}")
            return {
                "success": False,
                "numero": numero,
                "operador": operador_nombre,
                "error": "Operador no existe"
            }

        # Preparar mensaje
        mensaje_procesado = SMSSender.preparar_mensaje(mensaje, numero, row_data, link_dinamico)

        # Validar longitud
        if len(mensaje_procesado) == 0:
            logger.error(f"Mensaje vacío para {numero}")
            db.actualizar_intento(queue_id, operador_nombre, 'error',
                                error="Mensaje vacío después de procesar variables")
            return {
                "success": False,
                "numero": numero,
                "operador": operador_nombre,
                "error": "Mensaje vacío"
            }

        # Rate limiting
        if operador_nombre not in rate_limiters:
            rate_limiters[operador_nombre] = RateLimiter(operador.max_por_minuto)

        limiter = rate_limiters[operador_nombre]
        limiter.esperar()

        # Construir parámetros de API
        sign, timestamp = operador.generar_sign()

        params = {
            "account": operador.cuenta,
            "sign": sign,
            "datetime": timestamp
        }

        # Agregar prefijo de país si no existe
        numero_formateado = "57" + numero if not numero.startswith('57') else numero

        data = {
            "senderid": operador.sender_id,
            "numbers": numero_formateado,
            "content": mensaje_procesado
        }

        # ✨ Headers adicionales para ayudar a pasar filtros de operadores
        headers = {
            'User-Agent': 'Mozilla/5.0 (SMS-Gateway-Client)',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept': 'application/json',
        }

        inicio = time.time()

        try:
            response = requests.post(
                operador.url_api,
                params=params,
                json=data,
                headers=headers,  # ← Agregar headers para legibilidad
                timeout=operador.timeout_segundos
            )

            tiempo_ms = int((time.time() - inicio) * 1000)

            # Intentar parsear respuesta
            try:
                respuesta = response.json()
            except:
                respuesta = {"text": response.text, "status_code": response.status_code}

            logger.info(f"[{operador_nombre}] Enviado a {numero} en {tiempo_ms}ms")

            # Actualizar en BD
            db.actualizar_intento(
                queue_id,
                operador_nombre,
                'enviado',
                respuesta_api=str(respuesta),
                tiempo_ms=tiempo_ms
            )

            return {
                "success": True,
                "numero": numero,
                "operador": operador_nombre,
                "respuesta": respuesta,
                "tiempo_ms": tiempo_ms
            }

        except requests.Timeout:
            logger.error(f"[{operador_nombre}] Timeout enviando a {numero}")
            db.actualizar_intento(
                queue_id,
                operador_nombre,
                'error',
                error="Timeout de conexión"
            )
            return {
                "success": False,
                "numero": numero,
                "operador": operador_nombre,
                "error": "Timeout"
            }

        except requests.RequestException as e:
            logger.error(f"[{operador_nombre}] Error HTTP enviando a {numero}: {e}")
            db.actualizar_intento(
                queue_id,
                operador_nombre,
                'error',
                error=str(e)
            )
            return {
                "success": False,
                "numero": numero,
                "operador": operador_nombre,
                "error": str(e)
            }

        except Exception as e:
            logger.error(f"[{operador_nombre}] Error inesperado: {e}")
            db.actualizar_intento(
                queue_id,
                operador_nombre,
                'error',
                error=str(e)
            )
            return {
                "success": False,
                "numero": numero,
                "operador": operador_nombre,
                "error": str(e)
            }

    @staticmethod
    def procesar_cola():
        """
        Procesa SMS pendientes de la cola
        Se ejecuta en segundo plano periódicamente
        """
        pendientes = db.obtener_pendientes(limit=50)

        if not pendientes:
            return

        logger.info(f"Procesando {len(pendientes)} SMS pendientes")

        for item in pendientes:
            queue_id = item['id']
            numero = item['numero']
            mensaje = item['mensaje']
            intento = item['intentos']

            # Seleccionar operador siguiente
            operador = router.obtener_operador_siguiente(intento, item['operador'])

            logger.debug(f"Reintento {intento + 1} para {numero} con {operador.operador}")

            # Enviar
            resultado = SMSSender.enviar_sms_ahora(
                queue_id,
                numero,
                mensaje,
                operador.operador
            )

            # Log del resultado
            if resultado['success']:
                logger.info(f"✓ {numero} enviado con {operador.operador}")
            else:
                logger.warning(f"✗ {numero} falló: {resultado.get('error', 'Unknown')}")

    @staticmethod
    def reintentar_fallidos():
        """Procesa SMS que necesitan reintentarse"""
        pendientes = db.obtener_pendientes(limit=100)

        reintentos_iniciados = 0

        for item in pendientes:
            if item['estado'] == 'reintentando':
                reintentos_iniciados += 1
                queue_id = item['id']
                numero = item['numero']
                mensaje = item['mensaje']
                intento = item['intentos']

                # Cambiar operador en cada reintento
                operador = router.obtener_operador_siguiente(intento)

                logger.info(f"Reintentando SMS {queue_id} (intento {intento + 1}) con {operador.operador}")

                SMSSender.enviar_sms_ahora(
                    queue_id,
                    numero,
                    mensaje,
                    operador.operador
                )

        if reintentos_iniciados > 0:
            logger.info(f"Iniciados {reintentos_iniciados} reintentos")
