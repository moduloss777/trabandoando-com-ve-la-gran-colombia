"""
Servicio completo para integración con TraffiLink API v3.4
Proporciona funciones para envío, consulta de estado y webhooks
"""

import requests
import hashlib
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logger = logging.getLogger(__name__)


class TraffiLinkService:
    """
    Servicio para integración con TraffiLink API
    Maneja autenticación, envío de SMS y consulta de estados
    """

    def __init__(self):
        """Inicializa el servicio con credenciales de .env"""
        self.account = os.getenv('TRAFFILINK_ACCOUNT', '0152C274')
        self.password = os.getenv('TRAFFILINK_PASSWORD', 'G2o0jRnm')
        self.url_base = os.getenv('TRAFFILINK_URL', 'http://47.236.91.242:20003')
        self.sender_id = os.getenv('TRAFFILINK_SENDER_ID', 'Goleador')
        self.timeout = int(os.getenv('TRAFFILINK_TIMEOUT', '10'))
        self.enabled = os.getenv('TRAFFILINK_ENABLED', 'True') == 'True'

        logger.info(f"TraffiLink Service inicializado para account: {self.account}")

    def generar_sign(self, params_str: str) -> str:
        """
        Genera firma MD5 para validación de TraffiLink
        Formato: MD5(params + MD5(password))

        Args:
            params_str: String con parámetros (ej: "account=...&sendid=...")

        Returns:
            Firma MD5 hexadecimal
        """
        try:
            # Paso 1: Hash del password
            password_hash = hashlib.md5(self.password.encode()).hexdigest()

            # Paso 2: Concatenar parámetros + hash del password
            sign_str = f"{params_str}{password_hash}"

            # Paso 3: Hash final
            sign = hashlib.md5(sign_str.encode()).hexdigest()

            logger.debug(f"Sign generado: {sign}")
            return sign

        except Exception as e:
            logger.error(f"Error generando sign: {e}")
            raise

    def consultar_balance(self) -> Dict:
        """
        Consulta el saldo disponible en TraffiLink
        Endpoint: GET /queryBalance

        Returns:
            {
                'exito': bool,
                'balance': float (EUR),
                'moneda': 'EUR',
                'timestamp': datetime ISO,
                'error': str (si aplica),
                'codigo_error': str (si aplica)
            }
        """
        try:
            # Construir parámetros
            params = f"account={self.account}"
            sign = self.generar_sign(params)

            url = f"{self.url_base}/queryBalance"
            data = {
                'account': self.account,
                'sign': sign
            }

            logger.info(f"Consultando balance para {self.account}...")

            respuesta = requests.get(url, params=data, timeout=self.timeout)
            resultado = respuesta.json()

            logger.debug(f"Respuesta TraffiLink: {resultado}")

            if resultado.get('status') == '1':
                balance = float(resultado.get('balance', 0))
                logger.info(f"✓ Balance consultado: {balance} EUR")
                return {
                    'exito': True,
                    'balance': balance,
                    'moneda': 'EUR',
                    'timestamp': datetime.now().isoformat(),
                    'response_code': resultado.get('status')
                }
            else:
                error_msg = resultado.get('message', 'Error desconocido')
                logger.warning(f"Error en queryBalance: {error_msg}")
                return {
                    'exito': False,
                    'error': error_msg,
                    'codigo_error': resultado.get('status'),
                    'timestamp': datetime.now().isoformat()
                }

        except requests.Timeout:
            logger.error("Timeout consultando balance TraffiLink")
            return {
                'exito': False,
                'error': 'Timeout de conexión',
                'tipo_error': 'TimeoutError'
            }

        except Exception as e:
            logger.error(f"Error consultando balance: {e}")
            return {
                'exito': False,
                'error': str(e),
                'tipo_error': 'UnknownError'
            }

    def enviar_sms(self, numero_destino: str, contenido_mensaje: str,
                  queue_id: str) -> Dict:
        """
        Envía un SMS a través de TraffiLink
        Endpoint: POST /sendsmsV2

        Args:
            numero_destino: Número en formato internacional (ej: 573001234567)
            contenido_mensaje: Texto del mensaje (máx 160 caracteres)
            queue_id: ID único para tracking en Goleador

        Returns:
            {
                'exito': bool,
                'traffilink_id': str (si exito),
                'status': 'sent',
                'queue_id': str,
                'timestamp': datetime ISO,
                'error': str (si falla),
                'codigo_error': str (si falla)
            }
        """
        try:
            # Validar entrada
            if not numero_destino:
                logger.error("Número destino vacío")
                return {
                    'exito': False,
                    'error': 'Número destino requerido',
                    'queue_id': queue_id
                }

            if not contenido_mensaje:
                logger.error("Contenido del mensaje vacío")
                return {
                    'exito': False,
                    'error': 'Mensaje requerido',
                    'queue_id': queue_id
                }

            # Asegurar que el número tenga prefijo internacional
            if not numero_destino.startswith('57'):
                numero_formateado = '57' + numero_destino
            else:
                numero_formateado = numero_destino

            # Validar que sea número colombiano (573xx)
            if not numero_formateado.startswith('573'):
                logger.warning(f"Número no es colombiano: {numero_formateado}")

            logger.info(f"Enviando SMS a {numero_formateado} con queue_id {queue_id}")

            # Construir parámetros para firma
            # Formato: account=X&sendid=Y&mobile=Z&content=W
            params_para_firma = f"account={self.account}&sendid={queue_id}&mobile={numero_formateado}&content={contenido_mensaje}"
            sign = self.generar_sign(params_para_firma)

            # URL del endpoint
            url = f"{self.url_base}/sendsmsV2"

            # Datos POST
            data = {
                'account': self.account,
                'sendid': queue_id,
                'mobile': numero_formateado,
                'content': contenido_mensaje,
                'sign': sign
            }

            logger.debug(f"Enviando request a {url}")

            # Realizar request
            respuesta = requests.post(url, data=data, timeout=self.timeout)
            resultado = respuesta.json()

            logger.debug(f"Respuesta TraffiLink: {resultado}")

            # Procesar respuesta
            if resultado.get('status') == '1':
                traffilink_id = resultado.get('id')
                logger.info(f"✓ SMS enviado correctamente. ID: {traffilink_id}, Queue: {queue_id}")
                return {
                    'exito': True,
                    'traffilink_id': traffilink_id,
                    'status': 'sent',
                    'queue_id': queue_id,
                    'numero': numero_formateado,
                    'timestamp': datetime.now().isoformat(),
                    'response_code': resultado.get('status')
                }
            else:
                error_msg = resultado.get('message', 'Error desconocido')
                codigo_error = resultado.get('status', 'unknown')
                logger.error(f"Error enviando SMS: [{codigo_error}] {error_msg}")
                return {
                    'exito': False,
                    'error': error_msg,
                    'codigo_error': codigo_error,
                    'queue_id': queue_id,
                    'numero': numero_formateado,
                    'timestamp': datetime.now().isoformat()
                }

        except requests.Timeout:
            logger.error(f"Timeout enviando SMS a {numero_destino}")
            return {
                'exito': False,
                'error': 'Timeout de conexión',
                'tipo_error': 'TimeoutError',
                'queue_id': queue_id
            }

        except requests.RequestException as e:
            logger.error(f"Error HTTP enviando SMS: {e}")
            return {
                'exito': False,
                'error': f"Error de conexión: {str(e)}",
                'tipo_error': 'RequestError',
                'queue_id': queue_id
            }

        except json.JSONDecodeError:
            logger.error("Respuesta de TraffiLink no es JSON válido")
            return {
                'exito': False,
                'error': 'Respuesta inválida del servidor',
                'tipo_error': 'JSONError',
                'queue_id': queue_id
            }

        except Exception as e:
            logger.error(f"Error inesperado enviando SMS: {e}")
            return {
                'exito': False,
                'error': str(e),
                'tipo_error': 'UnknownError',
                'queue_id': queue_id
            }

    def consultar_estado(self, traffilink_id: str) -> Dict:
        """
        Consulta el estado de entrega de un SMS
        Endpoint: GET /queryReport

        Args:
            traffilink_id: ID retornado por TraffiLink en el envío

        Returns:
            {
                'exito': bool,
                'estado': 'delivered'|'failed'|'pending'|'invalid'|'unknown',
                'traffilink_id': str,
                'timestamp': datetime ISO,
                'error': str (si falla)
            }
        """
        try:
            # Construir parámetros
            params = f"account={self.account}&id={traffilink_id}"
            sign = self.generar_sign(params)

            url = f"{self.url_base}/queryReport"
            data = {
                'account': self.account,
                'id': traffilink_id,
                'sign': sign
            }

            logger.info(f"Consultando estado para ID: {traffilink_id}")

            respuesta = requests.get(url, params=data, timeout=self.timeout)
            resultado = respuesta.json()

            logger.debug(f"Respuesta TraffiLink: {resultado}")

            # Mapeo de estados de TraffiLink
            estado_map = {
                '1': 'delivered',      # Entregado
                '2': 'failed',         # Fallido
                '3': 'pending',        # Pendiente
                '4': 'invalid',        # Número inválido
            }

            if resultado.get('status') == '1':
                estado_codigo = resultado.get('deliverystatus', 'unknown')
                estado_final = estado_map.get(str(estado_codigo), 'unknown')

                logger.info(f"✓ Estado consultado: {estado_final}")
                return {
                    'exito': True,
                    'estado': estado_final,
                    'estado_codigo': estado_codigo,
                    'traffilink_id': traffilink_id,
                    'timestamp': datetime.now().isoformat(),
                    'response_code': resultado.get('status')
                }
            else:
                error_msg = resultado.get('message', 'Error desconocido')
                logger.warning(f"Error consultando estado: {error_msg}")
                return {
                    'exito': False,
                    'error': error_msg,
                    'codigo_error': resultado.get('status'),
                    'traffilink_id': traffilink_id
                }

        except Exception as e:
            logger.error(f"Error consultando estado: {e}")
            return {
                'exito': False,
                'error': str(e),
                'traffilink_id': traffilink_id
            }

    def procesar_webhook_reporte(self, datos: Dict) -> Dict:
        """
        Procesa un webhook recibido de TraffiLink con reporte de entrega
        Esperado:
        {
            'id': '123456',           # ID TraffiLink del SMS
            'deliverystatus': '1',    # Estado de entrega
            'timestamp': '2025-02-23 10:30:45'
        }

        Returns:
            {
                'exito': bool,
                'message': str,
                'estado': str,
                'traffilink_id': str
            }
        """
        try:
            if not datos:
                return {
                    'exito': False,
                    'error': 'Datos vacíos',
                    'message': 'El webhook debe contener datos'
                }

            traffilink_id = datos.get('id')
            estado_codigo = datos.get('deliverystatus')

            if not traffilink_id or estado_codigo is None:
                logger.warning(f"Webhook con datos incompletos: {datos}")
                return {
                    'exito': False,
                    'error': 'Datos incompletos',
                    'message': 'Requiere id y deliverystatus'
                }

            # Mapeo de estados
            estado_map = {
                '1': 'delivered',
                '2': 'failed',
                '3': 'pending',
                '4': 'invalid',
            }

            estado_final = estado_map.get(str(estado_codigo), 'unknown')

            logger.info(f"Webhook TraffiLink procesado: {traffilink_id} -> {estado_final}")

            return {
                'exito': True,
                'message': f'Reporte procesado: {estado_final}',
                'estado': estado_final,
                'estado_codigo': estado_codigo,
                'traffilink_id': traffilink_id,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Error procesando webhook: {e}")
            return {
                'exito': False,
                'error': str(e),
                'message': 'Error procesando webhook'
            }

    @staticmethod
    def validar_numero_colombiano(numero: str) -> str:
        """
        Valida que el número sea colombiano
        Acepta:
        - 10 dígitos (3001234567)
        - 12 dígitos con prefijo (573001234567)

        Returns:
            Número formateado con prefijo 57, o None si es inválido
        """
        try:
            import re
            numero = str(numero).strip()
            numero = re.sub(r'\D', '', numero)

            if len(numero) == 10 and numero.startswith('3'):
                return '57' + numero
            elif len(numero) == 12 and numero.startswith('57'):
                return numero
            else:
                logger.warning(f"Número inválido: {numero}")
                return None

        except Exception as e:
            logger.error(f"Error validando número: {e}")
            return None


# Instancia global para usar en toda la aplicación
traffilink_service = TraffiLinkService()
