"""
Configuración centralizada del sistema SMS
"""

import os
from datetime import timedelta

class Config:
    """Configuración base"""

    # Base de datos
    DB_PATH = os.environ.get('SMS_DB_PATH', 'sms_marketing.db')

    # API de acortador
    URL_ACORTADOR = os.environ.get('URL_ACORTADOR', 'http://localhost:5001')

    # Rate limiting
    MAX_SMS_POR_MINUTO = int(os.environ.get('MAX_SMS_POR_MINUTO', '100'))
    MAX_SMS_POR_SEGUNDO = int(os.environ.get('MAX_SMS_POR_SEGUNDO', '10'))

    # Reintentos
    MAX_REINTENTOS = int(os.environ.get('MAX_REINTENTOS', '5'))
    BACKOFF_DELAYS = [1, 5, 30, 300, 1800]  # segundos

    # Timeout
    TIMEOUT_API = int(os.environ.get('TIMEOUT_API_SEGUNDOS', '10'))

    # Monitoreo
    UMBRAL_TASA_ERROR_CRITICA = 0.5  # 50%
    UMBRAL_TASA_ERROR_ALTA = 0.2  # 20%
    UMBRAL_TIMEOUT_OPERADOR = 300  # 5 minutos

    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FILE = os.environ.get('LOG_FILE', 'sms_marketing.log')

    # Features
    HABILITAR_WEBHOOKS = os.environ.get('HABILITAR_WEBHOOKS', 'True') == 'True'
    HABILITAR_REINTENTOS = os.environ.get('HABILITAR_REINTENTOS', 'True') == 'True'
    HABILITAR_MULTI_OPERADOR = os.environ.get('HABILITAR_MULTI_OPERADOR', 'True') == 'True'

    # Desarrollo
    DEBUG = os.environ.get('DEBUG', 'False') == 'True'

    # ============ CONFIGURACIÓN TRAFFILINK ============
    TRAFFILINK_CONFIG = {
        'account': os.environ.get('TRAFFILINK_ACCOUNT', '0152C274'),
        'password': os.environ.get('TRAFFILINK_PASSWORD', 'G2o0jRnm'),
        'url': os.environ.get('TRAFFILINK_URL', 'http://47.236.91.242:20003'),
        'enabled': os.environ.get('TRAFFILINK_ENABLED', 'False') == 'True',
        'sender_id': os.environ.get('TRAFFILINK_SENDER_ID', 'Goleador'),
        'prioridad': int(os.environ.get('TRAFFILINK_PRIORIDAD', '1')),
        'timeout': int(os.environ.get('TRAFFILINK_TIMEOUT', '10')),
        'max_por_minuto': int(os.environ.get('TRAFFILINK_MAX_POR_MINUTO', '100')),
        'max_reintentos': int(os.environ.get('TRAFFILINK_MAX_REINTENTOS', '3')),
    }


class ConfigDesarrollo(Config):
    """Configuración para desarrollo"""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'
    MAX_SMS_POR_MINUTO = 50  # Más lento en desarrollo


class ConfigProduccion(Config):
    """Configuración para producción"""
    DEBUG = False
    LOG_LEVEL = 'INFO'
    MAX_SMS_POR_MINUTO = 200
    HABILITAR_WEBHOOKS = True
    HABILITAR_REINTENTOS = True


# Seleccionar configuración activa
ambiente = os.environ.get('AMBIENTE', 'desarrollo').lower()

if ambiente == 'produccion':
    config_activa = ConfigProduccion()
else:
    config_activa = ConfigDesarrollo()
