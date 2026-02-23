"""
Rate Limiter Adaptativo
Controla velocidad de envío respetando límites del operador
Se adapta automáticamente basado en tasa de errores
"""

import time
import logging
from collections import deque
from threading import Lock
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class RateLimiter:
    """Limitador de velocidad con algoritmo Token Bucket"""

    def __init__(self, max_por_minuto=100, adaptativo=True):
        """
        Args:
            max_por_minuto: Máximo SMS por minuto
            adaptativo: Si ajusta automáticamente según errores
        """
        self.max_por_minuto = max_por_minuto
        self.adaptativo = adaptativo
        self.tokens = max_por_minuto
        self.last_refill = time.time()
        self.lock = Lock()

        # Para estadísticas
        self.timestamps = deque(maxlen=max_por_minuto)
        self.errores_recientes = deque(maxlen=10)
        self.tasa_error_actual = 0

    def _refill(self):
        """Recarga tokens basado en tiempo transcurrido"""
        ahora = time.time()
        tiempo_pasado = ahora - self.last_refill

        # Refrescar tokens: 1 token por (60 / max_por_minuto) segundos
        tokens_a_agregar = tiempo_pasado * (self.max_por_minuto / 60.0)
        self.tokens = min(self.max_por_minuto, self.tokens + tokens_a_agregar)
        self.last_refill = ahora

    def esperar(self):
        """
        Bloquea hasta tener un token disponible
        Implementa Token Bucket Algorithm
        """
        with self.lock:
            self._refill()

            if self.tokens < 1:
                # Calcular tiempo de espera
                tiempo_espera = (1 - self.tokens) * (60.0 / self.max_por_minuto)
                logger.debug(f"Rate limit: esperando {tiempo_espera:.2f}s")
                time.sleep(tiempo_espera)
                self._refill()

            self.tokens -= 1
            self.timestamps.append(time.time())

    def registrar_error(self):
        """Registra un error para ajuste adaptativo"""
        self.errores_recientes.append(time.time())

        # Calcular tasa de error en últimos 60 segundos
        ahora = time.time()
        errores_recientes = sum(1 for t in self.errores_recientes if ahora - t < 60)
        total_intentos = len(self.timestamps)

        if total_intentos > 0:
            self.tasa_error_actual = errores_recientes / min(total_intentos, 100)

            # Si tasa de error > 20%, reducir velocidad
            if self.adaptativo and self.tasa_error_actual > 0.2:
                nueva_velocidad = max(10, int(self.max_por_minuto * 0.8))
                logger.warning(f"Tasa de error alta ({self.tasa_error_actual*100:.1f}%), "
                             f"reduciendo velocidad a {nueva_velocidad} SMS/min")
                self.max_por_minuto = nueva_velocidad

    def registrar_exito(self):
        """Registra un envío exitoso para ajuste adaptativo"""
        # Si tasa de error es baja, aumentar velocidad gradualmente
        if self.tasa_error_actual < 0.05 and self.adaptativo:
            nueva_velocidad = min(200, self.max_por_minuto + 5)
            if nueva_velocidad > self.max_por_minuto:
                logger.info(f"Tasa de error baja, aumentando velocidad a {nueva_velocidad} SMS/min")
                self.max_por_minuto = nueva_velocidad

    def obtener_velocidad_actual(self) -> float:
        """Retorna SMS por minuto actualmente"""
        return self.max_por_minuto

    def obtener_stats(self) -> dict:
        """Retorna estadísticas del rate limiter"""
        ahora = time.time()

        # SMS en último minuto
        sms_ultimo_minuto = sum(1 for t in self.timestamps if ahora - t < 60)

        return {
            'sms_por_minuto': self.max_por_minuto,
            'velocidad_actual': sms_ultimo_minuto,
            'tasa_error': round(self.tasa_error_actual, 4),
            'tokens_disponibles': round(self.tokens, 2),
            'adaptativo': self.adaptativo
        }


class RateLimiterGlobal:
    """Rate limiter global para todo el sistema"""

    def __init__(self, max_sms_por_segundo=10):
        self.max_sms_por_segundo = max_sms_por_segundo
        self.tokens = max_sms_por_segundo
        self.last_refill = time.time()
        self.lock = Lock()

    def esperar(self):
        """Bloquea hasta tener capacidad global"""
        with self.lock:
            ahora = time.time()
            tiempo_pasado = ahora - self.last_refill

            # Refrescar tokens
            tokens_a_agregar = tiempo_pasado * self.max_sms_por_segundo
            self.tokens = min(self.max_sms_por_segundo, self.tokens + tokens_a_agregar)
            self.last_refill = ahora

            if self.tokens < 1:
                tiempo_espera = (1 - self.tokens) / self.max_sms_por_segundo
                logger.debug(f"Global rate limit: esperando {tiempo_espera:.2f}s")
                time.sleep(tiempo_espera)
                self.tokens = 0

            self.tokens -= 1

    def obtener_stats(self) -> dict:
        """Retorna estadísticas"""
        return {
            'sms_por_segundo': self.max_sms_por_segundo,
            'tokens_disponibles': round(self.tokens, 2)
        }


# Instancias globales
rate_limiter_global = RateLimiterGlobal()
