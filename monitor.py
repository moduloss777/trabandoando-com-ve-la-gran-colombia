"""
Sistema de Monitoreo y Alertas
Monitorea salud del sistema, detecta anomalías y genera alertas
"""

import logging
from datetime import datetime, timedelta
from database import db
from operators import router

logger = logging.getLogger(__name__)


class SistemaMonitor:
    """Monitor del sistema SMS"""

    # Umbrales de alerta
    UMBRAL_TASA_ERROR_CRITICA = 0.5  # 50%
    UMBRAL_TASA_ERROR_ALTA = 0.2  # 20%
    UMBRAL_TIMEOUT_OPERADOR = 300  # 5 minutos sin envío exitoso
    UMBRAL_COLA_GRANDE = 1000  # SMS pendientes

    def __init__(self):
        self.ultimas_alertas = {}
        logger.info("Monitor del sistema iniciado")

    def verificar_salud_sistema(self) -> dict:
        """Verifica la salud general del sistema"""
        estado = db.obtener_estado_general()

        # Estadísticas de operadores
        stats_operadores = db.obtener_todas_stats()

        # Análisis
        alertas = []

        # Verificar tasa de error global
        total = estado['total']
        if total > 0:
            fallidos = estado['fallidos'] or 0
            tasa_error = fallidos / total

            if tasa_error >= self.UMBRAL_TASA_ERROR_CRITICA:
                alertas.append({
                    'tipo': 'CRITICA',
                    'severidad': 'CRITICAL',
                    'mensaje': f'Tasa de error crítica: {tasa_error*100:.1f}%',
                    'operador': 'GLOBAL'
                })
            elif tasa_error >= self.UMBRAL_TASA_ERROR_ALTA:
                alertas.append({
                    'tipo': 'ALERTA',
                    'severidad': 'WARNING',
                    'mensaje': f'Tasa de error alta: {tasa_error*100:.1f}%',
                    'operador': 'GLOBAL'
                })

        # Verificar cola grande
        pendientes = estado['pendientes'] or 0
        if pendientes > self.UMBRAL_COLA_GRANDE:
            alertas.append({
                'tipo': 'ALERTA',
                'severidad': 'WARNING',
                'mensaje': f'Cola muy grande: {pendientes} SMS pendientes'
            })

        # Verificar operadores
        for op_stat in stats_operadores:
            operador = op_stat['operador']
            tasa_error_op = op_stat['tasa_error_actual'] or 0
            ultimo_error = op_stat['ultimo_error_timestamp']

            # Revisar tasa de error por operador
            if tasa_error_op >= self.UMBRAL_TASA_ERROR_CRITICA:
                alertas.append({
                    'tipo': 'CRITICA',
                    'severidad': 'CRITICAL',
                    'mensaje': f'Operador {operador} con tasa de error crítica: {tasa_error_op*100:.1f}%',
                    'operador': operador
                })
            elif tasa_error_op >= self.UMBRAL_TASA_ERROR_ALTA:
                alertas.append({
                    'tipo': 'ALERTA',
                    'severidad': 'WARNING',
                    'mensaje': f'Operador {operador} con tasa de error alta: {tasa_error_op*100:.1f}%',
                    'operador': operador
                })

            # Revisar inactividad
            if ultimo_error:
                ahora = datetime.now().timestamp()
                tiempo_sin_exito = ahora - (op_stat['ultimo_exito_timestamp'] or ahora)

                if tiempo_sin_exito > self.UMBRAL_TIMEOUT_OPERADOR:
                    alertas.append({
                        'tipo': 'ALERTA',
                        'severidad': 'WARNING',
                        'mensaje': f'Operador {operador} sin envíos exitosos en {int(tiempo_sin_exito/60)} minutos',
                        'operador': operador
                    })

        return {
            'timestamp': datetime.now().isoformat(),
            'estado_general': estado,
            'stats_operadores': stats_operadores,
            'alertas': alertas,
            'hay_alertas': len(alertas) > 0
        }

    def generar_reporte(self, periodo_horas=24) -> dict:
        """Genera reporte de actividad"""
        estado = db.obtener_estado_general()
        stats_ops = db.obtener_todas_stats()

        # Calcular tasas
        total = estado['total'] or 1
        tasa_entrega = (estado['entregados'] or 0) / total * 100
        tasa_error = (estado['fallidos'] or 0) / total * 100

        return {
            'periodo': f'Últimas {periodo_horas} horas',
            'timestamp': datetime.now().isoformat(),
            'resumen': {
                'total_sms': estado['total'],
                'entregados': estado['entregados'],
                'enviados': estado['enviados'],
                'fallidos': estado['fallidos'],
                'pendientes': estado['pendientes'],
                'tasa_entrega': round(tasa_entrega, 2),
                'tasa_error': round(tasa_error, 2)
            },
            'por_operador': [
                {
                    'operador': op['operador'],
                    'enviados': op['total_enviados'],
                    'entregados': op['total_entregados'],
                    'fallidos': op['total_fallidos'],
                    'tasa_exito': round(
                        (op['total_entregados'] or 0) / max(op['total_enviados'] or 1, 1) * 100, 2
                    ),
                    'tiempo_promedio_ms': op['tiempo_promedio_ms']
                }
                for op in stats_ops
            ]
        }

    def obtener_dashboard_datos(self) -> dict:
        """Obtiene datos para el dashboard en tiempo real"""
        salud = self.verificar_salud_sistema()
        estado = salud['estado_general']

        return {
            'timestamp': datetime.now().isoformat(),
            'metricas': {
                'total_sms': estado['total'] or 0,
                'entregados': estado['entregados'] or 0,
                'enviados': estado['enviados'] or 0,
                'fallidos': estado['fallidos'] or 0,
                'pendientes': estado['pendientes'] or 0,
            },
            'operadores': salud['stats_operadores'],
            'alertas': salud['alertas'],
            'salud_general': 'BUENA' if not salud['hay_alertas'] else 'PROBLEMAS'
        }


# Instancia global
monitor = SistemaMonitor()
