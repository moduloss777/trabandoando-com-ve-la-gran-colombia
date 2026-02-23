"""
Sistema de Base de Datos para SMS Marketing
Proporciona persistencia y transacciones ACID
"""

import sqlite3
import json
import os
from datetime import datetime
from contextlib import contextmanager
from threading import Lock
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get('SMS_DB_PATH', 'sms_marketing.db')
db_lock = Lock()

class DatabaseManager:
    """Gestor centralizado de base de datos"""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.init_db()

    @contextmanager
    def get_connection(self):
        """Context manager para conexiones seguras"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en transacción DB: {e}")
            raise
        finally:
            conn.close()

    def init_db(self):
        """Inicializa las tablas del sistema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Tabla: Cola de SMS pendientes
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sms_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    numero TEXT NOT NULL,
                    mensaje TEXT NOT NULL,
                    campana_id TEXT,
                    estado TEXT DEFAULT 'pendiente',
                    intentos INTEGER DEFAULT 0,
                    max_intentos INTEGER DEFAULT 5,
                    operador TEXT DEFAULT 'principal',
                    operador_history TEXT,
                    proximo_reintento REAL,
                    error_ultimo TEXT,
                    respuesta_ultima TEXT,
                    webhook_confirmado INTEGER DEFAULT 0,
                    webhook_timestamp TEXT,
                    creado REAL NOT NULL,
                    primer_intento REAL,
                    ultimo_intento REAL,
                    entregado REAL,
                    metadata TEXT,
                    UNIQUE(numero, mensaje, campana_id)
                )
            ''')

            # Tabla: Log detallado de envíos
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sms_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_id INTEGER,
                    numero TEXT NOT NULL,
                    mensaje TEXT,
                    operador TEXT,
                    estado TEXT,
                    intento_numero INTEGER,
                    timestamp REAL NOT NULL,
                    respuesta_api TEXT,
                    error TEXT,
                    tiempo_respuesta_ms INTEGER,
                    FOREIGN KEY(queue_id) REFERENCES sms_queue(id)
                )
            ''')

            # Tabla: Estadísticas por operador
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operator_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operador TEXT UNIQUE NOT NULL,
                    total_enviados INTEGER DEFAULT 0,
                    total_entregados INTEGER DEFAULT 0,
                    total_fallidos INTEGER DEFAULT 0,
                    total_reintentos INTEGER DEFAULT 0,
                    tiempo_promedio_ms REAL DEFAULT 0,
                    ultimo_error TEXT,
                    ultimo_error_timestamp REAL,
                    ultimo_exito_timestamp REAL,
                    tasa_error_actual REAL DEFAULT 0,
                    estado TEXT DEFAULT 'activo',
                    actualizado REAL NOT NULL
                )
            ''')

            # Tabla: Configuración de operadores
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS operator_config (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operador TEXT UNIQUE NOT NULL,
                    url_api TEXT NOT NULL,
                    cuenta TEXT,
                    contraseña TEXT,
                    sender_id TEXT,
                    prioridad INTEGER DEFAULT 1,
                    max_por_minuto INTEGER DEFAULT 100,
                    max_reintentos INTEGER DEFAULT 5,
                    timeout_segundos INTEGER DEFAULT 10,
                    habilitado INTEGER DEFAULT 1,
                    actualizado REAL NOT NULL
                )
            ''')

            # Tabla: Webhooks recibidos (confirmación de entrega)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhooks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_id INTEGER,
                    numero TEXT NOT NULL,
                    estado TEXT,
                    codigo_error TEXT,
                    mensaje_error TEXT,
                    timestamp REAL NOT NULL,
                    recibido REAL NOT NULL,
                    procesado INTEGER DEFAULT 0,
                    FOREIGN KEY(queue_id) REFERENCES sms_queue(id)
                )
            ''')

            # Tabla: Alertas del sistema
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alertas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo TEXT NOT NULL,
                    operador TEXT,
                    descripcion TEXT,
                    severidad TEXT,
                    timestamp REAL NOT NULL,
                    procesada INTEGER DEFAULT 0
                )
            ''')

            # Crear índices para rendimiento
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_queue_estado ON sms_queue(estado)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_queue_proximo ON sms_queue(proximo_reintento)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_queue_numero ON sms_queue(numero)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_timestamp ON sms_log(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_operador ON sms_log(operador)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_queue ON webhooks(queue_id)')

            logger.info("Base de datos inicializada correctamente")

    def agregar_a_cola(self, numero, mensaje, campana_id=None, metadata=None):
        """Agrega un SMS a la cola de procesamiento"""
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.now().timestamp()

                try:
                    cursor.execute('''
                        INSERT INTO sms_queue
                        (numero, mensaje, campana_id, estado, creado, metadata, operador_history)
                        VALUES (?, ?, ?, 'pendiente', ?, ?, ?)
                    ''', (numero, mensaje, campana_id, now,
                          json.dumps(metadata) if metadata else None, '[]'))

                    queue_id = cursor.lastrowid
                    logger.debug(f"SMS agregado a cola: {queue_id} -> {numero}")
                    return queue_id
                except sqlite3.IntegrityError:
                    logger.warning(f"SMS duplicado: {numero}")
                    return None

    def obtener_pendientes(self, limit=50):
        """Obtiene SMS pendientes de enviar"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().timestamp()

            cursor.execute('''
                SELECT * FROM sms_queue
                WHERE (estado = 'pendiente' OR (estado = 'reintentando' AND proximo_reintento <= ?))
                AND intentos < max_intentos
                ORDER BY intentos ASC, creado ASC
                LIMIT ?
            ''', (now, limit))

            return [dict(row) for row in cursor.fetchall()]

    def actualizar_intento(self, queue_id, operador, estado, respuesta_api=None, error=None, tiempo_ms=None):
        """Actualiza el resultado de un intento de envío"""
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.now().timestamp()

                # Obtener datos actuales
                cursor.execute('SELECT operador_history, intentos FROM sms_queue WHERE id = ?', (queue_id,))
                row = cursor.fetchone()

                if not row:
                    logger.error(f"Queue ID no encontrado: {queue_id}")
                    return False

                # Actualizar historial de operadores
                history = json.loads(row['operador_history']) if row['operador_history'] else []
                history.append({
                    'operador': operador,
                    'intento': row['intentos'] + 1,
                    'timestamp': now,
                    'estado': estado,
                    'error': error
                })

                # Determinar próximo estado
                if estado == 'enviado':
                    nuevo_estado = 'enviado'
                    proximo_reintento = None
                elif estado == 'entregado':
                    nuevo_estado = 'entregado'
                    proximo_reintento = None
                else:  # error
                    nuevo_estado = 'reintentando' if row['intentos'] + 1 < row['max_intentos'] else 'fallido'
                    if nuevo_estado == 'reintentando':
                        # Backoff exponencial: 1s, 5s, 30s, 5min, 30min
                        delays = [1, 5, 30, 300, 1800]
                        proximo_reintento = now + delays[row['intentos']]
                    else:
                        proximo_reintento = None

                # Actualizar SMS en cola
                cursor.execute('''
                    UPDATE sms_queue SET
                        estado = ?,
                        intentos = intentos + 1,
                        operador = ?,
                        operador_history = ?,
                        respuesta_ultima = ?,
                        error_ultimo = ?,
                        ultimo_intento = ?,
                        proximo_reintento = ?,
                        primer_intento = COALESCE(primer_intento, ?)
                    WHERE id = ?
                ''', (nuevo_estado, operador, json.dumps(history), respuesta_api,
                      error, now, proximo_reintento, now, queue_id))

                # Registrar en log
                cursor.execute('''
                    INSERT INTO sms_log
                    (queue_id, numero, operador, estado, intento_numero, timestamp, respuesta_api, error, tiempo_respuesta_ms)
                    SELECT id, numero, ?, ?, intentos + 1, ?, ?, ?, ?
                    FROM sms_queue WHERE id = ?
                ''', (operador, estado, now, respuesta_api, error, tiempo_ms, queue_id))

                # Actualizar estadísticas del operador
                cursor.execute('''
                    INSERT INTO operator_stats (operador, actualizado) VALUES (?, ?)
                    ON CONFLICT(operador) DO UPDATE SET
                        total_enviados = total_enviados + 1,
                        ultimo_error_timestamp = CASE WHEN ? = 'error' THEN ? ELSE ultimo_error_timestamp END,
                        ultimo_error = CASE WHEN ? = 'error' THEN ? ELSE ultimo_error END,
                        ultimo_exito_timestamp = CASE WHEN ? IN ('enviado', 'entregado') THEN ? ELSE ultimo_exito_timestamp END,
                        actualizado = ?
                ''', (operador, now, estado, now, estado, error,
                      estado, now, now))

                logger.info(f"SMS {queue_id} actualizado: {operador} -> {nuevo_estado}")
                return True

    def confirmar_entrega(self, numero, codigo_error=None):
        """Marca un SMS como entregado (llamado por webhook del operador)"""
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                now = datetime.now().timestamp()

                cursor.execute('''
                    UPDATE sms_queue SET
                        estado = 'entregado',
                        webhook_confirmado = 1,
                        webhook_timestamp = ?,
                        entregado = ?
                    WHERE numero = ? AND estado != 'entregado'
                    LIMIT 1
                ''', (datetime.now().isoformat(), now, numero))

                if cursor.rowcount > 0:
                    # Registrar webhook
                    cursor.execute('''
                        INSERT INTO webhooks (numero, estado, timestamp, recibido)
                        VALUES (?, 'entregado', ?, ?)
                    ''', (numero, now, now))
                    logger.info(f"Entrega confirmada por webhook: {numero}")
                    return True

                return False

    def obtener_stats_operador(self, operador):
        """Obtiene estadísticas de un operador"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM operator_stats WHERE operador = ?', (operador,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def obtener_todas_stats(self):
        """Obtiene estadísticas de todos los operadores"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM operator_stats ORDER BY operador')
            return [dict(row) for row in cursor.fetchall()]

    def obtener_estado_general(self):
        """Obtiene estado general del sistema"""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Obtener conteos por estado
            cursor.execute('''
                SELECT
                    SUM(CASE WHEN estado = 'entregado' THEN 1 ELSE 0 END) as entregados,
                    SUM(CASE WHEN estado = 'enviado' THEN 1 ELSE 0 END) as enviados,
                    SUM(CASE WHEN estado = 'fallido' THEN 1 ELSE 0 END) as fallidos,
                    SUM(CASE WHEN estado IN ('pendiente', 'reintentando') THEN 1 ELSE 0 END) as pendientes,
                    COUNT(*) as total
                FROM sms_queue
            ''')

            estado = dict(cursor.fetchone())
            return estado

# Instancia global
db = DatabaseManager()
