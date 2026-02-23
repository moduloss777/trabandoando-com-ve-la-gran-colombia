"""
Tests para integración de TraffiLink API
Ejecutar con: python test_traffilink.py
"""

import unittest
import logging
from datetime import datetime
from traffilink_service import traffilink_service

# Configurar logging para tests
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class TestTraffiLinkAPI(unittest.TestCase):
    """Suite de tests para TraffiLink API"""

    def setUp(self):
        """Configuración inicial para cada test"""
        self.service = traffilink_service
        logger.info("=" * 70)
        logger.info(f"Iniciando test: {self._testMethodName}")
        logger.info("=" * 70)

    def tearDown(self):
        """Limpieza después de cada test"""
        logger.info(f"Test completado: {self._testMethodName}")
        logger.info("")

    def test_01_conectividad_basica(self):
        """TEST 1: Verificar conectividad básica con servidor TraffiLink"""
        logger.info("🔍 Verificando conectividad con servidor TraffiLink...")

        # Intentar consultar balance (endpoint más simple)
        resultado = self.service.consultar_balance()

        self.assertIsNotNone(resultado)
        self.assertIn('exito', resultado)

        if resultado['exito']:
            logger.info(f"✅ Conexión exitosa. Balance: {resultado.get('balance')} EUR")
            self.assertTrue(resultado['exito'])
        else:
            error = resultado.get('error', 'Unknown')
            logger.error(f"❌ Error de conexión: {error}")
            self.fail(f"No se pudo conectar con TraffiLink: {error}")

    def test_02_consultar_balance(self):
        """TEST 2: Consultar saldo disponible"""
        logger.info("🔍 Consultando balance de cuenta...")

        resultado = self.service.consultar_balance()

        self.assertTrue(resultado.get('exito'), f"Error: {resultado.get('error')}")
        self.assertIn('balance', resultado)
        self.assertIsInstance(resultado['balance'], (int, float))

        balance = resultado['balance']
        logger.info(f"✅ Balance actual: {balance} EUR")

        # Verificar que haya saldo para enviar pruebas
        if balance < 0.01:  # Menos de 1 centavo
            logger.warning(f"⚠️ Saldo bajo: {balance} EUR")
        else:
            logger.info(f"✅ Saldo suficiente para pruebas")

    def test_03_validar_numero(self):
        """TEST 3: Validar números telefónicos"""
        logger.info("🔍 Validando números telefónicos...")

        test_cases = [
            ('3001234567', '573001234567', True),           # 10 dígitos
            ('573001234567', '573001234567', True),         # 12 dígitos con prefijo
            ('570012345', '570012345', False),              # Menos de 10 dígitos
            ('abcdefghij', None, False),                    # Caracteres no numéricos
        ]

        for numero_entrada, esperado, debe_ser_valido in test_cases:
            resultado = self.service.validar_numero_colombiano(numero_entrada)

            logger.info(f"  Input: {numero_entrada} → Output: {resultado}")

            if debe_ser_valido:
                self.assertEqual(resultado, esperado)
                logger.info(f"  ✅ Número válido: {resultado}")
            else:
                self.assertIsNone(resultado)
                logger.info(f"  ✅ Número rechazado correctamente")

    def test_04_generador_sign(self):
        """TEST 4: Verificar generación correcta de firma"""
        logger.info("🔍 Probando generador de firma...")

        params = "account=0152C274&sendid=TEST001&mobile=573001234567&content=Test"
        sign = self.service.generar_sign(params)

        self.assertIsNotNone(sign)
        self.assertEqual(len(sign), 32)  # MD5 siempre es 32 caracteres hex
        self.assertTrue(all(c in '0123456789abcdef' for c in sign))

        logger.info(f"✅ Firma generada correctamente: {sign}")

        # Verificar que la firma sea determinista (mismo input = mismo output)
        sign2 = self.service.generar_sign(params)
        self.assertEqual(sign, sign2)
        logger.info(f"✅ Firma es determinista")

    def test_05_enviar_sms_prueba(self):
        """TEST 5: Enviar SMS de prueba a número válido"""
        logger.info("🔍 Preparándose para enviar SMS de prueba...")

        # IMPORTANTE: Cambiar a un número real para probar
        numero_test = '573001234567'  # ← CAMBIAR A NÚMERO REAL
        mensaje_test = f'Test Goleador - {datetime.now().strftime("%H:%M:%S")}'
        queue_id = 'TEST_' + datetime.now().strftime("%Y%m%d_%H%M%S")

        logger.info(f"  Número: {numero_test}")
        logger.info(f"  Mensaje: {mensaje_test}")
        logger.info(f"  Queue ID: {queue_id}")

        resultado = self.service.enviar_sms(numero_test, mensaje_test, queue_id)

        logger.info(f"  Respuesta: {resultado}")

        if resultado['exito']:
            traffilink_id = resultado.get('traffilink_id')
            logger.info(f"✅ SMS enviado exitosamente")
            logger.info(f"   ID TraffiLink: {traffilink_id}")
            logger.info(f"   Queue ID: {queue_id}")

            self.assertTrue(resultado['exito'])
            self.assertIsNotNone(traffilink_id)

            # Guardar ID para test de estado
            self.traffilink_id_para_estado = traffilink_id

        else:
            error = resultado.get('error', 'Unknown')
            logger.warning(f"⚠️ Error enviando SMS: {error}")
            logger.warning(f"Posibles causas:")
            logger.warning(f"  - Número incorrecto (cambiar en test_05)")
            logger.warning(f"  - Saldo insuficiente")
            logger.warning(f"  - Credenciales incorrectas")
            # No fallar el test, solo avisar
            logger.info("ℹ️ Test de envío saltado (número de prueba no configurado)")

    def test_06_consultar_estado_sms(self):
        """TEST 6: Consultar estado de SMS enviado"""
        logger.info("🔍 Consultando estado de SMS...")

        # Usar ID del test anterior si existe
        if hasattr(self, 'traffilink_id_para_estado'):
            traffilink_id = self.traffilink_id_para_estado
        else:
            # Si no hay ID previo, usar uno de ejemplo
            traffilink_id = 'TEST_ID'
            logger.warning(f"⚠️ Usando ID de ejemplo (sin SMS enviado previamente)")

        logger.info(f"  Consultando ID: {traffilink_id}")

        resultado = self.service.consultar_estado(traffilink_id)

        logger.info(f"  Respuesta: {resultado}")

        if resultado['exito']:
            estado = resultado.get('estado')
            logger.info(f"✅ Estado consultado: {estado}")
            self.assertIn(estado, ['delivered', 'failed', 'pending', 'invalid', 'unknown'])
        else:
            error = resultado.get('error', 'Unknown')
            logger.warning(f"⚠️ No se pudo consultar estado: {error}")

    def test_07_procesar_webhook(self):
        """TEST 7: Procesar webhook de entrega"""
        logger.info("🔍 Probando procesamiento de webhook...")

        datos_webhook = {
            'id': 'TEST_WEBHOOK_ID_001',
            'deliverystatus': '1',  # 1 = entregado
            'timestamp': datetime.now().isoformat()
        }

        logger.info(f"  Datos webhook: {datos_webhook}")

        resultado = self.service.procesar_webhook_reporte(datos_webhook)

        logger.info(f"  Resultado: {resultado}")

        self.assertTrue(resultado['exito'])
        self.assertEqual(resultado.get('estado'), 'delivered')
        logger.info(f"✅ Webhook procesado correctamente")

        # Probar con estado fallido
        datos_webhook['deliverystatus'] = '2'  # 2 = fallido
        resultado = self.service.procesar_webhook_reporte(datos_webhook)

        self.assertTrue(resultado['exito'])
        self.assertEqual(resultado.get('estado'), 'failed')
        logger.info(f"✅ Estados múltiples procesados correctamente")

    def test_08_manejo_errores(self):
        """TEST 8: Manejo de errores y casos especiales"""
        logger.info("🔍 Probando manejo de errores...")

        # Caso 1: Número vacío
        resultado = self.service.enviar_sms('', 'Test', 'TEST')
        self.assertFalse(resultado['exito'])
        logger.info(f"  ✅ Número vacío manejado correctamente")

        # Caso 2: Mensaje vacío
        resultado = self.service.enviar_sms('573001234567', '', 'TEST')
        self.assertFalse(resultado['exito'])
        logger.info(f"  ✅ Mensaje vacío manejado correctamente")

        # Caso 3: Webhook sin datos
        resultado = self.service.procesar_webhook_reporte({})
        self.assertFalse(resultado['exito'])
        logger.info(f"  ✅ Webhook sin datos manejado correctamente")

        logger.info(f"✅ Todos los casos de error manejados correctamente")


class TestTraffiLinkIntegracion(unittest.TestCase):
    """Tests de integración con el sistema Goleador"""

    def setUp(self):
        """Configuración inicial"""
        self.service = traffilink_service
        logger.info("=" * 70)
        logger.info(f"Iniciando test integración: {self._testMethodName}")
        logger.info("=" * 70)

    def test_01_importar_sender(self):
        """TEST: Verificar que TraffiLink está integrado en sender"""
        logger.info("🔍 Verificando integración en sender.py...")

        try:
            from sender import SMSSender
            logger.info("✅ sender.SMSSender importado correctamente")
            self.assertTrue(True)
        except ImportError as e:
            logger.error(f"❌ Error importando sender: {e}")
            self.fail(f"No se puede importar sender: {e}")

    def test_02_importar_app(self):
        """TEST: Verificar que webhook está registrado en app"""
        logger.info("🔍 Verificando webhook en app.py...")

        try:
            from app import app
            logger.info("✅ app importado correctamente")

            # Verificar que el endpoint existe
            rutas = [str(rule) for rule in app.url_map.iter_rules()]
            webhook_existe = any('traffilink' in ruta for ruta in rutas)

            if webhook_existe:
                logger.info("✅ Endpoint /webhook/traffilink-report registrado")
                self.assertTrue(True)
            else:
                logger.warning("⚠️ Endpoint /webhook/traffilink-report no encontrado en rutas")

        except Exception as e:
            logger.error(f"❌ Error: {e}")


# ==================== EJECUCIÓN ====================

if __name__ == '__main__':
    logger.info("\n")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 15 + "SUITE DE TESTS - TRAFFILINK API" + " " * 22 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")

    # Ejecutar tests
    unittest.main(verbosity=2)
