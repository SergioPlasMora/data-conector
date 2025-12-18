"""
Servicio Windows para Arrow Data Connector (WebSocket Mode).
Usa túnel inverso WebSocket para conectar al Gateway.

Uso:
  python service.py install  (Instalar servicio)
  python service.py start    (Iniciar servicio)
  python service.py --test   (Ejecutar en consola para pruebas)
"""
import sys
import asyncio
import logging
from pathlib import Path

import yaml

# Configurar logging basico antes de importar modulos que loguean
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

# Cargar configuración
CONFIG_PATH = Path(__file__).parent / "config.yml"

def load_config():
    """Carga configuración desde config.yml"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}

CONFIG = load_config()


def run_connector():
    """Ejecuta el conector según el modo de transporte configurado"""
    transport_mode = CONFIG.get('gateway', {}).get('transport_mode', 'websocket')
    
    if transport_mode == 'grpc':
        from connector_grpc import GRPCConnector
        logger.info("=== Starting Arrow Data Connector (gRPC mode) ===")
        connector = GRPCConnector()
    else:
        from connector import ArrowConnector
        logger.info("=== Starting Arrow Data Connector (WebSocket mode) ===")
        connector = ArrowConnector()
    
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Error: {e}")


def run_test_mode():
    """Ejecución manual para desarrollo/testing"""
    logger.info("--- RUNNING IN TEST MODE (Console) ---")
    run_connector()


# === Windows Service Support ===
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False
    logger.warning("pywin32 not installed - Windows service mode unavailable")


if HAS_WIN32:
    from connector import ArrowConnector
    
    class ArrowConnectorService(win32serviceutil.ServiceFramework):
        _svc_name_ = "ArrowConnector"
        _svc_display_name_ = "Arrow Data Connector"
        _svc_description_ = "Conecta datos on-premise con SaaS Gateway via WebSocket"

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self.connector = None
            self.loop = None

        def SvcStop(self):
            """Callback al detener el servicio"""
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            logger.info("Service stopping...")
            win32event.SetEvent(self.hWaitStop)
            
            if self.connector:
                self.connector.stop()

        def SvcDoRun(self):
            """Callback principal al iniciar"""
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE,
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, '')
            )
            self.main()

        def main(self):
            """Loop principal del servicio"""
            logger.info("Service started (WebSocket mode)")
            
            try:
                self.connector = ArrowConnector()
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self.loop.run_until_complete(self.connector.run())
                    
            except Exception as e:
                logger.error(f"Service error: {e}")
                servicemanager.LogErrorMsg(str(e))
            finally:
                if self.loop:
                    self.loop.close()
                logger.info("Service stopped.")


if __name__ == '__main__':
    if '--test' in sys.argv:
        run_test_mode()
    elif HAS_WIN32:
        # Modo servicio de Windows
        if len(sys.argv) == 1:
            # Iniciado por el Service Manager
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(ArrowConnectorService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            # Comandos de gestión (install, start, etc.)
            win32serviceutil.HandleCommandLine(ArrowConnectorService)
    else:
        # Sin soporte de Windows Service, ejecutar en modo test
        run_test_mode()
