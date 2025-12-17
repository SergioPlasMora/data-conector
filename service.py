"""
Servicio Windows para Arrow Data Connector.
Soporta dos modos de conexión:
  - websocket: Túnel inverso (connector inicia conexión al gateway)
  - grpc: Flight Server (gateway inicia conexión al connector)

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

def get_connector_mode():
    """Retorna el modo de conexión: 'websocket' o 'grpc'"""
    return CONFIG.get('gateway', {}).get('connector_mode', 'websocket')


def run_websocket_mode():
    """Ejecuta el modo WebSocket (túnel inverso)"""
    from connector import ArrowConnector
    logger.info("=== Starting in WEBSOCKET mode (reverse tunnel) ===")
    connector = ArrowConnector()
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Error: {e}")


def run_grpc_mode():
    """Ejecuta el modo gRPC (Flight Server pasivo)"""
    from flight_server import run_server
    
    flight_config = CONFIG.get('flight_server', {})
    host = flight_config.get('host', '0.0.0.0')
    port = flight_config.get('port', 50051)
    
    logger.info("=== Starting in GRPC mode (Flight Server) ===")
    logger.info(f"  Listening on {host}:{port}")
    logger.info("  Gateway must connect to this address")
    
    run_server(host=host, port=port)


def run_test_mode():
    """Ejecución manual para desarrollo/testing"""
    mode = get_connector_mode()
    logger.info(f"--- RUNNING IN TEST MODE (Console) - Mode: {mode.upper()} ---")
    
    if mode == 'grpc':
        run_grpc_mode()
    else:
        run_websocket_mode()


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
        _svc_description_ = "Conecta datos on-premise con SaaS Gateway (WebSocket o gRPC)"

        def __init__(self, args):
            win32serviceutil.ServiceFramework.__init__(self, args)
            self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
            self.mode = get_connector_mode()
            self.connector = None
            self.flight_server = None
            self.loop = None

        def SvcStop(self):
            """Callback al detener el servicio"""
            self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
            logger.info("Service stopping...")
            win32event.SetEvent(self.hWaitStop)
            
            if self.connector:
                self.connector.stop()
            if self.flight_server:
                self.flight_server.shutdown()

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
            logger.info(f"Service started in {self.mode.upper()} mode")
            
            try:
                if self.mode == 'grpc':
                    from flight_server import DataConnectorFlightServer
                    flight_config = CONFIG.get('flight_server', {})
                    host = flight_config.get('host', '0.0.0.0')
                    port = flight_config.get('port', 50051)
                    
                    self.flight_server = DataConnectorFlightServer(host=host, port=port)
                    self.flight_server.serve()
                else:
                    # WebSocket mode
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

