"""
Servicio Windows para Arrow Data Connector.
Uso:
  python service.py install  (Instalar servicio)
  python service.py start    (Iniciar servicio)
  python service.py --test   (Ejecutar en consola para pruebas)
"""
import sys
import asyncio
import logging
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket

# Configurar logging basico antes de importar modulos que loguean
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()] # Por defecto a stdout
)

from connector import ArrowConnector, GATEWAY_URI, TENANT_ID

class ArrowConnectorService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ArrowConnector"
    _svc_display_name_ = "Arrow Data Connector"
    _svc_description_ = "Conecta datos on-premise con SaaS Gateway via WebSocket Reverso"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.connector = ArrowConnector()  # Lee config desde config.yml
        self.loop = None

    def SvcStop(self):
        """Callback al detener el servicio"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        logging.info("Service stopping...")
        win32event.SetEvent(self.hWaitStop)
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
        logging.info("Service started.")
        # En modo servicio debemos crear un nuevo loop
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            # Ejecutar el connector junto con un monitor de parada
            # Nota: SvcStop se llama desde otro thread, asi que connector.stop() cambia un flag
            # y el loop deberia terminar.
            # Ojo: connector.connect_and_run es un bucle infinito mientras running=True
            self.loop.run_until_complete(self.connector.run())
            
        except Exception as e:
            logging.error(f"Service error: {e}")
            servicemanager.LogErrorMsg(str(e))
        finally:
            self.loop.close()
            logging.info("Service stopped.")


def run_test_mode():
    """Ejecución manual para desarrollo/testing"""
    logging.info("--- RUNNING IN TEST MODE (Console) ---")
    connector = ArrowConnector()  # Lee config desde config.yml
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.error(f"Error: {e}") 

if __name__ == '__main__':
    if '--test' in sys.argv:
        run_test_mode()
    else:
        # Modo servicio de Windows
        if len(sys.argv) == 1:
            # Iniciado por el Service Manager
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(ArrowConnectorService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
            # Comandos de gestión (install, start, etc.)
            win32serviceutil.HandleCommandLine(ArrowConnectorService)
