"""
Data Connector - Arrow Flight Server

Este módulo implementa el Data Connector como un servidor Arrow Flight,
permitiendo que el Gateway se conecte via gRPC para obtener datos.
"""

import pyarrow as pa
import pyarrow.flight as flight
import logging
import signal
import sys
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Importar el data_loader existente
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import DataLoader


class DataConnectorFlightServer(flight.FlightServerBase):
    """
    Servidor Arrow Flight que expone datasets a clientes gRPC.
    
    Endpoints:
    - GetFlightInfo: Retorna metadata del dataset (y lo carga en memoria)
    - DoGet: Retorna stream de RecordBatches con los datos
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 50051, **kwargs):
        location = flight.Location.for_grpc_tcp(host, port)
        super().__init__(location, **kwargs)
        
        self._host = host
        self._port = port
        self.data_loader = DataLoader()
        self._running = True
        
        logger.info(f"Flight server initialized on {host}:{port}")
    
    def get_flight_info(self, context, descriptor: flight.FlightDescriptor):
        """
        Retorna información del dataset y lo carga en memoria.
        
        El descriptor.path contiene el nombre del dataset.
        """
        dataset_name = None
        
        if descriptor.descriptor_type == flight.DescriptorType.PATH:
            if descriptor.path:
                dataset_name = descriptor.path[0].decode() if isinstance(descriptor.path[0], bytes) else descriptor.path[0]
        
        logger.info(f"GetFlightInfo request: {dataset_name}")
        
        # Cargar dataset
        if dataset_name:
            success = self.data_loader.load_from_file(dataset_name)
            if not success:
                logger.warning(f"Dataset '{dataset_name}' not found, generating synthetic")
                self.data_loader.load_or_generate_dataset(rows=100000)
        else:
            if self.data_loader.total_records == 0:
                self.data_loader.load_or_generate_dataset()
        
        # Crear schema y endpoint info
        schema = self.data_loader.get_schema()
        
        # Crear ticket (identificador para DoGet)
        ticket = flight.Ticket(dataset_name.encode() if dataset_name else b"default")
        
        # Crear endpoint
        endpoints = [
            flight.FlightEndpoint(
                ticket,
                [flight.Location.for_grpc_tcp(self._host, self._port)]
            )
        ]
        
        # Calcular tamaño aproximado
        total_bytes = self.data_loader.total_records * 100  # Estimado
        
        return flight.FlightInfo(
            schema,
            descriptor,
            endpoints,
            self.data_loader.total_records,
            total_bytes
        )
    
    def do_get(self, context, ticket: flight.Ticket):
        """
        Retorna un stream de RecordBatches con los datos.
        """
        dataset_name = ticket.ticket.decode() if ticket.ticket else "default"
        logger.info(f"DoGet request: {dataset_name}")
        
        # Obtener batches como RecordBatch objects (no bytes)
        batches = self.data_loader.get_record_batches(as_bytes=False)
        schema = self.data_loader.get_schema()
        
        logger.info(f"Streaming {len(batches)} batches, {self.data_loader.total_records} records")
        
        return flight.RecordBatchStream(
            pa.Table.from_batches(batches, schema=schema)
        )
    
    def list_flights(self, context, criteria):
        """Lista los datasets disponibles."""
        # Listar archivos en el directorio datasets
        datasets_dir = os.path.join(os.path.dirname(__file__), "datasets")
        
        if os.path.exists(datasets_dir):
            for filename in os.listdir(datasets_dir):
                if filename.endswith(('.csv', '.json', '.parquet')):
                    descriptor = flight.FlightDescriptor.for_path(filename)
                    
                    # Crear info básica sin cargar el archivo
                    yield flight.FlightInfo(
                        pa.schema([]),  # Schema vacío (se obtiene con GetFlightInfo)
                        descriptor,
                        [],  # Sin endpoints hasta que se solicite
                        -1,  # Records desconocido
                        -1   # Bytes desconocido
                    )
    
    def do_action(self, context, action: flight.Action):
        """Ejecuta acciones personalizadas."""
        if action.type == "healthcheck":
            yield flight.Result(b'{"status": "healthy"}')
        elif action.type == "reload":
            self.data_loader = DataLoader()
            yield flight.Result(b'{"status": "reloaded"}')
        else:
            raise flight.FlightUnavailableError(f"Unknown action: {action.type}")
    
    def shutdown(self):
        """Detiene el servidor."""
        self._running = False
        super().shutdown()


def run_server(host: str = "0.0.0.0", port: int = 50051):
    """
    Inicia el servidor Arrow Flight.
    """
    server = DataConnectorFlightServer(host=host, port=port)
    
    # Manejar señales de terminación
    def signal_handler(sig, frame):
        logger.info("Shutting down Flight server...")
        server.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(f"=== Data Connector Flight Server ===")
    logger.info(f"Listening on {host}:{port}")
    logger.info(f"Press Ctrl+C to stop")
    
    server.serve()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Connector Flight Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    
    args = parser.parse_args()
    
    run_server(host=args.host, port=args.port)
