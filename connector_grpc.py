"""
Cliente gRPC para conexión con el Gateway (modo túnel reverso)
Usa protobuf nativo generado por protoc para máximo rendimiento
"""
import asyncio
import logging
import base64
import platform
from pathlib import Path

import grpc
import yaml

# Importar tipos generados por protoc
from proto import connector_pb2
from proto import connector_pb2_grpc

from data_loader import data_loader

logger = logging.getLogger("ConnectorGRPC")

# Cargar configuración
CONFIG_PATH = Path(__file__).parent / "config.yml"

def load_config():
    """Carga configuración desde YAML"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    return {}

config = load_config()

# Configuración
GRPC_URI = config.get('gateway', {}).get('grpc_uri', 'localhost:50051')
_tenant_cfg = config.get('tenant', {}).get('id', 'auto')
TENANT_ID = f"tenant_{platform.node().replace('-', '_').lower()}" if _tenant_cfg == 'auto' else _tenant_cfg
RECONNECT_DELAY = config.get('performance', {}).get('reconnect_delay', 5)
PARALLEL_PARTITIONS = config.get('performance', {}).get('parallel_partitions', True)
# Compresión de transferencia: 'zstd' (recomendado) o None
TRANSFER_COMPRESSION = config.get('performance', {}).get('transfer_compression', 'zstd')
if TRANSFER_COMPRESSION and TRANSFER_COMPRESSION.lower() == 'none':
    TRANSFER_COMPRESSION = None


class GRPCConnector:
    """Conector gRPC bidireccional al Gateway con protobuf nativo"""
    
    def __init__(self, grpc_uri: str = None, tenant_id: str = None):
        self.grpc_uri = grpc_uri or GRPC_URI
        self.tenant_id = tenant_id or TENANT_ID
        self.running = False
        self.channel = None
        
        # Cargar datos al inicio
        data_loader.load_or_generate_dataset()
        
        logger.info(f"GRPCConnector initialized (native protobuf):")
        logger.info(f"  Gateway URI: {self.grpc_uri}")
        logger.info(f"  Tenant ID: {self.tenant_id}")
    
    async def run(self):
        """Loop principal de conexión"""
        self.running = True
        
        while self.running:
            try:
                logger.info(f"Connecting to gRPC server {self.grpc_uri}...")
                
                # Crear canal gRPC async
                self.channel = grpc.aio.insecure_channel(self.grpc_uri)
                
                # Establecer stream bidireccional
                await self._connect_and_run()
                
            except grpc.RpcError as e:
                logger.warning(f"gRPC connection lost: {e}. Retrying in {RECONNECT_DELAY}s...")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            
            if self.running:
                await asyncio.sleep(RECONNECT_DELAY)
    
    async def _connect_and_run(self):
        """Establece stream bidireccional con el Gateway usando protobuf nativo"""
        
        # Cola para mensajes salientes (protobuf messages)
        outgoing = asyncio.Queue()
        
        # Enviar mensaje de registro usando tipo nativo
        register_msg = connector_pb2.ConnectorMessage(
            request_id="",
            register=connector_pb2.RegisterRequest(
                tenant_id=self.tenant_id,
                version="1.0.0-native",
                datasets=["sales"]
            )
        )
        await outgoing.put(register_msg)
        
        async def message_generator():
            """Genera mensajes para el stream saliente"""
            while self.running:
                msg = await outgoing.get()
                if msg is None:
                    break
                yield msg
        
        # Crear stub del servicio generado
        stub = connector_pb2_grpc.ConnectorServiceStub(self.channel)
        
        # Iniciar stream bidireccional
        call = stub.Connect(message_generator())
        
        # Procesar comandos entrantes del Gateway (ya deserializados como protobuf)
        async for command in call:
            await self._handle_command(command, outgoing)
    
    async def _handle_command(self, command: connector_pb2.GatewayCommand, outgoing: asyncio.Queue):
        """Procesa comandos del Gateway (tipos nativos)"""
        req_id = command.request_id
        
        # Detectar qué comando es usando HasField con oneof
        if command.HasField('register_response'):
            resp = command.register_response
            if resp.status == "ok":
                logger.info(f"Registered successfully. Session: {resp.session_id}")
            else:
                logger.error(f"Registration failed: {resp.error}")
        
        elif command.HasField('get_flight_info'):
            await self._handle_get_flight_info(req_id, command.get_flight_info, outgoing)
        
        elif command.HasField('do_get'):
            await self._handle_do_get(req_id, command.do_get, outgoing)
        
        elif command.HasField('heartbeat'):
            response = connector_pb2.ConnectorMessage(
                request_id=req_id,
                heartbeat=connector_pb2.HeartbeatResponse(
                    tenant_id=self.tenant_id,
                    timestamp=command.heartbeat.timestamp
                )
            )
            await outgoing.put(response)
    
    async def _handle_get_flight_info(self, request_id: str, get_info: connector_pb2.GetFlightInfoRequest, outgoing: asyncio.Queue):
        """Maneja solicitud de FlightInfo con tipos nativos"""
        path = list(get_info.path)
        rows = get_info.rows
        
        dataset_name = path[0] if path else None
        
        # Cargar dataset
        if dataset_name and dataset_name != "sales":
            success = data_loader.load_from_file(dataset_name)
            if not success:
                data_loader.load_or_generate_dataset(rows=rows or 1_000_000)
        elif rows:
            data_loader.load_or_generate_dataset(rows=int(rows))
        else:
            if data_loader.total_records == 0:
                data_loader.load_or_generate_dataset()
        
        # Calcular particiones
        total_bytes = data_loader.total_bytes
        if PARALLEL_PARTITIONS:
            if total_bytes < 10 * 1024 * 1024:
                partitions = 1
            elif total_bytes < 50 * 1024 * 1024:
                partitions = 2
            elif total_bytes < 100 * 1024 * 1024:
                partitions = 4
            else:
                partitions = 8
        else:
            partitions = 1
        
        # Respuesta con tipo nativo (schema como bytes, no base64)
        response = connector_pb2.ConnectorMessage(
            request_id=request_id,
            flight_info=connector_pb2.FlightInfoResponse(
                status="ok",
                schema=data_loader.get_schema_bytes(),  # Bytes directos, no base64
                total_records=data_loader.total_records,
                total_bytes=total_bytes,
                dataset=data_loader.current_dataset,
                partitions=partitions
            )
        )
        
        logger.info(f"FlightInfo: {data_loader.current_dataset}, {data_loader.total_records:,} rows, {total_bytes/1024/1024:.2f} MB, {partitions} partitions")
        await outgoing.put(response)
    
    async def _handle_do_get(self, request_id: str, do_get: connector_pb2.DoGetRequest, outgoing: asyncio.Queue):
        """Maneja solicitud DoGet con streaming de Arrow IPC nativo"""
        import json
        
        ticket = do_get.ticket
        
        # Decodificar ticket para info de partición
        partition = 0
        total_partitions = 1
        
        if ticket:
            try:
                ticket_bytes = base64.b64decode(ticket)
                ticket_data = json.loads(ticket_bytes.decode('utf-8'))
                partition = ticket_data.get("partition", 0)
                total_partitions = ticket_data.get("total_partitions", 1)
                logger.info(f"Starting data transfer for {request_id} - Partition {partition}/{total_partitions}")
            except Exception:
                logger.debug(f"Ticket is plain dataset name")
        
        # Enviar stream_start con tipo nativo - incluyendo tipo de compresión
        compression = TRANSFER_COMPRESSION if TRANSFER_COMPRESSION else 'none'
        start_msg = connector_pb2.ConnectorMessage(
            request_id=request_id,
            stream_status=connector_pb2.StreamStatus(
                type="stream_start",
                schema=data_loader.get_schema_bytes(),  # Bytes directos
                partition=partition,
                total_partitions=total_partitions,
                compression=compression  # Indica al cliente cómo descomprimir
            )
        )
        await outgoing.put(start_msg)
        
        # Enviar chunks de Arrow IPC con compresión de transferencia
        total_bytes = 0
        try:
            all_batches = data_loader.get_record_batches(transfer_compression=TRANSFER_COMPRESSION)
            total_batches = len(all_batches)
            
            if total_partitions > 1 and total_batches > 1:
                batch_start = (total_batches * partition) // total_partitions
                batch_end = (total_batches * (partition + 1)) // total_partitions
                batches_to_send = all_batches[batch_start:batch_end]
            else:
                batches_to_send = all_batches
            
            for batch_bytes in batches_to_send:
                # Enviar como ArrowChunk con bytes directos (sin base64!)
                chunk_msg = connector_pb2.ConnectorMessage(
                    request_id=request_id,
                    arrow_chunk=connector_pb2.ArrowChunk(
                        data=batch_bytes,  # Bytes directos, protobuf los maneja eficientemente
                        partition=partition
                    )
                )
                await outgoing.put(chunk_msg)
                total_bytes += len(batch_bytes)
                await asyncio.sleep(0)
            
            # Enviar stream_end
            end_msg = connector_pb2.ConnectorMessage(
                request_id=request_id,
                stream_status=connector_pb2.StreamStatus(
                    type="stream_end",
                    partition=partition,
                    total_bytes=total_bytes
                )
            )
            await outgoing.put(end_msg)
            logger.info(f"Partition {partition} complete. {len(batches_to_send)} batches, {total_bytes/1024/1024:.2f} MB")
        
        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            error_msg = connector_pb2.ConnectorMessage(
                request_id=request_id,
                stream_status=connector_pb2.StreamStatus(
                    type="stream_end",
                    error=str(e)
                )
            )
            await outgoing.put(error_msg)
    
    def stop(self):
        self.running = False
        if self.channel:
            asyncio.create_task(self.channel.close())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connector = GRPCConnector()
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        connector.stop()
