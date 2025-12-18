"""
Cliente gRPC para conexión con el Gateway (modo túnel reverso)
Usa protobuf struct_pb2 para compatibilidad con el servidor Go
"""
import asyncio
import json
import logging
import base64
import platform
from pathlib import Path

import grpc
import yaml
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict

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


class GRPCConnector:
    """Conector gRPC bidireccional al Gateway"""
    
    def __init__(self, grpc_uri: str = None, tenant_id: str = None):
        self.grpc_uri = grpc_uri or GRPC_URI
        self.tenant_id = tenant_id or TENANT_ID
        self.running = False
        self.channel = None
        
        # Cargar datos al inicio
        data_loader.load_or_generate_dataset()
        
        logger.info(f"GRPCConnector initialized:")
        logger.info(f"  Gateway URI: {self.grpc_uri}")
        logger.info(f"  Tenant ID: {self.tenant_id}")
    
    async def run(self):
        """Loop principal de conexión"""
        self.running = True
        
        while self.running:
            try:
                logger.info(f"Connecting to gRPC server {self.grpc_uri}...")
                
                # Crear canal gRPC
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
        """Establece stream bidireccional con el Gateway"""
        
        # Cola para mensajes salientes (protobuf Struct serializado)
        outgoing = asyncio.Queue()
        
        # Helper para convertir dict a protobuf Struct serializado
        def dict_to_proto(d: dict) -> bytes:
            s = struct_pb2.Struct()
            s.update(d)
            return s.SerializeToString()
        
        # Enviar mensaje de registro
        register_msg = {
            "request_id": "",
            "type": "register",
            "register": {
                "tenant_id": self.tenant_id,
                "version": "1.0.0",
                "datasets": ["sales"]
            }
        }
        await outgoing.put(dict_to_proto(register_msg))
        
        async def message_generator():
            """Genera mensajes para el stream saliente"""
            while self.running:
                msg = await outgoing.get()
                if msg is None:
                    break
                yield msg
        
        # Crear stub del servicio
        stub = ConnectorServiceStub(self.channel)
        
        # Iniciar stream bidireccional
        call = stub.Connect(message_generator())
        
        # Helper para deserializar protobuf Struct a dict
        def proto_to_dict(data: bytes) -> dict:
            s = struct_pb2.Struct()
            s.ParseFromString(data)
            return MessageToDict(s)
        
        # Procesar comandos entrantes del Gateway
        async for command_bytes in call:
            command = proto_to_dict(command_bytes)
            await self._handle_command(command, outgoing, dict_to_proto)
    
    async def _handle_command(self, command: dict, outgoing: asyncio.Queue, dict_to_proto):
        """Procesa comandos del Gateway"""
        cmd_type = command.get("type")
        req_id = command.get("request_id", "")
        
        logger.debug(f"Received command: {cmd_type} [{req_id}]")
        
        if cmd_type == "register_response":
            resp = command.get("register_response", {})
            if resp.get("status") == "ok":
                session_id = resp.get("session_id")
                logger.info(f"Registered successfully. Session: {session_id}")
            else:
                logger.error(f"Registration failed: {resp.get('error')}")
        
        elif cmd_type == "get_flight_info":
            await self._handle_get_flight_info(req_id, command.get("get_flight_info", {}), outgoing, dict_to_proto)
        
        elif cmd_type == "do_get":
            await self._handle_do_get(req_id, command.get("do_get", {}), outgoing, dict_to_proto)
        
        elif cmd_type == "heartbeat":
            response = {
                "request_id": req_id,
                "type": "heartbeat",
                "heartbeat": {
                    "tenant_id": self.tenant_id,
                    "timestamp": command.get("heartbeat", {}).get("timestamp", 0)
                }
            }
            await outgoing.put(dict_to_proto(response))
    
    async def _handle_get_flight_info(self, request_id: str, get_info: dict, outgoing: asyncio.Queue, dict_to_proto):
        """Maneja solicitud de FlightInfo"""
        path = get_info.get("path", [])
        rows = get_info.get("rows")
        
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
        
        response = {
            "request_id": request_id,
            "type": "flight_info",
            "flight_info": {
                "status": "ok",
                "schema": base64.b64encode(data_loader.get_schema_bytes()).decode('ascii'),
                "total_records": data_loader.total_records,
                "total_bytes": total_bytes,
                "dataset": data_loader.current_dataset,
                "partitions": partitions
            }
        }
        
        logger.info(f"FlightInfo: {data_loader.current_dataset}, {data_loader.total_records:,} rows, {total_bytes/1024/1024:.2f} MB, {partitions} partitions")
        await outgoing.put(dict_to_proto(response))
    
    async def _handle_do_get(self, request_id: str, do_get: dict, outgoing: asyncio.Queue, dict_to_proto):
        """Maneja solicitud DoGet con streaming de Arrow IPC"""
        ticket = do_get.get("ticket", "")
        
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
        
        # Enviar stream_start
        start_msg = {
            "request_id": request_id,
            "type": "stream_status",
            "status": {
                "type": "stream_start",
                "schema": base64.b64encode(data_loader.get_schema_bytes()).decode('ascii'),
                "partition": partition,
                "total_partitions": total_partitions
            }
        }
        await outgoing.put(dict_to_proto(start_msg))
        
        # Enviar chunks de Arrow IPC
        total_bytes = 0
        try:
            all_batches = data_loader.get_record_batches()
            total_batches = len(all_batches)
            
            if total_partitions > 1 and total_batches > 1:
                batch_start = (total_batches * partition) // total_partitions
                batch_end = (total_batches * (partition + 1)) // total_partitions
                batches_to_send = all_batches[batch_start:batch_end]
            else:
                batches_to_send = all_batches
            
            for batch_bytes in batches_to_send:
                # Enviar como arrow_chunk
                chunk_msg = {
                    "request_id": request_id,
                    "type": "arrow_chunk",
                    "arrow_chunk": base64.b64encode(batch_bytes).decode('ascii')
                }
                await outgoing.put(dict_to_proto(chunk_msg))
                total_bytes += len(batch_bytes)
                await asyncio.sleep(0)
            
            # Enviar stream_end
            end_msg = {
                "request_id": request_id,
                "type": "stream_status",
                "status": {
                    "type": "stream_end",
                    "partition": partition,
                    "total_bytes": total_bytes
                }
            }
            await outgoing.put(dict_to_proto(end_msg))
            logger.info(f"Partition {partition} complete. {len(batches_to_send)} batches, {total_bytes/1024/1024:.2f} MB")
        
        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            error_msg = {
                "request_id": request_id,
                "type": "stream_status",
                "status": {"type": "stream_end", "error": str(e)}
            }
            await outgoing.put(dict_to_proto(error_msg))
    
    def stop(self):
        self.running = False
        if self.channel:
            asyncio.create_task(self.channel.close())


class ConnectorServiceStub:
    """Stub simplificado para el servicio gRPC con JSON codec"""
    
    def __init__(self, channel):
        self.channel = channel
    
    def Connect(self, request_iterator):
        """Llama al método Connect del servicio usando JSON codec"""
        # Crear multi-callable con codec JSON
        multi_callable = self.channel.stream_stream(
            '/connector.ConnectorService/Connect',
            request_serializer=lambda x: x,  # Ya son bytes JSON
            response_deserializer=lambda x: x,  # Retornar bytes JSON
        )
        # Llamar con metadata para especificar content-type json
        return multi_callable(
            request_iterator,
            metadata=[('content-type', 'application/grpc+json')]
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    connector = GRPCConnector()
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        connector.stop()
