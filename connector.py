"""
Lógica del Data Connector WebSocket con soporte para conexiones paralelas
"""
import asyncio
import json
import logging
import platform
import base64
from pathlib import Path

import yaml
import websockets
from websockets.exceptions import ConnectionClosed

from data_loader import data_loader

logger = logging.getLogger("Connector")

# Cargar configuración desde config.yml
CONFIG_PATH = Path(__file__).parent / "config.yml"

def load_config():
    """Carga configuración desde YAML"""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r') as f:
            return yaml.safe_load(f)
    return {}

config = load_config()

# Configuración con valores de config.yml o defaults
GATEWAY_URI = config.get('gateway', {}).get('uri', "ws://localhost:8080/ws/connect")
_tenant_cfg = config.get('tenant', {}).get('id', 'auto')
TENANT_ID = f"tenant_{platform.node().replace('-', '_').lower()}" if _tenant_cfg == 'auto' else _tenant_cfg
PARALLEL_CONNECTIONS = config.get('performance', {}).get('parallel_connections', 1)
PARALLEL_PARTITIONS = config.get('performance', {}).get('parallel_partitions', True)
MAX_CHUNK_SIZE = config.get('performance', {}).get('max_chunk_size', 65536)
RECONNECT_DELAY = config.get('performance', {}).get('reconnect_delay', 5)

class ArrowConnectorWorker:
    """Un worker que maneja una conexión WebSocket"""
    
    def __init__(self, worker_id: int, gateway_uri: str, tenant_id: str):
        self.worker_id = worker_id
        self.gateway_uri = gateway_uri
        self.tenant_id = tenant_id
        self.running = False
        self.websocket = None
        
    async def connect_and_run(self):
        """Loop principal de conexión y manejo de mensajes"""
        self.running = True
        logger.info(f"[Worker {self.worker_id}] Starting for tenant: {self.tenant_id}")
        
        while self.running:
            try:
                logger.info(f"[Worker {self.worker_id}] Connecting to {self.gateway_uri}...")
                async with websockets.connect(
                    self.gateway_uri,
                    max_size=None,
                    ping_interval=None,  # Disable client pings (server manages keepalive)
                    ping_timeout=None,   # Disable ping timeout completely
                    close_timeout=60     # Allow 60s for graceful close
                ) as websocket:
                    self.websocket = websocket
                    logger.info(f"[Worker {self.worker_id}] Connected!")
                    
                    if await self._register():
                        await self._message_loop()
                    
            except (ConnectionClosed, OSError) as e:
                logger.warning(f"[Worker {self.worker_id}] Connection lost: {e}. Retrying in {RECONNECT_DELAY}s...")
            except Exception as e:
                logger.error(f"[Worker {self.worker_id}] Unexpected error: {e}")
                
            if self.running:
                await asyncio.sleep(RECONNECT_DELAY)

    async def _register(self) -> bool:
        """Realiza el handshake de registro"""
        register_msg = {
            "action": "register",
            "tenant_id": self.tenant_id,
            "version": "1.0.0",
            "datasets": ["sales"]
        }
        await self.websocket.send(json.dumps(register_msg))
        
        response = await self.websocket.recv()
        data = json.loads(response)
        
        if data.get("status") == "ok":
            session_id = data.get('data', {}).get('session_id')
            logger.info(f"Registered successfully. Session: {session_id}")
            return True
        else:
            logger.error(f"Registration failed: {data}")
            return False

    async def _message_loop(self):
        """Escucha y procesa comandos del Gateway"""
        while True:
            try:
                msg_text = await self.websocket.recv()
                msg = json.loads(msg_text)
                
                # CLAVE: Procesar en PARALELO para máximo throughput
                # Cada request se maneja independientemente sin bloquear el loop de lectura
                asyncio.create_task(self._handle_message(msg))
                
            except ConnectionClosed:
                raise

    async def _handle_message(self, msg: dict):
        """Despacha la acción correspondiente"""
        action = msg.get("action")
        req_id = msg.get("request_id")
        
        logger.debug(f"Received action: {action} [{req_id}]")
        
        if action == "get_flight_info":
            await self._handle_get_flight_info(req_id, msg.get("descriptor"))
            
        elif action == "do_get":
            await self._handle_do_get(req_id, msg.get("ticket"))
            
        elif action == "heartbeat":
            await self.websocket.send(json.dumps({
                "action": "heartbeat",
                "tenant_id": self.tenant_id,
                "timestamp": msg.get("timestamp")
            }))

    async def _handle_get_flight_info(self, request_id: str, descriptor: dict):
        """Retorna metadata del dataset incluyendo número de particiones recomendadas"""
        # Extraer parámetros del descriptor
        dataset_name = None
        rows = None
        
        # El path puede traer [dataset_name] o puede venir como parámetro
        path = descriptor.get("path", [])
        if path:
            dataset_name = path[0] if isinstance(path[0], str) else path[0]
        
        # rows viene como parámetro adicional
        rows = descriptor.get("rows")
        
        # Decidir: si dataset_name parece un archivo conocido, cargarlo
        # De lo contrario, generar sintéticamente
        if dataset_name and dataset_name != "sales":
            # Intentar cargar desde archivo
            success = data_loader.load_from_file(dataset_name)
            if not success:
                # Fallback a generación si no existe
                logger.warning(f"Dataset '{dataset_name}' not found, generating synthetic data")
                data_loader.load_or_generate_dataset(rows=rows or 1_000_000)
        elif rows:
            # Generación sintética con rows específicos
            try:
                rows = int(rows)
                data_loader.load_or_generate_dataset(rows=rows)
            except ValueError:
                data_loader.load_or_generate_dataset()
        else:
            # Mantener dataset actual o generar default
            if data_loader.total_records == 0:
                data_loader.load_or_generate_dataset()
        
        schema_bytes = data_loader.get_schema_bytes()
        schema_b64 = base64.b64encode(schema_bytes).decode('ascii')
        
        # Calcular número óptimo de particiones basado en tamaño
        # Solo si parallel_partitions está habilitado en config
        total_bytes = data_loader.total_bytes
        if PARALLEL_PARTITIONS:
            if total_bytes < 10 * 1024 * 1024:        # < 10MB
                partitions = 1
            elif total_bytes < 50 * 1024 * 1024:      # 10-50MB
                partitions = 2
            elif total_bytes < 100 * 1024 * 1024:     # 50-100MB
                partitions = 4
            else:                                      # > 100MB
                partitions = 8
        else:
            partitions = 1  # Forzar 1 partición para pruebas
        
        response = {
            "request_id": request_id,
            "status": "ok",
            "data": {
                "schema": schema_b64,
                "total_records": data_loader.total_records,
                "total_bytes": data_loader.total_bytes,
                "dataset": data_loader.current_dataset,
                "partitions": partitions  # Número de particiones para paralelismo
            }
        }
        logger.info(f"FlightInfo: {data_loader.current_dataset}, {data_loader.total_records:,} rows, {total_bytes/1024/1024:.2f} MB, {partitions} partitions")
        await self.websocket.send(json.dumps(response))

    async def _handle_do_get(self, request_id: str, ticket: str):
        """Retorna stream de datos usando protocolo binario, soportando particiones"""
        
        # Decodificar ticket para obtener info de partición
        # El ticket puede ser:
        # 1. Un string base64 con JSON (partición info)
        # 2. Un string plano (nombre del dataset)
        partition = 0
        total_partitions = 1
        
        if ticket:
            try:
                import base64
                # Intentar decodificar como base64 + JSON
                ticket_bytes = base64.b64decode(ticket)
                ticket_data = json.loads(ticket_bytes.decode('utf-8'))
                partition = ticket_data.get("partition", 0)
                total_partitions = ticket_data.get("total_partitions", 1)
                logger.info(f"Starting data transfer for {request_id} - Partition {partition}/{total_partitions}")
            except Exception:
                # El ticket es probablemente solo el nombre del dataset - esto es normal
                logger.debug(f"Ticket is plain dataset name: {ticket[:50] if ticket else 'empty'}...")
        
        # 1. Enviar metadata de inicio (JSON)
        start_msg = {
            "request_id": request_id, 
            "status": "ok", 
            "type": "stream_start",
            "schema": base64.b64encode(data_loader.get_schema_bytes()).decode('ascii'),
            "partition": partition,
            "total_partitions": total_partitions
        }
        await self.websocket.send(json.dumps(start_msg))
        
        # 2. Obtener batches y calcular slice para esta partición
        total_bytes = 0
        
        try:
            all_batches = data_loader.get_record_batches()
            total_batches = len(all_batches)
            
            # Calcular qué batches enviar para esta partición
            if total_partitions > 1 and total_batches > 1:
                batch_start = (total_batches * partition) // total_partitions
                batch_end = (total_batches * (partition + 1)) // total_partitions
                batches_to_send = all_batches[batch_start:batch_end]
                logger.debug(f"Partition {partition}: sending batches {batch_start} to {batch_end} of {total_batches}")
            else:
                # Si solo hay 1 partición o 1 batch, enviar todo
                batches_to_send = all_batches
            
            # 3. Enviar los batches de esta partición
            for batch_bytes in batches_to_send:
                await self.websocket.send(batch_bytes)
                total_bytes += len(batch_bytes)
                await asyncio.sleep(0)  # Yield para no bloquear

            # 4. Enviar Fin de Stream (JSON)
            end_msg = {
                "request_id": request_id,
                "status": "ok",
                "type": "stream_end",
                "partition": partition,
                "total_bytes": total_bytes
            }
            await self.websocket.send(json.dumps(end_msg))
            logger.info(f"Partition {partition} complete. {len(batches_to_send)} batches, {total_bytes/1024/1024:.2f} MB")

        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            err_msg = {"request_id": request_id, "status": "error", "error": str(e)}
            await self.websocket.send(json.dumps(err_msg))

    def stop(self):
        self.running = False


class ArrowConnector:
    """Orquestador que lanza N workers en paralelo"""
    
    def __init__(self, gateway_uri: str = None, tenant_id: str = None, parallel_connections: int = None):
        self.gateway_uri = gateway_uri or GATEWAY_URI
        self.tenant_id = tenant_id or TENANT_ID
        self.parallel_connections = parallel_connections or PARALLEL_CONNECTIONS
        self.workers = []
        
        # Cargar datos al inicio (compartido entre workers)
        data_loader.load_or_generate_dataset()
        
        logger.info(f"ArrowConnector initialized:")
        logger.info(f"  Gateway URI: {self.gateway_uri}")
        logger.info(f"  Tenant ID: {self.tenant_id}")
        logger.info(f"  Parallel Connections: {self.parallel_connections}")
    
    async def run(self):
        """Inicia N workers en paralelo"""
        self.workers = [
            ArrowConnectorWorker(
                worker_id=i,
                gateway_uri=self.gateway_uri,
                tenant_id=self.tenant_id
            )
            for i in range(self.parallel_connections)
        ]
        
        # Ejecutar todos los workers concurrentemente
        await asyncio.gather(*[w.connect_and_run() for w in self.workers])
    
    def stop(self):
        """Detiene todos los workers"""
        for w in self.workers:
            w.stop()
