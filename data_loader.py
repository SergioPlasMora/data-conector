"""
Generador y cargador de datasets para el Data Connector
Soporta: CSV, Parquet, Feather/Arrow IPC, JSON, DuckDB, y generación sintética.
"""
import duckdb
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pyarrow.feather as feather
import pandas as pd
import numpy as np
import time
import logging
import os
from pathlib import Path

# Compresión ZSTD para transferencia
try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

logger = logging.getLogger(__name__)

# Directorio donde se almacenan los datasets
DATASETS_DIR = Path(__file__).parent / "datasets"

class DataLoader:
    """Gestiona la carga de datasets desde archivos o generación sintética"""
    
    def __init__(self):
        self._table = None
        self._current_dataset = None
        
    def list_available_datasets(self) -> list[str]:
        """Lista los datasets disponibles en el directorio"""
        if not DATASETS_DIR.exists():
            return []
        extensions = {'.csv', '.parquet', '.pq', '.feather', '.arrow', '.json', '.duckdb'}
        return [f.stem for f in DATASETS_DIR.iterdir() 
                if f.suffix.lower() in extensions]
    
    def load_from_file(self, dataset_name: str) -> bool:
        """Carga un dataset desde archivo. Retorna True si tuvo éxito."""
        
        # Normalizar: remover extensión si viene incluida
        known_extensions = ['.duckdb', '.parquet', '.pq', '.csv', '.feather', '.arrow', '.json']
        normalized_name = dataset_name
        for ext in known_extensions:
            if dataset_name.lower().endswith(ext):
                normalized_name = dataset_name[:-len(ext)]
                # Si el usuario pidió específicamente este formato, priorizarlo
                preferred_ext = ext
                break
        else:
            preferred_ext = None
        
        # Si ya está cargado, no recargar
        if self._current_dataset == normalized_name and self._table is not None:
            logger.info(f"Dataset '{normalized_name}' already loaded (cached).")
            return True
            
        # Buscar el archivo - priorizar el formato solicitado
        extensions = ['.duckdb', '.parquet', '.pq', '.csv', '.feather', '.arrow', '.json']
        if preferred_ext:
            extensions = [preferred_ext] + [e for e in extensions if e != preferred_ext]
            
        file_path = None
        
        for ext in extensions:
            candidate = DATASETS_DIR / f"{normalized_name}{ext}"
            if candidate.exists():
                file_path = candidate
                break
        
        if not file_path:
            logger.warning(f"Dataset '{normalized_name}' not found in {DATASETS_DIR}")
            return False
            
        logger.info(f"Loading dataset from {file_path}...")
        start_time = time.time()
        
        try:
            ext = file_path.suffix.lower()
            
            if ext in ['.parquet', '.pq']:
                self._table = pq.read_table(file_path)
            elif ext == '.csv':
                self._table = pcsv.read_csv(file_path)
            elif ext in ['.feather', '.arrow']:
                self._table = feather.read_table(file_path)
            elif ext == '.json':
                # JSON requiere pandas como intermediario
                df = pd.read_json(file_path)
                self._table = pa.Table.from_pandas(df)
            elif ext == '.duckdb':
                # DuckDB: conectar y leer la tabla 'data' como Arrow
                con = duckdb.connect(str(file_path), read_only=True)
                try:
                    self._table = con.execute("SELECT * FROM data").fetch_arrow_table()
                finally:
                    con.close()
            else:
                logger.error(f"Unsupported format: {ext}")
                return False
                
            self._current_dataset = normalized_name
            elapsed = time.time() - start_time
            logger.info(f"Dataset loaded in {elapsed:.2f}s. "
                       f"Rows: {self._table.num_rows:,}, "
                       f"Size: {self._table.nbytes / 1024 / 1024:.2f} MB")
            return True
            
        except Exception as e:
            logger.error(f"Error loading dataset: {e}")
            return False
    
    def load_or_generate_dataset(self, rows: int = 1_000_000):
        """Genera un dataset sintético de ventas (fallback)"""
        # Si ya existe y tiene las mismas filas, no regenerar
        if self._table is not None and self._table.num_rows == rows and self._current_dataset == "__synthetic__":
            return

        logger.info(f"Generating synthetic dataset with {rows:,} rows...")
        start_time = time.time()
        
        df = pd.DataFrame({
            'id': np.arange(rows, dtype=np.int64),
            'product_id': np.random.randint(1, 1000, size=rows),
            'store_id': np.random.choice(['NYC-01', 'LON-02', 'TOK-03', 'PAR-04'], size=rows),
            'date': pd.date_range(start='2024-01-01', periods=rows, freq='s').astype(str),
            'amount': np.random.uniform(10.5, 999.9, size=rows).astype(np.float64),
            'status': np.random.choice(['completed', 'pending', 'refunded'], size=rows)
        })
        
        self._table = pa.Table.from_pandas(df)
        self._current_dataset = "__synthetic__"
        
        elapsed = time.time() - start_time
        logger.info(f"Dataset generated in {elapsed:.2f}s. Size: {self._table.nbytes / 1024 / 1024:.2f} MB")

    def get_schema_bytes(self) -> bytes:
        """Retorna el esquema serializado en bytes"""
        if self._table is None:
            self.load_or_generate_dataset()
        return self._table.schema.serialize().to_pybytes()
    
    def get_schema(self) -> pa.Schema:
        """Retorna el esquema PyArrow"""
        if self._table is None:
            self.load_or_generate_dataset()
        return self._table.schema

    def get_record_batches(self, max_chunksize: int = 65536, as_bytes: bool = True, 
                           compression: str = None, transfer_compression: str = None) -> list:
        """
        Retorna los batches del dataset.
        
        Args:
            max_chunksize: Máximo número de filas por batch
            as_bytes: Si True, retorna bytes serializados. Si False, retorna RecordBatch objects.
            compression: DEPRECATED - Compresión Arrow IPC interna ('lz4', 'zstd'), NO soportada por Arrow JS
            transfer_compression: Compresión externa de bytes para transferencia ('zstd' o None)
                                  Esta compresión se aplica DESPUÉS de serializar Arrow IPC,
                                  permitiendo descompresión con fzstd en browser.
        
        Returns:
            Lista de bytes o RecordBatch según as_bytes
        """
        if self._table is None:
            self.load_or_generate_dataset()
        
        batches = self._table.to_batches(max_chunksize=max_chunksize)
        
        if not as_bytes:
            return batches
        
        # NO usar compresión Arrow IPC interna - Arrow JS no la soporta
        ipc_options = pa.ipc.IpcWriteOptions()
            
        batches_bytes = []
        total_uncompressed = 0
        total_arrow_bytes = 0
        total_compressed = 0
        
        # Preparar compresor ZSTD si está habilitado
        zstd_compressor = None
        if transfer_compression == 'zstd' and ZSTD_AVAILABLE:
            zstd_compressor = zstd.ZstdCompressor(level=3)  # Nivel 1 = balance velocidad/ratio
            logger.info("Using ZSTD compression for transfer (level 3)")
        elif transfer_compression == 'zstd' and not ZSTD_AVAILABLE:
            logger.warning("ZSTD requested but zstandard not installed. Sending uncompressed.")
        
        for batch in batches:
            total_uncompressed += batch.nbytes
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, self._table.schema, options=ipc_options) as writer:
                writer.write_batch(batch)
            batch_bytes = sink.getvalue().to_pybytes()
            total_arrow_bytes += len(batch_bytes)
            
            # Aplicar compresión ZSTD externa si está habilitada
            if zstd_compressor:
                batch_bytes = zstd_compressor.compress(batch_bytes)
                total_compressed += len(batch_bytes)
            else:
                total_compressed += len(batch_bytes)
            
            batches_bytes.append(batch_bytes)
        
        # Log de métricas de compresión
        if zstd_compressor:
            ratio = (1 - total_compressed / total_arrow_bytes) * 100 if total_arrow_bytes > 0 else 0
            logger.info(f"Transfer compression: {total_arrow_bytes / 1024 / 1024:.2f} MB → {total_compressed / 1024 / 1024:.2f} MB ({ratio:.1f}% reduction)")
        else:
            logger.info(f"No transfer compression: {total_arrow_bytes / 1024 / 1024:.2f} MB")
            
        return batches_bytes

    @property
    def total_records(self) -> int:
        return self._table.num_rows if self._table else 0
        
    @property
    def total_bytes(self) -> int:
        return self._table.nbytes if self._table else 0
    
    @property
    def current_dataset(self) -> str:
        return self._current_dataset or "None"

# Singleton
data_loader = DataLoader()
