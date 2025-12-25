"""
Metrics Reporter for Data Connector.

Sends metrics to luzzi-core-im via HTTP POST on a separate observability plane,
keeping the gRPC data plane clean for Arrow IPC streaming.

Usage:
    from metrics_reporter import MetricsReporter
    
    reporter = MetricsReporter(api_url="https://api.luzzi.com", tenant_id="...")
    asyncio.create_task(reporter.start(interval=30))
    
    # During data streaming
    reporter.record_bytes_sent(len(chunk))
    reporter.record_records_sent(batch.num_rows)
    reporter.record_query_processed()
"""
import aiohttp
import asyncio
import time
import logging
import platform
import sys
import socket
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger("MetricsReporter")


class MetricsReporter:
    """Async metrics reporter that sends data to the observability plane."""
    
    def __init__(self, api_url: str, tenant_id: str, version: str = "1.0.0", host_header: str = None):
        """
        Initialize the metrics reporter.
        
        Args:
            api_url: Base URL of luzzi-core-im (e.g., "https://api.luzzi.com")
            tenant_id: Unique identifier for this tenant/connector
            version: Connector version for reporting
            host_header: Optional Host header for reverse proxy routing (e.g., "api.localhost")
        """
        self.api_url = f"{api_url}/api/metrics/agent/{tenant_id}"
        self.tenant_id = tenant_id
        self.version = version
        self.host_header = host_header
        self.start_time = time.time()
        
        # Counters (monotonically increasing)
        self.bytes_sent = 0
        self.records_sent = 0
        self.queries_processed = 0
        self.errors = 0
        
        # Query timing
        self._query_durations: list[float] = []  # Last N query durations in ms
        self._last_query_timestamp: float | None = None
        
        # State
        self.connected = True
        self._running = False
        self._last_send_success = True
        
        # System info (collected once at startup)
        self._hostname = socket.gethostname()
        self._os_info = f"{platform.system()} {platform.release()}"
        self._python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        
        # Certificate expiry (optional)
        self._cert_expiry_days: int | None = None
        self._check_certificate_expiry()
        
        logger.info(f"MetricsReporter initialized for tenant {tenant_id}")
        logger.info(f"  Metrics URL: {self.api_url}")
        if host_header:
            logger.info(f"  Host Header: {host_header}")
    
    def _check_certificate_expiry(self):
        """Check client certificate expiry if exists."""
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            
            cert_path = Path(__file__).parent / "certs" / "client.crt"
            if cert_path.exists():
                with open(cert_path, "rb") as f:
                    cert_data = f.read()
                    cert = x509.load_pem_x509_certificate(cert_data, default_backend())
                    expiry = cert.not_valid_after_utc
                    now = datetime.utcnow().replace(tzinfo=expiry.tzinfo)
                    days_left = (expiry - now).days
                    self._cert_expiry_days = max(0, days_left)
                    logger.info(f"  Certificate expires in {self._cert_expiry_days} days")
        except ImportError:
            logger.debug("cryptography not installed, skipping cert expiry check")
        except Exception as e:
            logger.warning(f"Could not check certificate expiry: {e}")
    
    # =========================================================================
    # Recording methods (called during data streaming)
    # =========================================================================
    
    def record_bytes_sent(self, count: int):
        """Record bytes sent through the data plane."""
        self.bytes_sent += count
    
    def record_records_sent(self, count: int):
        """Record number of records/rows sent."""
        self.records_sent += count
    
    def record_query_processed(self, duration_ms: float = None):
        """Record that a query was processed."""
        self.queries_processed += 1
        self._last_query_timestamp = time.time()
        if duration_ms is not None:
            self._query_durations.append(duration_ms)
            # Keep only last 100 durations
            if len(self._query_durations) > 100:
                self._query_durations = self._query_durations[-100:]
    
    def record_error(self):
        """Record an error occurrence."""
        self.errors += 1
    
    def set_connected(self, status: bool):
        """Update connection status."""
        self.connected = status
    
    # =========================================================================
    # Async loop
    # =========================================================================
    
    async def start(self, interval: int = 30):
        """
        Start the metrics reporting loop.
        
        Args:
            interval: Seconds between metric reports (default: 30)
        """
        self._running = True
        logger.info(f"Starting metrics reporter (interval: {interval}s)")
        
        while self._running:
            await self._send_metrics()
            await asyncio.sleep(interval)
    
    def stop(self):
        """Stop the metrics reporting loop."""
        self._running = False
        logger.info("Metrics reporter stopped")
    
    async def _send_metrics(self):
        """Send current metrics to the observability plane."""
        uptime = int(time.time() - self.start_time)
        
        # Calculate average query duration
        avg_query_duration = None
        if self._query_durations:
            avg_query_duration = sum(self._query_durations) / len(self._query_durations)
        
        metrics = {
            "agent_type": "connector",
            "version": self.version,
            "uptime_seconds": uptime,
            "connected": self.connected,
            "errors_total": self.errors,
            "bytes_sent_total": self.bytes_sent,
            "records_sent_total": self.records_sent,
            "queries_processed": self.queries_processed,
            # System info
            "hostname": self._hostname,
            "os_info": self._os_info,
            "python_version": self._python_version,
            # Query timing
            "last_query_timestamp": self._last_query_timestamp,
            "avg_query_duration_ms": avg_query_duration,
        }
        
        # Add certificate expiry if available
        if self._cert_expiry_days is not None:
            metrics["certificate_expiry_days"] = self._cert_expiry_days
        
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            headers = {}
            if self.host_header:
                headers["Host"] = self.host_header
            
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.post(self.api_url, json=metrics) as resp:
                    if resp.status == 200:
                        if not self._last_send_success:
                            logger.info("Metrics reporting resumed")
                        self._last_send_success = True
                    else:
                        body = await resp.text()
                        logger.warning(f"Metrics send failed: {resp.status} - {body}")
                        self._last_send_success = False
        except asyncio.TimeoutError:
            logger.warning("Metrics send timeout")
            self._last_send_success = False
        except aiohttp.ClientError as e:
            if self._last_send_success:  # Only log first failure
                logger.warning(f"Metrics send error: {e}")
            self._last_send_success = False
        except Exception as e:
            logger.error(f"Unexpected metrics error: {e}")
            self._last_send_success = False


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        reporter = MetricsReporter(
            api_url="http://localhost:8000",
            tenant_id="test-tenant-123"
        )
        
        # Simulate some activity
        reporter.record_bytes_sent(1024 * 1024)
        reporter.record_records_sent(10000)
        reporter.record_query_processed()
        
        # Send once and exit
        await reporter._send_metrics()
        print("Test complete!")
    
    asyncio.run(test())
