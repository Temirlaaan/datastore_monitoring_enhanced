import os
import time
import requests
import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
import aiohttp
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging first
def setup_logging():
    """Setup structured logging"""
    log_level = logging.DEBUG if os.getenv('DEBUG', '').lower() == 'true' else logging.INFO

    # Create logs directory
    Path('logs').mkdir(exist_ok=True)

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'logs/datastore_monitor_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

@dataclass
class DatastoreInfo:
    """Datastore information structure"""
    id: str
    name: str
    total_mb: int
    used_mb: int
    provisioned_mb: int
    datastore_type: str
    cloud_name: str = ""  # Which cloud this datastore belongs to
    container_name: str = ""  # Storage container name

    @property
    def usage_percent(self) -> float:
        return (self.used_mb / self.total_mb * 100) if self.total_mb > 0 else 0

    @property
    def provisioned_percent(self) -> float:
        return (self.provisioned_mb / self.total_mb * 100) if self.total_mb > 0 else 0

@dataclass
class StorageContainerConfig:
    """Configuration for a single storage container"""
    id: str
    name: str  # Human-readable name for alerts

@dataclass
class CloudConfig:
    """Configuration for a single VCD cloud"""
    name: str  # Display name (e.g., "vcd", "vcd02")
    url: str
    api_token: str
    api_version: str
    storage_containers: List[StorageContainerConfig]

@dataclass
class AlertConfig:
    """Alert configuration"""
    critical_threshold: int = 90
    warning_threshold: int = 80
    check_interval: int = 3600  # 1 hour
    daily_report_hour: int = 9
    alert_cooldown: int = 7200  # 2 hours

class TokenManager:
    """Handles VCD API token management"""

    def __init__(self, base_url: str, refresh_token: str, cloud_name: str = ""):
        self.base_url = base_url
        self.refresh_token = refresh_token
        self.cloud_name = cloud_name
        self._token = None
        self._expires_at = 0
        self._lock = asyncio.Lock()

    async def get_token(self, force_refresh: bool = False) -> str:
        """Get valid bearer token"""
        async with self._lock:
            now = time.time()

            # Check if we need to refresh (5 minute buffer)
            if (self._token is None or
                now >= self._expires_at - 300 or
                force_refresh):

                await self._refresh_token()

            return self._token

    async def _refresh_token(self):
        """Refresh the bearer token - using requests style that worked before"""
        token_url = f"{self.base_url}/oauth/provider/token"

        for attempt in range(3):
            try:
                logger.info(f"[{self.cloud_name}] Refreshing token (attempt {attempt + 1})")

                # Use the same approach as in original working code
                import requests

                # Create a session like in original code
                session = requests.Session()
                session.verify = False  # Match original SSL settings

                response = session.post(
                    token_url,
                    params={
                        'grant_type': 'refresh_token',
                        'refresh_token': self.refresh_token
                    },
                    headers={'Accept': 'application/json'},
                    timeout=(10, 20)
                )

                logger.debug(f"[{self.cloud_name}] Token request status: {response.status_code}")

                if response.status_code == 401:
                    logger.error(f"[{self.cloud_name}] Token refresh failed - 401 Unauthorized: {response.text}")
                    raise RuntimeError(f"[{self.cloud_name}] Refresh token is invalid or expired")

                response.raise_for_status()
                data = response.json()

                if 'access_token' not in data:
                    logger.error(f"[{self.cloud_name}] No access_token in response: {data}")
                    raise ValueError("No access_token in response")

                self._token = data['access_token']
                expires_in = int(data.get('expires_in', 3600))
                self._expires_at = time.time() + expires_in

                logger.info(f"[{self.cloud_name}] Token refreshed successfully (expires in {expires_in}s)")
                return

            except requests.exceptions.RequestException as e:
                logger.warning(f"[{self.cloud_name}] Token refresh attempt {attempt + 1} failed: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    logger.error(f"Response body: {e.response.text}")
                if attempt == 2:  # Last attempt
                    raise
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"[{self.cloud_name}] Unexpected error refreshing token: {e}")
                if attempt == 2:
                    raise
                await asyncio.sleep(5)

class VCDDatastoreClient:
    """VCD Datastore API client with multi-container support"""

    def __init__(self, cloud_config: CloudConfig, token_manager: TokenManager):
        self.cloud_config = cloud_config
        self.token_manager = token_manager

    async def get_all_datastores(self) -> List[DatastoreInfo]:
        """Fetch datastore information from all storage containers"""
        all_datastores = []

        for container in self.cloud_config.storage_containers:
            datastores = await self._get_datastores_from_container(container)
            all_datastores.extend(datastores)

        return all_datastores

    async def _get_datastores_from_container(self, container: StorageContainerConfig) -> List[DatastoreInfo]:
        """Fetch datastores from a specific storage container"""
        # First try to get datastores inside the container
        datastores = await self._fetch_container_datastores(container)

        # If no datastores found, the container itself might be the datastore
        if not datastores:
            logger.debug(f"[{self.cloud_config.name}] No nested datastores in {container.name}, fetching container info")
            container_info = await self._fetch_container_as_datastore(container)
            if container_info:
                datastores = [container_info]

        return datastores

    async def _fetch_container_datastores(self, container: StorageContainerConfig) -> List[DatastoreInfo]:
        """Fetch datastores inside a storage container"""
        url = f"{self.cloud_config.url}/cloudapi/1.0.0/storageContainers/{container.id}/datastores"

        connector = aiohttp.TCPConnector(ssl=True, limit=20)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for attempt in range(3):
                try:
                    force_refresh = attempt > 0
                    token = await self.token_manager.get_token(force_refresh=force_refresh)

                    headers = {
                        'Accept': f'application/json;version={self.cloud_config.api_version}',
                        'Authorization': f'Bearer {token}'
                    }

                    async with session.get(url, headers=headers) as response:
                        if response.status == 401 and attempt < 2:
                            logger.warning(f"[{self.cloud_config.name}] 401 error, will retry with fresh token")
                            continue

                        if response.status in (400, 404):
                            # Container doesn't have nested datastores endpoint
                            # 400 = Bad Request (endpoint not valid for this container type)
                            # 404 = Not Found
                            logger.debug(f"[{self.cloud_config.name}] No datastores endpoint for {container.name} (status {response.status})")
                            return []

                        response.raise_for_status()
                        data = await response.json()

                        datastores = []
                        for ds_data in data.get('values', []):
                            datastore = DatastoreInfo(
                                id=ds_data['id'],
                                name=ds_data['name'],
                                total_mb=ds_data.get('totalStorageMb', 0),
                                used_mb=ds_data.get('usedStorageMb', 0),
                                provisioned_mb=ds_data.get('provisionedStorageMb', 0),
                                datastore_type=ds_data.get('datastoreType', 'Unknown'),
                                cloud_name=self.cloud_config.name,
                                container_name=container.name
                            )
                            datastores.append(datastore)

                        if datastores:
                            logger.debug(f"[{self.cloud_config.name}] Retrieved {len(datastores)} datastores from {container.name}")
                        return datastores

                except aiohttp.ClientError as e:
                    logger.error(f"[{self.cloud_config.name}] API request failed for {container.name} (attempt {attempt + 1}): {e}")
                    if attempt == 2:
                        return []
                    await asyncio.sleep(5)

        return []

    async def _fetch_container_as_datastore(self, container: StorageContainerConfig) -> Optional[DatastoreInfo]:
        """Fetch storage container info when it acts as the datastore itself"""
        url = f"{self.cloud_config.url}/cloudapi/1.0.0/storageContainers/{container.id}"

        connector = aiohttp.TCPConnector(ssl=True, limit=20)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for attempt in range(3):
                try:
                    force_refresh = attempt > 0
                    token = await self.token_manager.get_token(force_refresh=force_refresh)

                    headers = {
                        'Accept': f'application/json;version={self.cloud_config.api_version}',
                        'Authorization': f'Bearer {token}'
                    }

                    async with session.get(url, headers=headers) as response:
                        if response.status == 401 and attempt < 2:
                            logger.warning(f"[{self.cloud_config.name}] 401 error, will retry with fresh token")
                            continue

                        response.raise_for_status()
                        data = await response.json()

                        # Storage container has similar fields to datastore
                        datastore = DatastoreInfo(
                            id=data.get('id', container.id),
                            name=data.get('name', container.name),
                            total_mb=data.get('totalStorageMb', 0),
                            used_mb=data.get('usedStorageMb', 0),
                            provisioned_mb=data.get('provisionedStorageMb', 0),
                            datastore_type=data.get('storageContainerType', 'StorageContainer'),
                            cloud_name=self.cloud_config.name,
                            container_name=container.name
                        )

                        logger.debug(f"[{self.cloud_config.name}] Retrieved container {container.name} as datastore")
                        return datastore

                except aiohttp.ClientError as e:
                    logger.error(f"[{self.cloud_config.name}] Failed to fetch container {container.name} (attempt {attempt + 1}): {e}")
                    if attempt == 2:
                        return None
                    await asyncio.sleep(5)

        return None

class TelegramNotifier:
    """Handles Telegram notifications"""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    async def send_message(self, text: str, parse_mode: str = "Markdown") -> bool:
        """Send message to Telegram"""
        url = f"{self.base_url}/sendMessage"

        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': parse_mode
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.debug("Telegram message sent successfully")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Telegram API error: {response.status} - {error_text}")
                        return False

        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

class MultiCloudDatastoreMonitor:
    """Main monitoring class with multi-cloud support"""

    def __init__(self, clouds: List[CloudConfig], alert_config: AlertConfig, notifier: TelegramNotifier):
        self.clouds = clouds
        self.config = alert_config
        self.notifier = notifier
        self.alert_cache: Dict[str, Dict] = {}  # In-memory cache for alerts
        self.last_daily_report = 0

        # Initialize VCD clients for each cloud
        self.vcd_clients: List[VCDDatastoreClient] = []
        for cloud in clouds:
            token_manager = TokenManager(
                base_url=cloud.url,
                refresh_token=cloud.api_token,
                cloud_name=cloud.name
            )
            client = VCDDatastoreClient(cloud, token_manager)
            self.vcd_clients.append(client)
            logger.info(f"Initialized client for cloud: {cloud.name} ({cloud.url})")

    def _format_storage_size(self, mb: int) -> str:
        """Convert MB to human readable format"""
        if mb >= 1_048_576:  # >= 1 TB
            return f"{mb / 1_048_576:.2f} TB"
        elif mb >= 1024:   # >= 1 GB
            return f"{mb / 1024:.2f} GB"
        else:
            return f"{mb:.2f} MB"

    def _get_alert_level(self, usage_percent: float) -> tuple[Optional[str], Optional[str]]:
        """Determine alert level and emoji"""
        if usage_percent >= self.config.critical_threshold:
            return "CRITICAL", "🔴"
        elif usage_percent >= self.config.warning_threshold:
            return "WARNING", "🟡"
        else:
            return None, None

    async def _should_send_alert(self, datastore: DatastoreInfo, alert_level: str) -> bool:
        """Check if we should send an alert (respects cooldown)"""
        cache_key = f"{datastore.cloud_name}_{datastore.id}"
        now = time.time()

        if cache_key in self.alert_cache:
            last_alert = self.alert_cache[cache_key]
            # Don't send if same level and within cooldown
            if (last_alert['level'] == alert_level and
                now - last_alert['timestamp'] < self.config.alert_cooldown):
                return False

        return True

    async def get_all_datastores(self) -> List[DatastoreInfo]:
        """Fetch datastores from all clouds in parallel"""
        tasks = [client.get_all_datastores() for client in self.vcd_clients]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_datastores = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching from cloud {self.clouds[i].name}: {result}")
            else:
                all_datastores.extend(result)

        return all_datastores

    async def check_datastores(self):
        """Main monitoring check"""
        try:
            datastores = await self.get_all_datastores()
            if not datastores:
                logger.warning("No datastores found across all clouds")
                return

            alerts_sent = 0

            for ds in datastores:
                cache_key = f"{ds.cloud_name}_{ds.id}"
                alert_level, emoji = self._get_alert_level(ds.usage_percent)

                # Handle alerts
                if alert_level and await self._should_send_alert(ds, alert_level):
                    await self._send_alert(ds, alert_level, emoji)
                    self.alert_cache[cache_key] = {
                        'level': alert_level,
                        'usage': ds.usage_percent,
                        'timestamp': time.time()
                    }
                    alerts_sent += 1

                # Handle recovery
                elif not alert_level and cache_key in self.alert_cache:
                    await self._send_recovery_message(ds)
                    del self.alert_cache[cache_key]
                    alerts_sent += 1

            # Log summary per cloud
            cloud_counts = {}
            for ds in datastores:
                cloud_counts[ds.cloud_name] = cloud_counts.get(ds.cloud_name, 0) + 1

            summary = ", ".join([f"{name}: {count}" for name, count in cloud_counts.items()])
            logger.info(f"Checked {len(datastores)} datastores ({summary}), sent {alerts_sent} alerts")

        except Exception as e:
            logger.error(f"Error in datastore check: {e}")

    async def _send_alert(self, ds: DatastoreInfo, alert_level: str, emoji: str):
        """Send alert message"""
        message = (
            f"{emoji} *{alert_level}*: Datastore filling up!\n\n"
            f"☁️ *Cloud:* {ds.cloud_name}\n"
            f"📁 *Datastore:* {ds.name}\n"
            f"📦 *Container:* {ds.container_name}\n"
            f"📊 *Used:* {self._format_storage_size(ds.used_mb)} / "
            f"{self._format_storage_size(ds.total_mb)} ({ds.usage_percent:.1f}%)\n"
            f"📈 *Provisioned:* {self._format_storage_size(ds.provisioned_mb)} "
            f"({ds.provisioned_percent:.1f}%)\n"
            f"💾 *Type:* {ds.datastore_type}\n"
            f"🆔 *ID:* `{ds.id}`\n"
            f"🕐 *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        await self.notifier.send_message(message)

    async def _send_recovery_message(self, ds: DatastoreInfo):
        """Send recovery message"""
        message = (
            f"✅ *RECOVERED*: Datastore back to normal\n\n"
            f"☁️ *Cloud:* {ds.cloud_name}\n"
            f"📁 *Datastore:* {ds.name}\n"
            f"📦 *Container:* {ds.container_name}\n"
            f"📊 *Used:* {self._format_storage_size(ds.used_mb)} / "
            f"{self._format_storage_size(ds.total_mb)} ({ds.usage_percent:.1f}%)\n"
            f"🕐 *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        await self.notifier.send_message(message)

    async def send_daily_report(self):
        """Send daily status report grouped by cloud"""
        try:
            datastores = await self.get_all_datastores()
            if not datastores:
                return

            # Group by cloud
            by_cloud: Dict[str, List[DatastoreInfo]] = {}
            for ds in datastores:
                if ds.cloud_name not in by_cloud:
                    by_cloud[ds.cloud_name] = []
                by_cloud[ds.cloud_name].append(ds)

            report_lines = ["📋 *Daily Datastore Report*\n"]

            for cloud_name in sorted(by_cloud.keys()):
                cloud_datastores = by_cloud[cloud_name]
                report_lines.append(f"\n☁️ *{cloud_name}* ({len(cloud_datastores)} datastores):")

                # Sort by usage percent descending
                cloud_datastores.sort(key=lambda x: x.usage_percent, reverse=True)

                for ds in cloud_datastores:
                    _, emoji = self._get_alert_level(ds.usage_percent)
                    status_emoji = emoji if emoji else "🟢"

                    total_tb = ds.total_mb / 1_048_576
                    used_tb = ds.used_mb / 1_048_576

                    report_lines.append(
                        f"  {status_emoji} {ds.name}: {used_tb:.1f}/{total_tb:.1f} TB "
                        f"({ds.usage_percent:.1f}%)"
                    )

            report_lines.append(f"\n🕐 *Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            await self.notifier.send_message("\n".join(report_lines))
            logger.info("Daily report sent")

        except Exception as e:
            logger.error(f"Failed to send daily report: {e}")

    async def run(self):
        """Main monitoring loop"""
        cloud_names = [c.name for c in self.clouds]
        logger.info(f"Starting multi-cloud datastore monitor...")
        logger.info(f"Monitoring clouds: {', '.join(cloud_names)}")
        logger.info(f"Config: Critical={self.config.critical_threshold}%, "
                   f"Warning={self.config.warning_threshold}%, "
                   f"Interval={self.config.check_interval}s")

        # Send initial status report on startup
        logger.info("Sending initial status report...")
        await self.send_daily_report()
        self.last_daily_report = datetime.now().day

        while True:
            try:
                # Check datastores
                await self.check_datastores()

                # Check if we need to send daily report
                current_hour = datetime.now().hour
                current_day = datetime.now().day

                if (current_hour == self.config.daily_report_hour and
                    current_day != self.last_daily_report):
                    await self.send_daily_report()
                    self.last_daily_report = current_day

            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            await asyncio.sleep(self.config.check_interval)


def load_cloud_configs() -> List[CloudConfig]:
    """Load cloud configurations from environment variables"""
    clouds = []

    # Get list of cloud names
    cloud_names = os.getenv('VCD_CLOUDS', '').split(',')
    cloud_names = [name.strip() for name in cloud_names if name.strip()]

    if not cloud_names:
        logger.error("No clouds configured. Set VCD_CLOUDS environment variable.")
        return []

    for cloud_name in cloud_names:
        prefix = f"VCD_{cloud_name}_"

        url = os.getenv(f"{prefix}URL")
        api_token = os.getenv(f"{prefix}API_TOKEN")
        api_version = os.getenv(f"{prefix}API_VERSION", "37.0")
        containers_str = os.getenv(f"{prefix}STORAGE_CONTAINERS", "")

        if not url or not api_token:
            logger.warning(f"Skipping cloud '{cloud_name}': missing URL or API_TOKEN")
            continue

        # Parse storage containers: "id1|name1,id2|name2,..."
        # Using | as separator because IDs contain colons (urn:vcloud:storagecontainer:...)
        storage_containers = []
        for container_entry in containers_str.split(','):
            container_entry = container_entry.strip()
            if not container_entry:
                continue

            if '|' in container_entry:
                container_id, container_name = container_entry.split('|', 1)
            else:
                container_id = container_entry
                container_name = container_entry  # Use ID as name if no name provided

            storage_containers.append(StorageContainerConfig(
                id=container_id.strip(),
                name=container_name.strip()
            ))

        if not storage_containers:
            logger.warning(f"Skipping cloud '{cloud_name}': no storage containers configured")
            continue

        cloud = CloudConfig(
            name=cloud_name,
            url=url,
            api_token=api_token,
            api_version=api_version,
            storage_containers=storage_containers
        )
        clouds.append(cloud)
        logger.info(f"Loaded cloud '{cloud_name}' with {len(storage_containers)} storage containers")

    return clouds


async def main():
    """Application entry point"""
    # Validate basic environment variables
    required_vars = ['VCD_CLOUDS', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return 1

    # Load cloud configurations
    clouds = load_cloud_configs()
    if not clouds:
        logger.error("No valid cloud configurations found")
        return 1

    # Create alert configuration
    alert_config = AlertConfig(
        critical_threshold=int(os.getenv('CRITICAL_THRESHOLD', '90')),
        warning_threshold=int(os.getenv('WARNING_THRESHOLD', '80')),
        check_interval=int(os.getenv('CHECK_INTERVAL', '3600')),
        daily_report_hour=int(os.getenv('DAILY_REPORT_HOUR', '9'))
    )

    # Create notifier
    notifier = TelegramNotifier(
        bot_token=os.getenv('TELEGRAM_BOT_TOKEN'),
        chat_id=os.getenv('TELEGRAM_CHAT_ID')
    )

    # Start monitoring
    monitor = MultiCloudDatastoreMonitor(clouds, alert_config, notifier)

    try:
        await monitor.run()
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    # Install requirements: pip install aiohttp python-dotenv requests
    exit(asyncio.run(main()))
