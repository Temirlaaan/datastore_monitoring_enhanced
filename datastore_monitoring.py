import os
import time
import requests
import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
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
    
    @property
    def usage_percent(self) -> float:
        return (self.used_mb / self.total_mb * 100) if self.total_mb > 0 else 0
    
    @property
    def provisioned_percent(self) -> float:
        return (self.provisioned_mb / self.total_mb * 100) if self.total_mb > 0 else 0

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
    
    def __init__(self, base_url: str, refresh_token: str):
        self.base_url = base_url
        self.refresh_token = refresh_token
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
                logger.info(f"Refreshing token (attempt {attempt + 1})")
                
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
                
                logger.debug(f"Token request status: {response.status_code}")
                logger.debug(f"Token request headers: {dict(response.headers)}")
                
                if response.status_code == 401:
                    logger.error(f"Token refresh failed - 401 Unauthorized: {response.text}")
                    raise RuntimeError("Refresh token is invalid or expired")
                
                response.raise_for_status()
                data = response.json()
                
                if 'access_token' not in data:
                    logger.error(f"No access_token in response: {data}")
                    raise ValueError("No access_token in response")
                
                self._token = data['access_token']
                expires_in = int(data.get('expires_in', 3600))
                self._expires_at = time.time() + expires_in
                
                logger.info(f"Token refreshed successfully (expires in {expires_in}s)")
                return
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Token refresh attempt {attempt + 1} failed: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    logger.error(f"Response body: {e.response.text}")
                if attempt == 2:  # Last attempt
                    raise
                await asyncio.sleep(5)
            
            except Exception as e:
                logger.error(f"Unexpected error refreshing token: {e}")
                if attempt == 2:
                    raise
                await asyncio.sleep(5)

class VCDDatastoreClient:
    """VCD Datastore API client"""
    
    def __init__(self, base_url: str, token_manager: TokenManager, 
                 storage_container_id: str, api_version: str = "37.0"):
        self.base_url = base_url
        self.token_manager = token_manager
        self.storage_container_id = storage_container_id
        self.api_version = api_version
    
    async def get_datastores(self) -> List[DatastoreInfo]:
        """Fetch datastore information"""
        url = f"{self.base_url}/cloudapi/1.0.0/storageContainers/{self.storage_container_id}/datastores"
        
        connector = aiohttp.TCPConnector(ssl=True, limit=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for attempt in range(3):
                try:
                    # Get fresh token on retry after 401
                    force_refresh = attempt > 0
                    token = await self.token_manager.get_token(force_refresh=force_refresh)
                    
                    headers = {
                        'Accept': f'application/json;version={self.api_version}',
                        'Authorization': f'Bearer {token}'
                    }
                    
                    async with session.get(url, headers=headers) as response:
                        if response.status == 401 and attempt < 2:
                            logger.warning(f"401 error, will retry with fresh token")
                            continue
                        
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
                                datastore_type=ds_data.get('datastoreType', 'Unknown')
                            )
                            datastores.append(datastore)
                        
                        logger.info(f"Retrieved {len(datastores)} datastores")
                        return datastores
                
                except aiohttp.ClientError as e:
                    logger.error(f"API request failed (attempt {attempt + 1}): {e}")
                    if attempt == 2:
                        return []
                    await asyncio.sleep(5)
        
        return []

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

class DatastoreMonitor:
    """Main monitoring class"""
    
    def __init__(self, config: AlertConfig):
        self.config = config
        self.alert_cache: Dict[str, Dict] = {}  # In-memory cache for alerts
        self.last_daily_report = 0
        
        # Initialize components
        self.token_manager = TokenManager(
            base_url=os.getenv('VCD_URL'),
            refresh_token=os.getenv('VCD_API_TOKEN')
        )
        
        self.vcd_client = VCDDatastoreClient(
            base_url=os.getenv('VCD_URL'),
            token_manager=self.token_manager,
            storage_container_id=os.getenv('VCD_STORAGE_CONTAINER_ID'),
            api_version=os.getenv('VCD_API_VERSION', '37.0')
        )
        
        self.notifier = TelegramNotifier(
            bot_token=os.getenv('TELEGRAM_BOT_TOKEN'),
            chat_id=os.getenv('TELEGRAM_CHAT_ID')
        )
    
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
            return "🚨 CRITICAL", "🔴"
        elif usage_percent >= self.config.warning_threshold:
            return "⚠️ WARNING", "🟡"
        else:
            return None, None
    
    async def _should_send_alert(self, datastore: DatastoreInfo, alert_level: str) -> bool:
        """Check if we should send an alert (respects cooldown)"""
        cache_key = f"datastore_{datastore.id}"
        now = time.time()
        
        if cache_key in self.alert_cache:
            last_alert = self.alert_cache[cache_key]
            # Don't send if same level and within cooldown
            if (last_alert['level'] == alert_level and 
                now - last_alert['timestamp'] < self.config.alert_cooldown):
                return False
        
        return True
    
    async def check_datastores(self):
        """Main monitoring check"""
        try:
            datastores = await self.vcd_client.get_datastores()
            if not datastores:
                logger.warning("No datastores found")
                return
            
            alerts_sent = 0
            
            for ds in datastores:
                cache_key = f"datastore_{ds.id}"
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
            
            logger.info(f"Checked {len(datastores)} datastores, sent {alerts_sent} alerts")
            
        except Exception as e:
            logger.error(f"Error in datastore check: {e}")
    
    async def _send_alert(self, ds: DatastoreInfo, alert_level: str, emoji: str):
        """Send alert message"""
        message = (
            f"{emoji} {alert_level}: Datastore filling up!\n\n"
            f"📁 **{ds.name}**\n"
            f"📊 **Used:** {self._format_storage_size(ds.used_mb)} / "
            f"{self._format_storage_size(ds.total_mb)} ({ds.usage_percent:.1f}%)\n"
            f"📈 **Provisioned:** {self._format_storage_size(ds.provisioned_mb)} "
            f"({ds.provisioned_percent:.1f}%)\n"
            f"💾 **Type:** {ds.datastore_type}\n"
            f"🆔 **ID:** `{ds.id}`\n"
            f"🕐 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await self.notifier.send_message(message)
    
    async def _send_recovery_message(self, ds: DatastoreInfo):
        """Send recovery message"""
        message = (
            f"✅ **RECOVERED**: Datastore back to normal\n\n"
            f"📁 **{ds.name}**\n"
            f"📊 **Used:** {self._format_storage_size(ds.used_mb)} / "
            f"{self._format_storage_size(ds.total_mb)} ({ds.usage_percent:.1f}%)\n"
            f"🕐 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        await self.notifier.send_message(message)
    
    async def send_daily_report(self):
        """Send daily status report"""
        try:
            datastores = await self.vcd_client.get_datastores()
            if not datastores:
                return
            
            report_lines = ["📋 **Daily Datastore Report**\n"]
            
            for ds in datastores:
                _, emoji = self._get_alert_level(ds.usage_percent)
                status_emoji = emoji if emoji else "🟢"
                
                total_tb = ds.total_mb / 1_048_576
                used_tb = ds.used_mb / 1_048_576
                
                report_lines.append(
                    f"{status_emoji} **{ds.name}**: {used_tb:.1f}/{total_tb:.1f} TB "
                    f"({ds.usage_percent:.1f}%)"
                )
            
            report_lines.append(f"\n🕐 **Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            await self.notifier.send_message("\n".join(report_lines))
            logger.info("Daily report sent")
            
        except Exception as e:
            logger.error(f"Failed to send daily report: {e}")
    
    async def run(self):
        """Main monitoring loop"""
        logger.info("Starting datastore monitor...")
        logger.info(f"Config: Critical={self.config.critical_threshold}%, "
                   f"Warning={self.config.warning_threshold}%, "
                   f"Interval={self.config.check_interval}s")
        
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

async def main():
    """Application entry point"""
    # Validate environment variables
    required_vars = ['VCD_URL', 'VCD_API_TOKEN', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return 1
    
    # Create configuration
    config = AlertConfig(
        critical_threshold=int(os.getenv('CRITICAL_THRESHOLD', '90')),
        warning_threshold=int(os.getenv('WARNING_THRESHOLD', '80')),
        check_interval=int(os.getenv('CHECK_INTERVAL', '3600')),
        daily_report_hour=int(os.getenv('DAILY_REPORT_HOUR', '9'))
    )
    
    # Start monitoring
    monitor = DatastoreMonitor(config)
    
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
