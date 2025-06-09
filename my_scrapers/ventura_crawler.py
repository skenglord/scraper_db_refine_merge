"""
# Ventura Crawler - A sophisticated web scraper for event data extraction
import asyncio
import random
from pathlib import Path
from urllib.parse import urlparse
import time
import json
import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any, Tuple, Callable
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
from collections import defaultdict
import os
import signal
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor
import threading # For BrowserManager's queue access, though asyncio.Queue is better for async code
import queue as thread_queue # For BrowserManager, if truly mixing threads. For pure async, asyncio.Queue

# Core dependencies (install via: pip install playwright beautifulsoup4 lxml fake-useragent numpy)
try:
    from playwright.async_api import (
        async_playwright, Browser, BrowserContext, Page, Error as PlaywrightError,
        TimeoutError as PlaywrightTimeoutError
    )
    from bs4 import BeautifulSoup, NavigableString, Tag
    from fake_useragent import UserAgent
    import lxml # Used by BeautifulSoup for faster parsing
    import numpy as np # For human-like delays and statistical calculations
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Ensure you have installed: pip install playwright beautifulsoup4 lxml fake-useragent numpy")
    print("And run: playwright install chromium")
    sys.exit(1)

# --- Configuration ---
DEFAULT_CONFIG = {
    "db_path": "serpentscale_scraper_data.db",
    "browser_pool_size": 3,
    "max_concurrent_scrapes": 5,
    "max_retries_per_url": 3,
    "request_timeout_ms": 30000, # 30 seconds
    "cache_max_age_hours": 24, # How long to use cached successful scrapes
    "log_level": "INFO",
    "proxy_file": "proxies.txt", # One proxy per line, e.g., http://user:pass@host:port
    "captcha_solver_api_key": None, # Placeholder for 2Captcha, AntiCaptcha, etc.
    "captcha_service_name": None, # e.g., "2captcha"
    "output_data_format": "json", # "json" or "csv" (for file output if implemented)
    "user_agent_cache_size": 200,
    "min_delay_between_requests_ms": 1000, # Minimum delay
    "max_delay_between_requests_ms": 3000, # Maximum delay
    "screenshot_on_error": True,
    "error_screenshot_dir": "error_screenshots",
    "headless_browser": True,
    "max_elements_to_inspect_adaptive": 500, # For adaptive selector discovery
    "adaptive_selector_min_text_length": 5,
    "adaptive_selector_max_text_length": 500,
}

# Configure logging
logger = logging.getLogger("SerpentScaleScraper")

def setup_logging(level_str: str = "INFO", log_file: str = "serpentscale_scraper.log"):
    log_level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s')
    
    # File handler
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

# --- Dataclasses ---
@dataclass
class ScrapingMetrics:
    total_urls_processed: int = 0
    successful_scrapes: int = 0
    failed_scrapes: int = 0
    json_ld_extractions: int = 0
    microdata_extractions: int = 0
    adaptive_extractions: int = 0
    fallback_extractions: int = 0
    cache_hits: int = 0
    total_events_extracted: int = 0 # If multiple events per page
    total_response_time_ms: float = 0.0
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def success_rate(self) -> float:
        if self.total_urls_processed == 0:
            return 0.0
        return (self.successful_scrapes / self.total_urls_processed) * 100
    
    def avg_response_time_ms(self) -> float:
        if self.successful_scrapes == 0:
            return 0.0
        return self.total_response_time_ms / self.successful_scrapes

    def requests_per_minute(self) -> float:
        if not self.start_time: return 0.0
        elapsed_seconds = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        if elapsed_seconds == 0: return 0.0
        return (self.total_urls_processed / elapsed_seconds) * 60

@dataclass
class ScrapingResult:
    url: str
    success: bool
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    extraction_method: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    response_time_ms: float = 0.0
    status_code: Optional[int] = None # HTTP status code, if available
    is_from_cache: bool = False
"""Manages database operations for scraped data, metrics, and configurations"""
    screenshot_path: Optional[str] = None

# --- Database Manager ---
class DatabaseManager:
    """Manages database operations for scraped data, metrics, and configurations"""
    
    def __init__(self, db_path: str):
        """Initialize DatabaseManager with database path"""
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._极

    def _execute(self, query: str, params: tuple = (), commit: bool = False,
                 fetchone: bool = False, fetchall: bool = False, max_retries: int = 3):
        """
        Execute SQL query with retry mechanism
        
        Args:
            query: SQL query string
            params: Query parameters
            commit: Whether to commit transaction
            fetchone: Return single result
            fetchall: Return all results
            max_retries: Maximum retry attempts
            
        Returns:
            Query result based on fetch parameters
            
        Raises:
            sqlite3.Error: After all retries fail
        """
        for attempt in range(max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(query, params)
                    if commit:
                        conn.commit()
                    if fetchone:
                        return cursor.fetchone()
                    if fetchall:
                        return cursor.fetchall()
                    return cursor
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    wait = 0.1 * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Database locked, retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"SQLite error: {e} for query: {query[:100]}...")
                    raise
            except sqlite3.Error as e:
                logger.error(f"SQLite error: {e} for query: {query[:100]}...")
                raise

    def _init_database(self):
        script = """
            CREATE TABLE IF NOT EXISTS scraped_events (
                url_hash TEXT PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                event_data TEXT, -- JSON string
                extraction_method TEXT,
                last_scraped_utc TIMESTAMP,
                success BOOLEAN,
                error_message TEXT,
                response_time_ms REAL,
                status_code INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_scraped_events_url ON scraped_events(url);
            CREATE INDEX IF NOT EXISTS idx_scraped_events_timestamp ON scraped_events(last_scraped_utc);
            CREATE INDEX IF NOT EXISTS idx_scraped_events_success ON scraped_events(success);

CREATE TABLE IF NOT EXISTS scraping_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_urls_processed INTEGER,
    successful_scrapes INTEGER,
    failed_scrapes INTEGER,
    avg_response_time_ms REAL,
    success_rate REAL
);

CREATE TABLE IF NOT EXISTS selector_patterns (
    pattern_hash TEXT PRIMARY KEY, -- hash of (site_domain, element_type, selector)
    site_domain TEXT NOT NULL,
    element_type TEXT NOT NULL, -- e.g., 'title', 'date', 'venue'
    selector TEXT NOT NULL,
    success_count INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    last_used_utc TIMESTAMP,
    created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_selector_patterns_domain_type ON selector_patterns(site_domain, element_type);

CREATE TABLE IF NOT EXISTS proxy_health (
    proxy_url TEXT PRIMARY KEY,
    success_count INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    total_response_time_ms REAL DEFAULT 0.0,
    request_count INTEGER DEFAULT 0,
    last_used_utc TIMESTAMP,
    last_failed_utc TIMESTAMP,
    is_active BOOLEAN DEFAULT 1, -- 1 for true, 0 for false
    consecutive_failures INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_proxy_health_active ON proxy_health(is_active);
"""
        self._execute(script, commit=True) # executescript is not needed for CREATE TABLE IF NOT EXISTS

    def store_scraping_result(self, result: ScrapingResult):
        url_hash = hashlib.md5(result.url.encode('utf-8')).hexdigest()
        event_data_json = json.dumps(result.data) if result.data else None
        title = result.data.get('title') if result.data and isinstance(result.data.get('title'), str) else None

        query = """
            INSERT OR REPLACE INTO scraped_events
            (url_hash, url, title, event_data, extraction_method, last_scraped_utc, success, error_message, response_time_ms, status_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            url_hash, result.url, title, event_data_json, result.extraction_method,
            result.timestamp.isoformat(), result.success, result.error_message,
            result.response_time_ms, result.status_code
        )
        self._execute(query, params, commit=True)
        logger.debug(f"Stored result for {result.url} (Success: {result.success})")

    def get_cached_result(self, url: str, max_age_hours: int) -> Optional[ScrapingResult]:
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        query = """
            SELECT * FROM scraped_events
            WHERE url_hash = ? AND success = 1 AND last_scraped_utc >= ?
        """
        row = self._execute(query, (url_hash, cutoff_time.isoformat()), fetchone=True)
        
        if row:
            try:
                data = json.loads(row['event_data']) if row['event_data'] else None
                return ScrapingResult(
                    url=row['url'],
                    success=bool(row['success']),
                    data=data,
                    error_message=row['error_message'],
                    extraction_method=row['extraction_method'],
                    timestamp=datetime.fromisoformat(row['last_scraped_utc']),
                    response_time_ms=row['response_time_ms'],
                    status_code=row['status_code'],
                    is_from_cache=True
                )
            except Exception as e:
                logger.error(f"Error reconstructing cached result for {url}: {e}")
                return None
        return None

    def store_metrics(self, metrics: ScrapingMetrics):
        query = """
            INSERT INTO scraping_metrics
            (total_urls_processed, successful_scrapes, failed_scrapes, avg_response_time_ms, success_rate)
            VALUES (?, ?, ?, ?, ?)
        """
        params = (
            metrics.total_urls_processed, metrics.successful_scrapes, metrics.failed_scrapes,
            metrics.avg_response_time_ms(), metrics.success_rate()
        )
        self._execute(query, params, commit=True)
        logger.info("Scraping metrics stored.")

    def update_selector_pattern_stats(self, domain: str, element_type: str, selector: str, success: bool):
        pattern_hash = hashlib.md5(f"{domain}|{element_type}|{selector}".encode('utf-8')).hexdigest()
        now_utc_iso = datetime.now(timezone.utc).isoformat()

        if success:
            query = """
                INSERT INTO selector_patterns (pattern_hash, site_domain, element_type, selector, success_count, last_used_utc)
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(pattern_hash) DO UPDATE SET
                success_count = success_count + 1,
                last_used_utc = excluded.last_used_utc;
            """
        else:
            query = """
                INSERT INTO selector_patterns (pattern_hash, site_domain, element_type, selector, failure_count, last_used_utc)
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(pattern_hash) DO UPDATE SET
                failure_count = failure_count + 1,
                last_used_utc = excluded.last_used_utc;
            """
        params = (pattern_hash, domain, element_type, selector, now_utc_iso)
        self._execute(query, params, commit=True)

    def get_learned_selectors(self, domain: str, element_type: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
        query = """
            SELECT site_domain, element_type, selector, success_count, failure_count
            FROM selector_patterns
            WHERE site_domain = ?
        """
        params: List[Any] = [domain]
        if element_type:
            query += " AND element_type = ?"
            params.append(element_type)
        
        query += """
            ORDER BY (CAST(success_count AS REAL) / (success_count + failure_count + 1)) DESC, success_count DESC, last_used_utc DESC
            LIMIT ?
        """ # +1 in denominator to avoid division by zero and prefer newer selectors
        params.append(limit)
        
        rows = self._execute(query, tuple(params), fetchall=True)
        return [dict(row) for row in rows] if rows else []

    def update_proxy_health(self, proxy_url: str, success: bool, response_time_ms: float):
        now_utc_iso = datetime.now(timezone.utc).isoformat()
        
        if success:
            query = """
                INSERT INTO proxy_health (proxy_url, success_count, total_response_time_ms, request_count, last_used_utc, is_active, consecutive_failures)
                VALUES (?, 1, ?, 1, ?, 1, 0)
                ON CONFLICT(proxy_url) DO UPDATE SET
                success_count = success_count + 1,
                total_response_time_ms = total_response_time_ms + excluded.total_response_time_ms,
                request_count = request_count + 1,
                last_used_utc = excluded.last_used_utc,
                is_active = 1,
                consecutive_failures = 0;
            """
            params = (proxy_url, response_time_ms, now_utc_iso)
        else:
            query = """
                INSERT INTO proxy_health (proxy_url, failure_count, request_count, last_used_utc, last_failed_utc, consecutive_failures)
                VALUES (?, 1, 1, ?, ?, 1)
                ON CONFLICT(proxy_url) DO UPDATE SET
                failure_count = failure_count + 1,
                request_count = request_count + 1,
                last_used_utc = excluded.last_used_utc,
                last_failed_utc = excluded.last_failed_utc,
                consecutive_failures = consecutive_failures + 1,
                is_active = CASE WHEN consecutive_failures + 1 >= 5 THEN 0 ELSE 1 END; 
            """ # Deactivate after 5 consecutive failures
            params = (proxy_url, now_utc_iso, now_utc_iso)
        self._execute(query, params, commit=True)

    def get_active_proxies(self, limit: int = 100) -> List[str]:
        query = """
            SELECT proxy_url FROM proxy_health
            WHERE is_active = 1
            ORDER BY (CAST(success_count AS REAL) / (success_count + failure_count + 1)) DESC, last_used_utc ASC
            LIMIT ?
        """ # Prioritize healthy and less recently used proxies
        rows = self._execute(query, (limit,), fetchall=True)
        return [row['proxy_url'] for row in rows] if rows else []

    def add_proxies_if_not_exist(self, proxies: List[str]):
        now_utc_iso = datetime.now(timezone.utc).isoformat()
        data_to_insert = []
        for proxy_url in proxies:
            # Check if proxy exists
            row = self._execute("SELECT 1 FROM proxy_health WHERE proxy_url = ?", (proxy_url,), fetchone=True)
            if not row:
                data_to_insert.append((proxy_url, now_utc_iso))
        
        if data_to_insert:
            query = """
                INSERT INTO proxy_health (proxy_url, last_used_utc, is_active)
                VALUES (?, ?, 1)
            """ # Default to active
            try:
                with sqlite3.connect(self.db_path, timeout=10) as conn:
                    cursor = conn.cursor()
                    cursor.executemany(query, data_to_insert)
                    conn.commit()
                logger.info(f"Added {len(data_to_insert)} new proxies to database.")
            except sqlite3.Error as e:
                 logger.error(f"SQLite error during batch proxy insert: {e}")


# --- Anti-Detection Manager ---
class AntiDetectionManager:
    """
    Manages anti-detection techniques for web scraping
    
    Attributes:
        config: Configuration dictionary
        db_manager: Database manager instance
    """
    
    def __init__(self, config: Dict, db_manager: DatabaseManager):
        """
        Initialize anti-detection manager
        
        Args:
            config: Configuration dictionary
            db_manager: Database manager instance
        """
        self.config = config
        self.db_manager = db_manager
        try:
            self.ua_generator = UserAgent(cache=True, use_cache_server=False)
        except Exception as e:
            logger.warning(f"Failed to initialize UserAgent with cache, falling back: {e}")
            self.ua_generator = UserAgent(cache=False, use_cache_server=False)

        self.used_user_agents = set()
        self.fingerprints = self._load_fingerprints()
        self.proxy_list: List[str] = []
        self.current_proxy_index = 0
        self._load_and_register_proxies()
        
    def _validate_proxy_url(self, proxy_url: str) -> bool:
        """
        Validate proxy URL format
        
        Args:
            proxy_url: Proxy URL string
            
        Returns:
            True if valid, False otherwise
        """
        parsed = urlparse(proxy_url)
        if not parsed.scheme or not parsed.hostname:
            return False
        if parsed.scheme not in ['http', 'https', 'socks5']:
            return False
        return True

    def _load_and_register_proxies(self):
        """Load proxies from file, validate them, and register in DB"""
        proxy_file_path = self.config.get("proxy_file", "proxies.txt")
        raw_proxies = []
        if Path(proxy_file_path).exists():
            try:
                with open(proxy_file_path, 'r') as f:
                    raw_proxies = [line.strip() for line in f if line.strip() and not line.startswith("#")]
                
                # Validate proxies before registration
                valid_proxies = [p for p in raw_proxies if self._validate_proxy_url(p)]
                invalid_proxies = set(raw_proxies) - set(valid_proxies)
                
                if invalid_proxies:
                    logger.warning(f"Found {len(invalid_proxies)} invalid proxies in file")
                
                if valid_proxies:
                    self.db_manager.add_proxies_if_not_exist(valid_proxies)
                    logger.info(f"Loaded {len(valid_proxies)} valid proxies from {proxy_file_path}")
            except IOError as e:
                logger.warning(f"Could not read proxy file {proxy_file_path}: {e}")
        
        # Fetch active proxies from DB
        self.proxy_list = self.db_manager.get_active_proxies(limit=500)
        if not self.proxy_list and valid_proxies:
            self.proxy_list = valid_proxies
            logger.warning("Using raw proxies from file as DB returned no active proxies.")
        elif not self.proxy_list:
            logger.warning("No proxies loaded. Proxy rotation will be disabled.")
        else:
            random.shuffle(self.proxy_list)
            logger.info(f"Initialized with {len(self.proxy_list)} active proxies")


    def get_random_user_agent(self) -> str:
        for _ in range(10): # Try a few times to get a non-recently-used one
            ua = self.ua_generator.random
            if ua not in self.used_user_agents:
                self.used_user_agents.add(ua)
                if len(self.used_user_agents) > self.config.get("user_agent_cache_size", 200):
                    # Simple FIFO cache eviction
                    self.used_user_agents.pop() if self.used_user_agents else None 
                return ua
        return self.ua_generator.random # Fallback

    def get_random_headers(self) -> Dict[str, str]:
        common_accept_languages = [
            'en-US,en;q=0.9', 'en-GB,en;q=0.8', 'de-DE,de;q=0.7,en;q=0.3',
            'fr-FR,fr;q=0.9,en;q=0.8', 'es-ES,es;q=0.9,en;q=0.8'
        ]
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': random.choice(common_accept_languages),
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-CH-UA': f'" Not A;Brand";v="99", "Chromium";v="{random.randint(90, 110)}", "Google Chrome";v="{random.randint(90, 110)}"',
            'Sec-CH-UA-Mobile': '?0',
            'Sec-CH-UA-Platform': f'"{random.choice(["Windows", "macOS", "Linux"])}"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': random.choice(['none', 'same-origin', 'cross-site']),
            'Sec-Fetch-User': '?1',
            'TE': 'trailers',
            'DNT': str(random.choice([0,1])), # Do Not Track
        }
        if random.random() < 0.7: # Add Referer sometimes
            headers['Referer'] = f"https://{random.choice(['www.google.com', 'www.bing.com', 'duckduckgo.com'])}/"
        return headers

    def get_random_viewport(self) -> Dict[str, int]:
        common_resolutions = [
            (1920, 1080), (1366, 768), (1440, 900), (1536, 864),
            (2560, 1440), (1280, 720), (1600, 900), (1024, 768)
        ]
        width, height = random.choice(common_resolutions)
        # Add slight variations
        width += random.randint(-20, 20)
        height += random.randint(-20, 20)
        return {'width': max(800, width), 'height': max(600, height)}

    def get_human_like_delay_ms(self) -> float:
        """Generate human-like delays using a gamma distribution."""
        min_delay = self.config.get("min_delay_between_requests_ms", 1000)
        max_delay = self.config.get("max_delay_between_requests_ms", 3000)
        
        # Ensure min_delay is less than max_delay
        if min_delay >= max_delay:
            max_delay = min_delay + 1000 # Default spread if config is wrong

        # Calculate shape and scale for gamma distribution
        # Mean of gamma is shape * scale. Let's aim for mean around (min+max)/2
        mean_delay = (min_delay + max_delay) / 2
        # Variance can be tuned. A smaller shape gives more variance.
        shape = 2.0 
        scale = mean_delay / shape
        
        delay = np.random.gamma(shape, scale)
        
        # Clamp to min/max bounds
        return max(min_delay, min(delay, max_delay))


    def _load_fingerprints(self) -> List[Dict[str, Any]]:
        # More detailed fingerprints
        return [
            {
                'platform': 'Win32', 'languages': ['en-US', 'en'], 'timezone': 'America/New_York',
                'screen': {'width': 1920, 'height': 1080, 'colorDepth': 24, 'pixelRatio': 1},
                'webgl_vendor': 'Intel Inc.', 'webgl_renderer': 'Intel Iris OpenGL Engine'
            },
            {
                'platform': 'MacIntel', 'languages': ['en-GB', 'en'], 'timezone': 'Europe/London',
                'screen': {'width': 1440, 'height': 900, 'colorDepth': 24, 'pixelRatio': 2},
                'webgl_vendor': 'Apple Inc.', 'webgl_renderer': 'Apple M1'
            },
            {
                'platform': 'Linux x86_64', 'languages': ['de-DE', 'de', 'en-US'], 'timezone': 'Europe/Berlin',
                'screen': {'width': 1366, 'height': 768, 'colorDepth': 24, 'pixelRatio': 1},
                'webgl_vendor': 'Mozilla', 'webgl_renderer': 'llvmpipe (LLVM 13.0.0, 256 bits)'
            },
        ]

    def get_random_fingerprint(self) -> Dict[str, Any]:
        return random.choice(self.fingerprints)

    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        if not self.proxy_list:
            return None
        
        # Periodically refresh active proxies from DB
        if self.current_proxy_index == 0 and random.random() < 0.1: # 10% chance on loop reset
            fresh_proxies = self.db_manager.get_active_proxies(limit=500)
            if fresh_proxies:
                self.proxy_list = fresh_proxies
                random.shuffle(self.proxy_list)
                logger.info(f"Refreshed active proxy list: {len(self.proxy_list)} proxies.")

        if not self.proxy_list: # Check again if refresh yielded nothing
             return None

        proxy_url = self.proxy_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        # Parse proxy_url (e.g. http://user:pass@host:port)
        parsed_proxy = urlparse(proxy_url)
        server = f"{parsed_proxy.scheme}://{parsed_proxy.hostname}"
        if parsed_proxy.port:
            server += f":{parsed_proxy.port}"
        
        proxy_dict = {'server': server}
        if parsed_proxy.username:
            proxy_dict['username'] = unquote(parsed_proxy.username)
        if parsed_proxy.password:
            proxy_dict['password'] = unquote(parsed_proxy.password)
            
        return proxy_dict


# --- Captcha Solver ---
class CaptchaSolver:
    def __init__(self, config: Dict):
        self.config = config
        self.api_key = config.get("captcha_solver_api_key")
        self.service_name = config.get("captcha_service_name")
        # In a real scenario, you'd initialize the client for the chosen service here.
        # e.g., from twocaptcha import TwoCaptcha; self.solver = TwoCaptcha(self.api_key)

    async def detect_captcha(self, page: Page) -> Optional[Dict[str, Any]]:
        captcha_selectors = [
            {'type': 'reCAPTCHA_v2', 'iframe_selector': 'iframe[src*="api2/anchor"]', 'challenge_selector': 'iframe[src*="api2/bframe"]'},
            {'type': 'reCAPTCHA_v3', 'selector': '.grecaptcha-badge[data-style]'}, # Harder to detect actively
            {'type': 'hCaptcha', 'iframe_selector': 'iframe[src*="hcaptcha.com"]'},
            {'type': 'Cloudflare_Challenge', 'selector': '#cf-challenge-running, .cf-turnstile'},
            {'type': 'FunCAPTCHA', 'selector': '#fc-token[type=hidden][name=fc-token]'},
            {'type': 'Image_Captcha', 'selector': 'img[src*="captcha"], img[id*="captcha"], img[name*="captcha"]'},
        ]

        for captcha_type in captcha_selectors:
            try:
                if 'iframe_selector' in captcha_type:
                    iframe_element = page.frame_locator(captcha_type['iframe_selector'])
                    if await iframe_element.locator('*:visible').count() > 0 : # Check if anything visible in iframe
                         logger.info(f"Detected {captcha_type['type']} (iframe).")
                         return {'type': captcha_type['type'], 'page': page, 'iframe_locator': iframe_element}
                elif 'selector' in captcha_type:
                    element = page.locator(captcha_type['selector']).first # Use .first to avoid waiting for all
                    if await element.is_visible(timeout=1000): # Quick check
                        logger.info(f"Detected {captcha_type['type']}.")
                        return {'type': captcha_type['type'], 'page': page, 'element_locator': element}
            except PlaywrightTimeoutError:
                continue # Element not found quickly, move on
            except Exception as e:
                logger.debug(f"Error during captcha detection for {captcha_type['type']}: {e}")
                continue
        return None

    async def solve_captcha(self, captcha_info: Dict[str, Any]) -> bool:
        captcha_type = captcha_info['type']
        page = captcha_info['page']
        logger.warning(f"Captcha detected: {captcha_type}. Attempting to handle...")

        if captcha_type == 'Cloudflare_Challenge':
            logger.info("Cloudflare challenge detected. Waiting for it to resolve automatically...")
            try:
                # Wait for either the challenge to disappear or a known success element to appear
                # This might need to be site-specific or more robust
                await page.wait_for_load_state('networkidle', timeout=45000) # Increased timeout
                # Check if challenge is gone
                if await page.locator('#cf-challenge-running, .cf-turnstile').count() == 0:
                    logger.info("Cloudflare challenge seems to have resolved.")
                    return True
                else:
                    logger.warning("Cloudflare challenge still present after waiting.")
                    return False
            except PlaywrightTimeoutError:
                logger.warning("Timeout waiting for Cloudflare challenge to resolve.")
                return False
            except Exception as e:
                logger.error(f"Error handling Cloudflare challenge: {e}")
                return False

        if not self.api_key or not self.service_name:
            logger.warning("Captcha solving service not configured (API key or service name missing). Cannot solve.")
            return False

        # Placeholder for actual solving service integration
        # Example for reCAPTCHA v2:
        # if captcha_type == 'reCAPTCHA_v2':
        #     sitekey_element = await page.locator('[data-sitekey]').first()
        #     if sitekey_element:
        #         sitekey = await sitekey_element.get_attribute('data-sitekey')
        #         url = page.url
        #         logger.info(f"Attempting to solve {captcha_type} with sitekey: {sitekey} on {url}")
        #         try:
        #             # token = self.solver.recaptcha(sitekey=sitekey, url=url)
        #             # await page.evaluate(f"___grecaptcha_cfg.clients[0].L.L.callback('{token['code']}')") # This is highly variable
        #             logger.info("Placeholder: Captcha token would be submitted here.")
        #             return True # Assuming success for placeholder
        #         except Exception as e:
        #             logger.error(f"Error solving {captcha_type}: {e}")
        #             return False

        logger.warning(f"No specific solving logic implemented for {captcha_type} or solving failed.")
        return False


# --- Selector Learning Engine ---
class SelectorLearningEngine:
    """
    Learns and applies CSS selectors for data extraction
    
    Attributes:
        db_manager: Database manager instance
        config: Configuration dictionary
    """
    
    def __init__(self, db_manager: DatabaseManager, config: Dict):
        """
        Initialize selector learning engine
        
        Args:
            db_manager: Database manager instance
            config: Configuration dictionary
        """
        self.db_manager = db_manager
        self.config = config
        self.element_classifiers = self._init_classifiers()

    def _init_classifiers(self) -> Dict[str, Dict[str, Any]]:
        """
        Initialize element classifiers with optimized parameters
        
        Returns:
            Dictionary of element classifiers
        """
        return {
            'title': {
                'tags': ['h1', 'h2', 'title', 'meta[property="og:title"]', 'meta[name="twitter:title"]'],
                'attributes': {'itemprop': 'name', 'class': ['title', 'heading', 'event-title', 'event-name']},
                'keywords': ['event', 'concert', 'show', 'festival', 'conference', 'webinar', 'meetup'],
                'min_length': 5, 'max_length': 250, 'score_boost': 1.5
            },
            'date': { # Could be start_date, end_date, or general date
                'tags': ['time', 'meta[itemprop="startDate"]', 'meta[itemprop="endDate"]', 'span', 'div', 'p'],
                'attributes': {'itemprop': ['startDate', 'endDate', 'datePublished'], 'class': ['date', 'time', 'datetime', 'schedule', 'event-date']},
                'patterns': [
                    r'\b\d{1,2}[\/\-\.]\d{1,2}[\/\-\.](\d{2}|\d{4})\b', # 01/01/2023 or 01.01.23
                    r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}\b', # Jan 1st, 2023
                    r'\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?(?:,)?\s+\d{4}\b', # 1 January 2023
                    r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(?:[+-]\d{2}:\d{2}|Z)?\b', # ISO 8601
                ],
                'min_length': 6, 'max_length': 100, 'score_boost': 1.2
            },
            'venue_name': {
                'tags': ['div', 'span', 'p', 'a', 'meta[itemprop="name"]'], # Often within a location itemprop
                'attributes': {'itemprop': 'name', 'class': ['venue', 'location', 'place', 'address-name', 'event-venue']},
                'keywords': ['hall', 'center', 'theatre', 'club', 'arena', 'stadium', 'online', 'virtual'],
                'min_length': 3, 'max_length': 150, 'score_boost': 1.0
            },
            'venue_address': {
                'tags': ['div', 'span', 'p', 'address', 'meta[itemprop="address"]'],
                'attributes': {'itemprop': 'address', 'class': ['address', 'street-address', 'event-location-address']},
                'keywords': ['street', 'road', 'avenue', 'blvd', 'city', 'state', 'zip', 'country'],
                 'min_length': 10, 'max_length': 250, 'score_boost': 1.0
            },
            'description': {
                'tags': ['p', 'div', 'meta[property="og:description"]', 'meta[name="twitter:description"]', 'meta[name="description"]'],
                'attributes': {'itemprop': 'description', 'class': ['description', 'summary', 'details', 'event-details', 'event-description']},
                'min_length': 20, 'max_length': 5000, 'score_boost': 0.8 # Lower boost as it can be noisy
            },
            'price': {
                'tags': ['span', 'div', 'p', 'meta[itemprop="price"]'],
                'attributes': {'itemprop': 'price', 'class': ['price', 'cost', 'ticket-price', 'fee']},
                'patterns': [
                    r'[\$€£¥]\s?\d+(?:[\.,]\d{1,2})?', # $10, €10.50, £10,00
                    r'\d+(?:[\.,]\d{1,2})?\s?(?:USD|EUR|GBP|JPY|CAD|AUD)\b', # 10 USD, 10.50EUR
                    r'(?:free|gratis)\b'
                ],
                'min_length': 1, 'max_length': 50, 'score_boost': 1.1
            },
        }

    async def discover_selectors_on_page(self, page: Page, domain: str) -> Dict[str, List[str]]:
        """Discover selectors for key event elements using element sampling
        
        Args:
            page: Playwright page instance
            domain: Domain being scraped
            
        Returns:
            Dictionary of discovered selectors by element type
        """
        discovered_selectors = defaultdict(list)
        max_elements = self.config.get("max_elements_to_inspect_adaptive", 500)
        
        # Prioritize learned selectors
        for elem_type in self.element_classifiers.keys():
            learned = self.db_manager.get_learned_selectors(domain, elem_type, limit=1)
            if learned:
                discovered_selectors[elem_type].append(learned[0]['selector'])
        
        # Discover new selectors using element sampling
        if not discovered_selectors['title']:
            try:
                # Get all elements and sample a subset for efficiency
                all_elements = await page.locator('*').all()
                sampled_elements = random.sample(
                    all_elements,
                    min(max_elements, len(all_elements))
                ) if len(all_elements) > max_elements else all_elements
                
                # Process sampled elements
                for element in sampled_elements:
                    tag_name = await element.evaluate('el => el.tagName.toLowerCase()')
                    if tag_name in ['h1', 'h2', 'h3']:
                        text = await element.text_content()
                        if text and 10 < len(text.strip()) < 250:
                            # Generate selector based on classes
                            classes = await element.get_attribute("class")
                            if classes:
                                class_list = classes.split()
                                if class_list:
                                    discovered_selectors['title'].append(f'{tag_name}.{class_list[0]}')
                            else:
                                discovered_selectors['title'].append(tag_name)
                            break
            except Exception as e:
                logger.error(f"Selector discovery error: {e}")
        
        logger.info(f"Discovered selectors for {domain}: {dict(discovered_selectors)}")
        return {k: list(set(v))[:3] for k, v in discovered_selectors.items()}

    async def extract_data_with_discovered_selectors(self, page: Page, selectors: Dict[str, List[str]], domain: str) -> Dict[str, Any]:
        extracted_data = {}
        for element_type, selector_list in selectors.items():
            if not selector_list: continue
            
            for sel_idx, selector_str in enumerate(selector_list):
                try:
                    element = page.locator(selector_str).first
                    # Check visibility with a short timeout to avoid hanging
                    if await element.is_visible(timeout=1000):
                        content = None
                        # Handle meta tags differently
                        tag_name = await element.evaluate('el => el.tagName.toLowerCase()')
                        if tag_name == 'meta':
                            content = await element.get_attribute('content')
                        else:
                            content = await element.text_content()
                        
                        if content:
                            content = content.strip()
                            if len(content) >= self.config.get("adaptive_selector_min_text_length", 3):
                                extracted_data[element_type] = content
                                self.db_manager.update_selector_pattern_stats(domain, element_type, selector_str, success=True)
                                logger.debug(f"Successfully extracted {element_type} using adaptive selector '{selector_str}' for {domain}")
                                break # Found data with this selector type
                        else: # Content is None or empty
                            if sel_idx == len(selector_list) - 1: # Last selector for this type failed
                                self.db_manager.update_selector_pattern_stats(domain, element_type, selector_str, success=False)
                                logger.debug(f"Adaptive selector '{selector_str}' for {element_type} on {domain} yielded no content.")
                    else: # Element not visible
                        if sel_idx == len(selector_list) - 1:
                            self.db_manager.update_selector_pattern_stats(domain, element_type, selector_str, success=False)
                            logger.debug(f"Adaptive selector '{selector_str}' for {element_type} on {domain} not visible.")

                except PlaywrightTimeoutError: # Element not found or not visible in time
                    if sel_idx == len(selector_list) - 1:
                        self.db_manager.update_selector_pattern_stats(domain, element_type, selector_str, success=False)
                        logger.debug(f"Adaptive selector '{selector_str}' for {element_type} on {domain} timed out (not found/visible).")
                except Exception as e:
                    if sel_idx == len(selector_list) - 1:
                        self.db_manager.update_selector_pattern_stats(domain, element_type, selector_str, success=False)
                        logger.warning(f"Error extracting {element_type} with adaptive selector '{selector_str}' for {domain}: {e}")
                    # Continue to next selector if this one failed
        return extracted_data


# --- Browser Manager ---
class BrowserManager:
    """
    Manages a pool of browser instances for concurrent scraping
    
    Attributes:
        config: Configuration dictionary
        anti_detection_mgr: Anti-detection manager instance
    """
    
    def __init__(self, config: Dict, anti_detection_mgr: AntiDetectionManager):
        """
        Initialize browser manager
        
        Args:
            config: Configuration dictionary
            anti_detection_mgr: Anti-detection manager instance
        """
        self.config = config
        self.pool_size = config.get("browser_pool_size", 3)
        self.anti_detection_mgr = anti_detection_mgr
        self.playwright_instance = None
        self.browsers: List[Browser] = []
        self.contexts_queue: asyncio.Queue[BrowserContext] = asyncio.Queue()
        self._initialized = False
        
    async def __aenter__(self):
        """Context manager entry"""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        """Context manager exit - clean up resources"""
        await self.cleanup()
        
    async def cleanup(self):
        """Clean up all browser resources"""
        logger.info("Cleaning up browser resources")
        while not self.contexts_queue.empty():
            context = await self.contexts_queue.get()
            await context.close()
            
        for browser in self.browsers:
            await browser.close()
            
        if self.playwright_instance:
            await self.playwright_instance.stop()
            
        self._initialized = False
        logger.info("Browser resources cleaned up")

    async def initialize(self):
        if self._initialized: return
        logger.info(f"Initializing browser pool with size: {self.pool_size}")
        self.playwright_instance = await async_playwright().start()
        
        common_browser_args = [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-infobars',
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--disable-extensions',
            '--disable-gpu', # Often helps in headless environments
            '--window-size=1920,1080', # Start with a common size
            '--ignore-certificate-errors',
            '--enable-features=NetworkService,NetworkServiceInProcess',
            '--disable-features=VizDisplayCompositor' # May help with stability
        ]

        for i in range(self.pool_size):
            try:
                browser = await self.playwright_instance.chromium.launch(
                    headless=self.config.get("headless_browser", True),
                    args=common_browser_args,
                    # proxy=self.anti_detection_mgr.get_next_proxy() # Initial proxy for browser launch if needed
                )
                self.browsers.append(browser)
                context = await self._create_stealth_context(browser)
                await self.contexts_queue.put(context)
                logger.info(f"Browser instance {i+1} and context created.")
            except PlaywrightError as e:
                logger.error(f"Failed to launch browser instance {i+1}: {e}")
                # If some browsers fail to launch, we might operate with a smaller pool
        
        if not self.browsers:
            logger.critical("No browser instances could be launched. Scraper cannot proceed.")
            raise RuntimeError("Failed to initialize any browser instances.")
        
        self._initialized = True
        logger.info(f"Browser pool initialized with {self.contexts_queue.qsize()} contexts.")

    async def _create_stealth_context(self, browser: Browser) -> BrowserContext:
        fingerprint = self.anti_detection_mgr.get_random_fingerprint()
        viewport = self.anti_detection_mgr.get_random_viewport()
        user_agent = self.anti_detection_mgr.get_random_user_agent()

        context_options = {
            'user_agent': user_agent,
            'viewport': viewport,
            'locale': random.choice(fingerprint['languages']),
            'timezone_id': fingerprint['timezone'],
            'permissions': ['geolocation', 'notifications'], # Grant some common permissions
            'geolocation': {'latitude': random.uniform(25.0, 48.0), 'longitude': random.uniform(-125.0, -65.0)},
            'color_scheme': random.choice(['light', 'dark', 'no-preference']),
            'java_script_enabled': True,
            'bypass_csp': True, # Can help with some anti-bot measures, use cautiously
            # 'proxy': self.anti_detection_mgr.get_next_proxy() # Context-specific proxy
        }
        
        # Apply proxy if available
        proxy_settings = self.anti_detection_mgr.get_next_proxy()
        if proxy_settings:
            context_options['proxy'] = proxy_settings

        context = await browser.new_context(**context_options)

        # Stealth init scripts
        await context.add_init_script(f"""
            // Pass the User-Agent test
            Object.defineProperty(navigator, 'userAgent', {{ get: () => '{user_agent}' }});
            Object.defineProperty(navigator, 'platform', {{ get: () => '{fingerprint['platform']}' }});
            Object.defineProperty(navigator, 'languages', {{ get: () => {fingerprint['languages']} }});
            // Pass the Webdriver test
            Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
            // Pass the Chrome test
            window.chrome = {{ runtime: {{}} }}; // Basic mock
            // Pass the Permissions test
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({{ state: Notification.permission }}) :
                originalQuery(parameters)
            );
            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {{
                get: () => [
                    {{ name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' }},
                    {{ name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' }},
                    {{ name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }}
                ],
            }});
            // Mock screen properties
            if (window.screen) {{
                Object.defineProperty(window.screen, 'width', {{ get: () => {fingerprint['screen']['width']} }});
                Object.defineProperty(window.screen, 'height', {{ get: () => {fingerprint['screen']['height']} }});
                Object.defineProperty(window.screen, 'availWidth', {{ get: () => {fingerprint['screen']['width']} }});
                Object.defineProperty(window.screen, 'availHeight', {{ get: () => {fingerprint['screen']['height'] - 40} }}); // Typical taskbar
                Object.defineProperty(window.screen, 'colorDepth', {{ get: () => {fingerprint['screen']['colorDepth']} }});
                Object.defineProperty(window.screen, 'pixelDepth', {{ get: () => {fingerprint['screen']['colorDepth']} }});
            }}
            // WebGL Vendor and Renderer
            const getParameter = HTMLCanvasElement.prototype.getContext('webgl').getParameter;
            HTMLCanvasElement.prototype.getContext('webgl').getParameter = function(parameter) {{
                if (parameter === 37445) return '{fingerprint['webgl_vendor']}'; // VENDOR
                if (parameter === 37446) return '{fingerprint['webgl_renderer']}'; // RENDERER
                return getParameter(parameter);
            }};
        """)
        return context

    async def get_context(self) -> BrowserContext:
        if not self._initialized:
            await self.initialize()
        
        # Try to get a context from the queue
        try:
            return await asyncio.wait_for(self.contexts_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning("Timeout getting context from queue, pool might be exhausted or slow. Creating new one.")
            # If queue is empty or slow, create a new context on-the-fly with a random browser
            # This helps if contexts are getting stuck or closed unexpectedly
            if not self.browsers: # Should not happen if initialized
                raise RuntimeError("No browsers available to create new context.")
            browser = random.choice(self.browsers)
            if browser.is_connected():
                return await self._create_stealth_context(browser)
            else: # Browser died, try to find another or re-initialize
                logger.error("A browser in the pool died. Attempting to recover.")
                self.browsers = [b for b in self.browsers if b.is_connected()]
                if not self.browsers:
                    logger.critical("All browsers in pool died. Re-initializing pool.")
                    self._initialized = False # Force re-init
                    await self.initialize()
                    if not self.browsers: # Still no browsers after re-init
                         raise RuntimeError("Failed to recover browser pool after all browsers died.")
                # Try again with remaining/new browsers
                return await self.get_context()


    async def return_context(self, context: BrowserContext, needs_recycle: bool = False):
        if needs_recycle or not context.browser or not context.browser.is_connected():
            logger.info("Recycling browser context.")
            try:
                await context.close()
            except Exception as e:
                logger.warning(f"Error closing context during recycle: {e}")
            
            # Create a new one to replace it if browser is still good
            if context.browser and context.browser.is_connected():
                new_context = await self._create_stealth_context(context.browser)
                await self.contexts_queue.put(new_context)
            else: # Browser associated with this context is dead or gone
                logger.warning("Browser for returned context is dead. Not replacing immediately.")
                # Potentially reduce pool size or trigger a health check of browsers
        else:
            # Clear cookies and storage for reuse
            try:
                await context.clear_cookies()
                await context.storage_state() # Effectively clears session storage for next use
            except Exception as e:
                logger.warning(f"Error clearing context state: {e}")
                # If clearing fails, better to recycle it
                try: await context.close()
                except: pass
                if context.browser and context.browser.is_connected():
                    new_context = await self._create_stealth_context(context.browser)
                    await self.contexts_queue.put(new_context)
                return

            await self.contexts_queue.put(context)


    async def cleanup(self):
        logger.info("Cleaning up browser manager...")
        if not self._initialized: return

        while not self.contexts_queue.empty():
            context = await self.contexts_queue.get()
            try:
                await context.close()
            except Exception as e:
                logger.warning(f"Error closing context during cleanup: {e}")
        
        for browser in self.browsers:
            try:
                await browser.close()
            except Exception as e:
                logger.warning(f"Error closing browser during cleanup: {e}")
        
        if self.playwright_instance:
            try:
                await self.playwright_instance.stop()
            except Exception as e:
                logger.warning(f"Error stopping Playwright during cleanup: {e}")
        
        self.browsers.clear()
        self._initialized = False
        logger.info("Browser manager cleaned up.")


# --- Retry Manager ---
class RetryManager:
    def __init__(self, config: Dict):
        self.config = config
        self.max_retries = config.get("max_retries_per_url", 3)
        self.backoff_strategies = {
            'rate_limit': {'base_ms': 60000, 'multiplier': 1.5, 'max_ms': 300000, 'jitter_factor': 0.2}, # 1 min base
            'timeout': {'base_ms': 5000, 'multiplier': 1.5, 'max_ms': 60000, 'jitter_factor': 0.3},
            'connection': {'base_ms': 3000, 'multiplier': 2, 'max_ms': 60000, 'jitter_factor': 0.3},
            'captcha': {'base_ms': 10000, 'multiplier': 1.2, 'max_ms': 60000, 'jitter_factor': 0.1}, # Shorter retry for captcha if solving
            'default': {'base_ms': 2000, 'multiplier': 2, 'max_ms': 30000, 'jitter_factor': 0.3}
        }

    async def execute_with_retry(self, async_func: Callable, *args, **kwargs) -> Any:
        last_exception = None
        # Extract url for logging, assuming it's the first arg or a kwarg
        url_for_log = args[0] if args and isinstance(args[0], str) else kwargs.get('url', 'N/A')

        for attempt in range(self.max_retries + 1):
            try:
                # Pass along attempt number for adaptive strategies within the function
                kwargs['current_attempt'] = attempt 
                return await async_func(*args, **kwargs)
            except PlaywrightTimeoutError as e:
                last_exception = e
                error_msg = f"PlaywrightTimeoutError: {str(e)}"
                strategy_key = 'timeout'
            except PlaywrightError as e: # General Playwright errors
                last_exception = e
                error_msg = f"PlaywrightError: {str(e)}"
                if "net::ERR_PROXY_CONNECTION_FAILED" in str(e) or \
                   "net::ERR_NAME_NOT_RESOLVED" in str(e) or \
                   "net::ERR_CONNECTION_REFUSED" in str(e):
                    strategy_key = 'connection'
                    # Signal that proxy might be bad
                    kwargs['proxy_failed'] = True 
                else:
                    strategy_key = 'default'
            except CaptchaDetectedError as e: # Custom exception for captchas
                last_exception = e
                error_msg = f"CaptchaDetectedError: {str(e)}"
                strategy_key = 'captcha'
            except Exception as e: # Catch-all for other errors
                last_exception = e
                error_msg = f"UnexpectedError: {str(e)}"
                strategy_key = 'default'
                logger.error(f"Unexpected error type: {type(e)} - {e}\n{traceback.format_exc()}")


            logger.warning(f"Attempt {attempt + 1}/{self.max_retries + 1} for {url_for_log} failed: {error_msg}")

            if attempt < self.max_retries:
                delay_ms = self._calculate_delay_ms(strategy_key, attempt)
                logger.info(f"Retrying {url_for_log} in {delay_ms / 1000:.2f}s (strategy: {strategy_key})")
                await asyncio.sleep(delay_ms / 1000.0)
                
                # Adaptive strategy: e.g., tell BrowserManager to recycle context on repeated failures
                if attempt > 0 : # If not the first attempt
                    kwargs['recycle_context_on_next'] = True
            else:
                logger.error(f"All {self.max_retries + 1} attempts for {url_for_log} failed. Last error: {error_msg}")
                raise last_exception # Re-raise the last captured exception

    def _calculate_delay_ms(self, strategy_key: str, attempt: int) -> float:
        strategy = self.backoff_strategies.get(strategy_key, self.backoff_strategies['default'])
        
        delay = strategy['base_ms'] * (strategy['multiplier'] ** attempt)
        delay = min(delay, strategy['max_ms'])
        
        # Add jitter: delay +/- (delay * jitter_factor * random_uniform(-1,1))
        jitter = (delay * strategy['jitter_factor'] * (random.random() * 2 - 1))
        return max(500, delay + jitter) # Ensure minimum delay of 0.5s


# --- Custom Exceptions ---
class CaptchaDetectedError(Exception):
    """Custom exception for when a CAPTCHA is detected."""
    def __init__(self, message="CAPTCHA detected", captcha_type=None, url=None):
        super().__init__(message)
        self.captcha_type = captcha_type
        self.url = url

    def __str__(self):
        return f"{super().__str__()} (Type: {self.captcha_type}, URL: {self.url})"

# --- Main Scraper Class ---
class SerpentScaleScraper:
    def __init__(self, config_overrides: Optional[Dict] = None):
        self.config = {**DEFAULT_CONFIG, **(config_overrides or {})}
        setup_logging(self.config["log_level"])
        
        self.db_manager = DatabaseManager(self.config["db_path"])
        self.anti_detection_mgr = AntiDetectionManager(self.config, self.db_manager)
        self.browser_mgr = BrowserManager(self.config, self.anti_detection_mgr)
        self.selector_learner = SelectorLearningEngine(self.db_manager, self.config)
        self.captcha_solver = CaptchaSolver(self.config)
        self.retry_mgr = RetryManager(self.config)
        
        self.metrics = ScrapingMetrics()
        self.stop_event = asyncio.Event()
        self._active_tasks = 0
        self._lock = asyncio.Lock() # For thread-safe metric updates if needed, though primarily async

        Path(self.config["error_screenshot_dir"]).mkdir(parents=True, exist_ok=True)

    async def initialize(self):
        logger.info("Initializing SerpentScale Scraper...")
        await self.browser_mgr.initialize()
        logger.info("SerpentScale Scraper initialized.")

    async def _take_error_screenshot(self, page: Page, url: str) -> Optional[str]:
        if not self.config.get("screenshot_on_error", True):
            return None
        try:
            filename = f"error_{urlparse(url).netloc}_{hashlib.md5(url.encode()).hexdigest()[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            filepath = Path(self.config["error_screenshot_dir"]) / filename
            await page.screenshot(path=str(filepath), full_page=True)
            logger.info(f"Error screenshot saved to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to take error screenshot for {url}: {e}")
            return None

    async def _parse_html_with_bs(self, html_content: str, parser: str = 'lxml') -> BeautifulSoup:
        # Run BeautifulSoup parsing in a separate thread to avoid blocking asyncio event loop
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, BeautifulSoup, html_content, parser)

    def _normalize_text(self, text: Optional[str]) -> Optional[str]:
        if text is None: return None
        text = re.sub(r'\s+', ' ', text).strip()
        return text if text else None

    def _extract_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extracts and parses JSON-LD data from script tags."""
        json_ld_data = []
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                content = script.string
                if content:
                    data = json.loads(content)
                    if isinstance(data, list):
                        json_ld_data.extend(data)
                    else:
                        json_ld_data.append(data)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON-LD: {e}. Content: {str(content)[:100]}...")
            except Exception as e:
                logger.warning(f"Unexpected error parsing JSON-LD: {e}")
        return json_ld_data
    
    def _find_event_in_json_ld(self, json_ld_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Finds the most relevant event object from a list of JSON-LD objects."""
        for item in json_ld_data:
            item_type = item.get('@type', '')
            if isinstance(item_type, list): # Type can be an array
                if any(t in ['Event', 'MusicEvent', 'SportsEvent', 'Festival', 'ExhibitionEvent', 'BusinessEvent', 'ScreeningEvent'] for t in item_type):
                    return item
            elif isinstance(item_type, str) and item_type in ['Event', 'MusicEvent', 'SportsEvent', 'Festival', 'ExhibitionEvent', 'BusinessEvent', 'ScreeningEvent']:
                return item
        return None # No primary event found

    def _normalize_jsonld_event_data(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalizes extracted JSON-LD event data to a standard format."""
        normalized = {}
        normalized['title'] = self._normalize_text(event_data.get('name'))
        normalized['description'] = self._normalize_text(event_data.get('description'))
        normalized['url'] = event_data.get('url') # Original URL of the event page
        
        # Dates
        normalized['startDate'] = event_data.get('startDate')
        normalized['endDate'] = event_data.get('endDate')
        normalized['doorTime'] = event_data.get('doorTime')

        # Location
        location = event_data.get('location')
        if isinstance(location, dict):
            normalized['venue_name'] = self._normalize_text(location.get('name'))
            address = location.get('address')
            if isinstance(address, dict):
                normalized['venue_address'] = ", ".join(filter(None, [
                    self._normalize_text(address.get('streetAddress')),
                    self._normalize_text(address.get('addressLocality')),
                    self_normalize_text(address.get('addressRegion')),
                    self._normalize_text(address.get('postalCode')),
                    self._normalize_text(address.get('addressCountry'))
                ]))
            elif isinstance(address, str):
                 normalized['venue_address'] = self._normalize_text(address)
            if location.get('geo'):
                normalized['geo_latitude'] = location['geo'].get('latitude')
                normalized['geo_longitude'] = location['geo'].get('longitude')
        elif isinstance(location, list): # Sometimes location is a list
            # Take the first one, assuming it's most relevant
            if location and isinstance(location[0], dict):
                # Recursive call for the first item, simplified
                 normalized_location = self._normalize_jsonld_event_data({'location': location[0]})
                 normalized['venue_name'] = normalized_location.get('venue_name')
                 normalized['venue_address'] = normalized_location.get('venue_address')


        # Offers / Tickets
        offers = event_data.get('offers')
        if isinstance(offers, list): offers = offers[0] # Take first offer if multiple
        if isinstance(offers, dict):
            normalized['price'] = offers.get('price')
            normalized['priceCurrency'] = offers.get('priceCurrency')
            normalized['availability'] = offers.get('availability')
            normalized['ticket_url'] = offers.get('url')

        # Organizer
        organizer = event_data.get('organizer')
        if isinstance(organizer, dict):
            normalized['organizer_name'] = self._normalize_text(organizer.get('name'))
            normalized['organizer_url'] = organizer.get('url')
        elif isinstance(organizer, list) and organizer:
             if isinstance(organizer[0], dict):
                normalized['organizer_name'] = self._normalize_text(organizer[0].get('name'))
                normalized['organizer_url'] = organizer[0].get('url')


        # Performers
        performers = event_data.get('performer', [])
        if not isinstance(performers, list): performers = [performers]
        normalized['performers'] = []
        for p in performers:
            if isinstance(p, dict):
                normalized['performers'].append({'name': self._normalize_text(p.get('name')), 'type': p.get('@type')})
            elif isinstance(p, str):
                 normalized['performers'].append({'name': self._normalize_text(p), 'type': 'Person'})


        # Image
        image = event_data.get('image')
        if isinstance(image, list): image = image[0] if image else None
        if isinstance(image, dict): normalized['image_url'] = image.get('url')
        elif isinstance(image, str): normalized['image_url'] = image

        return {k: v for k, v in normalized.items() if v is not None and v != ''}


    def _extract_microdata(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extracts event data using Microdata (Schema.org vocab, typically)."""
        # Find top-level Event items
        event_elements = soup.find_all(itemscope=True, itemtype=re.compile(r"https://schema.org/(Event|MusicEvent|SportsEvent|Festival|ExhibitionEvent|BusinessEvent|ScreeningEvent)"))
        if not event_elements:
            event_elements = soup.find_all(itemscope=True, itemtype=re.compile(r"http://schema.org/(Event|MusicEvent|SportsEvent|Festival|ExhibitionEvent|BusinessEvent|ScreeningEvent)"))

        if not event_elements:
            return None

        # Process the first event element found
        # A more complex site might have multiple, requiring disambiguation
        event_element = event_elements[0]
        
        data = {}
        
        def get_prop_value(element, prop_name):
            prop_element = element.find(itemprop=prop_name)
            if not prop_element: return None

            if prop_element.name == 'meta': return self._normalize_text(prop_element.get('content'))
            if prop_element.name == 'link' or prop_element.name == 'a': return prop_element.get('href')
            if prop_element.name == 'img': return prop_element.get('src')
            if prop_element.get('datetime'): return self._normalize_text(prop_element.get('datetime'))
            
            # Handle nested itemscopes
            if prop_element.get('itemscope') is not None:
                nested_type = prop_element.get('itemtype', '').split('/')[-1]
                nested_data = {}
                for sub_prop in prop_element.find_all(itemprop=True, recursive=False): # Only direct children props
                    sub_prop_name = sub_prop.get('itemprop')
                    nested_data[sub_prop_name] = get_prop_value(prop_element, sub_prop_name)
                return {'@type': nested_type, **nested_data}

            return self._normalize_text(prop_element.get_text(separator=' ', strip=True))

        props_to_extract = {
            'title': 'name', 'description': 'description', 'url': 'url',
            'startDate': 'startDate', 'endDate': 'endDate', 'doorTime': 'doorTime',
            'location_info': 'location', 'offers_info': 'offers',
            'organizer_info': 'organizer', 'performer_info': 'performer', 'image_url': 'image'
        }

        for key, prop_name in props_to_extract.items():
            value = get_prop_value(event_element, prop_name)
            if value:
                if isinstance(value, dict) and '@type' in value: # Nested item
                    if key == 'location_info':
                        data['venue_name'] = self._normalize_text(value.get('name'))
                        addr = value.get('address')
                        if isinstance(addr, dict):
                             data['venue_address'] = self._normalize_text(addr.get('streetAddress')) # Simplified
                        elif isinstance(addr, str):
                             data['venue_address'] = self._normalize_text(addr)
                    elif key == 'offers_info':
                        data['price'] = value.get('price')
                        data['priceCurrency'] = value.get('priceCurrency')
                        data['ticket_url'] = value.get('url')
                    # Add more handling for other nested types if needed
                else:
                    data[key] = value
        
        return data if data.get('title') and data.get('startDate') else None


    def _extract_fallback_heuristics(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """A very basic fallback if no structured data is found."""
        data = {}
        # Title: Look for H1, then H2, then <title> tag
        for tag_name in ['h1', 'h2']:
            tag = soup.find(tag_name)
            if tag and self._normalize_text(tag.text):
                data['title'] = self._normalize_text(tag.text)
                break
        if not data.get('title') and soup.title and self._normalize_text(soup.title.string):
            data['title'] = self._normalize_text(soup.title.string)

        # Description: Look for meta description or a large paragraph
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            data['description'] = self._normalize_text(meta_desc.get('content'))
        else:
            # Find the largest text block in a <p> or <div>
            largest_text = ""
            for p_or_div in soup.find_all(['p', 'div']):
                text = self._normalize_text(p_or_div.get_text(separator=' ', strip=True))
                if text and len(text) > len(largest_text) and len(text) > 100 and len(text) < 3000:
                    largest_text = text
            if largest_text:
                data['description'] = largest_text
        
        # Dates: Look for common date patterns (very naive)
        text_content = soup.get_text()
        date_patterns = [
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}\b',
            r'\b\d{1,2}/\d{1,2}/\d{2,4}\b'
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text_content, re.IGNORECASE)
            if match:
                data['startDate'] = match.group(0) # Very rough
                break
        
        logger.info(f"Fallback heuristics extracted: { {k: (v[:50] + '...' if isinstance(v, str) and len(v) > 50 else v) for k,v in data.items()} }")
        return data if data.get('title') else {}


    def _validate_extracted_data(self, data: Dict[str, Any], url: str) -> bool:
        if not data: return False
        # Basic validation: must have a title and at least a date or venue
        has_title = bool(data.get('title'))
        has_date = bool(data.get('startDate'))
        has_venue = bool(data.get('venue_name')) or bool(data.get('venue_address'))
        
        is_valid = has_title and (has_date or has_venue)
        if not is_valid:
            logger.warning(f"Validation failed for data from {url}. Title: {has_title}, Date: {has_date}, Venue: {has_venue}")
        return is_valid

    async def _perform_actual_scraping(self, url: str, context: BrowserContext, **kwargs) -> ScrapingResult:
        page: Optional[Page] = None
        start_time = time.perf_counter()
        response_time_ms = 0
        status_code = None
        current_proxy = context.proxy # Get proxy from context if set
        
        try:
            page = await context.new_page()
            await page.set_extra_http_headers(self.anti_detection_mgr.get_random_headers())

            # Human-like mouse movements and scrolls before navigation (optional, can be slow)
            # await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            # await asyncio.sleep(random.uniform(0.1, 0.3))

            logger.info(f"Navigating to {url} (Attempt {kwargs.get('current_attempt', 0) + 1})")
            response = await page.goto(url, timeout=self.config["request_timeout_ms"], wait_until='domcontentloaded')
            status_code = response.status if response else None
            response_time_ms = (time.perf_counter() - start_time) * 1000

            if status_code and (status_code >= 400):
                logger.warning(f"Page {url} returned status {status_code}.")
                if status_code == 403 or status_code == 429: # Forbidden or Too Many Requests
                    if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], False, response_time_ms)
                    # Potentially trigger CAPTCHA or IP block logic
                    raise PlaywrightError(f"Access denied with status {status_code}") # Will be caught by retry
                # Other client/server errors might be retried or logged as failure.
                # For now, let retry_mgr handle it.

            # Wait for page to settle - dynamic delay
            await asyncio.sleep(self.anti_detection_mgr.get_human_like_delay_ms() / 2000) # Shorter initial settle

            # Handle overlays (e.g., cookie banners, popups)
            # This is a generic attempt; site-specific handlers are more robust.
            common_overlay_selectors = [
                '[id*="cookie"] button', '[class*="cookie"] button',
                '[aria-label*="close" i]', 'button:has-text("Accept")', 'button:has-text("Agree")'
            ]
            for sel in common_overlay_selectors:
                try:
                    overlay_button = page.locator(sel).first
                    if await overlay_button.is_visible(timeout=1500):
                        await overlay_button.click(timeout=2000, delay=random.uniform(50,150))
                        logger.info(f"Clicked potential overlay: {sel}")
                        await asyncio.sleep(random.uniform(0.5, 1.0)) # Wait for overlay to disappear
                        break 
                except PlaywrightTimeoutError: # Not found or not visible quickly
                    pass 
                except Exception as e_overlay:
                    logger.debug(f"Minor error clicking overlay {sel}: {e_overlay}")


            # Check for CAPTCHA
            captcha_info = await self.captcha_solver.detect_captcha(page)
            if captcha_info:
                if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], False, response_time_ms) # Captcha often means proxy is flagged
                solved = await self.captcha_solver.solve_captcha(captcha_info)
                if not solved:
                    raise CaptchaDetectedError(f"CAPTCHA ({captcha_info.get('type')}) detected and not solved", captcha_type=captcha_info.get('type'), url=url)
                await asyncio.sleep(self.anti_detection_mgr.get_human_like_delay_ms() / 1000) # Wait after captcha

            # Human-like interactions
            for _ in range(random.randint(1, 3)): # Scroll a few times
                await page.mouse.wheel(0, random.randint(200, 800))
                await asyncio.sleep(random.uniform(0.2, 0.6))
            
            await page.wait_for_load_state('networkidle', timeout=15000) # Wait for network activity to cease

            html_content = await page.content()
            soup = await self._parse_html_with_bs(html_content)
            domain = urlparse(url).netloc

            extracted_data = None
            extraction_method = None

            # 1. Try JSON-LD
            json_ld_items = self._extract_json_ld(soup)
            if json_ld_items:
                event_json_ld = self._find_event_in_json_ld(json_ld_items)
                if event_json_ld:
                    extracted_data = self._normalize_jsonld_event_data(event_json_ld)
                    extraction_method = "json-ld"
                    if self._validate_extracted_data(extracted_data, url):
                        logger.info(f"Successfully extracted data using JSON-LD for {url}")
                        self.metrics.json_ld_extractions +=1
                    else: extracted_data = None # Validation failed

            # 2. Try Microdata if JSON-LD failed or was invalid
            if not extracted_data:
                microdata = self._extract_microdata(soup)
                if microdata:
                    # Microdata might need less normalization if get_prop_value is good
                    extracted_data = microdata 
                    extraction_method = "microdata"
                    if self._validate_extracted_data(extracted_data, url):
                        logger.info(f"Successfully extracted data using Microdata for {url}")
                        self.metrics.microdata_extractions +=1
                    else: extracted_data = None

            # 3. Try Adaptive Selectors if previous methods failed
            if not extracted_data:
                discovered_selectors = await self.selector_learner.discover_selectors_on_page(page, domain)
                if discovered_selectors:
                    adaptive_data = await self.selector_learner.extract_data_with_discovered_selectors(page, discovered_selectors, domain)
                    if self._validate_extracted_data(adaptive_data, url):
                        extracted_data = adaptive_data
                        extraction_method = "adaptive"
                        logger.info(f"Successfully extracted data using adaptive selectors for {url}")
                        self.metrics.adaptive_extractions +=1
                    else: extracted_data = None
            
            # 4. Fallback to basic heuristics
            if not extracted_data:
                fallback_data = self._extract_fallback_heuristics(soup)
                if self._validate_extracted_data(fallback_data, url):
                    extracted_data = fallback_data
                    extraction_method = "fallback"
                    logger.info(f"Extracted data using fallback heuristics for {url}")
                    self.metrics.fallback_extractions +=1
                else:
                    logger.warning(f"All extraction methods failed to yield valid data for {url}")
                    # No valid data found by any method

            if extracted_data:
                if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], True, response_time_ms)
                return ScrapingResult(url=url, success=True, data=extracted_data, extraction_method=extraction_method,
                                      response_time_ms=response_time_ms, status_code=status_code)
            else:
                # If proxy was used and we got no data, it might be a soft block or bad proxy
                if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], False, response_time_ms)
                raise ValueError("No valid event data extracted after all methods.")

        except CaptchaDetectedError as e: # Propagate this specifically for retry logic
            if page: await self._take_error_screenshot(page, url)
            if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], False, response_time_ms if response_time_ms > 0 else 10000)
            raise e # Re-raise for retry manager
        except Exception as e:
            screenshot_path = None
            if page: screenshot_path = await self._take_error_screenshot(page, url)
            error_msg = f"Error scraping {url}: {type(e).__name__} - {e}"
            logger.error(f"{error_msg}\n{traceback.format_exc() if not isinstance(e, PlaywrightTimeoutError) else ''}")
            if current_proxy: self.db_manager.update_proxy_health(current_proxy['server'], False, response_time_ms if response_time_ms > 0 else 10000) # Assume failure took time
            # For retry manager to handle specific error types
            if isinstance(e, PlaywrightError): raise 
            # Wrap other exceptions if needed, or let retry_mgr categorize as 'default'
            raise RuntimeError(error_msg) from e # Wrap in a generic runtime error if not already a handled type
        finally:
            if page:
                try:
                    await page.close()
                except Exception as e_close:
                    logger.warning(f"Error closing page for {url}: {e_close}")


    async def scrape_single_url(self, url: str, **kwargs) -> ScrapingResult:
        async with self._lock: # Ensure thread-safe update to active_tasks
            self._active_tasks += 1
        
        logger.info(f"Processing URL: {url}")
        
        # Check DB cache first
        cached_result = self.db_manager.get_cached_result(url, self.config["cache_max_age_hours"])
        if cached_result:
            logger.info(f"Cache hit for {url}. Using cached data.")
            self.metrics.cache_hits += 1
            self.metrics.successful_scrapes += 1 # Count cache hit as success for metrics
            self.metrics.total_urls_processed +=1
            async with self._lock: self._active_tasks -=1
            return cached_result

        context: Optional[BrowserContext] = None
        recycle_ctx = kwargs.get('recycle_context_on_next', False) # From retry manager
        
        try:
            context = await self.browser_mgr.get_context()
            # Pass proxy_failed to retry_mgr if it was set by _perform_actual_scraping
            # This is a bit complex, might simplify. The idea is if a proxy fails, the retry might use a new one.
            # For now, context recycling handles proxy rotation implicitly if proxies are per-context.
            # If proxies are per-request, then _perform_actual_scraping needs to handle it.
            # Current BrowserManager sets proxy per-context.

            scrape_kwargs = {**kwargs} # Copy kwargs to avoid modifying original from retry_mgr
            if 'recycle_context_on_next' in scrape_kwargs: del scrape_kwargs['recycle_context_on_next']

            result = await self.retry_mgr.execute_with_retry(
                self._perform_actual_scraping, url, context, **scrape_kwargs
            )
            
            self.metrics.successful_scrapes += 1
            self.metrics.total_response_time_ms += result.response_time_ms
            
        except Exception as e: # Catch exceptions from retry_mgr (all attempts failed)
            error_message = f"Failed to scrape {url} after all retries: {type(e).__name__} - {str(e)}"
            logger.error(error_message)
            result = ScrapingResult(url=url, success=False, error_message=error_message, 
                                    timestamp=datetime.now(timezone.utc))
            self.metrics.failed_scrapes += 1
            recycle_ctx = True # Definitely recycle context on total failure
        
        finally:
            if context:
                await self.browser_mgr.return_context(context, needs_recycle=recycle_ctx)
            
            self.metrics.total_urls_processed += 1
            self.db_manager.store_scraping_result(result)
            
            async with self._lock: self._active_tasks -=1
            logger.info(f"Finished processing URL: {url}. Success: {result.success}. Active tasks: {self._active_tasks}")
            
        return result

    async def scrape_urls(self, urls: List[str]) -> List[ScrapingResult]:
        if not urls: return []
        
        semaphore = asyncio.Semaphore(self.config["max_concurrent_scrapes"])
        tasks = []

        async def _scrape_with_semaphore(url: str):
            async with semaphore:
                if self.stop_event.is_set():
                    logger.info(f"Stop event set, skipping {url}")
                    return ScrapingResult(url=url, success=False, error_message="Scraping stopped by user.")
                return await self.scrape_single_url(url)

        for url in urls:
            tasks.append(_scrape_with_semaphore(url))
        
        results = await asyncio.gather(*tasks, return_exceptions=False) # Exceptions handled in scrape_single_url
        return results

    async def shutdown(self, signal_num=None, frame=None):
        if self.stop_event.is_set(): return # Already shutting down
        
        if signal_num:
            logger.info(f"Shutdown initiated by signal {signal.Signals(signal_num).name}...")
        else:
            logger.info("Shutdown initiated...")
        
        self.stop_event.set()

        # Wait for active tasks to complete (with a timeout)
        # This part is tricky with asyncio.gather if tasks are long-running and don't check stop_event
        # For robust shutdown, tasks themselves need to check self.stop_event periodically.
        # The _scrape_with_semaphore checks it before starting a new URL.
        # For tasks already deep in Playwright calls, they might complete fully.
        
        # A simple way to wait for current semaphore-controlled tasks:
        # This doesn't perfectly track tasks inside scrape_single_url if they are long.
        # A more robust way would be to have each task register/unregister.
        # The self._active_tasks counter helps here.
        
        shutdown_timeout = 30 # seconds
        elapsed_shutdown_time = 0
        while self._active_tasks > 0 and elapsed_shutdown_time < shutdown_timeout:
            logger.info(f"Waiting for {self._active_tasks} active tasks to complete... ({shutdown_timeout - elapsed_shutdown_time}s left)")
            await asyncio.sleep(1)
            elapsed_shutdown_time +=1
        
        if self._active_tasks > 0:
            logger.warning(f"{self._active_tasks} tasks still active after timeout. Proceeding with cleanup.")

        await self.browser_mgr.cleanup()
        self.db_manager.store_metrics(self.metrics) # Store final metrics
        
        logger.info("SerpentScale Scraper shutdown complete.")
        # If running in a script that needs to exit, might call sys.exit here or let main handle it.

# --- Main Execution ---
async def main():
    # Example usage:
    config_overrides = {
        "max_concurrent_scrapes": 2, # For testing, keep it low
        "browser_pool_size": 1,      # For testing
        # "proxy_file": "my_proxies.txt", # Optional: if you have a proxy list
        "log_level": "INFO",
        "headless_browser": True, # Set to False to see browser actions
        "screenshot_on_error": True,
    }
    scraper = SerpentScaleScraper(config_overrides)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(scraper.shutdown(s)))
    
    try:
        await scraper.initialize()
        
        # Test URLs (replace with actual event sites, be mindful of their ToS)
        # These are placeholders and will likely fail or require specific handling.
        urls_to_scrape = [
            "https://www.eventbrite.com/d/online/free--events/", # Complex site, good test
            "https://www.ticketmaster.com/discover/concerts",    # Very complex, heavy anti-bot
            "https://www.meetup.com/find/events/?allMeetups=true&radius=Infinity&keywords=tech", # JS heavy
            # Add a known non-existent URL to test error handling
            "https://thissitedoesnotexist12345.com/event",
            # Add a simple HTML test page URL if you have one locally
            # "file:///path/to/your/local_event_test.html" 
        ]
        
        logger.info(f"Starting to scrape {len(urls_to_scrape)} URLs...")
        results = await scraper.scrape_urls(urls_to_scrape)
        
        successful_count = 0
        for res in results:
            if res.success:
                successful_count += 1
                logger.info(f"SUCCESS: {res.url}")
                logger.info(f"  Title: {res.data.get('title', 'N/A') if res.data else 'N/A'}")
                logger.info(f"  Method: {res.extraction_method}")
                # logger.info(f"  Data: {json.dumps(res.data, indent=2)[:500]}") # Print partial data
            else:
                logger.error(f"FAILURE: {res.url} - {res.error_message}")
        
        logger.info(f"Scraping run completed. {successful_count}/{len(urls_to_scrape)} URLs scraped successfully.")
        
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main: {e}\n{traceback.format_exc()}")
    finally:
        if not scraper.stop_event.is_set(): # If shutdown wasn't triggered by signal
            await scraper.shutdown()

        # Display final metrics
        final_metrics = scraper.metrics
        logger.info("--- FINAL METRICS ---")
        logger.info(f"Total URLs Processed: {final_metrics.total_urls_processed}")
        logger.info(f"Successful Scrapes: {final_metrics.successful_scrapes}")
        logger.info(f"Failed Scrapes: {final_metrics.failed_scrapes}")
        logger.info(f"Cache Hits: {final_metrics.cache_hits}")
        logger.info(f"Success Rate: {final_metrics.success_rate():.2f}%")
        logger.info(f"Avg Response Time (successful): {final_metrics.avg_response_time_ms():.2f} ms")
        logger.info(f"Extraction Methods Used:")
        logger.info(f"  JSON-LD: {final_metrics.json_ld_extractions}")
        logger.info(f"  Microdata: {final_metrics.microdata_extractions}")
        logger.info(f"  Adaptive: {final_metrics.adaptive_extractions}")
        logger.info(f"  Fallback: {final_metrics.fallback_extractions}")
        logger.info(f"Run duration: {(datetime.now(timezone.utc) - final_metrics.start_time)}")
        logger.info("---------------------")


if __name__ == "__main__":
    # To run playwright in asyncio, it's sometimes better to use its own event loop policy on Windows
    # if sys.platform == "win32":
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Before running, ensure Playwright browsers are installed: `playwright install chromium`
    asyncio.run(main())