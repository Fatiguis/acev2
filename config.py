"""
Configuration module for the Polymarket Arbitrage Bot.
Contains all configurable parameters and constants.
"""

import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dotenv import load_dotenv
from eth_account import Account

load_dotenv()


@dataclass
class ProxyApiCreds:
    """API credentials for a proxy wallet session."""
    api_key: str
    api_secret: str
    api_passphrase: str
    funder_address: str  # The proxy wallet address (where funds are held)

    @classmethod
    def from_dict(cls, d: Dict) -> "ProxyApiCreds":
        """Create from dictionary."""
        return cls(
            api_key=d.get("api_key", ""),
            api_secret=d.get("api_secret", ""),
            api_passphrase=d.get("api_passphrase", ""),
            funder_address=d.get("funder_address", "")
        )

    def is_valid(self) -> bool:
        """Check if credentials are complete."""
        return bool(self.api_key and self.api_secret and self.api_passphrase and self.funder_address)


@dataclass
class TradingConfig:
    """Trading parameters and thresholds."""

    # Minimum market volume in USD to consider for trading
    # Per Grok audit: Lowered to 5k for in-play focus - live sports games often have
    # lower individual market volume than politics but higher arb frequency.
    # rn1 captured many small in-play markets with tight spreads.
    min_volume_usd: float = 5_000  # Lowered from 10k to catch more in-play opps

    # Minimum orderbook depth in USD at best price level
    # Lowered to catch more opportunities with smaller depths
    min_depth_usd: float = 100  # Lowered from 150 for tighter markets

    # Maximum position size per trade as percentage of total capital
    # rn1 pattern: small frequent trades (~$27 avg)
    # Start conservative, scale with proven performance
    max_size_per_trade_percent: float = 5.0  # Reduced from 10%

    # rn1-style sizing: absolute cap on trade size
    # rn1 averaged $27/trade, max was ~$100-200
    # Start with cap, scale up as capital grows
    max_trade_size_usd: float = field(
        default_factory=lambda: float(os.getenv("MAX_TRADE_SIZE_USD", "50"))
    )

    # ABSOLUTE maximum trade size regardless of capital (safety cap per audit)
    # Even with $1M capital, never exceed this per trade
    # Per Grok Round 3: rn1 observed max was $129K - cap well below for safety
    absolute_max_trade_size_usd: float = 200.0

    # Per Grok Round 3: Hard cap at rn1's observed maximum ($129K from KuCoin data)
    # This is the lifetime max - never scale beyond this regardless of capital
    lifetime_max_trade_size_usd: float = 129_000.0

    # Minimum trade size (gas must be covered)
    min_trade_size_usd: float = 10.0

    # Maximum unhedged exposure before pausing new arbs (per audit)
    # If net exposure from partial fills exceeds this, stop taking new arbs
    max_unhedged_exposure_usd: float = field(
        default_factory=lambda: float(os.getenv("MAX_UNHEDGED_EXPOSURE_USD", "100"))
    )

    # Maker fill ratio target - pause taker orders if below this
    # rn1 preferred maker orders for fee rebates
    maker_ratio_target: float = field(
        default_factory=lambda: float(os.getenv("MAKER_RATIO_TARGET", "0.70"))
    )

    # Orderbook staleness threshold in milliseconds
    # Skip books older than this to avoid executing on stale data
    orderbook_max_age_ms: float = 200.0

    # Capital scaling: increase max_trade_size as capital grows
    # Formula: max_size = base_max * (current_capital / starting_capital) ^ scaling_factor
    # scaling_factor < 1 = conservative (recommended)
    capital_scaling_factor: float = field(
        default_factory=lambda: float(os.getenv("CAPITAL_SCALING_FACTOR", "0.5"))
    )

    # Base arbitrage threshold (will be adjusted dynamically with gas)
    # Buy arb: sum(asks) < 1 - dynamic_threshold
    # Sell arb: sum(bids) > 1 + dynamic_threshold
    # rn1 captured edges as low as 0.3%, median 0.8%
    arb_threshold_base: float = 0.002  # 0.2% base edge (tighter than before)

    # Buy arb only mode: skip sell arbs that require holding tokens
    # When True, only execute buy arbs (safer, no position requirements)
    buy_arb_only: bool = field(
        default_factory=lambda: os.getenv("BUY_ARB_ONLY", "false").lower() == "true"
    )

    # Post-only mode: try post-only limit orders first for maker rebates
    # Falls back to FOK market orders if post-only doesn't fill in timeout
    # Maker rebates add ~0.1-0.3% to edge
    # Per Grok audit: Changed default to true - rn1 preferred maker fills
    use_post_only: bool = field(
        default_factory=lambda: os.getenv("USE_POST_ONLY", "true").lower() == "true"
    )

    # Post-only for HEDGE legs specifically (profit-locking orders)
    # Hedges are less time-sensitive than primary arb legs, so can wait for maker rebates
    # Uses 30s timeout (vs 0.5s for primary) before FAK fallback
    use_post_only_hedges: bool = field(
        default_factory=lambda: os.getenv("USE_POST_ONLY_HEDGES", "true").lower() == "true"
    )

    # Post-only timeout in seconds before falling back to FAK
    # Per Grok audit: Increased from 0.5s to 1.0s for better maker fill chance
    post_only_timeout_seconds: float = 1.0

    # Post-only timeout for HEDGE orders (longer - hedges are less time-sensitive)
    post_only_hedge_timeout_seconds: float = 30.0

    # Price improvement for post-only orders (in cents/price units)
    # e.g., 0.01 = post 1 cent better than current best for more likely fills
    post_only_price_improvement: float = 0.01

    # Maker vs taker ratio tracking
    # Alert if maker fill ratio drops below this threshold
    maker_ratio_alert_threshold: float = 0.30  # Alert if <30% maker fills

    # Fixed gas buffer in USD (Polygon gas ~$0.01-0.05 per tx)
    gas_buffer_usd: float = 0.05

    # Polling interval for orderbooks in seconds
    # Reduced to 0.8s for faster arb detection during in-play bursts
    poll_interval_seconds: float = 0.8

    # Market list refresh interval in seconds
    market_refresh_interval_seconds: float = 300  # 5 minutes

    # Safety multiplier for position sizing (0.8 = use 80% of available depth)
    depth_safety_multiplier: float = 0.8

    # Price movement threshold for adding negative exposure (lock profits)
    # Lowered to 1.0% for faster profit locking like RN1
    profit_lock_threshold_percent: float = 1.0

    # Percentage of favorable move to lock (0.7 = lock 70%)
    profit_lock_ratio: float = 0.70

    # Maximum concurrent markets to monitor
    # Increased to 200 for broader coverage during active hours
    max_concurrent_markets: int = 200

    # Maximum priority markets to focus on (top N by arb frequency)
    max_priority_markets: int = 30

    # Burst mode poll interval (faster polling for hot markets)
    burst_poll_interval_seconds: float = 0.4

    # Rate limit backoff base delay in seconds
    rate_limit_backoff_base: float = 1.0

    # Maximum rate limit backoff in seconds
    rate_limit_backoff_max: float = 10.0

    # Maximum rate limit retries
    max_rate_limit_retries: int = 5

    # Stale orderbook detection: max deviation from sum=1
    stale_book_threshold: float = 0.05  # 5% deviation = stale

    # Slippage threshold for next level check
    slippage_threshold_percent: float = 5.0  # Skip if next level >5% worse

    # Post-fill monitoring timeout in seconds
    fill_monitor_timeout: float = 5.0

    # Partial fill threshold to trigger hedge (e.g., 0.1 = hedge if >10% unfilled)
    partial_fill_hedge_threshold: float = 0.10

    # Consecutive partial fills before pause
    max_consecutive_partials: int = 3

    # Partial pause duration in seconds
    partial_pause_duration: float = 600  # 10 minutes

    # Active trading hours (UTC) for optimized polling
    active_hours_start: int = 8   # 08:00 UTC
    active_hours_end: int = 23    # 23:00 UTC

    # Claim polling interval during active hours (seconds)
    claim_poll_interval_active: float = 30.0

    # Claim polling interval during inactive hours (seconds)
    claim_poll_interval_inactive: float = 120.0

    # Big arb alert threshold (>1.2% = rare, high-value opportunity)
    big_arb_alert_threshold: float = 0.012  # 1.2%

    # Profit threshold for alerts (as % of daily avg profit, e.g., 0.5% of daily avg)
    big_arb_profit_threshold_pct: float = 0.005  # 0.5% of daily avg profit triggers alert

    # Webhook URL for big arb alerts (optional, empty = disabled)
    # Supports Discord/Telegram webhook URLs
    big_arb_webhook_url: str = field(
        default_factory=lambda: os.getenv("BIG_ARB_WEBHOOK_URL", "")
    )

    # Burst parallel execution: minimum depth ratio (vs min_depth) to split across wallets
    # Default 2.0 = parallelize when depth >= 2x min_depth
    burst_parallel_depth_ratio: float = field(
        default_factory=lambda: float(os.getenv("BURST_PARALLEL_DEPTH_RATIO", "2.0"))
    )

    # Batch orderbook fetch size (max token_ids per request)
    # Polymarket batch /books endpoint: 500 req/10s limit
    # With ~100-200 token_ids per batch, 1 req/cycle is well under limits
    orderbook_batch_size: int = 200

    # WebSocket mode: use real-time orderbook updates instead of HTTP polling
    # Reduces latency from ~800ms to ~80ms for arb detection
    # Hybrid mode: WS for detection, HTTP for pre-execution verification
    use_websocket: bool = field(
        default_factory=lambda: os.getenv("USE_WEBSOCKET", "true").lower() == "true"
    )

    # WebSocket arb verification: verify WS-detected arbs via HTTP before execution
    # Prevents executing on stale WS data (adds ~100ms but increases reliability)
    ws_verify_before_execute: bool = True

    # MANDATORY WebSocket mode: require WS during active hours for competitive edge
    # If True and WS fails during active hours, bot will pause trading (not shutdown)
    # This prevents losing to faster competitors when we only have HTTP
    ws_required_active_hours: bool = field(
        default_factory=lambda: os.getenv("WS_REQUIRED_ACTIVE_HOURS", "true").lower() == "true"
    )

    # Grace period (seconds) to wait for WS reconnection before pausing
    ws_reconnect_grace_period: float = 30.0

    # How often to retry WS connection when paused (seconds)
    ws_retry_interval: float = 10.0

    # WebSocket disconnect alert threshold (seconds)
    # Logs warning if WS disconnected for >10s
    ws_disconnect_alert_seconds: float = 10.0

    # Check USDC MAX approval on startup and before execution batches
    check_usdc_max_approval: bool = True

    # IOC (Immediate or Cancel) mode for more fills
    # IOC allows partial fills unlike FOK, increasing trade frequency 2-5x
    use_ioc_orders: bool = field(
        default_factory=lambda: os.getenv("USE_IOC_ORDERS", "true").lower() == "true"
    )

    # Partial fill hedge slippage threshold (0.003 = 0.3%)
    # Only hedge partial fills if slippage is below this threshold
    partial_hedge_slippage_threshold: float = 0.003


@dataclass
class NetworkConfig:
    """Blockchain and API network configuration."""

    # Polygon mainnet chain ID - HARDCODED per Grok audit
    # Never allow override - wrong chain_id = lost funds
    chain_id: int = 137  # MUST be 137 for Polygon mainnet, DO NOT CHANGE

    # CLOB API endpoint
    clob_endpoint: str = "https://clob.polymarket.com"

    # Gamma API endpoint for market discovery
    gamma_endpoint: str = "https://gamma-api.polymarket.com"

    # Polygon RPC endpoint
    polygon_rpc: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

    # Contract addresses
    conditional_tokens_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    usdc_address: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

    # Exchange contract addresses (for approvals)
    exchange_addresses: List[str] = field(default_factory=lambda: [
        "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        "0xC5d563A36AE78145C45a50134d48A1215220f80a",
        "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
    ])

    # Primary USDC spender address for approvals (CTF Exchange)
    # This is the main contract that needs USDC allowance for trading
    usdc_spender_address: str = field(
        default_factory=lambda: os.getenv(
            "USDC_SPENDER_ADDRESS",
            "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # CTF Exchange
        )
    )


@dataclass
class WalletConfig:
    """Wallet and authentication configuration."""

    # Private key loaded from environment (primary wallet - EOA mode)
    private_key: str = field(default_factory=lambda: os.getenv("PRIVATE_KEY", ""))

    # Additional private keys for multi-wallet rotation (comma-separated in env)
    additional_private_keys: List[str] = field(default_factory=lambda: [
        pk.strip() for pk in os.getenv("ADDITIONAL_PRIVATE_KEYS", "").split(",")
        if pk.strip()
    ])

    # Funder address (proxy wallet address if using Magic/email login)
    funder_address: str = field(default_factory=lambda: os.getenv("FUNDER_ADDRESS", ""))

    # Signature type: 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
    signature_type: int = field(default_factory=lambda: int(os.getenv("SIGNATURE_TYPE", "0")))

    # Wallet rotation threshold (rotate after this USD volume per wallet)
    rotation_threshold_usd: float = field(
        default_factory=lambda: float(os.getenv("WALLET_ROTATION_THRESHOLD", "10000"))
    )

    # Primary proxy API credentials (for proxy wallet mode without private key)
    # Format: {"api_key": "...", "api_secret": "...", "api_passphrase": "..."}
    primary_api_creds: Optional[ProxyApiCreds] = field(default_factory=lambda: _parse_primary_api_creds())

    # Additional proxy API credentials for multi-session scaling
    # Format: JSON array of credential objects
    # ADDITIONAL_API_CREDS='[{"api_key":"...","api_secret":"...","api_passphrase":"...","funder_address":"0x..."},...]'
    additional_api_creds: List[ProxyApiCreds] = field(default_factory=lambda: _parse_additional_api_creds())

    @property
    def all_private_keys(self) -> List[str]:
        """Get all available private keys for rotation (EOA mode only)."""
        keys = []
        if self.private_key:
            keys.append(self.private_key)
        keys.extend(self.additional_private_keys)
        return keys

    @property
    def all_api_creds(self) -> List[ProxyApiCreds]:
        """Get all available API credentials for rotation (proxy mode)."""
        creds = []
        if self.primary_api_creds and self.primary_api_creds.is_valid():
            creds.append(self.primary_api_creds)
        creds.extend([c for c in self.additional_api_creds if c.is_valid()])
        return creds

    @property
    def wallet_count(self) -> int:
        """Get total number of available wallets (EOA + proxy)."""
        return len(self.all_private_keys) + len(self.all_api_creds)

    @property
    def is_proxy_mode(self) -> bool:
        """Check if running in proxy wallet mode (no private key, using API creds)."""
        return self.signature_type == 1 and not self.private_key

    @property
    def has_multi_wallet(self) -> bool:
        """Check if multiple wallets are available for rotation."""
        return self.wallet_count > 1


def _parse_primary_api_creds() -> Optional[ProxyApiCreds]:
    """Parse primary API credentials from environment."""
    api_key = os.getenv("API_KEY", "")
    api_secret = os.getenv("API_SECRET", "")
    api_passphrase = os.getenv("API_PASSPHRASE", "")
    funder_address = os.getenv("FUNDER_ADDRESS", "")

    if api_key and api_secret and api_passphrase:
        return ProxyApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            funder_address=funder_address
        )
    return None


def _parse_additional_api_creds() -> List[ProxyApiCreds]:
    """Parse additional API credentials from ADDITIONAL_API_CREDS JSON env var."""
    raw = os.getenv("ADDITIONAL_API_CREDS", "")
    if not raw:
        return []

    try:
        creds_list = json.loads(raw)
        if not isinstance(creds_list, list):
            return []

        return [ProxyApiCreds.from_dict(c) for c in creds_list if isinstance(c, dict)]
    except json.JSONDecodeError:
        return []


@dataclass
class SportsConfig:
    """Sports market discovery configuration."""

    # Priority sports series IDs for high-volume markets
    # Mapped from the Gamma API /sports endpoint
    priority_series: Dict[str, int] = field(default_factory=lambda: {
        # Football/Soccer
        "epl": 10188,      # English Premier League
        "lal": 10193,      # La Liga
        "bun": 10194,      # Bundesliga
        "fl1": 10195,      # Ligue 1
        "sea": 10203,      # Serie A
        "ucl": 10204,      # UEFA Champions League
        "uel": 10209,      # UEFA Europa League
        "mls": 10189,      # MLS
        "acn": 10786,      # African Cup of Nations

        # Basketball
        "nba": 10345,      # NBA
        "ncaab": 39,       # NCAA Basketball

        # American Football
        "nfl": 10187,      # NFL

        # Hockey
        "nhl": 10346,      # NHL

        # Tennis
        "atp": 10365,      # ATP
        "wta": 10366,      # WTA

        # Esports
        "cs2": 10310,      # CS2/CSGO
        "dota2": 10309,    # Dota 2
        "lol": 10311,      # League of Legends
        "val": 10369,      # Valorant

        # MMA/UFC
        "mma": 10500,      # MMA/UFC
    })

    # Tag ID for game bets filtering
    game_bets_tag_id: int = 100639

    # Estimated game durations in minutes for in-play detection
    game_durations: Dict[str, int] = field(default_factory=lambda: {
        "soccer": 120,     # 90 min + halftime + stoppage
        "basketball": 150, # NBA ~2.5 hours
        "football": 210,   # NFL ~3.5 hours
        "hockey": 150,     # NHL ~2.5 hours
        "tennis": 180,     # Variable, estimate 3 hours
        "esports": 90,     # Varies by game
        "mma": 30,         # Per fight ~30 min max
    })


@dataclass
class LoggingConfig:
    """Logging configuration."""

    # Log level
    level: str = os.getenv("LOG_LEVEL", "INFO")

    # Log file path
    log_file: str = os.getenv("LOG_FILE", "arb_bot.log")

    # Enable console logging
    console_logging: bool = True

    # Log format
    format: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


@dataclass
class BotConfig:
    """Main bot configuration aggregating all sub-configs."""

    trading: TradingConfig = field(default_factory=TradingConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    wallet: WalletConfig = field(default_factory=WalletConfig)
    sports: SportsConfig = field(default_factory=SportsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    # Starting capital for position sizing calculations
    # rn1 started with $1k - this is the minimum viable capital
    starting_capital_usd: float = field(
        default_factory=lambda: float(os.getenv("STARTING_CAPITAL", "1000"))
    )

    # Minimum capital required to start trading (hard block below this)
    # rn1 pattern: $1k start, conservative sizing. Below ~$900 is not viable.
    min_capital_required: float = 900.0

    # Dry run mode (no actual trades)
    dry_run: bool = field(
        default_factory=lambda: os.getenv("DRY_RUN", "true").lower() == "true"
    )


def _validate_private_key(pk: str, key_name: str = "private_key") -> None:
    """
    Validate a private key using eth_account.Account.from_key().

    This catches invalid curve points and other cryptographic issues
    that simple hex validation would miss.

    Args:
        pk: Private key string (with or without 0x prefix).
        key_name: Name for error messages.

    Raises:
        ValueError: If the private key is invalid.
    """
    if not pk:
        return

    # Remove 0x prefix if present for length check
    pk_clean = pk[2:] if pk.startswith("0x") else pk

    # Check length (64 hex chars = 32 bytes)
    if len(pk_clean) != 64:
        raise ValueError(f"Invalid {key_name} length: expected 64 hex chars, got {len(pk_clean)}")

    # Check if valid hex
    try:
        int(pk_clean, 16)
    except ValueError:
        raise ValueError(f"{key_name} must be a valid hex string")

    # Validate with eth_account - catches invalid curve points
    try:
        account = Account.from_key(pk)
        # Verify it's not the zero key
        if int(pk_clean, 16) == 0:
            raise ValueError(f"{key_name} cannot be zero")
    except Exception as e:
        raise ValueError(f"Invalid {key_name}: {e}")


def load_config() -> BotConfig:
    """Load and validate configuration."""
    config = BotConfig()

    # Per Grok audit: Enforce chain_id=137 for Polygon mainnet
    # Wrong chain_id = lost funds. This is non-negotiable.
    if config.network.chain_id != 137:
        raise ValueError(
            f"CRITICAL: chain_id must be 137 (Polygon mainnet), got {config.network.chain_id}. "
            f"Wrong chain_id will result in lost funds. This is hardcoded for safety."
        )

    # Check for valid authentication: either private key (EOA) or API creds (proxy)
    has_private_key = bool(config.wallet.private_key)
    has_api_creds = bool(config.wallet.all_api_creds)

    if not has_private_key and not has_api_creds and not config.dry_run:
        raise ValueError(
            "Live trading requires either PRIVATE_KEY (EOA) or "
            "API_KEY+API_SECRET+API_PASSPHRASE (proxy wallet)"
        )

    # Validate primary private key with Account.from_key() if provided
    if config.wallet.private_key:
        _validate_private_key(config.wallet.private_key, "PRIVATE_KEY")

    # Validate additional private keys for multi-wallet rotation
    for i, pk in enumerate(config.wallet.additional_private_keys):
        _validate_private_key(pk, f"ADDITIONAL_PRIVATE_KEYS[{i}]")

    # Validate API credentials format (funder_address required for trading)
    for i, creds in enumerate(config.wallet.all_api_creds):
        if not creds.funder_address:
            raise ValueError(
                f"API credentials [{i}] missing funder_address - "
                "required for balance checks and trading"
            )

    # Validate active hours
    if not (0 <= config.trading.active_hours_start < 24):
        raise ValueError("active_hours_start must be 0-23")
    if not (0 <= config.trading.active_hours_end < 24):
        raise ValueError("active_hours_end must be 0-23")

    return config


def calculate_dynamic_threshold(
    trade_size_usd: float,
    num_outcomes: int,
    config: TradingConfig
) -> float:
    """
    Calculate dynamic arbitrage threshold based on trade size and gas costs.

    Formula: threshold = base_threshold + (gas_buffer * num_outcomes) / trade_size

    Args:
        trade_size_usd: Planned trade size in USD.
        num_outcomes: Number of outcomes in the market.
        config: Trading configuration.

    Returns:
        Dynamic threshold as decimal (e.g., 0.012 for 1.2%).
    """
    if trade_size_usd <= 0:
        return 1.0  # Impossible threshold, will skip

    # Gas cost per outcome transaction
    total_gas_cost = config.gas_buffer_usd * num_outcomes

    # Dynamic threshold = base + gas drag
    gas_drag = total_gas_cost / trade_size_usd

    return config.arb_threshold_base + gas_drag


def calculate_rn1_style_size(
    current_capital: float,
    starting_capital: float,
    config: TradingConfig,
    depth_available: float = float('inf')
) -> float:
    """
    Calculate rn1-style position size with capital scaling.

    rn1 pattern:
    - Started with ~$1k, averaged $27/trade
    - Scaled up as capital grew (but stayed conservative)
    - Never went all-in, maintained high trade frequency

    Formula:
    base_size = min(max_trade_size_usd, capital * max_size_per_trade_percent)
    scaled_size = base_size * (current_capital / starting_capital) ^ scaling_factor

    Args:
        current_capital: Current available capital in USD.
        starting_capital: Initial capital for scaling reference.
        config: Trading configuration.
        depth_available: Available orderbook depth in USD.

    Returns:
        Optimal trade size in USD.
    """
    if current_capital <= 0 or starting_capital <= 0:
        return 0.0

    # Base size from percentage of capital
    pct_size = current_capital * (config.max_size_per_trade_percent / 100.0)

    # Cap at max_trade_size_usd
    base_size = min(pct_size, config.max_trade_size_usd)

    # CRITICAL: Hard-cap initial trade sizes to $30 until capital > $10k
    # Per audit: rn1 averaged $27/trade starting from $1k
    # Aggressive sizing too early = blowup risk on partials
    EARLY_STAGE_CAPITAL_THRESHOLD = 10000  # $10k
    EARLY_STAGE_MAX_SIZE = 30  # $30 hard cap

    if current_capital < EARLY_STAGE_CAPITAL_THRESHOLD:
        base_size = min(base_size, EARLY_STAGE_MAX_SIZE)

    # Apply capital scaling
    # As capital grows, we can increase size (but sub-linearly for safety)
    capital_ratio = current_capital / starting_capital
    if capital_ratio > 1.0:
        # Scale up with growth, but conservatively
        scaling_mult = capital_ratio ** config.capital_scaling_factor
        scaled_size = base_size * scaling_mult
    else:
        # Don't scale down if capital decreased (maintain same risk)
        scaled_size = base_size

    # Re-apply early stage cap after scaling (safety)
    if current_capital < EARLY_STAGE_CAPITAL_THRESHOLD:
        scaled_size = min(scaled_size, EARLY_STAGE_MAX_SIZE)

    # Apply depth constraint (safety multiplier already in depth_validator)
    final_size = min(scaled_size, depth_available)

    # Enforce minimum
    if final_size < config.min_trade_size_usd:
        return 0.0  # Skip trade if too small

    return final_size


def get_rn1_target_metrics(capital: float, days: int = 90) -> dict:
    """
    Get target metrics based on rn1's historical performance.

    rn1 pattern (Oct 2024 - Jan 2025):
    - $1k → $2M+ in ~90 days
    - ~15,000 trades total
    - Average edge: 0.8%
    - Daily geo return: ~2.5%

    Args:
        capital: Starting capital.
        days: Target number of days.

    Returns:
        Dict with target metrics.
    """
    # rn1's approximate daily geometric return
    rn1_daily_geo_return = 0.025  # 2.5%

    # Compound for target days
    target_multiple = (1 + rn1_daily_geo_return) ** days

    return {
        "rn1_daily_geo_return_pct": rn1_daily_geo_return * 100,
        "target_multiple": target_multiple,
        "target_capital": capital * target_multiple,
        "trades_per_day_target": 167,  # 15k trades / 90 days
        "avg_trade_size_target": 27.0,  # rn1's average
        "avg_edge_pct": 0.8,
        "days": days
    }
