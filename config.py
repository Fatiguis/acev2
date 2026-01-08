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
    # Per Grok Round 23 CRITICAL: rn1's $27 avg trade size means they captured
    # MANY small markets that higher thresholds miss. Dune shows rn1 traded
    # markets as low as $100-500 volume during live games.
    # Focus: Soccer 3-way (Home Win / Draw / Away Win), NBA, NFL, Tennis
    # $5k was missing 80% of rn1's opportunity set - lowered to $500
    min_volume_usd: float = 500  # Per Grok Round 23: rn1 traded tiny live markets

    # Minimum orderbook depth in USD at best price level
    # Per Grok Round 24: $20 was blocking tennis/small markets with $0.04-$10 depths
    # rn1 traded $27 avg → need depth >= trade_size, not flat $20
    # Set to $5 to allow $10 min trades with safety margin
    min_depth_usd: float = 5  # Lowered from $20 - tennis markets have thin books

    # Maximum position size per trade as percentage of total capital
    # rn1 pattern: small frequent trades (~$27 avg)
    # Start conservative, scale with proven performance
    max_size_per_trade_percent: float = 5.0  # Reduced from 10%

    # rn1-style sizing: absolute cap on trade size
    # Per RN1 7-Day Analysis (Jan 2026): RN1 avg=$172, median=$29, max=$6,919
    # RN1 uses DYNAMIC sizing: 34% trades <$10, 3.3% trades >$1,000
    # Key insight: RN1 scales with confidence, not fixed amounts
    # Increased from $100 to $200 to capture more value on high-edge arbs
    max_trade_size_usd: float = field(
        default_factory=lambda: float(os.getenv("MAX_TRADE_SIZE_USD", "200"))
    )

    # ABSOLUTE maximum trade size regardless of capital (safety cap per audit)
    # Per RN1 7-Day Analysis: RN1 max trade was $6,919 - very aggressive on high-edge
    # Allow up to $1,000 for larger capital accounts to capture high-confidence arbs
    absolute_max_trade_size_usd: float = field(
        default_factory=lambda: float(os.getenv("ABSOLUTE_MAX_TRADE_SIZE_USD", "1000"))
    )

    # Per Grok Round 3: Hard cap at rn1's observed maximum ($129K from KuCoin data)
    # This is the lifetime max - never scale beyond this regardless of capital
    lifetime_max_trade_size_usd: float = 129_000.0

    # Minimum trade size (gas must be covered)
    min_trade_size_usd: float = 10.0

    # ==========================================================================
    # SHARE-BASED SIZING (Per RN1 Analysis - Jan 2026)
    # ==========================================================================
    #
    # MATHEMATICAL FOUNDATION:
    # For arbitrage, profit = shares × edge, where edge = 1 - sum(ask_prices)
    # Example: sum_asks = $0.90 → edge = 10%
    #   - 100 shares: profit = 100 × $0.10 = $10
    #   - 1000 shares: profit = 1000 × $0.10 = $100
    #
    # RN1 DATA (6-month analysis):
    #   - Top trade: 38,209 shares @ $0.05 = $1,764 cost → $26,854 profit
    #   - Pattern: More shares on low prices (same USD = more shares)
    #   - Median trade: $28.97, but median SHARES varies by price
    #
    # POLYMARKET CONSTRAINT: Minimum 5 shares per order (API enforced)
    # ==========================================================================

    # Minimum shares per order leg (Polymarket API requirement)
    # Orders with < 5 shares are rejected with 400 Bad Request
    min_shares_per_order: float = 5.0

    # ==========================================================================
    # RN1 SIZING STRATEGY (Corrected - Jan 2026)
    # ==========================================================================
    #
    # RN1's ACTUAL pattern from 6-month data analysis:
    # - Small trades (<$100): 92.2% win rate, 268% ROI → HIGH edge
    # - Large trades ($1k+): 72.7% win rate, 3% ROI → LOW edge
    #
    # KEY INSIGHT: RN1 uses INVERSE sizing:
    # - SMALLER bets on high-edge (uncertain, probe the market)
    # - LARGER bets on low-edge (near-certain arb, capture more)
    #
    # This aligns with Kelly criterion: high edge often = high variance
    # ==========================================================================

    # Base trade size in USD (RN1 median: $28.97)
    base_trade_size_usd: float = field(
        default_factory=lambda: float(os.getenv("BASE_TRADE_SIZE_USD", "30.0"))
    )

    # Edge sensitivity for inverse scaling
    # Higher value = more aggressive size reduction on high edge
    # At sensitivity=10: 5% edge → 0.77x, 10% edge → 0.56x
    edge_sensitivity: float = field(
        default_factory=lambda: float(os.getenv("EDGE_SENSITIVITY", "10.0"))
    )

    # Minimum edge multiplier (floor for size reduction)
    # Even at very high edge, don't go below this fraction of base
    min_edge_multiplier: float = 0.2  # 20% of base minimum

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
    # Per RN1 Round 28 Deep Analysis (10,000 trades):
    # - RN1's median edge: 5.26% (not 0.8% as previously thought)
    # - RN1 takes edges from 2-65% (!!) - very aggressive
    # - 56.4% of trades on positive-edge markets
    # - RN1 is NOT pure arb - takes directional bets during live games
    # For our pure arb approach, use 2% base (catches good opportunities)
    arb_threshold_base: float = 0.02  # 2% base edge - RN1's lower bound

    # Per Grok Round 26: Polymarket tick sizes change at price extremes
    # Standard tick: 0.01 (1 cent) for prices 0.04 - 0.96
    # Fine tick: 0.001 (0.1 cent) for prices <0.04 or >0.96
    # Must use correct tick size or orders will be rejected
    tick_size_standard: float = 0.01  # 1 cent tick for normal prices
    tick_size_fine: float = 0.001  # 0.1 cent tick for extreme prices
    tick_boundary_low: float = 0.04  # Below this, use fine tick
    tick_boundary_high: float = 0.96  # Above this, use fine tick

    # Buy arb only mode: skip sell arbs that require holding tokens
    # When True, only execute buy arbs (safer, no position requirements)
    # Per Grok Round 12: Default False - enable both BUY and SELL arbs
    # Most detected arbs are SELL (sum_bids > 1.0) so blocking them loses opportunities
    buy_arb_only: bool = field(
        default_factory=lambda: os.getenv("BUY_ARB_ONLY", "false").lower() == "true"
    )

    # Post-only mode: try post-only limit orders first for maker rebates
    # Per RN1 Round 28 Deep Analysis: RN1 is NOT primarily a maker!
    # - 90.2% of trades in SAME SECOND bursts (aggressive taker sweeps)
    # - 88.2% of bursts = same side, sweeping multiple price levels
    # - RN1 takes liquidity fast, doesn't wait for fills
    # CHANGED: Default False - FOK first for speed like RN1
    # Post-only adds latency that lets faster bots capture the arb
    use_post_only: bool = field(
        default_factory=lambda: os.getenv("USE_POST_ONLY", "false").lower() == "true"
    )

    # Post-only for HEDGE legs specifically (profit-locking orders)
    # Hedges are less time-sensitive than primary arb legs, so can wait for maker rebates
    # Uses 30s timeout (vs 0.5s for primary) before FAK fallback
    use_post_only_hedges: bool = field(
        default_factory=lambda: os.getenv("USE_POST_ONLY_HEDGES", "true").lower() == "true"
    )

    # Post-only timeout in seconds before falling back to FAK
    # Per Grok Round 7: 500ms for arb legs - balance between maker fills and speed
    # Too short = miss maker rebates, too long = lose arb edge to other traders
    post_only_timeout_seconds: float = 0.5

    # Post-only timeout for HEDGE orders (longer - hedges are less time-sensitive)
    post_only_hedge_timeout_seconds: float = 30.0

    # Per RN1 Round 28: Batch order placement for arb legs
    # Polymarket POST /orders endpoint allows up to 15 orders per request
    # Reduces latency by ~50-80% vs sequential placement
    # RN1 achieves 18+ trades/second during bursts - batch is essential
    use_batch_orders: bool = field(
        default_factory=lambda: os.getenv("USE_BATCH_ORDERS", "true").lower() == "true"
    )

    # Price improvement for post-only orders (in cents/price units)
    # Per Grok Round 25: Increased from 0.01 to 0.02 - "no match" errors indicate
    # we're hitting stale prices. rn1 used aggressive improvement to beat competition.
    # Formula: post_price = best_ask + improvement (for BUY)
    # 0.02 = 2 cents improvement baseline, scaled up in hot markets
    post_only_price_improvement: float = 0.02

    # Maker vs taker ratio tracking
    # Alert if maker fill ratio drops below this threshold
    maker_ratio_alert_threshold: float = 0.30  # Alert if <30% maker fills

    # Per Grok Round 8: Maker rebate rate for PnL tracking
    # Per Grok Round 10: Polymarket main 0% fees as of Jan 2026 (docs confirmed)
    # Historical: ~0.02% (2 bps) maker rebate - no longer applicable
    # Keep field for future fee changes but set to 0.0
    maker_rebate_rate: float = 0.0  # 0% - Polymarket 0% fees (Jan 2026)

    # Fixed gas buffer in USD (Polygon gas ~$0.01-0.05 per tx)
    # Per RN1 Round 28: For proxy wallets (SIGNATURE_TYPE=1), gas is FREE
    # Polymarket's relayer pays gas. Only EOA wallets (SIGNATURE_TYPE=0) pay gas.
    # This is now handled dynamically in calculate_dynamic_threshold()
    # Set low default since most users should use proxy wallets
    gas_buffer_usd: float = 0.01  # Reduced from $0.05 - proxy wallets = $0 gas

    # Polling interval for orderbooks in seconds
    # Reduced to 0.8s for faster arb detection during in-play bursts
    poll_interval_seconds: float = 0.8

    # Market list refresh interval in seconds
    market_refresh_interval_seconds: float = 300  # 5 minutes

    # Safety multiplier for position sizing (0.8 = use 80% of available depth)
    depth_safety_multiplier: float = 0.8

    # Price movement threshold for adding negative exposure (lock profits)
    # Per RN1 Round 29: RN1 locks at ~3% edge (Wong+Diallo = 0.97 cost = 3% profit)
    # Tennis has wider spreads - can capture more edge before locking
    # Increased from 1.0% to 2.5% to match RN1's observed behavior
    profit_lock_threshold_percent: float = 2.5

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

    # Per Grok Round 23: Kelly mode toggle for variance-aware position sizing
    # When True: f* = max(0, (edge - fees) / variance) - smooth compounding like rn1
    # When False: Fixed percentage sizing (legacy mode)
    # rn1's smooth curve from $1k to $2M came from variance-aware Kelly
    use_kelly_sizing: bool = field(
        default_factory=lambda: os.getenv("USE_KELLY_SIZING", "true").lower() == "true"
    )

    # Kelly sizing safety multiplier (fractional Kelly)
    # 0.5 = half-Kelly (conservative), 1.0 = full Kelly (aggressive)
    # rn1 likely used 0.25-0.5 (ultra-conservative) given smooth compounding
    kelly_fraction: float = field(
        default_factory=lambda: float(os.getenv("KELLY_FRACTION", "0.5"))
    )

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

    # Per Grok Round 21: Webhook URL for volume farming stats (airdrop tracking)
    # Reports: total volume, trash trades, projected airdrop equity, volume:cost ratio
    # Sends periodic updates every N trash trades (default: every 10 trades)
    volume_farming_webhook_url: str = field(
        default_factory=lambda: os.getenv("VOLUME_FARMING_WEBHOOK_URL", "")
    )

    # How often to send volume farming webhook (every N trash trades)
    volume_farming_webhook_interval: int = field(
        default_factory=lambda: int(os.getenv("VOLUME_FARMING_WEBHOOK_INTERVAL", "10"))
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

    # Per RN1 7-Day Analysis: Micro-arb mode for edges 1-2%
    # RN1 captures 12% of arbs with edge <2% (small but frequent)
    # Enable micro-arb mode to capture these with smaller position sizes
    use_micro_arb_mode: bool = field(
        default_factory=lambda: os.getenv("USE_MICRO_ARB_MODE", "true").lower() == "true"
    )

    # Micro-arb threshold: minimum edge to consider (below main threshold)
    # Main threshold is 2%, micro-arb captures 1-2% edges
    micro_arb_threshold: float = field(
        default_factory=lambda: float(os.getenv("MICRO_ARB_THRESHOLD", "0.01"))
    )

    # Micro-arb max size: smaller trades for lower-edge opportunities
    # Per RN1: Small edges get small sizes ($10-20 range)
    micro_arb_max_size_usd: float = field(
        default_factory=lambda: float(os.getenv("MICRO_ARB_MAX_SIZE_USD", "25"))
    )


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

    # ==================== SIGNATURE_TYPE: CRITICAL FOR ZERO GAS ====================
    # Per RN1 Round 28 & Polymarket Docs:
    #
    # SIGNATURE_TYPE=0 (EOA - Default):
    #   - Uses your private key directly
    #   - YOU PAY GAS (~$0.01-0.05 per tx on Polygon)
    #   - Requires MATIC in wallet for gas
    #   - Set: SIGNATURE_TYPE=0, PRIVATE_KEY=your_key
    #
    # SIGNATURE_TYPE=1 (POLY_PROXY - Recommended for RN1 strategy):
    #   - Uses Polymarket's proxy wallet (relayer)
    #   - ZERO GAS - Polymarket's relayer pays all gas!
    #   - How to get proxy wallet:
    #     1. Login to polymarket.com with email/Magic
    #     2. Go to Profile > Export API Keys
    #     3. Copy your API_KEY, API_SECRET, API_PASSPHRASE
    #     4. Your FUNDER_ADDRESS = your proxy wallet address (shown in Polymarket UI)
    #   - Set: SIGNATURE_TYPE=1, API_KEY=..., API_SECRET=..., API_PASSPHRASE=..., FUNDER_ADDRESS=0x...
    #
    # RN1 PROFITABILITY NOTE:
    # RN1's avg $27 trades + tiny margins (2-3%) - gas would DESTROY profits on EOA.
    # Proxy wallet = $0 gas = every trade is pure edge.
    # =============================================================================
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

        # Tennis - Per RN1 Round 29: 95% of RN1's 5-hour trades were tennis
        # Tennis markets have: binary outcomes, high in-play volatility, thin books
        "atp": 10365,      # ATP
        "wta": 10366,      # WTA

        # Esports
        "cs2": 10310,      # CS2/CSGO
        "dota2": 10309,    # Dota 2
        "lol": 10311,      # League of Legends
        "val": 10369,      # Valorant
    })

    # Tag ID for game bets filtering
    game_bets_tag_id: int = 100639

    # Per RN1 Round 29: Tennis priority boost for market scoring
    # RN1 traded 95% tennis in analyzed 5-hour sample - they dominate tennis arb
    # Tennis keywords: "open", "international", "classic", "atp", "wta"
    # Multiplier applied to market score for tennis events
    tennis_priority_boost: float = field(
        default_factory=lambda: float(os.getenv("TENNIS_PRIORITY_BOOST", "2.0"))
    )

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
    # Per Grok Round 26: Dynamic capital - set from actual wallet balance at init
    # If env var set, use that as initial estimate; otherwise will be updated from balance
    # Set to 0 to auto-detect from wallet balance
    starting_capital_usd: float = field(
        default_factory=lambda: float(os.getenv("STARTING_CAPITAL", "0"))
    )

    # Minimum capital required to start trading
    # Per RN1 Round 28: Lowered to $5 (Polymarket minimum = 5 shares ≈ $2.50-5)
    # RN1 trades 33.6% of orders at $0-5 - allow minimum viable trades
    # Any balance >= $5 can execute at least one trade
    min_capital_required: float = 5.0

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
    config: TradingConfig,
    latency_ms: float = 80.0,
    is_ws_connected: bool = True,
    is_proxy_wallet: bool = False
) -> float:
    """
    Per Grok Round 22: Calculate dynamic arbitrage threshold based on trade size,
    gas costs, and latency.

    Formula: threshold = base + gas_drag + latency_penalty

    With good WS (<100ms), chase tighter 0.3-0.5% edges like rn1.
    With HTTP only (>500ms), require larger edge to compensate for stale data.

    Per Grok Round 26: Proxy wallets have ZERO gas costs (Polymarket relayer pays).
    This was causing 5%+ thresholds on small trades, blocking valid arbs.

    Args:
        trade_size_usd: Planned trade size in USD.
        num_outcomes: Number of outcomes in the market.
        config: Trading configuration.
        latency_ms: Current latency in milliseconds (WS ~50-80ms, HTTP ~500-800ms).
        is_ws_connected: True if WebSocket is connected.
        is_proxy_wallet: True if using proxy wallet (SIGNATURE_TYPE=1). Gas is free.

    Returns:
        Dynamic threshold as decimal (e.g., 0.012 for 1.2%).
    """
    if trade_size_usd <= 0:
        return 1.0  # Impossible threshold, will skip

    # Per Grok Round 26: Proxy wallets have ZERO gas costs - Polymarket relayer pays
    # EOA wallets pay ~$0.01-0.05 per tx on Polygon
    if is_proxy_wallet:
        total_gas_cost = 0.0
    else:
        total_gas_cost = config.gas_buffer_usd * num_outcomes

    # Dynamic threshold = base + gas drag
    gas_drag = total_gas_cost / trade_size_usd
    base_threshold = config.arb_threshold_base + gas_drag

    # Per Grok Round 22: Latency-based threshold adjustment
    # rn1 pattern: With fast WS, chase 0.3-0.5% edges
    # With slow HTTP, require 1.5%+ edge to compensate for stale data
    #
    # Formula: latency_penalty = λ * latency_ms / 1000
    # Where λ = 0.01 (1% penalty per second of latency)
    LATENCY_PENALTY_COEFFICIENT = 0.01  # 1% per second

    if is_ws_connected and latency_ms < 200:
        # Good WS connection - can chase tighter edges
        # Reduce threshold slightly for fast execution
        latency_bonus = max(0, 0.002 - (latency_ms / 50000))  # Up to 0.2% bonus
        threshold = max(0.003, base_threshold - latency_bonus)  # Floor at 0.3%
    else:
        # HTTP fallback or high latency - add penalty
        latency_penalty = LATENCY_PENALTY_COEFFICIENT * (latency_ms / 1000)
        threshold = base_threshold + latency_penalty

    return threshold


def calculate_rn1_style_size(
    current_capital: float,
    starting_capital: float,
    config: TradingConfig,
    depth_available: float = float('inf'),
    edge_pct: float = 0.02
) -> float:
    """
    Calculate rn1-style position size with capital scaling and edge-based sizing.

    Per RN1 7-Day Analysis (Jan 2026):
    - RN1 uses DYNAMIC sizing: $1-$6,919 range
    - 34% of trades are <$10 (low-confidence probes)
    - 3.3% of trades are >$1,000 (high-confidence big bets)
    - Key insight: Size scales with edge confidence

    Formula:
    base_size = min(max_trade_size_usd, capital * max_size_per_trade_percent)
    edge_multiplier = min(3.0, max(0.3, edge_pct / 0.02))  # Scale with edge
    scaled_size = base_size * edge_multiplier * capital_scaling

    Args:
        current_capital: Current available capital in USD.
        starting_capital: Initial capital for scaling reference.
        config: Trading configuration.
        depth_available: Available orderbook depth in USD.
        edge_pct: Arbitrage edge as decimal (e.g., 0.05 for 5%).

    Returns:
        Optimal trade size in USD.
    """
    if current_capital <= 0 or starting_capital <= 0:
        return 0.0

    # Base size from percentage of capital
    pct_size = current_capital * (config.max_size_per_trade_percent / 100.0)

    # Cap at max_trade_size_usd
    base_size = min(pct_size, config.max_trade_size_usd)

    # Per RN1 7-Day Analysis: Dynamic edge-based sizing
    # RN1 trades small on low edge (<3%), big on high edge (>5%)
    # Edge multiplier: 0.3x at 1% edge, 1.0x at 2% edge, 2.5x at 5% edge, 3.0x cap at 6%+
    # This matches RN1's pattern: median $29 but mean $172 (skewed by big high-edge trades)
    if edge_pct <= 0.01:
        edge_multiplier = 0.3  # Low edge = small probe trade
    elif edge_pct <= 0.02:
        edge_multiplier = 0.3 + (edge_pct - 0.01) * 70  # 0.3 to 1.0 linear
    elif edge_pct <= 0.05:
        edge_multiplier = 1.0 + (edge_pct - 0.02) * 50  # 1.0 to 2.5 linear
    else:
        edge_multiplier = min(3.0, 2.5 + (edge_pct - 0.05) * 10)  # Cap at 3.0x

    # Apply edge multiplier to base size
    edge_adjusted_size = base_size * edge_multiplier

    # CRITICAL: Hard-cap initial trade sizes until capital > $5k
    # Per RN1 7-Day: RN1 now has large capital, but early stage needs protection
    # Lowered threshold from $10k to $5k to allow faster scaling
    EARLY_STAGE_CAPITAL_THRESHOLD = 5000  # $5k
    EARLY_STAGE_MAX_SIZE = 50  # $50 hard cap (increased from $30)

    if current_capital < EARLY_STAGE_CAPITAL_THRESHOLD:
        edge_adjusted_size = min(edge_adjusted_size, EARLY_STAGE_MAX_SIZE)

    # Apply capital scaling
    # As capital grows, we can increase size (but sub-linearly for safety)
    capital_ratio = current_capital / starting_capital
    if capital_ratio > 1.0:
        # Scale up with growth, but conservatively
        scaling_mult = capital_ratio ** config.capital_scaling_factor
        scaled_size = edge_adjusted_size * scaling_mult
    else:
        # Don't scale down if capital decreased (maintain same risk)
        scaled_size = edge_adjusted_size

    # Re-apply early stage cap after scaling (safety)
    if current_capital < EARLY_STAGE_CAPITAL_THRESHOLD:
        scaled_size = min(scaled_size, EARLY_STAGE_MAX_SIZE)

    # Apply depth constraint (safety multiplier already in depth_validator)
    final_size = min(scaled_size, depth_available)

    # Apply absolute maximum cap
    final_size = min(final_size, config.absolute_max_trade_size_usd)

    # Enforce minimum
    if final_size < config.min_trade_size_usd:
        return 0.0  # Skip trade if too small

    return final_size


def calculate_share_based_size(
    config: TradingConfig,
    edge_pct: float,
    avg_price: float,
    capital: float,
    depth_shares: float = float('inf')
) -> tuple[float, float]:
    """
    Calculate optimal trade size using share-based strategy.

    CORRECTED MATHEMATICAL FOUNDATION (Per RN1 6-Month Analysis):
    ==============================================================

    RN1's ACTUAL Pattern (contrary to naive "more edge = more size"):
    -----------------------------------------------------------------
    | Size Tier    | Win Rate | ROI   | RN1's Strategy              |
    |--------------|----------|-------|----------------------------|
    | Small <$100  | 92.2%    | +268% | High edge, SMALL size      |
    | Medium $100-1k| 84.9%   | +62%  | Medium edge, medium size   |
    | Large $1k+   | 72.7%    | +3%   | Low edge, LARGE size       |

    KEY INSIGHT: RN1 uses INVERSE sizing - smaller bets on high-edge
    opportunities (which have higher uncertainty), larger bets on
    low-edge "sure things" (closer to guaranteed arb).

    WHY THIS MAKES SENSE:
    - High edge (>10%) = market is inefficient = higher uncertainty
    - Low edge (2-3%) = tight spread = more certain to fill
    - Kelly criterion naturally produces this: f* = edge / variance
    - High edge often has high variance → smaller Kelly fraction

    For PURE ARBITRAGE (sum < 1.0):
    - Profit = Shares × Edge (linear relationship)
    - But fill probability DECREASES with size (market impact)
    - Optimal: Size to capture edge without moving market

    Args:
        config: Trading configuration.
        edge_pct: Arbitrage edge as decimal (e.g., 0.05 for 5%).
        avg_price: Average price per share across legs.
        capital: Current trading capital.
        depth_shares: Available depth in shares at best price.

    Returns:
        Tuple of (target_shares, target_usd).
    """
    # =======================================================================
    # RN1 CORE STRATEGY: DEPTH-FIRST SIZING (Per Jan 2026 Heavy Analysis)
    # =======================================================================
    # KEY INSIGHT: RN1's decimal shares (8.1, 5.3191, 350.11) come from
    # ORDERBOOK DEPTH, not from USD calculations!
    #
    # RN1's approach:
    #   1. Look at available depth at best ask
    #   2. Size = min(target_shares, available_depth * utilization)
    #   3. The decimals preserve the EXACT orderbook depth
    #
    # Example: Orderbook has 8.1 shares at $0.06 → RN1 buys 8.1 shares
    # =======================================================================

    MIN_SHARES = config.min_shares_per_order  # 5.0

    # =======================================================================
    # STEP 1: Calculate target shares from USD budget
    # =======================================================================
    # Base USD target (RN1 median at low edge: $45.64)
    base_usd = 50.0

    # Edge-based scaling (inverse: high edge = smaller size)
    BASE_EDGE = 0.02
    EDGE_SENSITIVITY = 8.0

    if edge_pct > BASE_EDGE:
        edge_excess = edge_pct - BASE_EDGE
        edge_multiplier = 1.0 / (1.0 + EDGE_SENSITIVITY * edge_excess)
        edge_multiplier = max(0.2, edge_multiplier)
    else:
        edge_multiplier = 1.0

    budget_usd = base_usd * edge_multiplier

    # Capital constraint
    max_from_capital = capital * (config.max_size_per_trade_percent / 100)
    budget_usd = min(budget_usd, max_from_capital, config.absolute_max_trade_size_usd)

    # Convert to shares
    target_shares_from_budget = budget_usd / avg_price if avg_price > 0 else MIN_SHARES

    # =======================================================================
    # STEP 2: DEPTH-FIRST - Use EXACT depth if it's the limiting factor
    # =======================================================================
    # This is the KEY difference from before!
    # RN1's decimals come from taking EXACT orderbook depth
    #
    # Jan 2026 Analysis: RN1 takes 100% of depth, NOT 80%
    # Evidence: 8.1 shares appears 15 times EXACTLY at $0.06
    # If 80%: 8.1 = 80% of 10.125 (unlikely to repeat exactly)
    # If 100%: 8.1 IS the exact depth (matches repetition pattern)
    DEPTH_UTILIZATION = 1.0

    if depth_shares > 0 and depth_shares < float('inf'):
        # Depth is known - use EXACT depth (this creates the decimals!)
        depth_limited_shares = depth_shares * DEPTH_UTILIZATION

        if depth_limited_shares < target_shares_from_budget:
            # DEPTH IS THE LIMIT - use exact depth (this creates the decimals!)
            final_shares = depth_limited_shares
        else:
            # Budget is the limit - use calculated shares
            final_shares = target_shares_from_budget
    else:
        # No depth info - use budget-based shares
        final_shares = target_shares_from_budget

    # =======================================================================
    # STEP 3: Ensure minimum shares (Polymarket requires 5)
    # =======================================================================
    if final_shares < MIN_SHARES:
        final_shares = MIN_SHARES

    # Calculate final USD
    target_usd = final_shares * avg_price if avg_price > 0 else MIN_SHARES * 0.50

    return final_shares, target_usd


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
