"""
Execution Module.
Handles order placement and execution for arbitrage opportunities.

Per Grok Round 6: py-clob-client is synchronous (requests-based). Direct calls
block the asyncio event loop. This module uses run_sync_in_thread() to execute
CLOB client calls in a thread pool, keeping the event loop responsive for
WebSocket updates and other async tasks.

Per Grok Round 20 CRITICAL: Post-only MUST be primary strategy (rn1 pattern).
rn1 dominated via maker rebates (0% taker fees now but post-only = higher fill priority).
Strategy: Post-only primary → FOK fallback ONLY on post-only timeout.
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from config import BotConfig
from orderbook import ArbOpportunity, ArbType
from clob_client_patch import run_sync_in_thread

logger = logging.getLogger(__name__)

# Per Grok Round 17: FAK (Fill-And-Kill) removed from py-clob-client
# Per Grok Round 20: Actually FAK EXISTS in py-clob-client 0.34.1+
# Polymarket docs: FAK = Fill-And-Kill (partial fill + cancel rest = IOC equivalent)
# BUT rn1's edge came from MAKER rebates, so we still prioritize post-only
#
# Current py-clob-client (v0.34.1) supports: GTC, FOK, FAK, GTD
# Strategy: Post-Only primary (maker rebates) → FOK fallback (NOT FAK - we want all-or-nothing)
# FAK allows partial fills which leaves exposure - rn1 avoided this via post-only
HAS_FAK = hasattr(OrderType, 'FAK')
logger.info(f"Order types available: GTC, FOK, FAK={'Yes' if HAS_FAK else 'No'} - using Post-Only + FOK strategy (rn1 pattern)")

# Idempotency tracking for preventing duplicate arbs on restart
from collections import OrderedDict
import hashlib
import time

class IdempotencyTracker:
    """Track recent arb IDs to prevent duplicates on restart/reconnect."""

    def __init__(self, ttl_seconds: int = 1800):
        # Per Grok Round 21: TTL increased to 1800s (30 min) from 300s
        # Supervisor restarts or WS reconnects can re-detect slow-decaying arbs
        # Sports events can have arbs persist for 10+ minutes in low-liquidity windows
        self.ttl_seconds = ttl_seconds
        self._recent_arbs: OrderedDict[str, float] = OrderedDict()  # arb_id -> timestamp
        self._max_entries = 10000  # Per Grok Round 21: Increased from 1000 for 100+ arbs/day

    def _generate_arb_id(self, market_id: str, profit_margin: float, trade_size: float) -> str:
        """Generate unique arb ID from market + margin + size."""
        # Round margin and size to avoid float precision issues
        margin_key = f"{profit_margin:.5f}"
        size_key = f"{trade_size:.2f}"
        raw = f"{market_id}|{margin_key}|{size_key}"
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def is_duplicate(self, market_id: str, profit_margin: float, trade_size: float) -> bool:
        """Check if this arb was recently executed."""
        self._cleanup_expired()
        arb_id = self._generate_arb_id(market_id, profit_margin, trade_size)
        return arb_id in self._recent_arbs

    def record_execution(self, market_id: str, profit_margin: float, trade_size: float):
        """Record an arb execution."""
        self._cleanup_expired()
        arb_id = self._generate_arb_id(market_id, profit_margin, trade_size)
        self._recent_arbs[arb_id] = time.time()

        # Trim if too many entries
        while len(self._recent_arbs) > self._max_entries:
            self._recent_arbs.popitem(last=False)

    def _cleanup_expired(self):
        """Remove expired entries."""
        now = time.time()
        cutoff = now - self.ttl_seconds

        # Remove expired entries from the front (oldest first due to OrderedDict)
        while self._recent_arbs:
            oldest_id, oldest_time = next(iter(self._recent_arbs.items()))
            if oldest_time < cutoff:
                del self._recent_arbs[oldest_id]
            else:
                break  # Rest are newer

    def get_stats(self) -> dict:
        """Get tracker stats."""
        self._cleanup_expired()
        return {
            "tracked_arbs": len(self._recent_arbs),
            "ttl_seconds": self.ttl_seconds
        }

# ERC20 ABI for allowance check and approve
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    }
]

# Max uint256 for unlimited approval
MAX_UINT256 = 2**256 - 1


class OrderStatus(Enum):
    """Order execution status."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class OrderResult:
    """Result of an order execution."""
    token_id: str
    outcome_name: str
    side: str  # "BUY" or "SELL"
    requested_size_usd: float
    executed_size_usd: float = 0.0
    unfilled_size_usd: float = 0.0  # For partial fill tracking
    price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    order_id: Optional[str] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def fill_ratio(self) -> float:
        """Get fill ratio (0-1)."""
        if self.requested_size_usd > 0:
            return self.executed_size_usd / self.requested_size_usd
        return 0.0

    @property
    def is_partial(self) -> bool:
        """Check if order was partially filled."""
        return 0 < self.fill_ratio < 1.0


@dataclass
class ExecutionResult:
    """Result of executing an arbitrage opportunity."""
    opportunity: ArbOpportunity
    orders: List[OrderResult] = field(default_factory=list)
    hedge_orders: List[OrderResult] = field(default_factory=list)  # Orders placed to hedge partials
    total_cost_usd: float = 0.0
    expected_return_usd: float = 0.0
    realized_profit_usd: float = 0.0
    is_complete: bool = False
    has_partial_fill: bool = False
    was_hedged: bool = False  # True if partial fill required hedging
    execution_time_ms: float = 0.0  # Time from start to completion
    post_execution_exposure: float = 0.0  # Net exposure after execution
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def success(self) -> bool:
        """Check if execution was successful."""
        return self.is_complete and all(
            o.status in (OrderStatus.FILLED, OrderStatus.SUBMITTED) for o in self.orders
        )

    @property
    def partial_exposure_usd(self) -> float:
        """Calculate directional exposure from partial fills."""
        if not self.has_partial_fill:
            return 0.0
        # Sum of unfilled portions
        return sum(o.unfilled_size_usd for o in self.orders)

    @property
    def hedge_success(self) -> bool:
        """Check if hedge orders succeeded."""
        if not self.hedge_orders:
            return True  # No hedge needed
        return all(o.status == OrderStatus.FILLED for o in self.hedge_orders)

    def to_log_dict(self) -> Dict[str, Any]:
        """Convert to dict for detailed logging."""
        opp = self.opportunity
        return {
            "timestamp": self.timestamp.isoformat(),
            "market_question": opp.market.question[:80] if opp.market else "",
            "market_id": opp.market.condition_id if opp.market else "",
            "arb_type": opp.arb_type.value if opp.arb_type else "",
            "neg_risk": getattr(opp.market, 'neg_risk', False) if opp.market else False,
            "edge_pct": f"{opp.profit_margin * 100:.3f}%",
            "trade_size_usd": f"${self.total_cost_usd:.2f}",
            "trade_size_pct_capital": f"{(self.total_cost_usd / 1000) * 100:.1f}%",  # Normalized
            "realized_profit": f"${self.realized_profit_usd:.4f}",
            "executed": self.success,
            "partial_fill": self.has_partial_fill,
            "was_hedged": self.was_hedged,
            "execution_time_ms": f"{self.execution_time_ms:.0f}ms",
            "post_exposure": f"${self.post_execution_exposure:.2f}",
            "orders_filled": sum(1 for o in self.orders if o.status == OrderStatus.FILLED),
            "orders_total": len(self.orders),
        }


class GasEstimator:
    """Estimates real gas costs using web3.estimate_gas()."""

    # Base gas units for operations (used as fallback)
    BASE_GAS_UNITS = {
        "market_order": 150_000,  # CLOB market order base
        "redeem_positions": 100_000,  # ConditionalTokens redeem base
        "approve": 50_000,  # ERC20 approve
    }

    # Additional gas per outcome (for multi-outcome markets)
    GAS_PER_OUTCOME = 20_000  # ~20k extra gas per additional outcome

    def __init__(self, config: BotConfig):
        self.config = config
        self._web3: Optional[Web3] = None
        self._last_gas_price: Optional[int] = None
        self._last_gas_update: Optional[datetime] = None
        self._gas_cache_seconds = 30  # Cache gas price for 30 seconds
        self._estimated_gas_cache: Dict[str, Tuple[int, datetime]] = {}  # Cache for estimate_gas results

    def _init_web3(self) -> Optional[Web3]:
        """Initialize Web3 connection for gas estimation."""
        if self._web3 is None:
            try:
                self._web3 = Web3(Web3.HTTPProvider(self.config.network.polygon_rpc))
                self._web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
                if not self._web3.is_connected():
                    logger.warning("Failed to connect to Polygon RPC for gas estimation")
                    self._web3 = None
            except Exception as e:
                logger.warning(f"Error initializing web3 for gas estimation: {e}")
                self._web3 = None
        return self._web3

    def get_current_gas_price(self) -> Optional[int]:
        """
        Get current gas price from Polygon network.

        Returns:
            Gas price in wei, or None if unable to fetch.
        """
        # Check cache
        now = datetime.now(timezone.utc)
        if (self._last_gas_price is not None and
            self._last_gas_update is not None and
            (now - self._last_gas_update).total_seconds() < self._gas_cache_seconds):
            return self._last_gas_price

        web3 = self._init_web3()
        if web3 is None:
            return None

        try:
            gas_price = web3.eth.gas_price
            self._last_gas_price = gas_price
            self._last_gas_update = now
            logger.debug(f"Current gas price: {gas_price / 1e9:.2f} gwei")
            return gas_price
        except Exception as e:
            logger.warning(f"Error fetching gas price: {e}")
            return None

    def estimate_gas_units(
        self,
        operation: str = "market_order",
        num_outcomes: int = 2,
        use_web3_estimate: bool = True
    ) -> Tuple[int, bool]:
        """
        Estimate gas units for an operation using web3.estimate_gas() when possible.

        For multi-outcome markets, gas increases approximately linearly with outcomes.
        Uses actual estimate_gas() call for precision when web3 available.

        Args:
            operation: Type of operation.
            num_outcomes: Number of outcomes in the market.
            use_web3_estimate: Whether to try web3.estimate_gas().

        Returns:
            Tuple of (gas_units, is_estimate).
            is_estimate is True if using fallback calculation.
        """
        # Calculate variable gas based on outcomes
        base_gas = self.BASE_GAS_UNITS.get(operation, 150_000)
        # Binary market = base, ternary+ = base + (n-2) * per_outcome
        variable_gas = base_gas + max(0, num_outcomes - 2) * self.GAS_PER_OUTCOME

        if not use_web3_estimate:
            return variable_gas, True

        web3 = self._init_web3()
        if web3 is None:
            return variable_gas, True

        # Check cache (keyed by operation + num_outcomes)
        cache_key = f"{operation}_{num_outcomes}"
        now = datetime.now(timezone.utc)
        if cache_key in self._estimated_gas_cache:
            cached_gas, cache_time = self._estimated_gas_cache[cache_key]
            if (now - cache_time).total_seconds() < 60:  # 60s cache for estimates
                return cached_gas, False

        # Try to estimate gas via web3 for a sample transaction
        try:
            # Build a sample transaction for estimation
            # For market orders, we estimate based on a transfer-like operation
            # since actual CLOB orders go through their API, not direct contract calls
            sample_tx = {
                'from': Web3.to_checksum_address('0x0000000000000000000000000000000000000001'),
                'to': Web3.to_checksum_address(self.config.network.usdc_address),
                'value': 0,
                'data': '0x'  # Empty data for basic estimate
            }

            # For redeem operations, we can estimate more accurately
            if operation == "redeem_positions":
                # Use the ConditionalTokens address for redeem estimates
                sample_tx['to'] = Web3.to_checksum_address(
                    self.config.network.conditional_tokens_address
                )

            estimated = web3.eth.estimate_gas(sample_tx)

            # Scale by operation type and outcomes
            # estimate_gas returns base cost; we scale for actual operation complexity
            if operation == "market_order":
                # Market orders are ~3-5x more complex than basic transfer
                estimated = estimated * 4 * num_outcomes
            elif operation == "redeem_positions":
                # Redeem scales with outcomes
                estimated = estimated * 3 * num_outcomes
            else:
                estimated = estimated * num_outcomes

            # Ensure minimum based on our knowledge of operation costs
            estimated = max(estimated, variable_gas)

            # Cache the result
            self._estimated_gas_cache[cache_key] = (estimated, now)

            logger.debug(
                f"web3.estimate_gas for {operation} ({num_outcomes} outcomes): "
                f"{estimated} units (calculated: {variable_gas})"
            )

            return estimated, False

        except Exception as e:
            logger.debug(f"web3.estimate_gas failed, using fallback: {e}")
            return variable_gas, True

    def estimate_gas_cost_usd(
        self,
        operation: str = "market_order",
        num_operations: int = 1,
        num_outcomes: int = 2,
        matic_price_usd: float = 0.50  # Default MATIC price
    ) -> Tuple[float, bool]:
        """
        Estimate gas cost in USD for an operation.

        Uses web3.estimate_gas() for precision when available,
        with variable gas units based on outcome count.

        Args:
            operation: Type of operation (market_order, redeem_positions, approve).
            num_operations: Number of operations to perform.
            num_outcomes: Number of outcomes in the market (affects gas).
            matic_price_usd: Current MATIC price in USD.

        Returns:
            Tuple of (gas_cost_usd, is_estimate).
            is_estimate is True if using fallback values.
        """
        gas_price = self.get_current_gas_price()

        if gas_price is None:
            # Fallback to config buffer
            return self.config.trading.gas_buffer_usd * num_operations, True

        # Get variable gas units based on outcomes
        gas_units, is_estimate = self.estimate_gas_units(operation, num_outcomes)

        # Calculate cost in MATIC (wei to MATIC)
        total_gas_units = gas_units * num_operations
        gas_cost_wei = gas_price * total_gas_units
        gas_cost_matic = gas_cost_wei / 1e18

        # Convert to USD
        gas_cost_usd = gas_cost_matic * matic_price_usd

        logger.debug(
            f"Gas estimate: {total_gas_units} units ({num_outcomes} outcomes) "
            f"@ {gas_price / 1e9:.2f} gwei = ${gas_cost_usd:.4f} "
            f"({'fallback' if is_estimate else 'web3.estimate_gas'})"
        )

        return gas_cost_usd, is_estimate

    def get_dynamic_gas_buffer(self, num_outcomes: int) -> float:
        """
        Get dynamic gas buffer based on real gas prices and outcome count.

        Falls back to config buffer if web3 unavailable.
        Gas varies with num_outcomes: binary ~150k, ternary ~170k, etc.

        Args:
            num_outcomes: Number of outcomes in the market.

        Returns:
            Gas buffer in USD.
        """
        gas_cost, is_fallback = self.estimate_gas_cost_usd(
            operation="market_order",
            num_operations=1,
            num_outcomes=num_outcomes
        )

        if is_fallback:
            # Fallback with variable scaling
            base_buffer = self.config.trading.gas_buffer_usd
            return base_buffer * (1 + (num_outcomes - 2) * 0.15)  # +15% per extra outcome

        # Add 20% safety margin for gas price volatility
        return gas_cost * 1.2

    def check_gas_spike(self, max_gas_cost_usd: float = 0.10) -> Tuple[bool, float, str]:
        """
        Check if gas is currently spiking above safe threshold.

        Per Grok Round 3: Polygon gas can spike during network congestion.
        This check prevents executing trades when gas would eat the edge.

        Args:
            max_gas_cost_usd: Maximum acceptable gas cost per trade.

        Returns:
            Tuple of (is_safe, current_cost_usd, message).
        """
        gas_cost, is_fallback = self.estimate_gas_cost_usd(
            operation="market_order",
            num_operations=2,  # Typical arb = 2 orders
            num_outcomes=2
        )

        if is_fallback:
            # Can't determine - assume safe but warn
            return True, gas_cost, "Gas estimation unavailable (using fallback)"

        if gas_cost > max_gas_cost_usd:
            logger.warning(
                f"GAS SPIKE DETECTED: ${gas_cost:.4f} > max ${max_gas_cost_usd:.4f}. "
                f"Per Grok Round 3: Pausing trades until gas normalizes."
            )
            return False, gas_cost, f"Gas spike: ${gas_cost:.4f} exceeds max ${max_gas_cost_usd:.4f}"

        return True, gas_cost, f"Gas OK: ${gas_cost:.4f}"

    def is_trade_profitable_after_gas(
        self,
        profit_usd: float,
        num_outcomes: int = 2
    ) -> Tuple[bool, float, str]:
        """
        Check if trade is profitable after accounting for gas costs.

        Per Grok Round 3: Always verify profit > gas before executing.

        Args:
            profit_usd: Expected raw profit in USD.
            num_outcomes: Number of outcomes (affects gas).

        Returns:
            Tuple of (is_profitable, net_profit, message).
        """
        gas_cost, is_fallback = self.estimate_gas_cost_usd(
            operation="market_order",
            num_operations=num_outcomes,  # One order per outcome
            num_outcomes=num_outcomes
        )

        # Add 30% buffer for gas price movement during execution
        gas_cost_with_buffer = gas_cost * 1.3

        net_profit = profit_usd - gas_cost_with_buffer

        if net_profit <= 0:
            return False, net_profit, (
                f"Trade not profitable after gas: profit=${profit_usd:.4f}, "
                f"gas=${gas_cost_with_buffer:.4f}, net=${net_profit:.4f}"
            )

        return True, net_profit, f"Profitable: net=${net_profit:.4f} after gas=${gas_cost_with_buffer:.4f}"


class ExecutionEngine:
    """Handles order execution for arbitrage opportunities."""

    def __init__(self, config: BotConfig, client: Optional[ClobClient] = None):
        """
        Initialize the execution engine.

        Args:
            config: Bot configuration.
            client: Authenticated CLOB client (optional for dry run).
        """
        self.config = config
        self.client = client
        self.gas_estimator = GasEstimator(config)
        self._execution_count = 0
        self._total_volume = 0.0
        self._total_profit = 0.0
        self._partial_fills = 0
        self._hedges_executed = 0
        self._consecutive_partials = 0
        self._approvals_executed = 0
        self._consecutive_failures = 0  # Track consecutive order failures

        # Web3 and contract instances for allowance/approval
        self._web3: Optional[Web3] = None
        self._usdc_contract = None
        self._wallet_address: Optional[str] = None
        self._private_key: Optional[str] = None
        self._funder_address: Optional[str] = None  # For proxy mode balance/allowance checks

        # Multi-wallet support
        self._wallet_manager = None  # WalletManager instance for multi-wallet execution

        # Exposure tracking per market (for neutralization)
        self._market_exposure: Dict[str, float] = {}  # market_id -> net exposure in USD
        self._max_exposure_pct = 0.05  # 5% of capital max directional exposure per market

        # Idempotency tracker (per Grok Round 15: 900s TTL for WS reconnect scenarios)
        # WS reconnects during sports bursts can re-detect same arb within minutes
        # Extended to 900s (15 min) to safely cover reconnect + market resolution windows
        self._idempotency_tracker = IdempotencyTracker(ttl_seconds=900)

        # Enhanced circuit breaker: >5 consecutive partials/fails OR >3% drawdown = 30min pause
        self._circuit_breaker_active = False
        self._circuit_breaker_until: Optional[datetime] = None
        self._circuit_breaker_pause_seconds = 1800  # 30 minutes
        self._max_consecutive_failures = 5  # Trigger on >5 consecutive partials or fails
        self._max_drawdown_pct = 0.03  # 3% drawdown triggers circuit breaker
        self._session_high_capital = config.starting_capital_usd  # Track session high for drawdown
        self._current_capital = config.starting_capital_usd

        # FAK (Fill-And-Kill) mode settings - Polymarket's IOC equivalent
        self._use_ioc = True  # Use FAK instead of FOK for more fills (2-5x fill rate)
        self._partial_hedge_slippage_threshold = 0.003  # 0.3% max slippage for partial hedging

        # Per Grok Round 23: High-confidence IOC toggle
        # Only use IOC/FAK when fill probability is high enough to justify partial risk
        # In hot/blazing markets with low fill prob, stick to post-only (safer)
        self._ioc_min_fill_prob = 0.60  # 60% fill prob threshold for IOC
        self._ioc_high_confidence_only = True  # Enable confidence-based IOC toggle

        # Maker vs Taker tracking for post-only orders
        self._maker_fills = 0  # Post-only GTC orders that filled as maker
        self._taker_fills = 0  # FAK/FOK orders that filled as taker
        self._maker_volume_usd = 0.0  # Volume from maker fills
        self._taker_volume_usd = 0.0  # Volume from taker fills
        self._maker_ratio_alert_sent = False  # Only alert once per session

        # Per Grok Round 8: Track estimated maker rebates for PnL
        # Polymarket maker rebate is ~0.02% (2 bps), compounds significantly over thousands of trades
        self._estimated_maker_rebates = 0.0

        # Per Grok Round 23: Variance tracking for Kelly integration
        # Track returns per heat level for variance-adjusted sizing
        # f* = max(0, (edge - fees) / variance) - rn1's smooth compounding formula
        from collections import deque
        self._returns_by_heat: Dict[str, deque] = {
            "cold": deque(maxlen=100),
            "warm": deque(maxlen=100),
            "hot": deque(maxlen=100),
            "blazing": deque(maxlen=100),
        }
        self._partial_rates_by_heat: Dict[str, deque] = {
            "cold": deque(maxlen=100),
            "warm": deque(maxlen=100),
            "hot": deque(maxlen=100),
            "blazing": deque(maxlen=100),
        }

        # Per Grok Round 23: Minimum EV threshold (0.1% post-fees/slippage)
        # rn1's ~0.8% avg edge filter - don't trade if expected value too low
        self._min_ev_threshold = 0.001  # 0.1% minimum expected value

    def set_client(self, client: ClobClient):
        """Set the CLOB client."""
        self.client = client

    def set_wallet_manager(self, wallet_manager):
        """
        Set the wallet manager for multi-wallet execution.

        Args:
            wallet_manager: WalletManager instance for wallet rotation.
        """
        self._wallet_manager = wallet_manager
        logger.info(f"Wallet manager set with {wallet_manager.get_wallet_count()} wallet(s)")

    def record_trade_return(self, return_pct: float, heat: str, was_partial: bool = False):
        """
        Per Grok Round 23: Record trade return for variance calculation.

        Used by Kelly formula: f* = max(0, (edge - fees) / variance)
        Tracks returns per heat level for heat-specific variance estimation.

        Args:
            return_pct: Return as decimal (e.g., 0.008 for 0.8% profit).
            heat: Market heat level ("cold", "warm", "hot", "blazing").
            was_partial: True if this was a partial fill.
        """
        heat_lower = heat.lower()
        if heat_lower in self._returns_by_heat:
            self._returns_by_heat[heat_lower].append(return_pct)
            self._partial_rates_by_heat[heat_lower].append(1.0 if was_partial else 0.0)

    def get_variance_for_heat(self, heat: str) -> float:
        """
        Per Grok Round 23: Get variance of returns for a heat level.

        Used for Kelly sizing: f* = edge / variance
        Returns default variance if insufficient data.

        Args:
            heat: Market heat level.

        Returns:
            Variance of returns (minimum 1e-6 to avoid division by zero).
        """
        heat_lower = heat.lower()
        returns = list(self._returns_by_heat.get(heat_lower, []))

        if len(returns) < 10:
            # Default variance by heat (higher heat = higher variance)
            default_variances = {
                "cold": 0.0001,    # 0.01% std dev
                "warm": 0.0004,    # 0.02% std dev
                "hot": 0.0009,     # 0.03% std dev
                "blazing": 0.0016, # 0.04% std dev
            }
            return default_variances.get(heat_lower, 0.0004)

        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)

        # Boost variance for high partial rate (partials = more volatility)
        partial_rates = list(self._partial_rates_by_heat.get(heat_lower, []))
        if partial_rates:
            partial_rate = sum(partial_rates) / len(partial_rates)
            variance *= (1 + partial_rate * 0.5)  # Up to 50% boost

        return max(variance, 1e-6)

    def get_partial_rate_for_heat(self, heat: str) -> float:
        """
        Per Grok Round 23: Get partial fill rate for a heat level.

        Args:
            heat: Market heat level.

        Returns:
            Partial fill rate (0.0 to 1.0).
        """
        heat_lower = heat.lower()
        partial_rates = list(self._partial_rates_by_heat.get(heat_lower, []))
        if not partial_rates:
            # Default partial rates by heat (higher heat = more partials)
            defaults = {"cold": 0.10, "warm": 0.18, "hot": 0.25, "blazing": 0.35}
            return defaults.get(heat_lower, 0.22)
        return sum(partial_rates) / len(partial_rates)

    def check_ev_threshold(self, edge_pct: float, fill_probability: float, fees_pct: float = 0.0) -> bool:
        """
        Per Grok Round 23: Check if expected value meets minimum threshold.

        rn1's ~0.8% avg edge filter - don't trade if EV too low after fees/slippage.

        Args:
            edge_pct: Raw edge as decimal.
            fill_probability: Probability of fill (0-1).
            fees_pct: Expected fees as decimal.

        Returns:
            True if EV >= min threshold (0.1%).
        """
        expected_value = (edge_pct - fees_pct) * fill_probability
        if expected_value < self._min_ev_threshold:
            logger.debug(
                f"EV below threshold: {expected_value*100:.3f}% < {self._min_ev_threshold*100:.1f}% "
                f"(edge={edge_pct*100:.2f}%, fill_prob={fill_probability:.0%})"
            )
            return False
        return True

    def should_use_ioc(self, fill_probability: float, heat: str = "warm") -> bool:
        """
        Per Grok Round 23: Determine if IOC/FAK should be used based on confidence.

        High-confidence IOC toggle logic:
        - In cold/warm markets with good fill prob (>60%): Use IOC for more fills
        - In hot/blazing markets OR low fill prob: Stick to post-only (safer)

        IOC trades faster but risks partials. Post-only is safer but slower.
        rn1 used post-only primarily but may have used IOC in high-confidence scenarios.

        Args:
            fill_probability: Estimated fill probability (0-1).
            heat: Market heat level.

        Returns:
            True if IOC should be used, False for post-only.
        """
        if not self._use_ioc:
            return False

        if not self._ioc_high_confidence_only:
            return True  # Always use IOC if high-confidence mode disabled

        # Per Grok Round 23: High-confidence IOC logic
        heat_lower = heat.lower()

        # Never use IOC in blazing markets (too competitive, partials hurt)
        if heat_lower == "blazing":
            logger.debug(f"IOC disabled: blazing market (fill_prob={fill_probability:.0%})")
            return False

        # In hot markets, require higher fill probability
        if heat_lower == "hot":
            hot_threshold = self._ioc_min_fill_prob + 0.15  # 75% for hot
            if fill_probability < hot_threshold:
                logger.debug(f"IOC disabled: hot market fill_prob {fill_probability:.0%} < {hot_threshold:.0%}")
                return False

        # Standard threshold for cold/warm
        if fill_probability < self._ioc_min_fill_prob:
            logger.debug(f"IOC disabled: fill_prob {fill_probability:.0%} < {self._ioc_min_fill_prob:.0%}")
            return False

        logger.debug(f"IOC enabled: {heat_lower} market, fill_prob={fill_probability:.0%}")
        return True

    async def get_execution_client(self, min_balance: float = 0.0) -> Tuple[Optional[ClobClient], Optional[Any]]:
        """
        Get a CLOB client for execution, using wallet rotation if available.

        Args:
            min_balance: Minimum USDC balance required.

        Returns:
            Tuple of (client, wallet_state) or (self.client, None) if no wallet manager.
        """
        if self._wallet_manager is not None:
            wallet = await self._wallet_manager.get_next_wallet(min_balance=min_balance)
            if wallet and wallet.client:
                return wallet.client, wallet
            # Fall back to primary wallet
            wallet = self._wallet_manager.get_primary_wallet()
            if wallet and wallet.client:
                return wallet.client, wallet

        return self.client, None

    def set_wallet(self, wallet_address: str, private_key: str, funder_address: Optional[str] = None):
        """
        Set wallet address and private key for approval transactions.

        Args:
            wallet_address: The signer wallet address (derived from private key).
            private_key: The private key for signing transactions.
            funder_address: The funder/proxy wallet address (where USDC is held).
                           For proxy mode, this is where balance/allowance is checked.
        """
        self._wallet_address = wallet_address
        self._private_key = private_key
        # For proxy wallets, use funder address for balance/allowance checks
        self._funder_address = funder_address or wallet_address

    def _init_web3_and_contracts(self) -> bool:
        """
        Initialize Web3 and USDC contract for allowance/approval operations.

        Returns:
            True if initialization successful, False otherwise.
        """
        if self._web3 is not None and self._usdc_contract is not None:
            return True

        try:
            self._web3 = Web3(Web3.HTTPProvider(self.config.network.polygon_rpc))
            self._web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if not self._web3.is_connected():
                logger.warning("Failed to connect to Polygon RPC for approval operations")
                return False

            self._usdc_contract = self._web3.eth.contract(
                address=Web3.to_checksum_address(self.config.network.usdc_address),
                abi=ERC20_ABI
            )
            return True

        except Exception as e:
            logger.error(f"Error initializing web3 for approvals: {e}")
            return False

    def check_allowance(self, spender_address: Optional[str] = None) -> Optional[float]:
        """
        Check current USDC allowance for the spender.

        For proxy wallets, checks the funder address (where USDC is held).

        Args:
            spender_address: The spender to check allowance for.
                            Defaults to config.network.usdc_spender_address.

        Returns:
            Allowance in USD (USDC with 6 decimals), or None if check fails.
        """
        if self.config.dry_run:
            return float('inf')  # Unlimited in dry run

        # Use funder address for proxy mode (where USDC is held)
        check_address = self._funder_address or self._wallet_address
        if not check_address:
            logger.warning("Wallet address not set for allowance check")
            return None

        if not self._init_web3_and_contracts():
            return None

        spender = spender_address or self.config.network.usdc_spender_address
        spender = Web3.to_checksum_address(spender)
        owner = Web3.to_checksum_address(check_address)

        try:
            allowance_raw = self._usdc_contract.functions.allowance(owner, spender).call()
            allowance_usd = allowance_raw / 1e6  # USDC has 6 decimals
            logger.debug(f"Current allowance for {owner[:10]}... -> {spender[:10]}...: ${allowance_usd:,.2f}")
            return allowance_usd

        except Exception as e:
            logger.error(f"Error checking allowance: {e}")
            return None

    def check_balance(self) -> Optional[float]:
        """
        Check USDC balance for the wallet.

        For proxy wallets, checks the funder address (where USDC is held).

        Returns:
            Balance in USD, or None if check fails.
        """
        if self.config.dry_run:
            return self.config.starting_capital_usd

        # Use funder address for proxy mode (where USDC is held)
        check_address = self._funder_address or self._wallet_address
        if not check_address:
            logger.warning("Wallet address not set for balance check")
            return None

        if not self._init_web3_and_contracts():
            return None

        owner = Web3.to_checksum_address(check_address)

        try:
            balance_raw = self._usdc_contract.functions.balanceOf(owner).call()
            balance_usd = balance_raw / 1e6
            return balance_usd

        except Exception as e:
            logger.error(f"Error checking balance: {e}")
            return None

    async def verify_trading_ready(self) -> Tuple[bool, str]:
        """
        Verify the wallet is ready for trading (balance + allowance).

        HARD ERRORS on RPC failures - rn1 needs flawless on-chain operations.

        Note: For proxy wallets (most Polymarket users), USDC allowance must be
        approved manually via the Polymarket UI. Auto-approve is not supported
        for proxy wallets as on-chain transactions require relayer handling.

        Returns:
            Tuple of (ready, message).
        """
        if self.config.dry_run:
            return True, "Dry run mode - ready"

        # Use funder address for display (where USDC is held)
        display_wallet = self._funder_address or self._wallet_address

        # Check balance - HARD ERROR on RPC failure (rn1 needs flawless on-chain)
        balance = self.check_balance()
        if balance is None:
            return False, "Failed to check USDC balance (RPC issue) - fix RPC connection"

        # Hard block on insufficient capital (rn1 pattern: $1k min start)
        min_capital = self.config.min_capital_required
        if balance < min_capital:
            return False, (
                f"Insufficient capital: ${balance:.2f} < ${min_capital:.2f} required. "
                f"rn1 started with $1k - deposit USDC to wallet {display_wallet}"
            )

        # Check allowance - HARD ERROR on RPC failure
        allowance = self.check_allowance()
        if allowance is None:
            return False, "Failed to check USDC allowance (RPC issue) - fix RPC connection"

        if allowance < 1.0:
            # Log helpful message for manual approval
            logger.warning("=" * 60)
            logger.warning("MANUAL USDC APPROVAL REQUIRED")
            logger.warning("=" * 60)
            logger.warning(f"Current allowance: ${allowance:.2f}")
            logger.warning(f"Wallet (funder): {display_wallet}")
            logger.warning(f"Spender: {self.config.network.usdc_spender_address}")
            logger.warning("")
            logger.warning("For proxy wallets, approve USDC via Polymarket UI:")
            logger.warning("  1. Go to https://polymarket.com")
            logger.warning("  2. Connect your wallet")
            logger.warning("  3. Try to place any buy order")
            logger.warning("  4. Approve the USDC spending prompt (set to unlimited)")
            logger.warning("  5. Restart the bot")
            logger.warning("=" * 60)
            return False, "Manual USDC approval required via Polymarket UI"

        return True, f"Ready to trade. Balance: ${balance:.2f}, Allowance: ${allowance:.2f}"

    def check_max_approval(self, spender_address: Optional[str] = None) -> bool:
        """
        Check if USDC approval is set to MAX (unlimited).

        MAX approval = 2^256 - 1 (or effectively >$1 trillion)
        This prevents needing to re-approve during operation.

        Args:
            spender_address: The spender to check allowance for.

        Returns:
            True if MAX approved, False otherwise.
        """
        if self.config.dry_run:
            return True

        allowance = self.check_allowance(spender_address)
        if allowance is None:
            return False

        # MAX_UINT256 in USDC terms is ~115 trillion
        # Anything above $1 trillion is effectively MAX
        max_threshold = 1_000_000_000_000  # $1 trillion
        return allowance >= max_threshold

    async def ensure_max_approval(self, spender_address: Optional[str] = None) -> Tuple[bool, str]:
        """
        Ensure USDC MAX approval is set. Log warning if not MAX approved.

        For proxy wallets, MAX approval must be done via Polymarket UI.
        For EOA wallets, we can auto-approve if private key available.

        Note: This does NOT auto-approve for proxy wallets (most users).
        It only checks and logs instructions.

        Args:
            spender_address: The spender to approve for.

        Returns:
            Tuple of (is_max_approved, message).
        """
        if self.config.dry_run:
            return True, "Dry run - MAX approval assumed"

        if not self.config.trading.check_usdc_max_approval:
            return True, "MAX approval check disabled"

        is_max = self.check_max_approval(spender_address)

        if is_max:
            logger.debug("USDC MAX approval confirmed")
            return True, "MAX approval confirmed"

        # Not MAX approved - log warning
        display_wallet = self._funder_address or self._wallet_address
        spender = spender_address or self.config.network.usdc_spender_address

        logger.warning("=" * 60)
        logger.warning("USDC MAX APPROVAL NOT SET")
        logger.warning("=" * 60)
        logger.warning(f"Wallet: {display_wallet}")
        logger.warning(f"Spender: {spender}")
        logger.warning("")
        logger.warning("Set MAX approval via Polymarket UI for uninterrupted trading:")
        logger.warning("  1. Go to https://polymarket.com")
        logger.warning("  2. Connect your wallet")
        logger.warning("  3. Place any buy order")
        logger.warning("  4. When prompted to approve USDC, select 'Unlimited'")
        logger.warning("=" * 60)

        # Check if we can auto-approve (EOA with private key)
        if self._private_key and not self.config.wallet.is_proxy_mode:
            logger.info("EOA wallet detected - attempting auto-approval...")
            try:
                success = await self._auto_approve_max(spender)
                if success:
                    self._approvals_executed += 1
                    return True, "MAX approval set successfully"
            except Exception as e:
                logger.error(f"Auto-approval failed: {e}")

        # Still allow trading with limited approval (warning logged)
        current = self.check_allowance(spender_address) or 0
        return False, f"Limited approval: ${current:,.2f}. Consider setting MAX."

    async def _auto_approve_max(self, spender_address: str) -> bool:
        """
        Auto-approve MAX USDC spending for EOA wallets.

        Only works for EOA wallets with private key.
        Proxy wallets must approve via Polymarket UI.

        Args:
            spender_address: The spender to approve.

        Returns:
            True if approval succeeded.
        """
        if not self._private_key or not self._wallet_address:
            return False

        if not self._init_web3_and_contracts():
            return False

        try:
            # Build approval transaction
            spender = Web3.to_checksum_address(spender_address)
            wallet = Web3.to_checksum_address(self._wallet_address)

            # Get nonce
            nonce = self._web3.eth.get_transaction_count(wallet)

            # Build approve tx with MAX_UINT256
            approve_fn = self._usdc_contract.functions.approve(spender, MAX_UINT256)

            # Estimate gas
            gas_estimate = approve_fn.estimate_gas({'from': wallet})
            gas_price = self._web3.eth.gas_price

            # Build and sign tx
            tx = approve_fn.build_transaction({
                'from': wallet,
                'nonce': nonce,
                'gas': int(gas_estimate * 1.2),  # 20% buffer
                'gasPrice': gas_price,
                'chainId': self.config.network.chain_id
            })

            signed = self._web3.eth.account.sign_transaction(tx, self._private_key)
            tx_hash = self._web3.eth.send_raw_transaction(signed.raw_transaction)

            # Wait for confirmation
            receipt = self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt['status'] == 1:
                logger.info(f"MAX approval tx confirmed: {tx_hash.hex()}")
                return True
            else:
                logger.error(f"MAX approval tx failed: {tx_hash.hex()}")
                return False

        except Exception as e:
            logger.error(f"Auto MAX approval error: {e}")
            return False

    def is_paused_for_partials(self) -> bool:
        """Check if execution is paused due to consecutive partials."""
        return self._consecutive_partials >= self.config.trading.max_consecutive_partials

    def check_circuit_breaker(self) -> Tuple[bool, str]:
        """
        Check enhanced circuit breaker status.

        Triggers on:
        - >5 consecutive partials or failures
        - >3% drawdown from session high

        Returns:
            Tuple of (is_blocked, reason).
        """
        now = datetime.now(timezone.utc)

        # Check if already in cooldown
        if self._circuit_breaker_active and self._circuit_breaker_until:
            if now < self._circuit_breaker_until:
                remaining = (self._circuit_breaker_until - now).total_seconds()
                return True, f"Circuit breaker active, {remaining:.0f}s remaining"
            else:
                # Cooldown expired, reset
                self._circuit_breaker_active = False
                self._circuit_breaker_until = None
                self._consecutive_failures = 0
                self._consecutive_partials = 0
                logger.info("Circuit breaker reset after cooldown")

        # Check consecutive failures (partials + order failures)
        total_consecutive = self._consecutive_partials + self._consecutive_failures
        if total_consecutive > self._max_consecutive_failures:
            self._trigger_circuit_breaker(
                f">{self._max_consecutive_failures} consecutive partials/failures"
            )
            return True, f"Too many consecutive failures: {total_consecutive}"

        # Check drawdown from session high
        if self._session_high_capital > 0:
            drawdown = (self._session_high_capital - self._current_capital) / self._session_high_capital
            if drawdown > self._max_drawdown_pct:
                self._trigger_circuit_breaker(
                    f">{self._max_drawdown_pct*100:.1f}% drawdown"
                )
                return True, f"Drawdown limit exceeded: {drawdown*100:.2f}%"

        return False, "OK"

    def _trigger_circuit_breaker(self, reason: str):
        """Activate circuit breaker with 30min pause."""
        self._circuit_breaker_active = True
        self._circuit_breaker_until = datetime.now(timezone.utc) + timedelta(
            seconds=self._circuit_breaker_pause_seconds
        )
        logger.warning("=" * 60)
        logger.warning(f"CIRCUIT BREAKER TRIGGERED: {reason}")
        logger.warning(f"Trading paused for {self._circuit_breaker_pause_seconds/60:.0f} minutes")
        logger.warning(f"Resume at: {self._circuit_breaker_until.isoformat()}")
        logger.warning("=" * 60)

    def update_capital(self, profit_usd: float):
        """Update capital tracking for drawdown calculation."""
        self._current_capital += profit_usd
        if self._current_capital > self._session_high_capital:
            self._session_high_capital = self._current_capital

    def is_duplicate_arb(self, opportunity: ArbOpportunity) -> bool:
        """Check if this arb was recently executed (idempotency check)."""
        return self._idempotency_tracker.is_duplicate(
            market_id=opportunity.market.condition_id,
            profit_margin=opportunity.profit_margin,
            trade_size=opportunity.trade_size_usd
        )

    def record_arb_execution(self, opportunity: ArbOpportunity):
        """Record arb execution for idempotency tracking."""
        self._idempotency_tracker.record_execution(
            market_id=opportunity.market.condition_id,
            profit_margin=opportunity.profit_margin,
            trade_size=opportunity.trade_size_usd
        )

    async def _verify_sell_arb_ready(self, opportunity: ArbOpportunity) -> Tuple[bool, str]:
        """
        Verify we can execute a sell_arb trade.

        For sell arbs:
        - neg_risk markets (winner-take-all): need free USDC collateral with 20% buffer
        - standard markets: not supported without holding tokens (skip)

        Args:
            opportunity: The sell_arb opportunity to verify.

        Returns:
            Tuple of (can_execute, reason).
        """
        market = opportunity.market
        trade_size = opportunity.trade_size_usd

        # Check if this is a neg_risk market (winner-take-all)
        if hasattr(market, 'neg_risk') and market.neg_risk:
            # Neg-risk markets allow shorting with USDC collateral
            # Need trade_size + 20% buffer for collateral
            required_collateral = trade_size * 1.2

            balance = self.check_balance()
            if balance is None:
                return False, "Could not check USDC balance"

            if balance < required_collateral:
                return False, (
                    f"Insufficient collateral for neg_risk sell_arb: "
                    f"${balance:.2f} < ${required_collateral:.2f} required"
                )

            logger.debug(
                f"Neg-risk sell_arb: collateral OK "
                f"(${balance:.2f} >= ${required_collateral:.2f})"
            )
            return True, "Neg-risk market, collateral sufficient"

        else:
            # Standard markets require holding the tokens to sell
            # TODO: Check token positions via CLOB API or on-chain
            # For now, skip sell arbs on non-neg-risk markets
            # This prevents "not enough balance" errors from trying to sell tokens we don't have
            return False, "Standard market sell_arb skipped (requires holding tokens)"

    async def _verify_book_before_execution(
        self,
        opportunity: ArbOpportunity,
        orderbook_poller: Any
    ) -> Optional[ArbOpportunity]:
        """
        Re-fetch orderbook immediately before execution for slippage check.

        This prevents executing on stale WS or cached data. If the arb has
        decayed or slippage exceeds threshold, returns None to skip.

        Args:
            opportunity: The arb opportunity to verify.
            orderbook_poller: The OrderbookPoller instance for fresh book fetch.

        Returns:
            Updated opportunity with fresh prices, or None if arb is gone.
        """
        try:
            # Get fresh orderbooks for all outcomes
            token_ids = [o.token_id for o in opportunity.market.outcomes]
            fresh_books = await orderbook_poller.fetch_orderbooks_batch(token_ids)

            if not fresh_books:
                logger.debug("Pre-exec verify: failed to fetch fresh books")
                return None

            # Calculate fresh sum to check if arb still exists
            if opportunity.arb_type == ArbType.BUY_ARB:
                # Sum of best asks
                fresh_sum = sum(
                    fresh_books.get(tid, {}).get("best_ask", 0.5)
                    for tid in token_ids
                )
                arb_exists = fresh_sum < 1.0 - self.config.trading.arb_threshold_base
            else:
                # Sum of best bids
                fresh_sum = sum(
                    fresh_books.get(tid, {}).get("best_bid", 0.5)
                    for tid in token_ids
                )
                arb_exists = fresh_sum > 1.0 + self.config.trading.arb_threshold_base

            if not arb_exists:
                logger.debug(
                    f"Pre-exec verify: arb gone (fresh sum={fresh_sum:.4f})"
                )
                return None

            # Calculate slippage from original prices
            original_sum = 0.0
            for outcome in opportunity.market.outcomes:
                ob = opportunity.orderbooks.get_orderbook(outcome.token_id)
                if ob:
                    if opportunity.arb_type == ArbType.BUY_ARB:
                        original_sum += ob.best_ask_price if ob.best_ask else 0.5
                    else:
                        original_sum += ob.best_bid_price if ob.best_bid else 0.5

            if original_sum > 0:
                slippage = abs(fresh_sum - original_sum) / original_sum
                slippage_threshold = self.config.trading.slippage_threshold_percent / 100

                if slippage > slippage_threshold:
                    logger.debug(
                        f"Pre-exec verify: slippage too high "
                        f"({slippage*100:.2f}% > {slippage_threshold*100:.1f}%)"
                    )
                    return None

            # Update opportunity with fresh profit margin
            if opportunity.arb_type == ArbType.BUY_ARB:
                fresh_margin = 1.0 - fresh_sum
            else:
                fresh_margin = fresh_sum - 1.0

            opportunity.profit_margin = max(0, fresh_margin)
            opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin

            logger.debug(
                f"Pre-exec verify: OK (fresh margin={fresh_margin*100:.3f}%, "
                f"slippage={slippage*100:.3f}%)"
            )
            return opportunity

        except Exception as e:
            logger.debug(f"Pre-exec verify error: {e}")
            # On error, proceed with original (conservative choice would be to skip)
            return opportunity

    def get_real_gas_cost(self, num_outcomes: int) -> float:
        """
        Get real gas cost estimate for executing trades.

        Args:
            num_outcomes: Number of outcomes to trade.

        Returns:
            Estimated gas cost in USD.
        """
        return self.gas_estimator.get_dynamic_gas_buffer(num_outcomes)

    async def execute_opportunity(
        self,
        opportunity: ArbOpportunity,
        orderbook_poller: Optional[Any] = None  # For pre-execution book refresh
    ) -> ExecutionResult:
        """
        Execute an arbitrage opportunity with post-fill monitoring and auto-hedging.

        Enhanced with:
        - Idempotency check (60s TTL to prevent duplicates)
        - Circuit breaker (>5 consecutive failures OR >3% drawdown = 30min pause)
        - Pre-execution book refresh for slippage check
        - IOC orders with partial fill hedging

        Args:
            opportunity: The arbitrage opportunity to execute.
            orderbook_poller: Optional poller for pre-execution book refresh.

        Returns:
            ExecutionResult containing order details.
        """
        if self.config.dry_run:
            return await self._dry_run_execute(opportunity)

        if not self.client:
            raise RuntimeError("CLOB client not initialized for live trading")

        if not opportunity.is_valid:
            logger.warning("Opportunity expired before execution")
            return ExecutionResult(
                opportunity=opportunity,
                is_complete=False,
            )

        # Enhanced circuit breaker check (>5 consecutive partials/fails OR >3% drawdown)
        cb_blocked, cb_reason = self.check_circuit_breaker()
        if cb_blocked:
            logger.warning(f"Circuit breaker: {cb_reason}")
            return ExecutionResult(opportunity=opportunity, is_complete=False)

        # Idempotency check: prevent duplicate arbs on restart/reconnect (60s TTL)
        if self.is_duplicate_arb(opportunity):
            logger.debug(
                f"Duplicate arb skipped (idempotency): {opportunity.market.condition_id[:8]}..."
            )
            return ExecutionResult(opportunity=opportunity, is_complete=False)

        # Check if paused due to consecutive partials (legacy check, circuit breaker is primary)
        if self.is_paused_for_partials():
            logger.warning(
                f"Execution paused: {self._consecutive_partials} consecutive partial fills. "
                f"Waiting {self.config.trading.partial_pause_duration}s before resuming."
            )
            return ExecutionResult(opportunity=opportunity, is_complete=False)

        # Pre-execution book refresh for slippage check
        if orderbook_poller:
            verified_opp = await self._verify_book_before_execution(opportunity, orderbook_poller)
            if verified_opp is None:
                logger.debug("Pre-execution book refresh: arb gone or slippage too high")
                return ExecutionResult(opportunity=opportunity, is_complete=False)
            opportunity = verified_opp

        # Exposure-aware sizing: reduce trade size if net exposure would exceed limit
        # Target: <0.5% of capital as net exposure per market
        opportunity = self._apply_exposure_aware_sizing(opportunity)

        if opportunity.trade_size_usd <= 0:
            logger.debug("Trade size reduced to 0 due to exposure limits")
            return ExecutionResult(opportunity=opportunity, is_complete=False)

        # For sell_arb: verify we can execute the trade
        # - If buy_arb_only mode enabled: skip all sell arbs
        # - neg_risk markets: need free USDC collateral (can short without tokens)
        # - standard markets: need to hold the tokens (can't short without them)
        if opportunity.arb_type == ArbType.SELL_ARB:
            # Check buy_arb_only mode first
            if self.config.trading.buy_arb_only:
                logger.info("Skipped SELL_ARB (buy_only mode - RN1 alignment, no shorts on Polymarket)")
                return ExecutionResult(
                    opportunity=opportunity,
                    is_complete=False,
                )

            can_sell, sell_reason = await self._verify_sell_arb_ready(opportunity)
            if not can_sell:
                logger.debug(f"Skipping sell_arb: {sell_reason}")
                return ExecutionResult(
                    opportunity=opportunity,
                    is_complete=False,
                )

        # Detailed per-arb logging
        neg_risk_str = "[NEG-RISK]" if getattr(opportunity.market, 'neg_risk', False) else ""
        logger.info(
            f"EXECUTING {opportunity.arb_type.value.upper()} {neg_risk_str} | "
            f"Edge: {opportunity.profit_margin * 100:.3f}% | "
            f"Size: ${opportunity.trade_size_usd:.2f} | "
            f"Market: {opportunity.market.question[:40]}..."
        )

        result = ExecutionResult(opportunity=opportunity)
        execution_start = datetime.now(timezone.utc)

        try:
            if opportunity.arb_type == ArbType.BUY_ARB:
                orders = await self._execute_buy_arb(opportunity)
            else:
                orders = await self._execute_sell_arb(opportunity)

            result.orders = orders

            # Post-fill monitoring: check actual fill status
            await self._monitor_fills(orders)

            # Recalculate after monitoring
            result.is_complete = all(o.status == OrderStatus.FILLED for o in orders)
            result.has_partial_fill = any(o.status == OrderStatus.PARTIAL for o in orders)

            # Calculate unfilled amounts
            for order in orders:
                order.unfilled_size_usd = order.requested_size_usd - order.executed_size_usd

            # Calculate totals
            result.total_cost_usd = sum(o.executed_size_usd for o in orders)
            result.expected_return_usd = result.total_cost_usd * (1 + opportunity.profit_margin)

            if result.is_complete:
                result.realized_profit_usd = result.total_cost_usd * opportunity.profit_margin
                self._total_profit += result.realized_profit_usd
                self._execution_count += 1
                self._total_volume += result.total_cost_usd
                self._consecutive_partials = 0  # Reset on success
                self._consecutive_failures = 0  # Reset consecutive failures
                # Update capital tracking for circuit breaker
                self.update_capital(result.realized_profit_usd)
                # Record for idempotency (prevent re-execution on restart)
                self.record_arb_execution(opportunity)
                logger.info(
                    f"Execution complete! Profit: ${result.realized_profit_usd:.4f} "
                    f"| Total volume: ${result.total_cost_usd:.2f}"
                )
            elif result.has_partial_fill:
                self._partial_fills += 1
                self._consecutive_partials += 1

                # Calculate partial exposure
                partial_exposure = result.partial_exposure_usd
                hedge_threshold = self.config.trading.partial_fill_hedge_threshold

                logger.warning(
                    f"Partial fill detected! Exposure: ${partial_exposure:.2f} "
                    f"| Filled: {sum(1 for o in orders if o.status == OrderStatus.FILLED)}/{len(orders)} "
                    f"| Consecutive partials: {self._consecutive_partials}"
                )

                # Auto-hedge if exposure exceeds threshold
                if partial_exposure > 0 and (partial_exposure / opportunity.trade_size_usd) >= hedge_threshold:
                    hedge_orders = await self._hedge_partial_fills(result, opportunity)
                    result.hedge_orders = hedge_orders
                    result.was_hedged = len(hedge_orders) > 0

                    if result.was_hedged:
                        self._hedges_executed += 1
                        logger.info(
                            f"Auto-hedge executed: {len(hedge_orders)} orders placed"
                        )

            # Calculate execution time
            execution_end = datetime.now(timezone.utc)
            result.execution_time_ms = (execution_end - execution_start).total_seconds() * 1000

            # Track post-execution exposure
            result.post_execution_exposure = self.get_market_exposure(opportunity.market.condition_id)

            # Detailed completion log
            log_data = result.to_log_dict()
            if result.success:
                logger.info(
                    f"ARB COMPLETE | {log_data['edge_pct']} edge | "
                    f"Profit: {log_data['realized_profit']} | "
                    f"Time: {log_data['execution_time_ms']} | "
                    f"Exposure: {log_data['post_exposure']}"
                )
            else:
                logger.warning(
                    f"ARB INCOMPLETE | {log_data['edge_pct']} edge | "
                    f"Filled: {log_data['orders_filled']}/{log_data['orders_total']} | "
                    f"Hedged: {log_data['was_hedged']} | "
                    f"Exposure: {log_data['post_exposure']}"
                )

        except Exception as e:
            logger.error(f"Execution failed: {e}")
            result.is_complete = False
            # Calculate execution time even on failure
            execution_end = datetime.now(timezone.utc)
            result.execution_time_ms = (execution_end - execution_start).total_seconds() * 1000

        return result

    async def _monitor_fills(self, orders: List[OrderResult]):
        """
        Monitor order fills after submission.

        Polls order status for a short period to detect partial fills.

        Args:
            orders: List of orders to monitor.
        """
        if not self.client:
            return

        monitor_timeout = self.config.trading.fill_monitor_timeout
        poll_interval = 0.5  # Poll every 500ms

        start_time = datetime.now(timezone.utc)

        while (datetime.now(timezone.utc) - start_time).total_seconds() < monitor_timeout:
            all_terminal = True

            for order in orders:
                if order.status not in (OrderStatus.FILLED, OrderStatus.FAILED, OrderStatus.CANCELLED):
                    all_terminal = False

                    # Try to get order status from CLOB
                    # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
                    if order.order_id:
                        try:
                            order_status = await run_sync_in_thread(self.client.get_order, order.order_id)
                            if order_status:
                                filled = float(order_status.get("sizeFilled", 0))
                                if filled > 0:
                                    order.executed_size_usd = filled * order.price
                                    if filled >= float(order_status.get("size", 0)):
                                        order.status = OrderStatus.FILLED
                                    else:
                                        order.status = OrderStatus.PARTIAL
                        except Exception as e:
                            logger.debug(f"Error checking order status: {e}")

            if all_terminal:
                break

            await asyncio.sleep(poll_interval)

    async def _hedge_partial_fills(
        self,
        result: ExecutionResult,
        opportunity: ArbOpportunity
    ) -> List[OrderResult]:
        """
        Hedge partial fills using post-only GTC for maker rebates, with FAK fallback.

        When we have partial fills, we're left with directional exposure.
        If use_post_only_hedges is enabled (default True), first tries post-only GTC
        orders with 30s timeout for maker rebates. Falls back to FAK if not filled.

        Args:
            result: Execution result with partial fills.
            opportunity: Original opportunity.

        Returns:
            List of hedge order results.
        """
        hedge_orders = []
        use_post_only_hedges = self.config.trading.use_post_only_hedges
        hedge_timeout = self.config.trading.post_only_hedge_timeout_seconds

        for order in result.orders:
            if order.status == OrderStatus.FILLED and order.executed_size_usd > 0:
                # Sell the filled position to neutralize
                hedge_side = SELL if order.side == "BUY" else BUY
                remaining_size = order.executed_size_usd

                # Try post-only GTC first for maker rebates (hedges are less time-sensitive)
                if use_post_only_hedges and remaining_size > 0:
                    try:
                        hedge_order = await self._place_hedge_post_only(
                            token_id=order.token_id,
                            outcome_name=f"hedge_{order.outcome_name}",
                            side=hedge_side,
                            size_usd=remaining_size,
                            limit_price=order.price,
                            timeout_seconds=hedge_timeout
                        )
                        hedge_orders.append(hedge_order)

                        if hedge_order.status == OrderStatus.FILLED:
                            remaining_size -= hedge_order.executed_size_usd
                            # Track as maker fill
                            self._maker_fills += 1
                            self._maker_volume_usd += hedge_order.executed_size_usd
                            logger.info(
                                f"Hedge filled (MAKER): {hedge_side} {order.outcome_name} "
                                f"${hedge_order.executed_size_usd:.2f} (rebate earned!)"
                            )
                        elif hedge_order.status == OrderStatus.PARTIAL:
                            remaining_size -= hedge_order.executed_size_usd
                            # Partial counts as maker for filled portion
                            self._maker_fills += 1
                            self._maker_volume_usd += hedge_order.executed_size_usd
                            logger.warning(
                                f"Partial hedge (MAKER): {order.outcome_name} "
                                f"${hedge_order.executed_size_usd:.2f} filled, "
                                f"${remaining_size:.2f} needs FAK fallback"
                            )
                    except Exception as e:
                        logger.warning(f"Post-only hedge failed: {e}, falling back to FAK")

                # FAK fallback for remaining size (aggressive hedge)
                max_hedge_retries = 5
                retry_delay = 0.1  # 100ms between retries

                for retry in range(max_hedge_retries):
                    if remaining_size <= 0:
                        break

                    try:
                        hedge_order = await self._place_market_order(
                            token_id=order.token_id,
                            outcome_name=f"hedge_{order.outcome_name}",
                            side=hedge_side,
                            size_usd=remaining_size,
                            limit_price=order.price,
                            retry_on_no_match=False  # Already retrying in this loop
                        )
                        hedge_orders.append(hedge_order)

                        if hedge_order.status == OrderStatus.FILLED:
                            remaining_size -= hedge_order.executed_size_usd
                            # Track as taker fill
                            self._taker_fills += 1
                            self._taker_volume_usd += hedge_order.executed_size_usd
                            logger.info(
                                f"Hedge filled (TAKER/FAK): {hedge_side} {order.outcome_name} "
                                f"${hedge_order.executed_size_usd:.2f}"
                            )
                            break
                        elif hedge_order.status == OrderStatus.PARTIAL:
                            remaining_size -= hedge_order.executed_size_usd
                            self._taker_fills += 1
                            self._taker_volume_usd += hedge_order.executed_size_usd
                            logger.warning(
                                f"Partial hedge (TAKER): {order.outcome_name} "
                                f"${hedge_order.executed_size_usd:.2f} filled, "
                                f"${remaining_size:.2f} remaining"
                            )
                        else:
                            logger.warning(
                                f"Hedge attempt {retry + 1}/{max_hedge_retries} failed: "
                                f"{hedge_order.error}"
                            )

                    except Exception as e:
                        logger.error(f"Hedge exception attempt {retry + 1}: {e}")

                    if retry < max_hedge_retries - 1:
                        await asyncio.sleep(retry_delay)

                # Track remaining exposure
                if remaining_size > 0:
                    logger.error(
                        f"UNHEDGED EXPOSURE: {order.outcome_name} "
                        f"${remaining_size:.2f} could not be hedged"
                    )
                    self._update_market_exposure(
                        opportunity.market.condition_id,
                        remaining_size if order.side == "BUY" else -remaining_size
                    )

        # Check maker ratio and alert if below threshold
        self._check_maker_ratio_alert()

        return hedge_orders

    def _update_market_exposure(self, market_id: str, exposure_delta: float):
        """
        Update tracked exposure for a market.

        Args:
            market_id: Market identifier.
            exposure_delta: Change in exposure (+ve = long, -ve = short).
        """
        current = self._market_exposure.get(market_id, 0.0)
        self._market_exposure[market_id] = current + exposure_delta

        # Check if exposure exceeds max threshold
        max_exposure = self.config.starting_capital_usd * self._max_exposure_pct
        new_exposure = abs(self._market_exposure[market_id])

        if new_exposure > max_exposure:
            logger.warning(
                f"EXPOSURE WARNING: {market_id} exposure ${new_exposure:.2f} "
                f"exceeds max ${max_exposure:.2f} ({self._max_exposure_pct*100:.0f}% of capital)"
            )

    async def _place_hedge_post_only(
        self,
        token_id: str,
        outcome_name: str,
        side: str,
        size_usd: float,
        limit_price: float,
        timeout_seconds: float = 30.0,
        client: Optional[ClobClient] = None
    ) -> OrderResult:
        """
        Place a post-only GTC order for hedge/profit-lock with longer timeout.

        Hedges are less time-sensitive than primary arb legs, so we can wait
        longer for maker fills to earn rebates (~0.1-0.3%).

        Args:
            token_id: Token ID to trade.
            outcome_name: Name of outcome for logging.
            side: BUY or SELL.
            size_usd: Size in USD.
            limit_price: Reference price (will be improved for post-only).
            timeout_seconds: How long to wait before returning (default 30s).
            client: Optional CLOB client to use.

        Returns:
            OrderResult with execution details.
        """
        execution_client = client or self.client
        base_improvement = self.config.trading.post_only_price_improvement

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="BUY" if side == BUY else "SELL",
            requested_size_usd=size_usd,
            price=limit_price
        )

        # Conservative price improvement for hedges (we want fills, not best price)
        # Hedges just need to close position, so be more aggressive on price
        price_improvement = base_improvement + 0.005  # Add 0.5 cents for hedges

        # Calculate improved price for post-only
        # For BUY hedge: post above current best bid (more likely to fill)
        # For SELL hedge: post below current best ask (more likely to fill)
        if side == BUY:
            post_price = min(limit_price + price_improvement, 0.99)
        else:
            post_price = max(limit_price - price_improvement, 0.01)

        try:
            # Calculate size in shares from USD
            size_shares = size_usd / post_price if post_price > 0 else 0

            order_args = OrderArgs(
                token_id=token_id,
                price=post_price,
                size=size_shares,
                side=side,
            )

            # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
            signed_order = await run_sync_in_thread(execution_client.create_order, order_args)

            # Submit as GTC (Good Till Cancelled)
            response = await run_sync_in_thread(execution_client.post_order, signed_order, OrderType.GTC)

            if response and response.get("success"):
                order_id = response.get("orderID")
                result.order_id = order_id

                # Wait for fill with longer timeout for hedges
                filled = await self._wait_for_fill(execution_client, order_id, timeout_seconds)

                if filled:
                    result.status = OrderStatus.FILLED
                    result.executed_size_usd = size_usd
                    logger.info(
                        f"Hedge post-only FILLED: {side} {outcome_name} @ {post_price:.4f} "
                        f"for ${size_usd:.2f} (MAKER REBATE!)"
                    )
                    return result
                else:
                    # Cancel unfilled order - will fall back to FAK in caller
                    try:
                        # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
                        await run_sync_in_thread(execution_client.cancel, order_id)
                        logger.debug(f"Hedge post-only timed out after {timeout_seconds}s, cancelled {order_id}")
                    except Exception:
                        pass

                    result.status = OrderStatus.CANCELLED
                    result.error = f"Timeout after {timeout_seconds}s"
                    return result
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"
                result.status = OrderStatus.FAILED
                result.error = error_msg
                return result

        except Exception as e:
            result.status = OrderStatus.FAILED
            result.error = str(e)
            return result

    def _check_maker_ratio_alert(self):
        """
        Check maker vs taker ratio and log alert if below threshold.

        Only alerts once per session to avoid log spam.
        """
        total_fills = self._maker_fills + self._taker_fills
        if total_fills < 10:
            return  # Need minimum sample size

        maker_ratio = self._maker_fills / total_fills if total_fills > 0 else 0

        if maker_ratio < self.config.trading.maker_ratio_alert_threshold:
            if not self._maker_ratio_alert_sent:
                self._maker_ratio_alert_sent = True
                logger.warning(
                    f"MAKER RATIO ALERT: Only {maker_ratio*100:.1f}% maker fills "
                    f"(threshold: {self.config.trading.maker_ratio_alert_threshold*100:.0f}%). "
                    f"Maker: {self._maker_fills}, Taker: {self._taker_fills}. "
                    f"Consider increasing post_only_hedge_timeout_seconds."
                )

    def get_maker_taker_stats(self) -> dict:
        """Get maker vs taker statistics."""
        total_fills = self._maker_fills + self._taker_fills
        total_volume = self._maker_volume_usd + self._taker_volume_usd

        return {
            "maker_fills": self._maker_fills,
            "taker_fills": self._taker_fills,
            "total_fills": total_fills,
            "maker_ratio": self._maker_fills / total_fills if total_fills > 0 else 0,
            "maker_volume_usd": self._maker_volume_usd,
            "taker_volume_usd": self._taker_volume_usd,
            "total_volume_usd": total_volume,
            "maker_volume_ratio": self._maker_volume_usd / total_volume if total_volume > 0 else 0,
        }

    def get_market_exposure(self, market_id: str) -> float:
        """Get current exposure for a market."""
        return self._market_exposure.get(market_id, 0.0)

    def get_total_exposure(self) -> float:
        """Get total absolute exposure across all markets."""
        return sum(abs(e) for e in self._market_exposure.values())

    def check_exposure_limit(self, market_id: str, additional_size: float) -> bool:
        """
        Check if adding more exposure would exceed limits.

        Args:
            market_id: Market to check.
            additional_size: Size to potentially add.

        Returns:
            True if within limits, False if would exceed.
        """
        current = abs(self.get_market_exposure(market_id))
        max_exposure = self.config.starting_capital_usd * self._max_exposure_pct
        return (current + additional_size) <= max_exposure

    def reset_partial_pause(self):
        """Reset the consecutive partials counter (manual action)."""
        self._consecutive_partials = 0
        logger.info("Partial fill pause reset")

    def _apply_exposure_aware_sizing(self, opportunity: ArbOpportunity) -> ArbOpportunity:
        """
        Reduce trade size if it would exceed net exposure limits.
        Also applies exposure-offset sizing to push neutrality tighter.

        Target: <0.5% of capital as net exposure per market after trade.
        This prevents directional risk accumulation from partial fills.

        For neg-risk markets with larger edges (>0.6%), allows higher sizing (up to 8%)
        since neg-risk has better liquidation characteristics.

        Exposure-offset: If already long on certain outcomes, de-emphasize buy_arb
        sizes on those outcomes to push toward neutrality.

        Args:
            opportunity: The arb opportunity with initial sizing.

        Returns:
            Modified opportunity with exposure-adjusted sizing.
        """
        market_id = opportunity.market.condition_id
        current_exposure = self.get_market_exposure(market_id)

        # Net exposure target: 0.5% of capital (configurable)
        net_exposure_target_pct = 0.005  # 0.5%
        is_neg_risk = getattr(opportunity.market, 'neg_risk', False)

        # Neg-risk boldness: 8% exposure allowed for >0.6% edge
        if is_neg_risk and opportunity.profit_margin >= 0.006:  # 0.6%+ edge
            net_exposure_target_pct = 0.08  # 8% for high-edge neg-risk (capital efficient)
        elif is_neg_risk:
            net_exposure_target_pct = 0.04  # 4% for lower-edge neg-risk

        max_net_exposure = self.config.starting_capital_usd * net_exposure_target_pct

        # Calculate available exposure room
        # Positive exposure = we're long, negative = short
        # buy_arb adds positive exposure, sell_arb adds negative
        if opportunity.arb_type == ArbType.BUY_ARB:
            # Buying all outcomes adds exposure
            available_room = max_net_exposure - current_exposure
        else:
            # Selling all outcomes reduces/reverses exposure
            available_room = max_net_exposure + current_exposure

        if available_room <= 0:
            logger.debug(
                f"Market {market_id[:8]} at exposure limit: "
                f"current=${current_exposure:.2f}, max=${max_net_exposure:.2f}"
            )
            opportunity.trade_size_usd = 0
            return opportunity

        # Reduce trade size if it would exceed available room
        original_size = opportunity.trade_size_usd
        if opportunity.trade_size_usd > available_room:
            reduction_ratio = available_room / opportunity.trade_size_usd
            opportunity.trade_size_usd = available_room

            # Scale down outcome sizes proportionally
            for token_id in opportunity.outcome_sizes:
                opportunity.outcome_sizes[token_id] *= reduction_ratio

            opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin

            logger.debug(
                f"Exposure-adjusted sizing: ${original_size:.2f} -> ${opportunity.trade_size_usd:.2f} "
                f"(current exposure: ${current_exposure:.2f}, limit: ${max_net_exposure:.2f})"
            )

        # Exposure-offset sizing: de-emphasize outcomes where we're already exposed
        # If long on an outcome, reduce buy size on that outcome to push neutrality
        if current_exposure != 0 and opportunity.trade_size_usd > 0:
            opportunity = self._apply_exposure_offset(opportunity, current_exposure)

        return opportunity

    def _apply_exposure_offset(
        self,
        opportunity: ArbOpportunity,
        current_exposure: float
    ) -> ArbOpportunity:
        """
        De-emphasize outcomes where we already have exposure to push neutrality tighter.

        If we're long (positive exposure) and doing a buy_arb, reduce sizes on
        outcomes that increase our directional risk. Vice versa for sell_arb.

        Args:
            opportunity: The opportunity with initial sizing.
            current_exposure: Current net exposure in this market.

        Returns:
            Modified opportunity with offset-adjusted outcome sizes.
        """
        # Only apply offset if we have meaningful exposure
        exposure_threshold = self.config.starting_capital_usd * 0.001  # 0.1% threshold
        if abs(current_exposure) < exposure_threshold:
            return opportunity

        # Determine offset direction
        # Positive exposure = we're already long, so de-emphasize further longs
        # For buy_arb: reduce sizes uniformly (all outcomes add exposure)
        # For sell_arb: actually helps neutralize, so slight boost if possible
        if opportunity.arb_type == ArbType.BUY_ARB and current_exposure > 0:
            # We're long and buying more - reduce size by exposure ratio
            # Max reduction: 30% to still capture the arb
            exposure_ratio = min(current_exposure / self.config.starting_capital_usd, 0.3)
            offset_multiplier = 1.0 - exposure_ratio

            for token_id in opportunity.outcome_sizes:
                opportunity.outcome_sizes[token_id] *= offset_multiplier

            opportunity.trade_size_usd *= offset_multiplier
            opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin

            logger.debug(
                f"Exposure-offset applied: {offset_multiplier:.2%} multiplier "
                f"(exposure: ${current_exposure:.2f})"
            )

        elif opportunity.arb_type == ArbType.SELL_ARB and current_exposure < 0:
            # We're short and selling more - reduce size similarly
            exposure_ratio = min(abs(current_exposure) / self.config.starting_capital_usd, 0.3)
            offset_multiplier = 1.0 - exposure_ratio

            for token_id in opportunity.outcome_sizes:
                opportunity.outcome_sizes[token_id] *= offset_multiplier

            opportunity.trade_size_usd *= offset_multiplier
            opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin

            logger.debug(
                f"Exposure-offset applied: {offset_multiplier:.2%} multiplier "
                f"(exposure: ${current_exposure:.2f})"
            )

        return opportunity

    async def _execute_buy_arb(
        self,
        opportunity: ArbOpportunity,
        client: Optional[ClobClient] = None,
        wallet_state: Optional[Any] = None
    ) -> List[OrderResult]:
        """
        Execute a buy arbitrage (buy all outcomes) with atomic-or-failsafe pattern.

        Per Grok Round 21 CRITICAL: Polymarket has NO atomic multi-leg bundles.
        Sequential/parallel placement of individual orders = race window where
        1-2 legs fill before arb disappears → naked directional exposure.

        Strategy (atomic-or-failsafe):
        1. Place ALL legs as post-only limits simultaneously (maker priority)
        2. Wait 300ms strict timeout for fills
        3. Check fill status - if ANY leg unfilled, cancel ALL + hedge filled legs
        4. Only accept complete fills across all legs

        This is how rn1 avoids exposure: tiny sizes + immediate hedge on partials.

        Args:
            opportunity: The arb opportunity.
            client: Optional specific client to use (for wallet rotation).
            wallet_state: Optional wallet state for tracking.
        """
        orders = []
        orderbooks = opportunity.orderbooks

        # Per Grok Round 21: Strict atomic timeout - 300ms max for arb window
        ATOMIC_TIMEOUT_MS = 300

        # Get execution client (use provided or get from wallet manager)
        exec_client = client
        exec_wallet = wallet_state
        if exec_client is None:
            exec_client, exec_wallet = await self.get_execution_client(
                min_balance=opportunity.trade_size_usd
            )

        # Per Grok Round 21: Pre-execution depth check at EXACT prices
        # Verify depth still exists before placing orders
        for outcome in opportunity.market.outcomes:
            size_usd = opportunity.outcome_sizes.get(outcome.token_id, 0)
            if size_usd > 0:
                ob = orderbooks.get_orderbook(outcome.token_id)
                if not ob or not ob.best_ask:
                    logger.warning(f"Pre-exec depth check: No ask for {outcome.name}")
                    return orders  # Abort - missing depth

                available_depth = ob.best_ask_size * ob.best_ask_price
                if available_depth < size_usd * 0.8:  # Need at least 80% depth
                    logger.warning(
                        f"Pre-exec depth check: Insufficient depth for {outcome.name} "
                        f"(need ${size_usd:.2f}, have ${available_depth:.2f})"
                    )
                    return orders  # Abort - insufficient depth

        # Phase 1: Place ALL legs as post-only limits simultaneously
        # Per Grok Round 21: Use _place_atomic_leg for strict post-only (no FAK fallback)
        placement_tasks = []
        leg_info = []  # Track order info for cancel/hedge

        for outcome in opportunity.market.outcomes:
            size_usd = opportunity.outcome_sizes.get(outcome.token_id, 0)
            if size_usd > 0:
                ob = orderbooks.get_orderbook(outcome.token_id)
                price = ob.best_ask_price if ob and ob.best_ask else 0

                leg_info.append({
                    "token_id": outcome.token_id,
                    "outcome_name": outcome.name,
                    "size_usd": size_usd,
                    "price": price
                })

                placement_tasks.append(
                    self._place_atomic_leg(
                        token_id=outcome.token_id,
                        outcome_name=outcome.name,
                        side=BUY,
                        size_usd=size_usd,
                        limit_price=price,
                        edge_pct=opportunity.profit_margin,
                        client=exec_client
                    )
                )

        # Execute all placements simultaneously
        placement_start = time.time()
        placement_results = await asyncio.gather(*placement_tasks, return_exceptions=True)

        # Collect order IDs and initial results
        order_ids = []
        for i, result in enumerate(placement_results):
            if isinstance(result, OrderResult):
                orders.append(result)
                if result.order_id:
                    order_ids.append((result.order_id, leg_info[i]))
            elif isinstance(result, Exception):
                logger.error(f"Atomic leg placement failed: {result}")
                orders.append(OrderResult(
                    token_id=leg_info[i]["token_id"] if i < len(leg_info) else "unknown",
                    outcome_name=leg_info[i]["outcome_name"] if i < len(leg_info) else "unknown",
                    side="BUY",
                    requested_size_usd=leg_info[i]["size_usd"] if i < len(leg_info) else 0,
                    status=OrderStatus.FAILED,
                    error=str(result)
                ))

        # Phase 2: Wait strict timeout then check fills
        elapsed_ms = (time.time() - placement_start) * 1000
        remaining_wait = max(0, (ATOMIC_TIMEOUT_MS - elapsed_ms) / 1000)
        if remaining_wait > 0:
            await asyncio.sleep(remaining_wait)

        # Phase 3: Check fill status for ALL legs
        filled_legs = []
        unfilled_legs = []

        for order in orders:
            if order.order_id and exec_client:
                try:
                    order_status = await run_sync_in_thread(exec_client.get_order, order.order_id)
                    if order_status:
                        filled_size = float(order_status.get("sizeFilled", 0))
                        total_size = float(order_status.get("size", 0))

                        if filled_size > 0:
                            order.executed_size_usd = filled_size * order.price
                            if filled_size >= total_size * 0.95:  # 95%+ = filled
                                order.status = OrderStatus.FILLED
                                filled_legs.append(order)
                            else:
                                order.status = OrderStatus.PARTIAL
                                unfilled_legs.append(order)
                        else:
                            unfilled_legs.append(order)
                except Exception as e:
                    logger.debug(f"Error checking order {order.order_id}: {e}")
                    unfilled_legs.append(order)
            else:
                if order.status == OrderStatus.FILLED:
                    filled_legs.append(order)
                else:
                    unfilled_legs.append(order)

        # Phase 4: Atomic decision - ALL filled or CANCEL + HEDGE
        all_legs_filled = len(unfilled_legs) == 0 and len(filled_legs) == len(leg_info)

        if not all_legs_filled:
            # Per Grok Round 21: NOT atomic - cancel unfilled + hedge filled immediately
            logger.warning(
                f"ATOMIC FAILSAFE: {len(filled_legs)}/{len(leg_info)} legs filled, "
                f"cancelling {len(unfilled_legs)} unfilled + hedging filled"
            )

            # Cancel all unfilled orders immediately
            cancel_tasks = []
            for order in orders:
                if order.order_id and order.status not in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                    cancel_tasks.append(self._cancel_order_safe(exec_client, order.order_id))

            if cancel_tasks:
                await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # Immediately hedge any filled legs (sell what we bought)
            if filled_legs:
                hedge_tasks = []
                for filled_order in filled_legs:
                    if filled_order.executed_size_usd > 0:
                        hedge_tasks.append(
                            self._place_atomic_hedge(
                                token_id=filled_order.token_id,
                                outcome_name=f"hedge_{filled_order.outcome_name}",
                                size_usd=filled_order.executed_size_usd,
                                limit_price=filled_order.price,
                                client=exec_client
                            )
                        )

                if hedge_tasks:
                    hedge_results = await asyncio.gather(*hedge_tasks, return_exceptions=True)
                    hedged_usd = sum(
                        r.executed_size_usd for r in hedge_results
                        if isinstance(r, OrderResult) and r.status == OrderStatus.FILLED
                    )
                    logger.info(f"Atomic hedge completed: ${hedged_usd:.2f} hedged of ${sum(o.executed_size_usd for o in filled_legs):.2f}")

            # Mark result as incomplete
            for order in orders:
                if order.status not in (OrderStatus.FILLED, OrderStatus.CANCELLED):
                    order.status = OrderStatus.CANCELLED
        else:
            # All legs filled - success!
            self._maker_fills += len(filled_legs)
            self._maker_volume_usd += sum(o.executed_size_usd for o in filled_legs)
            logger.info(f"ATOMIC SUCCESS: All {len(leg_info)} legs filled")

        # Record execution in wallet manager
        if exec_wallet and self._wallet_manager:
            total_executed = sum(o.executed_size_usd for o in orders if o.status == OrderStatus.FILLED)
            success = all_legs_filled
            self._wallet_manager.record_execution(exec_wallet, total_executed, success)

        return orders

    async def _place_atomic_leg(
        self,
        token_id: str,
        outcome_name: str,
        side: str,
        size_usd: float,
        limit_price: float,
        edge_pct: float = 0.0,
        client: Optional[ClobClient] = None
    ) -> OrderResult:
        """
        Place a single leg of an atomic arb - pure post-only, no FAK fallback.

        Per Grok Round 21: Atomic legs MUST be post-only. FAK fallback defeats
        atomicity because we'd accept partial fills on individual legs.

        Returns OrderResult with order_id for status checking.
        """
        execution_client = client or self.client
        base_improvement = self.config.trading.post_only_price_improvement

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="BUY" if side == BUY else "SELL",
            requested_size_usd=size_usd,
            price=limit_price
        )

        # Dynamic price improvement for maker fills
        price_improvement = base_improvement + (edge_pct * 0.5)
        price_improvement = min(price_improvement, 0.03)

        if side == BUY:
            post_price = min(limit_price + price_improvement, 0.99)
        else:
            post_price = max(limit_price - price_improvement, 0.01)

        try:
            size_shares = size_usd / post_price if post_price > 0 else 0

            order_args = OrderArgs(
                token_id=token_id,
                price=post_price,
                size=size_shares,
                side=side,
            )

            signed_order = await run_sync_in_thread(execution_client.create_order, order_args)
            response = await run_sync_in_thread(execution_client.post_order, signed_order, OrderType.GTC)

            if response and response.get("success"):
                result.order_id = response.get("orderID")
                result.status = OrderStatus.SUBMITTED
                result.price = post_price
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"
                result.status = OrderStatus.FAILED
                result.error = error_msg

        except Exception as e:
            result.status = OrderStatus.FAILED
            result.error = str(e)

        return result

    async def _place_atomic_hedge(
        self,
        token_id: str,
        outcome_name: str,
        size_usd: float,
        limit_price: float,
        client: Optional[ClobClient] = None
    ) -> OrderResult:
        """
        Place immediate hedge for atomic failsafe - aggressive FOK to close exposure.

        Per Grok Round 21: When atomic arb fails, we MUST hedge immediately.
        Use FOK (all-or-nothing) to guarantee full hedge or nothing.
        """
        execution_client = client or self.client

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="SELL",  # Hedging = selling what we bought
            requested_size_usd=size_usd,
            price=limit_price
        )

        # Aggressive price for immediate fill - accept up to 2% worse
        hedge_price = max(limit_price * 0.98, 0.01)

        try:
            size_shares = size_usd / hedge_price if hedge_price > 0 else 0

            order_args = OrderArgs(
                token_id=token_id,
                price=hedge_price,
                size=size_shares,
                side=SELL,
            )

            signed_order = await run_sync_in_thread(execution_client.create_order, order_args)
            response = await run_sync_in_thread(execution_client.post_order, signed_order, OrderType.FOK)

            if response and response.get("success"):
                result.order_id = response.get("orderID")
                result.status = OrderStatus.FILLED
                result.executed_size_usd = size_usd
                result.price = hedge_price
                logger.debug(f"Atomic hedge FILLED: SELL {outcome_name} ${size_usd:.2f}")
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"
                result.status = OrderStatus.FAILED
                result.error = error_msg
                logger.warning(f"Atomic hedge FAILED: {error_msg}")

        except Exception as e:
            result.status = OrderStatus.FAILED
            result.error = str(e)
            logger.error(f"Atomic hedge exception: {e}")

        return result

    async def _cancel_order_safe(self, client: ClobClient, order_id: str) -> bool:
        """Cancel an order safely, ignoring errors if already filled/cancelled."""
        try:
            await run_sync_in_thread(client.cancel, order_id)
            return True
        except Exception:
            return False  # Already filled or cancelled

    async def _execute_sell_arb(
        self,
        opportunity: ArbOpportunity,
        client: Optional[ClobClient] = None,
        wallet_state: Optional[Any] = None
    ) -> List[OrderResult]:
        """
        Execute a sell arbitrage (sell all outcomes).

        Args:
            opportunity: The arb opportunity.
            client: Optional specific client to use (for wallet rotation).
            wallet_state: Optional wallet state for tracking.
        """
        orders = []
        orderbooks = opportunity.orderbooks

        # Get execution client (use provided or get from wallet manager)
        exec_client = client
        exec_wallet = wallet_state
        if exec_client is None:
            exec_client, exec_wallet = await self.get_execution_client(
                min_balance=opportunity.trade_size_usd * 1.2  # Need collateral buffer
            )

        # Determine order placement method (post-only or FOK)
        use_post_only = self.config.trading.use_post_only

        tasks = []
        for outcome in opportunity.market.outcomes:
            size_usd = opportunity.outcome_sizes.get(outcome.token_id, 0)
            if size_usd > 0:
                ob = orderbooks.get_orderbook(outcome.token_id)
                price = ob.best_bid_price if ob and ob.best_bid else 0

                if use_post_only:
                    tasks.append(
                        self._place_post_only_order(
                            token_id=outcome.token_id,
                            outcome_name=outcome.name,
                            side=SELL,
                            size_usd=size_usd,
                            limit_price=price,
                            edge_pct=opportunity.profit_margin,
                            client=exec_client,
                            wallet_state=exec_wallet
                        )
                    )
                else:
                    tasks.append(
                        self._place_market_order(
                            token_id=outcome.token_id,
                            outcome_name=outcome.name,
                            side=SELL,
                            size_usd=size_usd,
                            limit_price=price,
                            client=exec_client,
                            wallet_state=exec_wallet
                        )
                    )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, OrderResult):
                orders.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Order failed: {result}")
                orders.append(OrderResult(
                    token_id="unknown",
                    outcome_name="unknown",
                    side="SELL",
                    requested_size_usd=0,
                    status=OrderStatus.FAILED,
                    error=str(result)
                ))

        # Record execution in wallet manager
        if exec_wallet and self._wallet_manager:
            total_executed = sum(o.executed_size_usd for o in orders)
            success = any(o.status == OrderStatus.FILLED for o in orders)
            self._wallet_manager.record_execution(exec_wallet, total_executed, success)

        return orders

    async def execute_burst_parallel(
        self,
        opportunity: ArbOpportunity,
        is_burst_mode: bool = False
    ) -> ExecutionResult:
        """
        Execute an arb with burst parallelization for deep opportunities.

        During burst mode, deep arbs (>2x min_depth) are split across top-2 wallets
        for parallel execution, maximizing fill probability in race conditions.

        Args:
            opportunity: The arb opportunity.
            is_burst_mode: Whether we're currently in burst mode.

        Returns:
            Combined ExecutionResult from all wallet executions.
        """
        # Check if this qualifies for burst parallelization
        # Requirements: burst_mode active, wallet_manager available, deep arb (>2x min_depth)
        if not is_burst_mode or self._wallet_manager is None:
            return await self.execute_opportunity(opportunity)

        # Calculate depth ratio (how much deeper than min_depth)
        min_depth = self.config.trading.min_depth_usd
        depth_ratio = opportunity.trade_size_usd / min_depth if min_depth > 0 else 0

        # Only parallelize if depth >= configured ratio (default 2x min_depth)
        parallel_threshold = self.config.trading.burst_parallel_depth_ratio
        if depth_ratio < parallel_threshold:
            return await self.execute_opportunity(opportunity)

        # Get top-2 wallets for burst parallel execution
        top_wallets = await self._wallet_manager.get_top_wallets_for_burst(
            count=2,
            min_balance=opportunity.trade_size_usd / 2  # Each wallet needs half
        )

        if len(top_wallets) < 2:
            # Not enough wallets, fall back to single execution
            logger.debug("Burst parallel: not enough wallets, using single execution")
            return await self.execute_opportunity(opportunity)

        # Split the opportunity across wallets
        half_size = opportunity.trade_size_usd / 2
        logger.info(
            f"BURST PARALLEL: Splitting ${opportunity.trade_size_usd:.2f} arb across 2 wallets "
            f"(depth ratio: {depth_ratio:.1f}x)"
        )

        # Create split opportunities (each gets half the size)
        split_results = []

        # Execute on both wallets in parallel
        tasks = []
        for i, wallet in enumerate(top_wallets):
            # Create a copy with halved sizes
            split_opp = ArbOpportunity(
                market=opportunity.market,
                arb_type=opportunity.arb_type,
                orderbooks=opportunity.orderbooks,
                profit_margin=opportunity.profit_margin,
                trade_size_usd=half_size,
                outcome_sizes={
                    token_id: size / 2
                    for token_id, size in opportunity.outcome_sizes.items()
                },
                expected_profit_usd=opportunity.expected_profit_usd / 2,
                timestamp=opportunity.timestamp
            )

            if opportunity.arb_type == ArbType.BUY_ARB:
                tasks.append(
                    self._execute_buy_arb(split_opp, wallet.client, wallet)
                )
            else:
                tasks.append(
                    self._execute_sell_arb(split_opp, wallet.client, wallet)
                )

        # Execute both in parallel
        parallel_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        all_orders = []
        total_executed = 0.0

        for result in parallel_results:
            if isinstance(result, list):  # List of OrderResults
                all_orders.extend(result)
                total_executed += sum(o.executed_size_usd for o in result)
            elif isinstance(result, Exception):
                logger.error(f"Burst parallel execution error: {result}")

        # Build combined result
        combined_result = ExecutionResult(
            opportunity=opportunity,
            orders=all_orders,
            total_cost_usd=total_executed,
            expected_return_usd=total_executed * (1 + opportunity.profit_margin),
            is_complete=all(o.status == OrderStatus.FILLED for o in all_orders),
            has_partial_fill=any(o.status == OrderStatus.PARTIAL for o in all_orders)
        )

        if combined_result.is_complete:
            combined_result.realized_profit_usd = total_executed * opportunity.profit_margin
            self._total_profit += combined_result.realized_profit_usd
            self._execution_count += 1
            self._total_volume += total_executed
            logger.info(
                f"Burst parallel complete! Wallets: {len(top_wallets)}, "
                f"Profit: ${combined_result.realized_profit_usd:.4f}"
            )

        return combined_result

    async def _place_market_order(
        self,
        token_id: str,
        outcome_name: str,
        side: str,
        size_usd: float,
        limit_price: float,
        retry_on_no_match: bool = True,
        client: Optional[ClobClient] = None,
        wallet_state: Optional[Any] = None,
        use_ioc: bool = True
    ) -> OrderResult:
        """
        Place a market order using FAK (Fill-And-Kill) for more fills.

        FAK is Polymarket's IOC equivalent - fills available liquidity and auto-cancels remainder.
        This increases trade frequency 2-5x vs FOK (all-or-nothing).
        On partial fills, immediately hedges the unbalanced leg if slippage < 0.3%.

        Args:
            token_id: Token ID to trade.
            outcome_name: Name of outcome for logging.
            side: BUY or SELL.
            size_usd: Size in USD.
            limit_price: Limit price for the order.
            retry_on_no_match: If True, retry once on "no match" error.
            client: Optional CLOB client to use (for wallet rotation).
            wallet_state: Optional wallet state for tracking (from WalletManager).
            use_ioc: If True, use FAK mode (partial fills allowed). Default True.

        Returns:
            OrderResult with execution details.
        """
        # Use provided client or fall back to default
        execution_client = client or self.client

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="BUY" if side == BUY else "SELL",
            requested_size_usd=size_usd,
            price=limit_price
        )

        max_attempts = 2 if retry_on_no_match else 1

        # Per Grok Round 17: FAK removed from py-clob-client - use FOK only
        # FOK = Fill or Kill (all or nothing) - clip size to L1 depth for higher fill rate
        # rn1 pattern: Post-Only primary → FOK fallback (clipped)
        # Note: use_ioc parameter kept for API compatibility but FAK path removed

        for attempt in range(max_attempts):
            try:
                # Per Grok Round 17: Always use FOK (FAK not available)
                # FOK requires full fill - clip to available depth for success
                order_type_to_use = OrderType.FOK
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=size_usd,
                    side=side,
                    order_type=order_type_to_use
                )

                # Create and sign the order
                # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
                signed_order = await run_sync_in_thread(execution_client.create_market_order, order_args)

                # Submit the order
                response = await run_sync_in_thread(execution_client.post_order, signed_order, order_type_to_use)

                # Parse response
                if response and response.get("success"):
                    order_id = response.get("orderID")
                    result.order_id = order_id

                    # Per Grok Round 17: FOK mode only - all or nothing
                    # Success means full fill (FOK rejects if can't fill entirely)
                    result.status = OrderStatus.FILLED
                    result.executed_size_usd = size_usd
                    self._taker_fills += 1
                    self._taker_volume_usd += size_usd
                    logger.debug(
                        f"FOK filled: {side} {outcome_name} @ {limit_price:.4f} "
                        f"for ${size_usd:.2f}"
                    )

                    return result

                else:
                    error_msg = response.get("errorMsg", "Unknown error") if response else "No response"

                    # Check for "no match" error - orderbook moved, retry once
                    if "no match" in error_msg.lower() and attempt < max_attempts - 1:
                        logger.warning(
                            f"Order got 'no match' on {outcome_name}, "
                            f"retrying (attempt {attempt + 2}/{max_attempts})..."
                        )
                        await asyncio.sleep(0.2)  # Brief pause before retry
                        continue

                    result.status = OrderStatus.FAILED
                    result.error = error_msg
                    self._consecutive_failures += 1
                    logger.warning(f"Order failed: {result.error}")
                    return result

            except Exception as e:
                error_str = str(e)

                # Retry on "no match" exception as well
                if "no match" in error_str.lower() and attempt < max_attempts - 1:
                    logger.warning(
                        f"Order exception 'no match' on {outcome_name}, "
                        f"retrying (attempt {attempt + 2}/{max_attempts})..."
                    )
                    await asyncio.sleep(0.2)
                    continue

                result.status = OrderStatus.FAILED
                result.error = error_str
                self._consecutive_failures += 1
                logger.error(f"Order exception: {e}")
                return result

        return result

    async def _place_post_only_order(
        self,
        token_id: str,
        outcome_name: str,
        side: str,
        size_usd: float,
        limit_price: float,
        edge_pct: float = 0.0,
        client: Optional[ClobClient] = None,
        wallet_state: Optional[Any] = None,
        is_trash_trade: bool = False
    ) -> OrderResult:
        """
        Place a post-only limit order for maker rebates - absolute primary strategy.

        Per Grok Round 8: rn1 NEVER pays taker fees - accepts non-fills over crosses.
        This matches on-chain evidence: >90% maker volume, rebates are core edge.

        Strategy:
        1. Post-only GTC order with price improvement
        2. Wait 500ms for fill
        3. Check fill status - if >=50% filled, ACCEPT IT (rn1 tolerated partials)
        4. Only FAK remaining unfilled portion as LAST RESORT

        Per Grok Round 9: When is_trash_trade=True (volume farming mode):
        - Use PURE post-only with NO FAK retry
        - Accept ANY partial or zero fill
        - rn1 bought cheap losers without chasing crosses - volume over fills

        Key insight: rn1 preferred losing an arb opportunity over paying taker fees.
        Maker rebates (~0.02%) compound significantly over thousands of trades.

        Args:
            token_id: Token ID to trade.
            outcome_name: Name of outcome for logging.
            side: BUY or SELL.
            size_usd: Size in USD.
            limit_price: Reference price (will be improved for post-only).
            edge_pct: Arb edge as decimal (e.g., 0.005 for 0.5%).
            client: Optional CLOB client to use.
            wallet_state: Optional wallet state for tracking.
            is_trash_trade: If True, pure post-only mode (no FAK retry, accept any fill).

        Returns:
            OrderResult with execution details.
        """
        execution_client = client or self.client
        timeout = self.config.trading.post_only_timeout_seconds
        base_improvement = self.config.trading.post_only_price_improvement

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="BUY" if side == BUY else "SELL",
            requested_size_usd=size_usd,
            price=limit_price
        )

        # Dynamic price improvement: improvement = base + (edge_pct * 0.5)
        price_improvement = base_improvement + (edge_pct * 0.5)
        price_improvement = min(price_improvement, 0.03)  # Cap at 3 cents

        # Calculate improved price for post-only
        if side == BUY:
            post_price = min(limit_price + price_improvement, 0.99)
        else:
            post_price = max(limit_price - price_improvement, 0.01)

        try:
            size_shares = size_usd / post_price if post_price > 0 else 0

            order_args = OrderArgs(
                token_id=token_id,
                price=post_price,
                size=size_shares,
                side=side,
            )

            signed_order = await run_sync_in_thread(execution_client.create_order, order_args)
            response = await run_sync_in_thread(execution_client.post_order, signed_order, OrderType.GTC)

            if response and response.get("success"):
                order_id = response.get("orderID")
                result.order_id = order_id

                # Per Grok Round 8: Wait and check for partial fills
                fill_status = await self._wait_for_fill_with_partial(
                    execution_client, order_id, timeout, size_shares
                )

                filled_shares = fill_status.get("filled_shares", 0)
                filled_usd = filled_shares * post_price
                fill_ratio = filled_shares / size_shares if size_shares > 0 else 0

                if fill_ratio >= 1.0:
                    # Full fill - best case
                    result.status = OrderStatus.FILLED
                    result.executed_size_usd = size_usd
                    self._maker_fills += 1
                    self._maker_volume_usd += size_usd
                    # Track maker rebate estimate
                    self._track_maker_rebate(size_usd)
                    logger.debug(
                        f"Post-only FULL FILL (MAKER): {side} {outcome_name} @ {post_price:.4f} "
                        f"${size_usd:.2f} (rebate: ${size_usd * 0.0002:.4f})"
                    )
                    return result

                elif fill_ratio >= 0.5:
                    # Per Grok Round 8: >=50% filled - ACCEPT IT (rn1 pattern)
                    # Cancel remaining, don't chase with taker order
                    try:
                        await run_sync_in_thread(execution_client.cancel, order_id)
                    except Exception:
                        pass

                    result.status = OrderStatus.PARTIAL
                    result.executed_size_usd = filled_usd
                    self._maker_fills += 1
                    self._maker_volume_usd += filled_usd
                    self._track_maker_rebate(filled_usd)
                    logger.info(
                        f"Post-only PARTIAL ACCEPTED (MAKER): {side} {outcome_name} "
                        f"${filled_usd:.2f}/{size_usd:.2f} ({fill_ratio*100:.0f}%) - rn1 pattern"
                    )
                    return result

                elif fill_ratio > 0:
                    # <50% filled - cancel order
                    try:
                        await run_sync_in_thread(execution_client.cancel, order_id)
                    except Exception:
                        pass

                    # Track the partial maker fill
                    self._maker_fills += 1
                    self._maker_volume_usd += filled_usd
                    self._track_maker_rebate(filled_usd)

                    # Per Grok Round 9: For trash trades, accept ANY partial - no FAK chase
                    # rn1 bought cheap losers for volume, not fill optimization
                    if is_trash_trade:
                        result.status = OrderStatus.PARTIAL
                        result.executed_size_usd = filled_usd
                        logger.info(
                            f"TRASH TRADE partial accepted (MAKER): {side} {outcome_name} "
                            f"${filled_usd:.2f}/{size_usd:.2f} ({fill_ratio*100:.0f}%) - volume farming"
                        )
                        return result

                    # Per Grok Round 22: STRICT POST-ONLY - retry with improved price instead of FAK
                    # rn1 NEVER pays taker fees - price improvement retry captures more maker fills
                    remaining_usd = size_usd - filled_usd
                    logger.debug(
                        f"Post-only partial ({fill_ratio*100:.0f}%), retrying with improved price "
                        f"${remaining_usd:.2f}"
                    )

                    # Retry with price improvement (1 tick = $0.001)
                    improved_price = post_price + 0.001 if side == BUY else post_price - 0.001
                    improved_price = max(0.01, min(0.99, improved_price))

                    retry_result = await self._place_post_only_retry(
                        token_id=token_id,
                        outcome_name=outcome_name,
                        side=side,
                        size_usd=remaining_usd,
                        limit_price=improved_price,
                        client=client,
                        wallet_state=wallet_state,
                        max_retries=2  # Up to 2 price improvement retries
                    )

                    # Combine results
                    result.executed_size_usd = filled_usd + retry_result.executed_size_usd
                    result.status = OrderStatus.FILLED if result.executed_size_usd >= size_usd * 0.9 else OrderStatus.PARTIAL
                    return result

                else:
                    # Zero fill - cancel order
                    try:
                        await run_sync_in_thread(execution_client.cancel, order_id)
                    except Exception:
                        pass

                    # Per Grok Round 9: For trash trades, accept zero fill - no FAK chase
                    # rn1 prioritized volume (order count) over actual fills
                    if is_trash_trade:
                        result.status = OrderStatus.CANCELLED
                        result.executed_size_usd = 0
                        logger.info(
                            f"TRASH TRADE zero fill accepted: {side} {outcome_name} "
                            f"${size_usd:.2f} @ ${limit_price:.4f} - pure post-only, no FAK"
                        )
                        return result

                    # Per Grok Round 22: STRICT POST-ONLY - retry with improved price, NOT FAK
                    # rn1 never paid taker fees - accept non-fill over crossing spread
                    logger.debug(
                        f"Post-only zero fill after {timeout}s, retrying with improved price (no FAK)"
                    )

                    # Retry with price improvement
                    improved_price = post_price + 0.001 if side == BUY else post_price - 0.001
                    improved_price = max(0.01, min(0.99, improved_price))

                    return await self._place_post_only_retry(
                        token_id=token_id,
                        outcome_name=outcome_name,
                        side=side,
                        size_usd=size_usd,
                        limit_price=improved_price,
                        client=client,
                        wallet_state=wallet_state,
                        max_retries=2
                    )
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"

                # Per Grok Round 9: For trash trades, don't fall back on submission failure
                if is_trash_trade:
                    logger.debug(f"TRASH TRADE submission failed: {error_msg} - no retry")
                    result.status = OrderStatus.FAILED
                    result.error = error_msg
                    return result

                # Per Grok Round 22: STRICT POST-ONLY - no FAK fallback on submission failure
                logger.debug(f"Post-only submission failed: {error_msg}, accepting non-fill (no FAK)")
                result.status = OrderStatus.FAILED
                result.error = error_msg
                return result

        except Exception as e:
            # Per Grok Round 9/22: No FAK fallback on exception - accept non-fill
            logger.debug(f"Post-only exception: {e}, accepting non-fill (no FAK)")
            result.status = OrderStatus.FAILED
            result.error = str(e)
            return result

    async def _place_post_only_retry(
        self,
        token_id: str,
        outcome_name: str,
        side: str,
        size_usd: float,
        limit_price: float,
        client: Optional[ClobClient] = None,
        wallet_state: Optional[Any] = None,
        max_retries: int = 2
    ) -> OrderResult:
        """
        Per Grok Round 22: Price-improvement retry for post-only orders.

        rn1 NEVER paid taker fees. Instead of FAK fallback, retry with improved price
        to capture more maker fills while maintaining maker rebate.

        Args:
            token_id: Token ID to trade.
            outcome_name: Name of outcome for logging.
            side: BUY or SELL.
            size_usd: Remaining size in USD.
            limit_price: Starting price (already improved from original).
            client: Optional CLOB client.
            wallet_state: Optional wallet state.
            max_retries: Maximum price improvement retries.

        Returns:
            OrderResult with execution details.
        """
        execution_client = client or self.client
        timeout = 0.3  # 300ms per retry (faster)

        result = OrderResult(
            token_id=token_id,
            outcome_name=outcome_name,
            side="BUY" if side == BUY else "SELL",
            requested_size_usd=size_usd,
            price=limit_price
        )

        current_price = limit_price
        total_filled_usd = 0.0

        for retry in range(max_retries):
            try:
                size_shares = (size_usd - total_filled_usd) / current_price if current_price > 0 else 0
                if size_shares <= 0:
                    break

                order_args = OrderArgs(
                    token_id=token_id,
                    price=current_price,
                    size=size_shares,
                    side=side,
                )

                signed_order = await run_sync_in_thread(execution_client.create_order, order_args)
                response = await run_sync_in_thread(execution_client.post_order, signed_order, OrderType.GTC)

                if response and response.get("success"):
                    order_id = response.get("orderID")

                    # Short wait for fill
                    fill_status = await self._wait_for_fill_with_partial(
                        execution_client, order_id, timeout, size_shares
                    )

                    filled_shares = fill_status.get("filled_shares", 0)
                    filled_usd = filled_shares * current_price

                    # Cancel remaining
                    try:
                        await run_sync_in_thread(execution_client.cancel, order_id)
                    except Exception:
                        pass

                    if filled_usd > 0:
                        total_filled_usd += filled_usd
                        self._maker_fills += 1
                        self._maker_volume_usd += filled_usd
                        self._track_maker_rebate(filled_usd)

                        if total_filled_usd >= size_usd * 0.9:
                            # Good enough fill
                            result.status = OrderStatus.FILLED
                            result.executed_size_usd = total_filled_usd
                            logger.debug(
                                f"Post-only retry {retry+1}: FILLED ${total_filled_usd:.2f} (MAKER)"
                            )
                            return result

                # Improve price for next retry
                current_price = current_price + 0.001 if side == BUY else current_price - 0.001
                current_price = max(0.01, min(0.99, current_price))

            except Exception as e:
                logger.debug(f"Post-only retry {retry+1} error: {e}")
                break

        # Return whatever we got
        result.executed_size_usd = total_filled_usd
        result.status = OrderStatus.PARTIAL if total_filled_usd > 0 else OrderStatus.CANCELLED
        logger.debug(
            f"Post-only retry complete: ${total_filled_usd:.2f}/{size_usd:.2f} "
            f"after {max_retries} retries (MAKER only, no FAK)"
        )
        return result

    def _track_maker_rebate(self, volume_usd: float):
        """
        Per Grok Round 8: Track estimated maker rebates for PnL.

        Polymarket maker rebate is ~0.02% (2 bps). This compounds significantly
        over thousands of trades - core to rn1's edge.

        Args:
            volume_usd: Volume that earned maker rebate.
        """
        rebate_rate = self.config.trading.maker_rebate_rate
        rebate = volume_usd * rebate_rate
        self._estimated_maker_rebates += rebate

    async def _wait_for_fill(
        self,
        client: ClobClient,
        order_id: str,
        timeout_seconds: float
    ) -> bool:
        """
        Wait for an order to fill within timeout.

        Args:
            client: CLOB client to check order status.
            order_id: Order ID to monitor.
            timeout_seconds: Maximum time to wait.

        Returns:
            True if filled, False if timeout or still open.
        """
        start = datetime.now(timezone.utc)
        check_interval = 0.1  # 100ms between checks

        while (datetime.now(timezone.utc) - start).total_seconds() < timeout_seconds:
            try:
                # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
                order = await run_sync_in_thread(client.get_order, order_id)
                if order:
                    status = order.get("status", "").lower()
                    if status in ("matched", "filled"):
                        return True
                    elif status in ("cancelled", "expired"):
                        return False
            except Exception:
                pass

            await asyncio.sleep(check_interval)

        return False

    async def _wait_for_fill_with_partial(
        self,
        client: ClobClient,
        order_id: str,
        timeout_seconds: float,
        total_size_shares: float
    ) -> dict:
        """
        Per Grok Round 8: Wait for order fill and return partial fill status.

        Unlike _wait_for_fill which returns bool, this returns fill details
        to support rn1's partial acceptance strategy (>=50% filled = accept).

        Args:
            client: CLOB client to check order status.
            order_id: Order ID to monitor.
            timeout_seconds: Maximum time to wait.
            total_size_shares: Total order size in shares for fill ratio calculation.

        Returns:
            Dict with:
            - filled_shares: Number of shares filled (0 if none)
            - status: Order status string
            - is_complete: True if fully filled or cancelled/expired
        """
        start = datetime.now(timezone.utc)
        check_interval = 0.1  # 100ms between checks
        filled_shares = 0.0

        while (datetime.now(timezone.utc) - start).total_seconds() < timeout_seconds:
            try:
                # Per Grok Round 6: Use run_sync_in_thread to avoid blocking event loop
                order = await run_sync_in_thread(client.get_order, order_id)
                if order:
                    status = order.get("status", "").lower()

                    # Check for filled amount (may be partial)
                    # py-clob-client returns 'size_matched' for filled portion
                    size_matched = float(order.get("size_matched", 0) or 0)
                    original_size = float(order.get("original_size", total_size_shares) or total_size_shares)

                    filled_shares = size_matched

                    if status in ("matched", "filled"):
                        # Fully filled
                        return {
                            "filled_shares": original_size,  # Full size for complete fill
                            "status": status,
                            "is_complete": True
                        }
                    elif status in ("cancelled", "expired"):
                        # Cancelled/expired - return whatever was filled
                        return {
                            "filled_shares": filled_shares,
                            "status": status,
                            "is_complete": True
                        }
                    # Still open - continue waiting

            except Exception as e:
                logger.debug(f"Error checking order status: {e}")

            await asyncio.sleep(check_interval)

        # Timeout - return current fill state
        return {
            "filled_shares": filled_shares,
            "status": "timeout",
            "is_complete": False
        }

    async def _dry_run_execute(self, opportunity: ArbOpportunity) -> ExecutionResult:
        """Simulate execution in dry run mode."""
        logger.info(f"[DRY RUN] Would execute {opportunity.arb_type.value}")
        logger.info(f"[DRY RUN] Market: {opportunity.market.question[:60]}...")
        logger.info(f"[DRY RUN] Profit margin: {opportunity.profit_margin * 100:.3f}%")
        logger.info(f"[DRY RUN] Trade size: ${opportunity.trade_size_usd:.2f}")
        logger.info(f"[DRY RUN] Expected profit: ${opportunity.expected_profit_usd:.4f}")

        # Simulate successful fills
        orders = []
        for outcome in opportunity.market.outcomes:
            size_usd = opportunity.outcome_sizes.get(outcome.token_id, 0)
            ob = opportunity.orderbooks.get_orderbook(outcome.token_id)

            if opportunity.arb_type == ArbType.BUY_ARB:
                price = ob.best_ask_price if ob else 0
                side = "BUY"
            else:
                price = ob.best_bid_price if ob else 0
                side = "SELL"

            orders.append(OrderResult(
                token_id=outcome.token_id,
                outcome_name=outcome.name,
                side=side,
                requested_size_usd=size_usd,
                executed_size_usd=size_usd,
                price=price,
                status=OrderStatus.FILLED,
                order_id=f"dry_run_{outcome.token_id[:8]}"
            ))

        result = ExecutionResult(
            opportunity=opportunity,
            orders=orders,
            total_cost_usd=opportunity.trade_size_usd,
            expected_return_usd=opportunity.trade_size_usd * (1 + opportunity.profit_margin),
            realized_profit_usd=opportunity.expected_profit_usd,
            is_complete=True
        )

        self._execution_count += 1
        self._total_profit += result.realized_profit_usd
        self._total_volume += result.total_cost_usd

        return result

    async def add_negative_exposure(
        self,
        token_id: str,
        current_position_side: str,
        size_usd: float,
        current_price: float = 0.5
    ) -> Optional[OrderResult]:
        """
        Add negative exposure to lock in profits using post-only GTC for maker rebates.

        When price moves favorably, buy the opposing side to lock in gains.
        Per audit: Profit-lock orders are NOT time-sensitive (position already favorable),
        so use post-only GTC with 30s timeout for maker rebates, same as hedge orders.

        Args:
            token_id: Token to hedge.
            current_position_side: Current position side ("BUY" or "SELL").
            size_usd: Size to hedge in USD.
            current_price: Current market price for price improvement.

        Returns:
            OrderResult or None.
        """
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would add negative exposure: hedge {current_position_side} with ${size_usd:.2f}")
            return None

        # Opposite side to hedge
        hedge_side = SELL if current_position_side == "BUY" else BUY
        use_post_only = self.config.trading.use_post_only_hedges
        hedge_timeout = self.config.trading.post_only_hedge_timeout_seconds

        try:
            result = None

            # Try post-only GTC first for maker rebates (profit-lock is not time-sensitive)
            if use_post_only:
                try:
                    result = await self._place_hedge_post_only(
                        token_id=token_id,
                        outcome_name="profit_lock",
                        side=hedge_side,
                        size_usd=size_usd,
                        limit_price=current_price,
                        timeout_seconds=hedge_timeout
                    )

                    if result and result.status == OrderStatus.FILLED:
                        # Track as maker fill
                        self._maker_fills += 1
                        self._maker_volume_usd += result.executed_size_usd
                        logger.info(
                            f"Profit-lock FILLED (MAKER): {hedge_side} ${result.executed_size_usd:.2f} "
                            f"(rebate earned!)"
                        )
                        self._check_maker_ratio_alert()
                        return result

                except Exception as e:
                    logger.warning(f"Post-only profit-lock failed: {e}, falling back to FAK")

            # FAK fallback
            result = await self._place_market_order(
                token_id=token_id,
                outcome_name="profit_lock",
                side=hedge_side,
                size_usd=size_usd,
                limit_price=current_price
            )

            if result and result.status == OrderStatus.FILLED:
                # Track as taker fill
                self._taker_fills += 1
                self._taker_volume_usd += result.executed_size_usd
                logger.info(f"Profit-lock FILLED (TAKER): {hedge_side} ${result.executed_size_usd:.2f}")
                self._check_maker_ratio_alert()

            return result

        except Exception as e:
            logger.error(f"Failed to add negative exposure: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics."""
        total_exposure = self.get_total_exposure()
        max_single_exposure = max(abs(e) for e in self._market_exposure.values()) if self._market_exposure else 0

        # Calculate drawdown
        drawdown_pct = 0.0
        if self._session_high_capital > 0:
            drawdown_pct = (self._session_high_capital - self._current_capital) / self._session_high_capital * 100

        # Get maker/taker stats
        maker_taker = self.get_maker_taker_stats()

        return {
            "execution_count": self._execution_count,
            "total_volume_usd": self._total_volume,
            "total_profit_usd": self._total_profit,
            "avg_profit_per_trade": self._total_profit / self._execution_count if self._execution_count > 0 else 0,
            "win_rate_pct": 100.0 if self._execution_count > 0 else 0,  # Arbs should always win if complete
            "partial_fills": self._partial_fills,
            "hedges_executed": self._hedges_executed,
            "consecutive_partials": self._consecutive_partials,
            "consecutive_failures": self._consecutive_failures,
            "is_paused": self.is_paused_for_partials(),
            "circuit_breaker_active": self._circuit_breaker_active,
            "approvals_executed": self._approvals_executed,
            "total_exposure_usd": total_exposure,
            "max_single_market_exposure": max_single_exposure,
            "markets_with_exposure": len([e for e in self._market_exposure.values() if abs(e) > 0.01]),
            "current_capital": self._current_capital,
            "session_high_capital": self._session_high_capital,
            "drawdown_pct": drawdown_pct,
            "idempotency_tracked": self._idempotency_tracker.get_stats()["tracked_arbs"],
            "fak_mode": self._use_ioc,  # FAK = Fill-And-Kill (Polymarket's IOC)
            "has_fak_support": HAS_FAK,
            # Maker vs Taker tracking (for post-only hedge orders)
            "maker_fills": maker_taker["maker_fills"],
            "taker_fills": maker_taker["taker_fills"],
            "maker_ratio_pct": maker_taker["maker_ratio"] * 100,
            "maker_volume_usd": maker_taker["maker_volume_usd"],
            "taker_volume_usd": maker_taker["taker_volume_usd"],
        }
