"""
Execution Module.
Handles order placement and execution for arbitrage opportunities.
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

logger = logging.getLogger(__name__)

# IOC (Immediate or Cancel) - allows partial fills unlike FOK
# py-clob-client may not have IOC directly, we'll use GTC with immediate cancel as fallback
try:
    from py_clob_client.clob_types import OrderType as ClobOrderType
    HAS_IOC = hasattr(ClobOrderType, 'IOC')
except:
    HAS_IOC = False

# Idempotency tracking for preventing duplicate arbs on restart
from collections import OrderedDict
import hashlib
import time

class IdempotencyTracker:
    """Track recent arb IDs to prevent duplicates on restart/reconnect."""

    def __init__(self, ttl_seconds: int = 300):
        # TTL increased to 300s (5 min) from 60s per audit
        # Fast reconnects or WS->HTTP fallback can re-detect same arb within minutes
        self.ttl_seconds = ttl_seconds
        self._recent_arbs: OrderedDict[str, float] = OrderedDict()  # arb_id -> timestamp
        self._max_entries = 1000  # Prevent unbounded growth

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

        # Idempotency tracker (60s TTL to prevent duplicate arbs on restart)
        self._idempotency_tracker = IdempotencyTracker(ttl_seconds=60)

        # Enhanced circuit breaker: >5 consecutive partials/fails OR >3% drawdown = 30min pause
        self._circuit_breaker_active = False
        self._circuit_breaker_until: Optional[datetime] = None
        self._circuit_breaker_pause_seconds = 1800  # 30 minutes
        self._max_consecutive_failures = 5  # Trigger on >5 consecutive partials or fails
        self._max_drawdown_pct = 0.03  # 3% drawdown triggers circuit breaker
        self._session_high_capital = config.starting_capital_usd  # Track session high for drawdown
        self._current_capital = config.starting_capital_usd

        # IOC mode settings
        self._use_ioc = True  # Use IOC instead of FOK for more fills
        self._partial_hedge_slippage_threshold = 0.003  # 0.3% max slippage for partial hedging

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

        Note: For proxy wallets (most Polymarket users), USDC allowance must be
        approved manually via the Polymarket UI. Auto-approve is not supported
        for proxy wallets as on-chain transactions require relayer handling.

        Returns:
            Tuple of (ready, message).
        """
        if self.config.dry_run:
            return True, "Dry run mode - ready"

        # Check balance
        balance = self.check_balance()
        if balance is None:
            return False, "Failed to check USDC balance"

        # Use funder address for display (where USDC is held)
        display_wallet = self._funder_address or self._wallet_address

        if balance < 1.0:  # Minimum $1 to trade
            return False, f"Insufficient USDC balance: ${balance:.2f}. Deposit USDC to wallet {display_wallet}"

        # Check allowance (no auto-approve for proxy wallets)
        allowance = self.check_allowance()
        if allowance is None:
            logger.warning("Could not check allowance - will attempt trades anyway")
            return True, f"Balance: ${balance:.2f}, Allowance: unknown"

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
                logger.debug("Skipping sell_arb: BUY_ARB_ONLY mode enabled")
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
                    if order.order_id:
                        try:
                            order_status = self.client.get_order(order.order_id)
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
        Aggressively hedge partial fills by selling filled positions.

        When we have partial fills, we're left with directional exposure.
        This method immediately places opposing market orders on over-exposed
        outcomes with up to 5 retries and 100ms spacing.

        Args:
            result: Execution result with partial fills.
            opportunity: Original opportunity.

        Returns:
            List of hedge order results.
        """
        hedge_orders = []
        max_hedge_retries = 5
        retry_delay = 0.1  # 100ms between retries

        for order in result.orders:
            if order.status == OrderStatus.FILLED and order.executed_size_usd > 0:
                # Sell the filled position to neutralize
                hedge_side = SELL if order.side == "BUY" else BUY
                remaining_size = order.executed_size_usd

                # Aggressive retry loop for hedge orders
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
                            logger.info(
                                f"Hedge filled: {hedge_side} {order.outcome_name} "
                                f"${hedge_order.executed_size_usd:.2f}"
                            )
                            break
                        elif hedge_order.status == OrderStatus.PARTIAL:
                            remaining_size -= hedge_order.executed_size_usd
                            logger.warning(
                                f"Partial hedge: {order.outcome_name} "
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
        Execute a buy arbitrage (buy all outcomes).

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
                min_balance=opportunity.trade_size_usd
            )

        # Determine order placement method (post-only or FOK)
        use_post_only = self.config.trading.use_post_only

        # Create tasks for all buy orders
        tasks = []
        for outcome in opportunity.market.outcomes:
            size_usd = opportunity.outcome_sizes.get(outcome.token_id, 0)
            if size_usd > 0:
                ob = orderbooks.get_orderbook(outcome.token_id)
                price = ob.best_ask_price if ob and ob.best_ask else 0

                if use_post_only:
                    tasks.append(
                        self._place_post_only_order(
                            token_id=outcome.token_id,
                            outcome_name=outcome.name,
                            side=BUY,
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
                            side=BUY,
                            size_usd=size_usd,
                            limit_price=price,
                            client=exec_client,
                            wallet_state=exec_wallet
                        )
                    )

        # Execute all orders concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, OrderResult):
                orders.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Order failed: {result}")
                orders.append(OrderResult(
                    token_id="unknown",
                    outcome_name="unknown",
                    side="BUY",
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
        Place a market order using IOC (Immediate or Cancel) for more fills.

        IOC allows partial fills unlike FOK, increasing trade frequency 2-5x.
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
            use_ioc: If True, use IOC mode (partial fills allowed). Default True.

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

        # Determine order type: IOC for more fills, FOK as fallback
        # IOC = Immediate or Cancel (partial fills allowed)
        # FOK = Fill or Kill (all or nothing)
        use_ioc_mode = use_ioc and self._use_ioc

        for attempt in range(max_attempts):
            try:
                # Create market order args
                # Note: py-clob-client may not have IOC directly
                # We simulate IOC using GTC with immediate status check and cancel
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=size_usd,
                    side=side,
                    order_type=OrderType.GTC if use_ioc_mode else OrderType.FOK
                )

                # Create and sign the order
                signed_order = execution_client.create_market_order(order_args)

                # Submit the order
                order_type_used = OrderType.GTC if use_ioc_mode else OrderType.FOK
                response = execution_client.post_order(signed_order, order_type_used)

                # Parse response
                if response and response.get("success"):
                    order_id = response.get("orderID")
                    result.order_id = order_id

                    if use_ioc_mode:
                        # IOC mode: check fill status after brief wait, cancel remainder
                        await asyncio.sleep(0.1)  # 100ms for fills to process

                        try:
                            order_status = execution_client.get_order(order_id)
                            if order_status:
                                filled_size = float(order_status.get("sizeFilled", 0))
                                total_size = float(order_status.get("size", size_usd))

                                # Calculate filled USD
                                fill_price = float(order_status.get("price", limit_price))
                                result.executed_size_usd = filled_size * fill_price

                                if filled_size >= total_size * 0.99:  # ~100% filled
                                    result.status = OrderStatus.FILLED
                                    logger.debug(
                                        f"IOC filled: {side} {outcome_name} @ {limit_price:.4f} "
                                        f"for ${result.executed_size_usd:.2f}"
                                    )
                                elif filled_size > 0:
                                    # Partial fill - cancel remainder
                                    result.status = OrderStatus.PARTIAL
                                    result.unfilled_size_usd = size_usd - result.executed_size_usd
                                    try:
                                        execution_client.cancel(order_id)
                                    except Exception:
                                        pass  # Best effort cancel

                                    logger.info(
                                        f"IOC partial: {side} {outcome_name} "
                                        f"${result.executed_size_usd:.2f} filled, "
                                        f"${result.unfilled_size_usd:.2f} cancelled"
                                    )
                                else:
                                    # No fill, cancel order
                                    result.status = OrderStatus.CANCELLED
                                    try:
                                        execution_client.cancel(order_id)
                                    except Exception:
                                        pass
                                    logger.debug(f"IOC no fill: {outcome_name}")

                        except Exception as e:
                            # If can't check status, assume filled (optimistic)
                            logger.debug(f"IOC status check failed: {e}, assuming filled")
                            result.status = OrderStatus.FILLED
                            result.executed_size_usd = size_usd

                    else:
                        # FOK mode: all or nothing
                        result.status = OrderStatus.FILLED
                        result.executed_size_usd = size_usd
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
        wallet_state: Optional[Any] = None
    ) -> OrderResult:
        """
        Place a post-only limit order for maker rebates, with sized-down FOK fallback.

        Post-only orders earn maker rebates (~0.1-0.3% on Polymarket).
        If not filled within timeout, falls back to FOK at 80% size to capture
        most of the edge in race conditions.

        Dynamic price improvement formula: improvement = base + (edge_pct * 0.5)
        - Scales linearly with edge: bigger edges get bolder pricing for fill probability
        - Still earns maker rebates on successful post-only fills

        Args:
            token_id: Token ID to trade.
            outcome_name: Name of outcome for logging.
            side: BUY or SELL.
            size_usd: Size in USD.
            limit_price: Reference price (will be improved for post-only).
            edge_pct: Arb edge as decimal (e.g., 0.005 for 0.5%).
            client: Optional CLOB client to use.
            wallet_state: Optional wallet state for tracking.

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
        # e.g., 0.5% edge (0.005) -> 0.005 + 0.005*0.5 = 0.0075 (0.75 cents)
        # e.g., 1% edge (0.01) -> 0.005 + 0.01*0.5 = 0.01 (1 cent)
        # e.g., 2% edge (0.02) -> 0.005 + 0.02*0.5 = 0.015 (1.5 cents)
        # Bolder on bigger edges for fill probability, still earns maker rebates
        price_improvement = base_improvement + (edge_pct * 0.5)

        # Cap improvement at 3 cents to preserve margin
        price_improvement = min(price_improvement, 0.03)

        # Calculate improved price for post-only
        # For BUY: post above current best bid (to be at top of book)
        # For SELL: post below current best ask (to be at top of book)
        if side == BUY:
            post_price = min(limit_price + price_improvement, 0.99)  # Cap at 0.99
        else:
            post_price = max(limit_price - price_improvement, 0.01)  # Floor at 0.01

        try:
            # Create limit order args with post-only flag
            # Calculate size in shares from USD
            size_shares = size_usd / post_price if post_price > 0 else 0

            order_args = OrderArgs(
                token_id=token_id,
                price=post_price,
                size=size_shares,
                side=side,
            )

            # Create order with post-only option
            signed_order = execution_client.create_order(order_args)

            # Submit as GTC (Good Till Cancelled) - will act as post-only
            response = execution_client.post_order(signed_order, OrderType.GTC)

            if response and response.get("success"):
                order_id = response.get("orderID")
                result.order_id = order_id

                # Wait for fill or timeout
                filled = await self._wait_for_fill(execution_client, order_id, timeout)

                if filled:
                    result.status = OrderStatus.FILLED
                    result.executed_size_usd = size_usd
                    logger.debug(
                        f"Post-only filled: {side} {outcome_name} @ {post_price:.4f} "
                        f"for ${size_usd:.2f} (maker rebate earned!)"
                    )
                    return result
                else:
                    # Cancel unfilled order and fall back to FOK at reduced size
                    try:
                        execution_client.cancel(order_id)
                        logger.debug(f"Post-only timed out, cancelled {order_id}, falling back to FOK at 80% size")
                    except Exception:
                        pass

                    # Fall back to FOK at 80% size (captures most edge in races)
                    reduced_size = size_usd * 0.80
                    return await self._place_market_order(
                        token_id=token_id,
                        outcome_name=outcome_name,
                        side=side,
                        size_usd=reduced_size,
                        limit_price=limit_price,
                        retry_on_no_match=True,
                        client=client,
                        wallet_state=wallet_state
                    )
            else:
                error_msg = response.get("errorMsg", "Unknown error") if response else "No response"
                logger.debug(f"Post-only submission failed: {error_msg}, falling back to FOK at 80% size")

                # Fall back to FOK at reduced size
                reduced_size = size_usd * 0.80
                return await self._place_market_order(
                    token_id=token_id,
                    outcome_name=outcome_name,
                    side=side,
                    size_usd=reduced_size,
                    limit_price=limit_price,
                    retry_on_no_match=True,
                    client=client,
                    wallet_state=wallet_state
                )

        except Exception as e:
            logger.debug(f"Post-only exception: {e}, falling back to FOK at 80% size")

            # Fall back to FOK at reduced size on any error
            reduced_size = size_usd * 0.80
            return await self._place_market_order(
                token_id=token_id,
                outcome_name=outcome_name,
                side=side,
                size_usd=reduced_size,
                limit_price=limit_price,
                retry_on_no_match=True,
                client=client,
                wallet_state=wallet_state
            )

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
                order = client.get_order(order_id)
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
        size_usd: float
    ) -> Optional[OrderResult]:
        """
        Add negative exposure to lock in profits.

        When price moves favorably, buy the opposing side to lock in gains.

        Args:
            token_id: Token to hedge.
            current_position_side: Current position side ("BUY" or "SELL").
            size_usd: Size to hedge in USD.

        Returns:
            OrderResult or None.
        """
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would add negative exposure: hedge {current_position_side} with ${size_usd:.2f}")
            return None

        # Opposite side to hedge
        hedge_side = SELL if current_position_side == "BUY" else BUY

        try:
            return await self._place_market_order(
                token_id=token_id,
                outcome_name="hedge",
                side=hedge_side,
                size_usd=size_usd,
                limit_price=0.5  # Will be market order
            )
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
            "ioc_mode": self._use_ioc
        }
