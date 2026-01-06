"""
Main Loop Orchestrator.
Coordinates all modules for the Polymarket arbitrage bot.
"""

import asyncio
import signal
import sys
import csv
import os
import math
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
import logging

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from config import load_config, BotConfig, calculate_rn1_style_size, get_rn1_target_metrics
from logger import setup_logging
from auth import AuthManager
from wallet_manager import WalletManager
from market_discovery import MarketDiscovery, Market
from orderbook import OrderbookPoller, ArbOpportunity, ArbType
from execution import ExecutionEngine, ExecutionResult
from positions import PositionMonitor, ProfitLocker
from risk import RiskManager, DepthValidator, TradeRecord
from edge_model import EdgeExpectancyModel, MarketHeat
from supervisor import get_supervisor, heartbeat as supervisor_heartbeat
from clob_client_patch import shutdown_executor as shutdown_clob_executor
# Note: Monkey-patching deprecated per audit - rate limiting handled at application level

# Optional WebSocket support
try:
    from websocket_client import HybridOrderbookManager, PolymarketWebSocket
    HAS_WEBSOCKET = True
except ImportError:
    HAS_WEBSOCKET = False
    HybridOrderbookManager = None

logger = logging.getLogger(__name__)


class MetricsExporter:
    """Exports session metrics to CSV for analysis."""

    def __init__(self, output_dir: str = "metrics"):
        """
        Initialize the metrics exporter.

        Args:
            output_dir: Directory to write CSV files.
        """
        self.output_dir = output_dir
        self._session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._trade_records: List[Dict[str, Any]] = []
        self._hourly_snapshots: List[Dict[str, Any]] = []

        # Daily geometric return tracking
        self._daily_returns: List[float] = []  # List of (1 + return%) multipliers
        self._session_start = datetime.now(timezone.utc)
        self._last_daily_reset = self._session_start.date()
        self._daily_profit_usd = 0.0
        self._starting_capital = 0.0

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    def set_starting_capital(self, capital: float):
        """Set starting capital for return calculations."""
        self._starting_capital = capital

    def record_trade(self, trade_data: Dict[str, Any]):
        """Record a trade for later export."""
        trade_data["timestamp"] = datetime.now(timezone.utc).isoformat()
        trade_data["session_id"] = self._session_id
        self._trade_records.append(trade_data)

        # Track daily profit for geometric return
        profit = trade_data.get("profit_usd", 0)
        self._daily_profit_usd += profit

        # Check for day rollover
        today = datetime.now(timezone.utc).date()
        if today != self._last_daily_reset:
            self._finalize_daily_return()
            self._last_daily_reset = today
            self._daily_profit_usd = profit  # Start new day with this trade

    def _finalize_daily_return(self):
        """Finalize and record daily return when day changes."""
        if self._starting_capital > 0:
            daily_return_pct = self._daily_profit_usd / self._starting_capital
            # Store as multiplier (1 + return)
            self._daily_returns.append(1.0 + daily_return_pct)

    def get_geometric_return(self) -> Dict[str, float]:
        """
        Calculate geometric mean return across days.

        Returns:
            Dict with geo_mean_daily_pct, compounded_return_pct, trading_days.
        """
        # Include current partial day
        all_returns = self._daily_returns.copy()
        if self._starting_capital > 0 and self._daily_profit_usd != 0:
            current_day_return = 1.0 + (self._daily_profit_usd / self._starting_capital)
            all_returns.append(current_day_return)

        if not all_returns:
            return {
                "geo_mean_daily_pct": 0.0,
                "compounded_return_pct": 0.0,
                "trading_days": 0
            }

        # Geometric mean = (product of all returns)^(1/n)
        product = 1.0
        for r in all_returns:
            product *= r

        n = len(all_returns)
        geo_mean = product ** (1.0 / n) if n > 0 else 1.0

        # Compounded return = product - 1
        compounded = product - 1.0

        return {
            "geo_mean_daily_pct": (geo_mean - 1.0) * 100,
            "compounded_return_pct": compounded * 100,
            "trading_days": n
        }

    def get_daily_avg_profit(self) -> float:
        """Get average daily profit in USD."""
        if not self._trade_records:
            return 0.0

        total_profit = sum(t.get("profit_usd", 0) for t in self._trade_records)
        days = max(1, (datetime.now(timezone.utc) - self._session_start).days + 1)
        return total_profit / days

    def record_hourly_snapshot(self, stats: Dict[str, Any]):
        """Record an hourly stats snapshot."""
        stats["timestamp"] = datetime.now(timezone.utc).isoformat()
        stats["session_id"] = self._session_id
        self._hourly_snapshots.append(stats)

    def export_trades_csv(self) -> str:
        """
        Export all trades to CSV.

        Returns:
            Path to the exported CSV file.
        """
        if not self._trade_records:
            logger.info("No trades to export")
            return ""

        filepath = os.path.join(self.output_dir, f"trades_{self._session_id}.csv")

        # Get all unique keys across all records
        all_keys = set()
        for record in self._trade_records:
            all_keys.update(record.keys())
        fieldnames = sorted(all_keys)

        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._trade_records)

        logger.info(f"Exported {len(self._trade_records)} trades to {filepath}")
        return filepath

    def export_session_summary(self, stats: Dict[str, Any]) -> str:
        """
        Export session summary to CSV.

        Args:
            stats: Final session statistics.

        Returns:
            Path to the exported CSV file.
        """
        filepath = os.path.join(self.output_dir, f"session_{self._session_id}.csv")

        stats["session_id"] = self._session_id
        stats["export_timestamp"] = datetime.now(timezone.utc).isoformat()

        fieldnames = sorted(stats.keys())

        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(stats)

        logger.info(f"Exported session summary to {filepath}")
        return filepath

    def export_hourly_snapshots(self) -> str:
        """
        Export hourly snapshots to CSV.

        Returns:
            Path to the exported CSV file.
        """
        if not self._hourly_snapshots:
            logger.info("No hourly snapshots to export")
            return ""

        filepath = os.path.join(self.output_dir, f"hourly_{self._session_id}.csv")

        all_keys = set()
        for record in self._hourly_snapshots:
            all_keys.update(record.keys())
        fieldnames = sorted(all_keys)

        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._hourly_snapshots)

        logger.info(f"Exported {len(self._hourly_snapshots)} hourly snapshots to {filepath}")
        return filepath

    def export_all(self, final_stats: Dict[str, Any]) -> Dict[str, str]:
        """
        Export all metrics to CSV files.

        Args:
            final_stats: Final session statistics.

        Returns:
            Dict of export type to filepath.
        """
        return {
            "trades": self.export_trades_csv(),
            "session": self.export_session_summary(final_stats),
            "hourly": self.export_hourly_snapshots()
        }


class ArbBot:
    """Main arbitrage bot orchestrator."""

    def __init__(self, config: BotConfig):
        """
        Initialize the arbitrage bot.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Initialize modules
        self.auth_manager = AuthManager(config)
        self.wallet_manager = WalletManager(config)  # Multi-wallet support
        self.market_discovery = MarketDiscovery(config)
        self.orderbook_poller = OrderbookPoller(config)
        self.execution_engine = ExecutionEngine(config)
        self.position_monitor = PositionMonitor(config)
        self.profit_locker = ProfitLocker(config)
        self.risk_manager = RiskManager(config)
        self.depth_validator = DepthValidator(config)
        self.metrics_exporter = MetricsExporter()
        self.metrics_exporter.set_starting_capital(config.starting_capital_usd)

        # Edge expectancy model for latency-adjusted sizing
        self.edge_model = EdgeExpectancyModel()

        # WebSocket support (hybrid mode: WS for detection, HTTP for verification)
        self.hybrid_manager: Optional[HybridOrderbookManager] = None
        self._use_websocket = HAS_WEBSOCKET and config.trading.use_websocket

        # WebSocket mandatory mode tracking
        self._ws_required = config.trading.ws_required_active_hours
        self._ws_disconnected_time: Optional[datetime] = None
        self._ws_paused_for_reconnect = False
        self._ws_reconnect_attempts = 0
        self._ws_disconnect_alerted = False  # Track if 10s disconnect alert sent

        # Circuit breaker for rate limiting and error protection
        self._circuit_breaker_open = False
        self._circuit_breaker_failures = 0
        self._circuit_breaker_threshold = 5  # Open after 5 consecutive failures
        self._circuit_breaker_reset_time: Optional[datetime] = None
        self._circuit_breaker_cooldown_seconds = 60  # 1 minute cooldown

        # Request rate tracking
        self._request_count_window = 0
        self._request_window_start: Optional[datetime] = None
        self._max_requests_per_minute = 80  # Stay under 100/min limit

        # State
        self._target_markets: List[Market] = []
        self._last_market_refresh: Optional[datetime] = None

        # Big arb notification tracking
        self._big_arb_alerts_sent = 0

        # Stats
        self._start_time: Optional[datetime] = None
        self._poll_count = 0
        self._opportunities_found = 0
        self._trades_executed = 0

        # rn1-style sizing tracking
        # If starting_capital_usd is 0, we'll set this from actual balance at init
        self._current_capital = config.starting_capital_usd
        self._last_balance_refresh: Optional[datetime] = None
        self._balance_refresh_interval = 300  # Refresh balance every 5 minutes

    def _is_active_hours(self) -> bool:
        """Check if current time is within active trading hours."""
        now = datetime.now(timezone.utc)
        hour = now.hour
        start = self.config.trading.active_hours_start
        end = self.config.trading.active_hours_end

        if start <= end:
            return start <= hour < end
        else:  # Wraps around midnight
            return hour >= start or hour < end

    def _get_claim_interval(self) -> float:
        """Get the claim polling interval based on active hours."""
        if self._is_active_hours():
            return self.config.trading.claim_poll_interval_active
        else:
            return self.config.trading.claim_poll_interval_inactive

    def _check_ws_status(self) -> bool:
        """
        Check WebSocket status and handle mandatory WS mode.

        Features:
        - Alert if disconnected >10s
        - Pause trading if disconnected past grace period during active hours

        Returns:
            True if trading is allowed, False if paused for WS reconnect.
        """
        if not self._ws_required or not self._is_active_hours():
            # WS not required or outside active hours
            self._ws_paused_for_reconnect = False
            self._ws_disconnect_alerted = False
            return True

        # Check if WS is connected
        ws_connected = (
            self.hybrid_manager is not None and
            self.hybrid_manager.ws_client is not None and
            self.hybrid_manager.ws_client.is_connected
        )

        if ws_connected:
            # WS is up, reset tracking
            if self._ws_paused_for_reconnect:
                logger.info("WebSocket reconnected, resuming trading")
            self._ws_disconnected_time = None
            self._ws_paused_for_reconnect = False
            self._ws_reconnect_attempts = 0
            self._ws_disconnect_alerted = False
            return True

        # WS is disconnected during active hours with mandatory mode
        now = datetime.now(timezone.utc)

        if self._ws_disconnected_time is None:
            self._ws_disconnected_time = now
            self._ws_disconnect_alerted = False
            logger.warning(
                f"WebSocket disconnected during active hours. "
                f"Grace period: {self.config.trading.ws_reconnect_grace_period}s"
            )

        # Check disconnect duration for 10s alert
        disconnect_duration = (now - self._ws_disconnected_time).total_seconds()
        alert_threshold = self.config.trading.ws_disconnect_alert_seconds

        if disconnect_duration >= alert_threshold and not self._ws_disconnect_alerted:
            # Send 10s disconnect alert
            logger.warning("=" * 60)
            logger.warning(f"WS DISCONNECT ALERT: WebSocket offline for >{alert_threshold:.0f}s!")
            logger.warning(f"Duration: {disconnect_duration:.0f}s")
            logger.warning("Trading with HTTP fallback (slower, less competitive)")
            logger.warning("=" * 60)
            self._ws_disconnect_alerted = True

        # Check grace period
        grace_period = self.config.trading.ws_reconnect_grace_period

        if disconnect_duration < grace_period:
            # Within grace period, try to reconnect but continue trading
            return True

        # Past grace period, pause trading and attempt reconnect
        if not self._ws_paused_for_reconnect:
            logger.warning(
                f"WebSocket offline for {disconnect_duration:.0f}s during active hours. "
                f"PAUSING TRADING until WS reconnects (mandatory mode enabled)"
            )
            self._ws_paused_for_reconnect = True

        return False

    async def _attempt_ws_reconnect(self) -> bool:
        """
        Attempt to reconnect WebSocket.

        Returns:
            True if reconnected successfully.
        """
        self._ws_reconnect_attempts += 1
        logger.info(f"WebSocket reconnect attempt #{self._ws_reconnect_attempts}...")

        try:
            if self.hybrid_manager:
                # Try to reconnect existing manager
                success = await self.hybrid_manager.reconnect()
                if success:
                    logger.info("WebSocket reconnected successfully!")
                    # Re-subscribe to markets
                    if self._target_markets:
                        await self.hybrid_manager.subscribe_markets(self._target_markets)
                    return True
            else:
                # Initialize fresh
                await self._init_websocket()
                if self._use_websocket and self.hybrid_manager:
                    return True

        except Exception as e:
            logger.warning(f"WebSocket reconnect failed: {e}")

        return False

    def _get_rn1_style_trade_size(self, depth_available: float) -> float:
        """
        Get rn1-style trade size with capital scaling.

        Args:
            depth_available: Available orderbook depth in USD.

        Returns:
            Optimal trade size in USD.
        """
        return calculate_rn1_style_size(
            current_capital=self._current_capital,
            starting_capital=self.config.starting_capital_usd,
            config=self.config.trading,
            depth_available=depth_available
        )

    def _update_capital_tracking(self, profit_usd: float):
        """Update tracked capital after a trade."""
        self._current_capital += profit_usd

    async def _refresh_balance(self) -> Optional[float]:
        """
        Refresh USDC balance from chain.

        Returns:
            Current balance in USD, or None if failed.
        """
        try:
            balance = self.execution_engine.check_balance()
            if balance is not None:
                self._last_balance_refresh = datetime.now(timezone.utc)
                logger.debug(f"Balance refreshed: ${balance:,.2f}")
                return balance
        except Exception as e:
            logger.warning(f"Failed to refresh balance: {e}")
        return None

    async def _maybe_refresh_balance(self):
        """Periodically refresh balance to keep capital tracking accurate."""
        if self._last_balance_refresh is None:
            return

        now = datetime.now(timezone.utc)
        elapsed = (now - self._last_balance_refresh).total_seconds()

        if elapsed >= self._balance_refresh_interval:
            balance = await self._refresh_balance()
            if balance is not None:
                # Update current capital to actual balance
                # This catches deposits/withdrawals and corrects drift
                old_capital = self._current_capital
                self._current_capital = balance
                if abs(old_capital - balance) > 1.0:
                    logger.info(f"Capital updated: ${old_capital:,.2f} -> ${balance:,.2f}")

                # Re-check allowance after balance refresh (per audit)
                # Ensure allowance >= 2x new capital for safe trading headroom
                if not self.config.dry_run and self.config.wallet.signature_type == 0:
                    required_allowance = balance * 2
                    client = self.execution_engine._client
                    if client:
                        self.auth_manager.ensure_sufficient_allowance(client, required_allowance)

    async def initialize(self):
        """Initialize all modules and connections."""
        logger.info("=" * 60)
        logger.info("POLYMARKET MICROSTRUCTURE ARBITRAGE BOT v2.1")
        logger.info("=" * 60)
        logger.info(f"Mode: {'DRY RUN' if self.config.dry_run else 'LIVE TRADING'}")
        logger.info(f"Starting capital: ${self.config.starting_capital_usd:,.2f}")
        logger.info(f"Min capital required: ${self.config.min_capital_required:,.2f}")
        logger.info(f"Min volume filter: ${self.config.trading.min_volume_usd:,.0f}")
        logger.info(f"Min depth: ${self.config.trading.min_depth_usd:,.0f}")
        logger.info(f"Base arb threshold: {self.config.trading.arb_threshold_base * 100:.2f}%")
        logger.info(f"Gas buffer: ${self.config.trading.gas_buffer_usd:.3f}/tx")
        logger.info(f"Max concurrent markets: {self.config.trading.max_concurrent_markets}")
        logger.info(f"Active hours (UTC): {self.config.trading.active_hours_start:02d}:00 - {self.config.trading.active_hours_end:02d}:00")

        # rn1-style sizing info
        logger.info("-" * 60)
        logger.info("RN1-STYLE SIZING CONFIG")
        logger.info(f"Max trade size: ${self.config.trading.max_trade_size_usd:.0f}")
        logger.info(f"Max % per trade: {self.config.trading.max_size_per_trade_percent:.1f}%")
        logger.info(f"Capital scaling factor: {self.config.trading.capital_scaling_factor}")
        logger.info(f"Min trade size: ${self.config.trading.min_trade_size_usd:.0f}")

        # Show rn1 target metrics
        rn1_targets = get_rn1_target_metrics(self.config.starting_capital_usd, days=90)
        logger.info("-" * 60)
        logger.info("RN1 BENCHMARK TARGETS (90 days)")
        logger.info(f"Target daily geo return: {rn1_targets['rn1_daily_geo_return_pct']:.1f}%")
        logger.info(f"Target capital (90d): ${rn1_targets['target_capital']:,.0f}")
        logger.info(f"Target trades/day: {rn1_targets['trades_per_day_target']}")
        logger.info(f"Target avg edge: {rn1_targets['avg_edge_pct']:.1f}%")

        # WebSocket mandatory mode
        if self._ws_required:
            logger.info("-" * 60)
            logger.info("WEBSOCKET MANDATORY MODE: ENABLED")
            logger.info(f"  Grace period: {self.config.trading.ws_reconnect_grace_period}s")
            logger.info(f"  Trading will PAUSE if WS down during active hours")

        logger.info("=" * 60)

        # Initialize authentication
        if not self.config.dry_run:
            try:
                # Check if multi-wallet mode is available
                wallet_count = self.config.wallet.wallet_count
                use_multi_wallet = wallet_count > 1

                if use_multi_wallet:
                    # Multi-wallet mode: use WalletManager for rotation
                    logger.info(f"Initializing multi-wallet mode with {wallet_count} wallets...")
                    initialized = await self.wallet_manager.initialize()
                    if initialized > 0:
                        self.execution_engine.set_wallet_manager(self.wallet_manager)
                        # Set primary client for compatibility
                        primary = self.wallet_manager.get_primary_wallet()
                        if primary and primary.client:
                            self.execution_engine.set_client(primary.client)
                        logger.info(f"Multi-wallet mode active: {initialized} wallet(s) ready")
                    else:
                        raise RuntimeError("No wallets initialized in multi-wallet mode")
                else:
                    # Single wallet mode: use AuthManager
                    client = self.auth_manager.initialize()
                    self.execution_engine.set_client(client)

                # Set wallet info for approval transactions (primary wallet)
                wallet_address = self.auth_manager.get_wallet_address()
                private_key = self.config.wallet.private_key
                funder_address = self.config.wallet.funder_address  # For proxy mode
                if wallet_address and private_key:
                    self.execution_engine.set_wallet(wallet_address, private_key, funder_address)

                self.position_monitor.initialize()
                logger.info("Authentication successful")

                # Verify MATIC balance for gas (FATAL if insufficient for EOA wallets)
                gas_ok, gas_msg = self.auth_manager.verify_gas_balance()
                if gas_ok:
                    logger.info(gas_msg)
                else:
                    logger.error("=" * 60)
                    logger.error("FATAL: INSUFFICIENT MATIC FOR GAS")
                    logger.error("=" * 60)
                    logger.error(gas_msg)
                    logger.error("=" * 60)
                    raise RuntimeError(f"Insufficient MATIC for gas: {gas_msg}")

                # Verify USDC balance (aggregate for multi-wallet)
                if use_multi_wallet:
                    total_balance = self.wallet_manager.get_total_available_balance()
                    logger.info(f"Total USDC across all wallets: ${total_balance:,.2f}")
                    actual_balance = total_balance
                else:
                    balance_ok, balance_msg = self.auth_manager.verify_sufficient_balance()
                    if balance_ok:
                        logger.info(balance_msg)
                    else:
                        logger.warning(balance_msg)
                    actual_balance = self.auth_manager.get_usdc_balance() or 0.0

                self._last_balance_refresh = datetime.now(timezone.utc)

                # Verify trading readiness (balance + allowance) - FATAL if not ready
                ready, ready_msg = await self.execution_engine.verify_trading_ready()
                if ready:
                    logger.info(ready_msg)
                else:
                    logger.error("=" * 60)
                    logger.error("FATAL: NOT READY FOR LIVE TRADING")
                    logger.error("=" * 60)
                    logger.error(ready_msg)
                    logger.error("")
                    logger.error("Options:")
                    logger.error("  1. Set DRY_RUN=true in .env for paper trading")
                    logger.error("  2. Deposit USDC to your wallet")
                    logger.error("  3. Approve USDC spending via Polymarket UI")
                    logger.error("=" * 60)
                    raise RuntimeError(f"Not ready for live trading: {ready_msg}")

                # Check USDC MAX approval (non-fatal but recommended)
                max_ok, max_msg = await self.execution_engine.ensure_max_approval()
                if max_ok:
                    logger.info(f"USDC approval: {max_msg}")
                else:
                    logger.warning(f"USDC approval: {max_msg}")

                # Per Grok Round 4: Ensure CTF approvals for neg-risk trading
                # Required for neg-risk-ctf-adapter to work with multi-outcome markets
                if use_multi_wallet:
                    ctf_ok = await self.wallet_manager.ensure_ctf_approvals()
                    if ctf_ok:
                        logger.info("CTF approvals verified for neg-risk trading")
                    else:
                        logger.warning("Some CTF approvals may be missing - neg-risk markets may fail")

            except Exception as e:
                logger.error(f"Authentication failed: {e}")
                raise
        else:
            # Initialize read-only client for dry run
            try:
                self.auth_manager.initialize()
                logger.info("Read-only client initialized for dry run")
            except Exception as e:
                logger.warning(f"Could not initialize read-only client: {e}")

        self._start_time = datetime.now(timezone.utc)

        # Initialize WebSocket if enabled
        if self._use_websocket:
            await self._init_websocket()

        logger.info("Bot initialized successfully")

    async def _init_websocket(self):
        """Initialize WebSocket connection for real-time orderbook updates."""
        if not HAS_WEBSOCKET:
            logger.warning("WebSocket support not available (websockets package not installed)")
            self._use_websocket = False
            return

        try:
            self.hybrid_manager = HybridOrderbookManager(self.config, self.orderbook_poller)
            success = await self.hybrid_manager.initialize()

            if success and self.hybrid_manager.ws_client.is_connected:
                logger.info("=" * 60)
                logger.info("WEBSOCKET MODE ENABLED")
                logger.info("  Detection latency: ~80ms (vs ~800ms HTTP)")
                logger.info("  Fill probability: ~6x higher on hot markets")
                logger.info("  Mode: Hybrid (WS detect → HTTP verify → Execute)")
                logger.info("=" * 60)
            else:
                logger.warning("WebSocket unavailable, falling back to HTTP-only mode")
                self._use_websocket = False

        except Exception as e:
            logger.warning(f"WebSocket initialization failed: {e}, using HTTP-only mode")
            self._use_websocket = False

    async def shutdown(self):
        """Graceful shutdown of all modules."""
        logger.info("Shutting down...")
        self._running = False
        self._shutdown_event.set()

        # Close WebSocket
        if self.hybrid_manager:
            await self.hybrid_manager.shutdown()

        # Close async resources
        await self.market_discovery.close()
        await self.orderbook_poller.close()
        await self.position_monitor.close()

        # Print final stats
        self._print_stats()
        logger.info("Shutdown complete")

    def _check_circuit_breaker(self) -> bool:
        """
        Check if circuit breaker allows operations.

        Returns:
            True if operations allowed, False if circuit is open.
        """
        if not self._circuit_breaker_open:
            return True

        # Check if cooldown has passed
        if self._circuit_breaker_reset_time:
            if datetime.now(timezone.utc) >= self._circuit_breaker_reset_time:
                logger.info("Circuit breaker reset after cooldown")
                self._circuit_breaker_open = False
                self._circuit_breaker_failures = 0
                self._circuit_breaker_reset_time = None
                return True

        return False

    def _record_success(self):
        """Record successful operation, reset failure count."""
        self._circuit_breaker_failures = 0

    def _record_failure(self):
        """Record failed operation, potentially open circuit breaker."""
        self._circuit_breaker_failures += 1

        if self._circuit_breaker_failures >= self._circuit_breaker_threshold:
            self._circuit_breaker_open = True
            self._circuit_breaker_reset_time = datetime.now(timezone.utc) + timedelta(
                seconds=self._circuit_breaker_cooldown_seconds
            )
            logger.warning(
                f"Circuit breaker OPEN after {self._circuit_breaker_failures} failures. "
                f"Cooldown: {self._circuit_breaker_cooldown_seconds}s"
            )

    def _check_rate_limit(self) -> bool:
        """
        Check if we're within rate limits.

        Returns:
            True if request allowed, False if should wait.
        """
        now = datetime.now(timezone.utc)

        # Reset window if >1 minute has passed
        if self._request_window_start is None or (now - self._request_window_start).total_seconds() >= 60:
            self._request_window_start = now
            self._request_count_window = 0

        # Check if under limit
        if self._request_count_window >= self._max_requests_per_minute:
            return False

        self._request_count_window += 1
        return True

    async def refresh_markets(self):
        """Refresh the list of target markets."""
        # Check circuit breaker
        if not self._check_circuit_breaker():
            logger.warning("Circuit breaker open, skipping market refresh")
            return

        logger.info("Refreshing target markets...")
        try:
            self._target_markets = await self.market_discovery.discover_target_markets(
                include_pre_match=True  # Include both in-play and pre-match
            )
            self._last_market_refresh = datetime.now(timezone.utc)
            logger.info(f"Found {len(self._target_markets)} target markets")

            # Log top markets by volume
            if self._target_markets:
                top_markets = sorted(self._target_markets, key=lambda m: m.volume, reverse=True)[:5]
                logger.info("Top 5 markets by volume:")
                for m in top_markets:
                    logger.info(f"  - ${m.volume:,.0f}: {m.question[:60]}...")

            # Subscribe to WebSocket updates for target markets
            if self._use_websocket and self.hybrid_manager:
                await self.hybrid_manager.subscribe_markets(self._target_markets)
                logger.info(f"WebSocket subscribed to {len(self._target_markets)} markets")

            self._record_success()

        except Exception as e:
            logger.error(f"Failed to refresh markets: {e}")
            self._record_failure()

    async def poll_and_detect(self) -> List[ArbOpportunity]:
        """
        Poll orderbooks and detect arbitrage opportunities.

        Returns:
            List of detected opportunities.
        """
        if not self._target_markets:
            return []

        opportunities = await self.orderbook_poller.poll_markets_for_arbs(self._target_markets)
        self._poll_count += 1
        self._opportunities_found += len(opportunities)

        return opportunities

    async def process_opportunity(self, opportunity: ArbOpportunity) -> Optional[ExecutionResult]:
        """
        Process and potentially execute an arbitrage opportunity.

        Uses burst parallel execution for deep arbs during burst mode.
        Includes:
        - Pre-execution book refresh for slippage check
        - Circuit breaker check
        - Idempotency check

        Args:
            opportunity: The opportunity to process.

        Returns:
            ExecutionResult if executed, None otherwise.
        """
        # Validate depth
        depth_valid, depth_reason = self.depth_validator.validate_opportunity(opportunity)
        if not depth_valid:
            logger.debug(f"Depth validation failed: {depth_reason}")
            return None

        # Check risk limits
        trade_allowed, risk_reason = self.risk_manager.check_trade_allowed(opportunity)
        if not trade_allowed:
            logger.debug(f"Risk check failed: {risk_reason}")
            return None

        # Calculate optimal position size
        safe_size = self.depth_validator.get_safe_trade_size(opportunity)
        if safe_size < 10:  # Minimum $10 trade
            logger.debug(f"Trade size too small: ${safe_size:.2f}")
            return None

        # Update opportunity with safe size
        opportunity.trade_size_usd = min(safe_size, opportunity.trade_size_usd)

        # Check if market is in burst mode for parallel execution
        is_burst = self.orderbook_poller.is_burst_market(opportunity.market.condition_id)

        # Execute the trade (use burst parallel for deep arbs during burst mode)
        # Pass orderbook_poller for pre-execution book refresh
        logger.info(f"Executing opportunity: {opportunity}")
        result = await self.execution_engine.execute_opportunity(
            opportunity,
            orderbook_poller=self.orderbook_poller
        )

        if result.success:
            self._trades_executed += 1

            # Record the trade for risk tracking
            trade_record = TradeRecord(
                timestamp=datetime.now(timezone.utc),
                arb_type=opportunity.arb_type,
                trade_size_usd=result.total_cost_usd,
                profit_usd=result.realized_profit_usd,
                market_id=opportunity.market.market_id,
                is_complete=result.is_complete,
                was_partial=result.has_partial_fill,
                was_hedged=result.was_hedged
            )
            self.risk_manager.record_trade(trade_record)

            # Record for CSV export
            self.metrics_exporter.record_trade({
                "market_id": opportunity.market.market_id,
                "market_question": opportunity.market.question[:100],
                "arb_type": opportunity.arb_type.value,
                "trade_size_usd": result.total_cost_usd,
                "profit_usd": result.realized_profit_usd,
                "profit_margin_pct": opportunity.profit_margin * 100,
                "is_complete": result.is_complete,
                "was_partial": result.has_partial_fill,
                "was_hedged": result.was_hedged,
                "num_outcomes": len(opportunity.market.outcomes)
            })

            # Check for big arb alert
            self._check_big_arb_alert(opportunity, result)

            # Verify book normalized post-execution
            await self._verify_post_execution_book(opportunity)

        return result

    def _check_big_arb_alert(self, opportunity: ArbOpportunity, result: ExecutionResult):
        """
        Check if arb qualifies for big arb alert notification.

        Triggers on:
        - Edge >1.2% (big_arb_alert_threshold)
        - Profit >0.5% of daily avg (big_arb_profit_threshold_pct)
        """
        edge_threshold = self.config.trading.big_arb_alert_threshold
        profit_threshold_pct = self.config.trading.big_arb_profit_threshold_pct

        is_big_edge = opportunity.profit_margin >= edge_threshold
        daily_avg = self.metrics_exporter.get_daily_avg_profit()
        is_big_profit = daily_avg > 0 and result.realized_profit_usd >= (daily_avg * profit_threshold_pct)

        if is_big_edge or is_big_profit:
            self._send_big_arb_alert(opportunity, result, is_big_edge, is_big_profit)

    def _send_big_arb_alert(
        self,
        opportunity: ArbOpportunity,
        result: ExecutionResult,
        is_big_edge: bool,
        is_big_profit: bool
    ):
        """Send big arb alert via log and optional webhook."""
        reason = []
        if is_big_edge:
            reason.append(f"edge {opportunity.profit_margin*100:.2f}%")
        if is_big_profit:
            reason.append(f"profit ${result.realized_profit_usd:.4f}")

        alert_msg = (
            f"BIG ARB ALERT | {' + '.join(reason)} | "
            f"{opportunity.arb_type.value} | "
            f"${result.total_cost_usd:.2f} size | "
            f"{opportunity.market.question[:50]}..."
        )

        logger.warning("=" * 60)
        logger.warning(alert_msg)
        logger.warning("=" * 60)

        self._big_arb_alerts_sent += 1

        # Send webhook if configured
        webhook_url = self.config.trading.big_arb_webhook_url
        if webhook_url and HAS_REQUESTS:
            self._send_webhook_notification(webhook_url, opportunity, result)

    def _send_webhook_notification(
        self,
        webhook_url: str,
        opportunity: ArbOpportunity,
        result: ExecutionResult
    ):
        """Send notification to Discord/Telegram webhook."""
        try:
            geo_stats = self.metrics_exporter.get_geometric_return()

            # Format for Discord/Telegram
            payload = {
                "content": (
                    f"**BIG ARB ALERT**\n"
                    f"Edge: {opportunity.profit_margin*100:.2f}%\n"
                    f"Profit: ${result.realized_profit_usd:.4f}\n"
                    f"Size: ${result.total_cost_usd:.2f}\n"
                    f"Market: {opportunity.market.question[:80]}\n"
                    f"Daily Geo Return: {geo_stats['geo_mean_daily_pct']:.3f}%"
                )
            }

            # Discord uses "content", Telegram uses "text"
            if "telegram" in webhook_url.lower():
                payload = {"text": payload["content"]}

            response = requests.post(webhook_url, json=payload, timeout=5)
            if response.status_code in (200, 204):
                logger.debug("Webhook notification sent")
            else:
                logger.debug(f"Webhook returned {response.status_code}")

        except Exception as e:
            logger.debug(f"Webhook notification failed: {e}")

    def _send_low_gas_alert(self, webhook_url: str, matic_balance: float):
        """
        Per Grok Round 15: Send webhook alert when MATIC balance is low.

        Polygon gas can spike during high network activity, draining MATIC fast.
        Alert operators to top up before trades start failing.
        """
        try:
            payload = {
                "content": (
                    f"**⛽ LOW GAS ALERT**\n"
                    f"MATIC Balance: {matic_balance:.4f}\n"
                    f"Warning Threshold: 2.0 MATIC\n"
                    f"Minimum Required: 1.0 MATIC\n"
                    f"Action: Fund wallet with MATIC for gas!\n"
                    f"Wallet: {self.config.wallet.wallet_address[:10]}..."
                )
            }

            if "telegram" in webhook_url.lower():
                payload = {"text": payload["content"]}

            response = requests.post(webhook_url, json=payload, timeout=5)
            if response.status_code in (200, 204):
                logger.info("Low gas webhook alert sent")
            else:
                logger.debug(f"Low gas webhook returned {response.status_code}")

        except Exception as e:
            logger.debug(f"Low gas webhook failed: {e}")

    async def _verify_post_execution_book(self, opportunity: ArbOpportunity):
        """
        Verify orderbook normalized after successful arb execution.

        Refreshes the book once and logs if sum normalized (confirms edge captured).
        Helps detect slippage or stale data issues.
        """
        try:
            # Refresh orderbook for this market
            token_ids = [o.token_id for o in opportunity.market.outcomes]
            books = await self.orderbook_poller.fetch_orderbooks_batch(token_ids)

            if not books:
                return

            # Calculate new sum
            if opportunity.arb_type == ArbType.BUY_ARB:
                # Sum of best asks
                total = sum(
                    books.get(tid, {}).get("best_ask", 0.5)
                    for tid in token_ids
                )
                normalized = abs(total - 1.0) < 0.005  # Within 0.5% of 1.0
            else:
                # Sum of best bids
                total = sum(
                    books.get(tid, {}).get("best_bid", 0.5)
                    for tid in token_ids
                )
                normalized = abs(total - 1.0) < 0.005

            if normalized:
                logger.debug(
                    f"Post-exec verify: book normalized (sum={total:.4f}), edge captured"
                )
            else:
                logger.info(
                    f"Post-exec verify: book still imbalanced (sum={total:.4f}), "
                    f"possible slippage or continued opportunity"
                )

        except Exception as e:
            logger.debug(f"Post-exec verify failed: {e}")

    async def check_and_claim_winnings(self):
        """Check for and claim any resolved market winnings."""
        try:
            claims = await self.position_monitor.auto_claim_all()
            if claims > 0:
                logger.info(f"Auto-claimed {claims} winning positions")
        except Exception as e:
            logger.error(f"Error checking claims: {e}")

    def _get_dynamic_poll_interval(self) -> float:
        """
        Get dynamic poll interval based on burst market count.

        Global burst ramp: auto-drop to 0.6s if >20 burst markets during active hours.
        This captures more opportunities during high-activity periods.

        Returns:
            Poll interval in seconds.
        """
        base_interval = self.config.trading.poll_interval_seconds

        # Only ramp during active hours
        if not self._is_active_hours():
            return base_interval

        # Get current burst market count
        ob_stats = self.orderbook_poller.get_stats()
        burst_count = ob_stats.get("burst_markets", 0)

        # Global burst ramp: >20 burst markets = 0.6s poll
        burst_ramp_threshold = 20
        burst_ramp_interval = 0.6

        if burst_count > burst_ramp_threshold:
            if base_interval > burst_ramp_interval:
                logger.debug(
                    f"Global burst ramp: {burst_count} burst markets, "
                    f"poll interval {base_interval}s -> {burst_ramp_interval}s"
                )
            return burst_ramp_interval

        return base_interval

    async def run_polling_loop(self):
        """Main polling loop for orderbook scanning."""
        last_claim_check = datetime.now(timezone.utc)
        last_hourly_stats = datetime.now(timezone.utc)

        # Use hybrid mode if websocket available
        if self._use_websocket and self.hybrid_manager:
            await self._run_hybrid_loop(last_claim_check, last_hourly_stats)
        else:
            await self._run_http_only_loop(last_claim_check, last_hourly_stats)

    async def _run_hybrid_loop(self, last_claim_check: datetime, last_hourly_stats: datetime):
        """
        Hybrid polling loop: WebSocket for detection, HTTP for verification.

        This achieves <100ms detection latency while maintaining reliability
        through HTTP verification before execution.

        Includes mandatory WS mode: pauses trading if WS disconnects during active hours.
        """
        logger.info("Starting HYBRID polling loop (WS detection + HTTP verify)")
        last_ws_retry = datetime.now(timezone.utc)

        while self._running and not self._shutdown_event.is_set():
            try:
                loop_start = datetime.now(timezone.utc)
                now = loop_start

                # Check WebSocket status (mandatory mode)
                ws_ok = self._check_ws_status()

                if not ws_ok:
                    # WS required but disconnected - attempt reconnect periodically
                    if (now - last_ws_retry).total_seconds() >= self.config.trading.ws_retry_interval:
                        reconnected = await self._attempt_ws_reconnect()
                        last_ws_retry = now
                        if reconnected:
                            continue  # Resume normal operation

                    # Still disconnected, wait and retry
                    await asyncio.sleep(1)
                    continue

                # Check if markets need refresh
                if self.market_discovery.needs_refresh():
                    await self.refresh_markets()

                # Check for WS-detected arbs (non-blocking, 100ms timeout)
                ws_opp = await self.hybrid_manager.get_next_arb(timeout=0.1)

                if ws_opp:
                    # WS detected an arb! Fast-path execution per rn1 pattern
                    # HTTP verification takes 800ms+ and loses 90%+ of arbs
                    edge_pct = ws_opp.profit_margin * 100
                    base_threshold_pct = self.config.trading.arb_threshold_base * 100

                    # Calculate depth available from orderbooks
                    # Per Grok Round 11: Use .orderbooks (Dict[str, Orderbook]) not .books
                    depth_available = 0
                    if ws_opp.orderbooks and ws_opp.orderbooks.orderbooks:
                        for ob in ws_opp.orderbooks.orderbooks.values():
                            if ob.asks:
                                depth_available += sum(lvl.size for lvl in ob.asks[:3])
                            if ob.bids:
                                depth_available += sum(lvl.size for lvl in ob.bids[:3])
                        depth_available = depth_available / 2  # Average of both sides

                    # Calculate WS freshness (ms since detection)
                    ws_freshness_ms = (datetime.now(timezone.utc) - ws_opp.detected_at).total_seconds() * 1000

                    # AGGRESSIVE WS FAST PATH (per Round 3 audit):
                    # - WS latency: <100ms (fast)
                    # - HTTP verification: 800ms+ (too slow, arb gone by then)
                    # - rn1 pattern: Small, fast trades ($27 avg)
                    #
                    # Fast-path conditions (execute IMMEDIATELY on WS):
                    # 1. Edge >= 1.2x threshold AND depth > 2x size AND freshness < 200ms
                    # 2. Size <= $50 (small trades - rn1 pattern) - always fast
                    # 3. Strong signal (edge >= 1.5x threshold) regardless of size
                    #
                    # NEVER verify via HTTP - it kills the edge (800ms delay)
                    strong_edge_threshold = base_threshold_pct * 1.2  # 1.2x per audit
                    very_strong_threshold = base_threshold_pct * 1.5  # 1.5x for any size
                    is_very_strong = edge_pct >= very_strong_threshold
                    is_strong_with_depth = (
                        edge_pct >= strong_edge_threshold and
                        depth_available > ws_opp.trade_size_usd * 2 and
                        ws_freshness_ms < 200
                    )
                    is_small_size = ws_opp.trade_size_usd <= 50  # rn1 pattern ($27 avg)
                    is_medium_size = ws_opp.trade_size_usd <= 200
                    meets_base_threshold = edge_pct >= base_threshold_pct
                    is_fresh = ws_freshness_ms < 500  # <500ms is fresh enough

                    # Fast path: execute immediately on WS signal
                    # Per audit: NEVER use HTTP verification - kills edge
                    use_fast_path = (
                        is_very_strong or  # Very strong edge: always fast
                        is_strong_with_depth or  # 1.2x + depth + fresh: fast
                        is_small_size or  # Small size: always fast (rn1 pattern)
                        (is_medium_size and meets_base_threshold and is_fresh)  # Medium + valid + fresh
                    )

                    if use_fast_path:
                        # Execute immediately on WS signal (fast path)
                        path_reason = 'very_strong' if is_very_strong else \
                                     'strong+depth' if is_strong_with_depth else \
                                     'small' if is_small_size else 'medium+fresh'
                        logger.info(
                            f"[WS FAST] Executing: {edge_pct:.2f}% edge, ${ws_opp.trade_size_usd:.0f}, "
                            f"depth=${depth_available:.0f}, fresh={ws_freshness_ms:.0f}ms ({path_reason})"
                        )
                        # Apply rn1-style sizing
                        rn1_size = self._get_rn1_style_trade_size(max(depth_available, 50))
                        if rn1_size > 0:
                            ws_opp.trade_size_usd = min(ws_opp.trade_size_usd, rn1_size)

                        result = await self.process_opportunity(ws_opp)
                        if result and result.success:
                            self._update_capital_tracking(result.realized_profit_usd)
                    else:
                        # Large size, weak signal, stale data - skip or execute with caution
                        # Per audit: HTTP verification loses 90%+ of arbs, so just execute
                        # but log the risk factors
                        logger.info(
                            f"[WS RISK] Executing with caution: {edge_pct:.2f}% edge, "
                            f"${ws_opp.trade_size_usd:.0f}, depth=${depth_available:.0f}, "
                            f"fresh={ws_freshness_ms:.0f}ms"
                        )
                        # Reduce size for risky trades
                        cautious_size = min(ws_opp.trade_size_usd, 30)  # Cap at $30 for risky
                        ws_opp.trade_size_usd = cautious_size

                        result = await self.process_opportunity(ws_opp)
                        if result and result.success:
                            self._update_capital_tracking(result.realized_profit_usd)

                # Also do periodic HTTP poll as backup (every 2s)
                http_poll_interval = 2.0  # Reduced frequency since WS handles detection

                if (now - loop_start).total_seconds() >= http_poll_interval:
                    http_opps = await self.poll_and_detect()
                    for opp in http_opps:
                        if not self._running:
                            break
                        self.hybrid_manager._http_arbs += 1
                        result = await self.process_opportunity(opp)
                        if result and result.success:
                            self._update_capital_tracking(result.realized_profit_usd)

                # Periodic claim check
                claim_interval = self._get_claim_interval()
                if (now - last_claim_check).total_seconds() >= claim_interval:
                    await self.check_and_claim_winnings()
                    last_claim_check = now

                # Periodic balance refresh (every 5 minutes)
                await self._maybe_refresh_balance()

                # Hourly stats logging
                if (now - last_hourly_stats).total_seconds() >= 3600:
                    self._log_hourly_stats()
                    last_hourly_stats = now

                # Small sleep to prevent busy loop (WS handles real-time)
                await asyncio.sleep(0.05)  # 50ms tick

                # Record heartbeat for supervisor watchdog
                supervisor_heartbeat()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in hybrid loop: {e}")
                await asyncio.sleep(1)

    async def _run_http_only_loop(self, last_claim_check: datetime, last_hourly_stats: datetime):
        """HTTP-only polling loop (fallback when WS unavailable)."""
        logger.info("Starting HTTP-only polling loop")

        while self._running and not self._shutdown_event.is_set():
            try:
                loop_start = datetime.now(timezone.utc)

                # Check if markets need refresh
                if self.market_discovery.needs_refresh():
                    await self.refresh_markets()

                # Poll for opportunities
                opportunities = await self.poll_and_detect()

                # Process each opportunity
                for opp in opportunities:
                    if not self._running:
                        break
                    await self.process_opportunity(opp)

                # Periodic claim check (interval based on active hours)
                now = datetime.now(timezone.utc)
                claim_interval = self._get_claim_interval()
                if (now - last_claim_check).total_seconds() >= claim_interval:
                    await self.check_and_claim_winnings()
                    last_claim_check = now

                # Periodic balance refresh (every 5 minutes)
                await self._maybe_refresh_balance()

                # Hourly stats logging
                if (now - last_hourly_stats).total_seconds() >= 3600:
                    self._log_hourly_stats()
                    last_hourly_stats = now

                # Calculate time to next poll (dynamic based on burst activity)
                poll_interval = self._get_dynamic_poll_interval()
                elapsed = (datetime.now(timezone.utc) - loop_start).total_seconds()
                sleep_time = max(0, poll_interval - elapsed)

                if sleep_time > 0:
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=sleep_time
                        )
                    except asyncio.TimeoutError:
                        pass  # Normal timeout, continue loop

                # Record heartbeat for supervisor watchdog
                supervisor_heartbeat()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in HTTP polling loop: {e}")
                await asyncio.sleep(1)  # Brief pause on error

    def _log_hourly_stats(self):
        """Log hourly statistics and record snapshot for CSV export."""
        risk_stats = self.risk_manager.get_stats()
        exec_stats = self.execution_engine.get_stats()
        ob_stats = self.orderbook_poller.get_stats()
        geo_stats = self.metrics_exporter.get_geometric_return()

        logger.info("-" * 40)
        logger.info("HOURLY STATS")
        logger.info(f"Polls: {self._poll_count} | Opps: {self._opportunities_found} | Trades: {self._trades_executed}")
        logger.info(f"Capital: ${risk_stats['current_capital_usd']:,.2f} | Return: {risk_stats['total_return_pct']:.2f}%")
        logger.info(f"Profit: ${exec_stats['total_profit_usd']:.4f} | Partials: {exec_stats['partial_fills']}")
        logger.info(f"Geo daily: {geo_stats['geo_mean_daily_pct']:.3f}% | Compounded: {geo_stats['compounded_return_pct']:.3f}%")
        logger.info(f"Stale skipped: {ob_stats['stale_books_skipped']} | Slippage rejected: {ob_stats['slippage_rejected']}")
        logger.info(f"Burst markets: {ob_stats['burst_markets']} | Poll interval: {self._get_dynamic_poll_interval():.1f}s")
        logger.info(f"Big arb alerts: {self._big_arb_alerts_sent} | Active hours: {self._is_active_hours()}")
        # New enhanced stats
        logger.info(f"Drawdown: {exec_stats.get('drawdown_pct', 0):.2f}% | Circuit breaker: {'ACTIVE' if exec_stats.get('circuit_breaker_active') else 'OK'}")
        logger.info(f"FAK mode: {exec_stats.get('fak_mode', False)} | Idempotency tracked: {exec_stats.get('idempotency_tracked', 0)}")
        # Per Grok Round 10: Airdrop projection for volume farming motivation
        if risk_stats.get('trash_mode_enabled'):
            trash_vol = risk_stats.get('trash_mode_volume_usd', 0)
            projected_airdrop = risk_stats.get('projected_airdrop_equity_usd', 0)
            logger.info(f"TRASH MODE: Volume ${trash_vol:.2f} | Projected airdrop: ${projected_airdrop:.4f}")

        # Periodic allowance check (for EOA wallets) - detect approval issues early
        if not self.config.dry_run and self.config.wallet.signature_type == 0:
            client = self.execution_engine._client
            if client:
                has_allowance, allowance_usd = self.auth_manager.check_usdc_allowance(
                    client, min_required=self._current_capital * 2
                )
                if not has_allowance:
                    logger.warning(f"USDC allowance LOW: ${allowance_usd:,.2f} - may cause 400 errors!")
                else:
                    logger.info(f"USDC allowance OK: ${allowance_usd:,.2f}")

            # Per Grok Round 15/16: MATIC gas balance check with webhook alert
            # Warning threshold raised to 2.0 MATIC for HF burst trading safety
            matic_balance = self.auth_manager.get_matic_balance()
            if matic_balance is not None:
                if matic_balance < 2.0:  # Warning threshold (MIN_MATIC_FOR_GAS=1.0)
                    logger.warning(f"MATIC balance LOW: {matic_balance:.4f} - fund wallet for gas!")
                    # Send webhook alert if configured
                    webhook_url = self.config.trading.big_arb_webhook_url
                    if webhook_url and HAS_REQUESTS:
                        self._send_low_gas_alert(webhook_url, matic_balance)
                else:
                    logger.info(f"MATIC balance OK: {matic_balance:.4f}")

        # WebSocket stats if enabled
        if self._use_websocket and self.hybrid_manager:
            ws_stats = self.hybrid_manager.get_stats()
            logger.info(
                f"WS Mode: {ws_stats['mode']} | WS Arbs: {ws_stats['ws_arbs_detected']} | "
                f"HTTP Arbs: {ws_stats['http_arbs_detected']} | "
                f"Latency: {ws_stats['ws']['latency_avg_ms']:.0f}ms avg"
            )

        logger.info("-" * 40)

        # Record hourly snapshot for CSV export
        self.metrics_exporter.record_hourly_snapshot({
            "poll_count": self._poll_count,
            "opportunities_found": self._opportunities_found,
            "trades_executed": self._trades_executed,
            "current_capital_usd": risk_stats['current_capital_usd'],
            "total_return_pct": risk_stats['total_return_pct'],
            "total_profit_usd": exec_stats['total_profit_usd'],
            "partial_fills": exec_stats['partial_fills'],
            "stale_skipped": ob_stats['stale_books_skipped'],
            "slippage_rejected": ob_stats['slippage_rejected'],
            "burst_markets": ob_stats['burst_markets'],
            "poll_interval_seconds": self._get_dynamic_poll_interval(),
            "is_active_hours": self._is_active_hours(),
            "win_rate_pct": risk_stats['win_rate_pct'],
            "max_drawdown_pct": risk_stats['max_drawdown_pct'],
            "geo_mean_daily_pct": geo_stats['geo_mean_daily_pct'],
            "compounded_return_pct": geo_stats['compounded_return_pct'],
            "big_arb_alerts": self._big_arb_alerts_sent,
            # Per Grok Round 10: Airdrop projection for hourly tracking
            "trash_mode_volume_usd": risk_stats.get('trash_mode_volume_usd', 0),
            "projected_airdrop_usd": risk_stats.get('projected_airdrop_equity_usd', 0)
        })

    async def run(self):
        """Main entry point to run the bot."""
        self._running = True

        try:
            await self.initialize()
            await self.refresh_markets()

            logger.info("Starting polling loop...")
            logger.info(f"Poll interval: {self.config.trading.poll_interval_seconds}s")

            await self.run_polling_loop()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            await self.shutdown()

    def _print_stats(self):
        """Print final statistics."""
        if not self._start_time:
            return

        runtime = (datetime.now(timezone.utc) - self._start_time).total_seconds()
        hours = runtime / 3600

        logger.info("=" * 60)
        logger.info("SESSION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"Runtime: {runtime/60:.1f} minutes ({hours:.2f} hours)")
        logger.info(f"Total polls: {self._poll_count}")
        logger.info(f"Polls per hour: {self._poll_count/hours:.1f}" if hours > 0 else "N/A")
        logger.info(f"Opportunities found: {self._opportunities_found}")
        logger.info(f"Trades executed: {self._trades_executed}")

        # Orderbook stats
        ob_stats = self.orderbook_poller.get_stats()
        logger.info(f"Arbs detected: {ob_stats['arbs_found']}")
        logger.info(f"Big arb alerts (>2%): {ob_stats['big_arb_alerts']}")
        logger.info(f"Stale books skipped: {ob_stats['stale_books_skipped']}")
        logger.info(f"Slippage rejected: {ob_stats['slippage_rejected']}")
        logger.info(f"Batch fetches: {ob_stats['batch_fetches']}")
        logger.info(f"Batch fetch errors: {ob_stats['batch_fetch_errors']}")

        # Execution stats
        exec_stats = self.execution_engine.get_stats()
        logger.info(f"Total volume: ${exec_stats['total_volume_usd']:,.2f}")
        logger.info(f"Total profit: ${exec_stats['total_profit_usd']:.4f}")
        if exec_stats['execution_count'] > 0:
            logger.info(f"Avg profit per trade: ${exec_stats['avg_profit_per_trade']:.4f}")
        logger.info(f"Partial fills: {exec_stats['partial_fills']}")
        logger.info(f"Hedges executed: {exec_stats['hedges_executed']}")
        logger.info(f"Approvals executed: {exec_stats['approvals_executed']}")

        # Risk stats
        risk_stats = self.risk_manager.get_stats()
        logger.info(f"Initial capital: ${risk_stats['initial_capital_usd']:,.2f}")
        logger.info(f"Final capital: ${risk_stats['current_capital_usd']:,.2f}")
        logger.info(f"Peak capital: ${risk_stats['peak_capital_usd']:,.2f}")
        logger.info(f"Total return: {risk_stats['total_return_pct']:.2f}%")
        logger.info(f"Win rate: {risk_stats['win_rate_pct']:.1f}%")
        logger.info(f"Max drawdown: {risk_stats['max_drawdown_pct']:.2f}%")
        logger.info(f"Geometric mean return: {risk_stats['geometric_mean_return_pct']:.4f}%")
        logger.info(f"Projected 30-day multiple: {risk_stats['projected_30d_multiple']:.2f}x")

        # Claiming stats
        claim_stats = self.position_monitor.get_stats()
        logger.info(f"Positions claimed: {claim_stats['claims_count']}")
        logger.info(f"Total claimed: ${claim_stats['total_claimed_usd']:.2f}")

        # Geometric return stats
        geo_stats = self.metrics_exporter.get_geometric_return()
        logger.info(f"Daily geo mean return: {geo_stats['geo_mean_daily_pct']:.4f}%")
        logger.info(f"Compounded return: {geo_stats['compounded_return_pct']:.4f}%")
        logger.info(f"Trading days: {geo_stats['trading_days']}")
        logger.info(f"Big arb alerts sent: {self._big_arb_alerts_sent}")

        logger.info("=" * 60)

        # Export metrics to CSV
        logger.info("Exporting metrics to CSV...")
        final_stats = {
            "runtime_seconds": runtime,
            "runtime_hours": hours,
            "total_polls": self._poll_count,
            "polls_per_hour": self._poll_count / hours if hours > 0 else 0,
            "opportunities_found": self._opportunities_found,
            "trades_executed": self._trades_executed,
            "arbs_detected": ob_stats['arbs_found'],
            "big_arb_alerts": ob_stats['big_arb_alerts'],
            "stale_books_skipped": ob_stats['stale_books_skipped'],
            "slippage_rejected": ob_stats['slippage_rejected'],
            "batch_fetches": ob_stats['batch_fetches'],
            "batch_fetch_errors": ob_stats['batch_fetch_errors'],
            "total_volume_usd": exec_stats['total_volume_usd'],
            "total_profit_usd": exec_stats['total_profit_usd'],
            "avg_profit_per_trade": exec_stats['avg_profit_per_trade'],
            "partial_fills": exec_stats['partial_fills'],
            "hedges_executed": exec_stats['hedges_executed'],
            "approvals_executed": exec_stats['approvals_executed'],
            "initial_capital_usd": risk_stats['initial_capital_usd'],
            "final_capital_usd": risk_stats['current_capital_usd'],
            "peak_capital_usd": risk_stats['peak_capital_usd'],
            "total_return_pct": risk_stats['total_return_pct'],
            "win_rate_pct": risk_stats['win_rate_pct'],
            "max_drawdown_pct": risk_stats['max_drawdown_pct'],
            "geometric_mean_return_pct": risk_stats['geometric_mean_return_pct'],
            "projected_30d_multiple": risk_stats['projected_30d_multiple'],
            "claims_count": claim_stats['claims_count'],
            "total_claimed_usd": claim_stats['total_claimed_usd'],
            "mode": "dry_run" if self.config.dry_run else "live",
            # Per Grok Round 10: Airdrop projection for volume farming tracking
            "trash_mode_enabled": risk_stats.get('trash_mode_enabled', False),
            "trash_mode_volume_usd": risk_stats.get('trash_mode_volume_usd', 0),
            "trash_mode_trades": risk_stats.get('trash_mode_trades', 0),
            "projected_airdrop_usd": risk_stats.get('projected_airdrop_equity_usd', 0)
        }
        exported = self.metrics_exporter.export_all(final_stats)
        if exported.get("session"):
            logger.info(f"Metrics exported to: {self.metrics_exporter.output_dir}/")


def handle_signal(sig, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {sig}, initiating shutdown...")
    # Will be caught by the main loop


async def main():
    """Main entry point."""
    # Load configuration
    config = load_config()

    # Setup logging
    setup_logging(config.logging)

    # Note: Monkey-patching removed per audit (fragile, can break on library updates)
    # Rate limiting now handled at application level via rate_limiter module
    logger.info("Rate limiting handled at application level (monkey-patch disabled)")

    logger.info("Starting Polymarket Arbitrage Bot with supervisor...")

    # Create bot instance (will be recreated on restart)
    bot: Optional[ArbBot] = None

    async def create_and_run_bot():
        """Factory function for supervised bot execution."""
        nonlocal bot
        bot = ArbBot(config)
        await bot.run()

    async def cleanup_bot():
        """Cleanup function between restarts."""
        nonlocal bot
        if bot:
            try:
                await bot.shutdown()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
            bot = None

        # Per Grok Round 6 optimization: Explicit executor shutdown prevents
        # resource leaks on restarts in long-running async apps
        try:
            shutdown_clob_executor(wait=True, cancel_futures=False)
        except Exception as e:
            logger.debug(f"Executor shutdown note: {e}")

    # Use supervisor for crash recovery
    # Supervisor handles: automatic restarts, heartbeat monitoring, signal handling
    supervisor = get_supervisor()

    await supervisor.run(
        main_coro_factory=create_and_run_bot,
        cleanup_coro=cleanup_bot,
    )

    logger.info("Bot shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
