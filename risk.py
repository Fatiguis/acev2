"""
Risk Management Module.
Handles position sizing, exposure limits, and safety checks.

Per Grok Round 7: Integrated edge_model expectancy for smarter sizing.
Hot markets get reduced sizing due to lower fill probability.
"""

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, TYPE_CHECKING
from collections import deque

from config import BotConfig, calculate_dynamic_threshold
from orderbook import ArbOpportunity, ArbType
from positions import Position

# Per Grok Round 7: Integrate edge model for expectancy-based sizing
if TYPE_CHECKING:
    from edge_model import EdgeExpectancyModel

logger = logging.getLogger(__name__)


@dataclass
class RiskMetrics:
    """Current risk metrics snapshot."""
    total_exposure_usd: float = 0.0
    max_single_position_usd: float = 0.0
    position_count: int = 0
    unrealized_pnl_usd: float = 0.0
    unrealized_drawdown_pct: float = 0.0  # Unrealized loss as % of capital
    daily_pnl_usd: float = 0.0
    daily_trades: int = 0
    consecutive_losses: int = 0
    win_rate: float = 0.0
    sharpe_estimate: float = 0.0
    max_drawdown_pct: float = 0.0
    capital_at_risk_pct: float = 0.0
    geometric_mean_return: float = 0.0  # For compounding analysis
    total_return_pct: float = 0.0
    force_hedge_triggered: bool = False  # True if unrealized drawdown >5%


@dataclass
class TradeRecord:
    """Record of a completed trade for risk tracking."""
    timestamp: datetime
    arb_type: ArbType
    trade_size_usd: float
    profit_usd: float
    market_id: str
    is_complete: bool
    was_partial: bool = False
    was_hedged: bool = False

    @property
    def return_pct(self) -> float:
        """Calculate return percentage."""
        if self.trade_size_usd > 0:
            return self.profit_usd / self.trade_size_usd
        return 0.0

    @property
    def log_return(self) -> float:
        """Calculate log return for geometric mean calculation."""
        r = self.return_pct
        if r > -1:  # Avoid log of non-positive
            return math.log(1 + r)
        return -float('inf')


class RiskManager:
    """Manages risk controls and position sizing."""

    # Unrealized drawdown threshold to trigger force hedge (5%)
    FORCE_HEDGE_THRESHOLD_PCT = 5.0

    def __init__(self, config: BotConfig):
        """
        Initialize the risk manager.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self._initial_capital = config.starting_capital_usd
        self._current_capital = config.starting_capital_usd
        self._peak_capital = config.starting_capital_usd
        self._trade_history: deque = deque(maxlen=1000)  # Keep last 1000 trades
        self._daily_trades: List[TradeRecord] = []
        self._daily_pnl = 0.0
        self._last_reset_date: Optional[datetime] = None
        self._consecutive_losses = 0
        self._consecutive_partials = 0
        self._is_halted = False
        self._halt_reason: Optional[str] = None
        self._log_returns_sum = 0.0  # Sum of log returns for geometric mean
        self._unrealized_pnl = 0.0  # Current unrealized PnL from open positions
        self._force_hedge_triggered = False

        # Maker ratio tracking (per audit)
        self._maker_fills = 0
        self._taker_fills = 0
        self._maker_ratio_warning_shown = False

        # Unhedged exposure tracking (per audit)
        self._current_unhedged_exposure = 0.0

        # Per Grok Round 7: Edge model for expectancy-based sizing
        self._edge_model: Optional['EdgeExpectancyModel'] = None
        self._init_edge_model()

        # Per Grok Round 8: Optional trash_mode for volume farming
        # When enabled, allows buying near-certain losers (<$0.03) for volume farming
        # This is HIGHLY SPECULATIVE - only for airdrop farming speculation
        # Default: OFF (disabled)
        self._trash_mode_enabled = os.getenv("TRASH_MODE", "false").lower() == "true"
        self._trash_mode_max_price = 0.03  # Only buy at <$0.03 (3 cents)
        self._trash_mode_max_size_usd = 5.0  # Per Grok Round 9: Max $5 per trash trade (ultra-conservative)
        self._trash_mode_trades = 0  # Track trash volume trades
        self._trash_mode_volume_usd = 0.0  # Track trash volume in USD

        # Per Grok Round 9: Airdrop allocation speculation
        # Rumored allocation: $0.01-0.05 per $1k volume (highly speculative)
        # rn1 farmed heavily (confirmed 2026 posts) - this motivates trash safely
        self._airdrop_allocation_per_1k_volume = 0.03  # $0.03 per $1k volume (mid estimate)

        if self._trash_mode_enabled:
            logger.warning(
                "TRASH MODE ENABLED: Buying near-certain losers at <$0.03 for volume farming. "
                "This is HIGHLY SPECULATIVE and for airdrop farming only!"
            )

    def _init_edge_model(self):
        """
        Per Grok Round 7: Initialize edge model for expectancy-based sizing.

        The edge model adjusts sizing based on:
        - Market heat (hot markets = lower fill probability = smaller size)
        - Fill probability decay with latency
        - Competition estimation
        """
        try:
            from edge_model import EdgeExpectancyModel
            self._edge_model = EdgeExpectancyModel()
            logger.debug("Edge model initialized for expectancy-based sizing")
        except ImportError:
            logger.debug("Edge model not available, using standard sizing")
            self._edge_model = None

    def update_capital(self, new_capital: float):
        """Update current capital."""
        self._current_capital = new_capital
        if new_capital > self._peak_capital:
            self._peak_capital = new_capital

    def record_trade(self, trade: TradeRecord):
        """Record a completed trade."""
        self._trade_history.append(trade)
        self._daily_trades.append(trade)
        self._daily_pnl += trade.profit_usd

        # Track consecutive losses
        if trade.profit_usd < 0:
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        # Track consecutive partials
        if trade.was_partial:
            self._consecutive_partials += 1
        else:
            self._consecutive_partials = 0

        # Update capital
        self._current_capital += trade.profit_usd

        # Track log returns for geometric mean calculation
        if trade.log_return > -float('inf'):
            self._log_returns_sum += trade.log_return

        # Update peak capital
        if self._current_capital > self._peak_capital:
            self._peak_capital = self._current_capital

    def _reset_daily_stats(self):
        """Reset daily statistics at midnight UTC."""
        now = datetime.now(timezone.utc)
        if self._last_reset_date is None or self._last_reset_date.date() != now.date():
            self._daily_trades = []
            self._daily_pnl = 0.0
            self._last_reset_date = now
            logger.info("Daily stats reset")

    def check_trade_allowed(self, opportunity: ArbOpportunity) -> tuple[bool, str]:
        """
        Check if a trade is allowed based on risk parameters.

        Uses dynamic threshold that accounts for gas costs.

        Args:
            opportunity: The arbitrage opportunity to check.

        Returns:
            Tuple of (allowed, reason).
        """
        self._reset_daily_stats()

        # Check if trading is halted
        if self._is_halted:
            return False, f"Trading halted: {self._halt_reason}"

        # Check unhedged exposure (per audit)
        exposure_ok, exposure_msg = self.check_unhedged_exposure_ok()
        if not exposure_ok:
            return False, exposure_msg

        # Check maker ratio (per audit: rn1 preferred maker for rebates + lower rate pressure)
        # Only enforce after sufficient fills to have meaningful data
        maker_ok, maker_msg = self.check_maker_ratio_ok()
        if not maker_ok:
            # Don't hard block, but log warning - can still trade but prefer post-only
            logger.debug(f"Maker ratio low: {maker_msg}")

        # Calculate dynamic threshold based on trade size and outcomes
        num_outcomes = len(opportunity.market.outcomes)
        dynamic_threshold = calculate_dynamic_threshold(
            opportunity.trade_size_usd,
            num_outcomes,
            self.config.trading
        )

        # Check minimum profit margin against dynamic threshold
        if opportunity.profit_margin < dynamic_threshold:
            return False, (
                f"Margin {opportunity.profit_margin*100:.3f}% below dynamic threshold "
                f"{dynamic_threshold*100:.3f}% (gas-adjusted for ${opportunity.trade_size_usd:.2f})"
            )

        # Check trade size limits
        max_trade_size = self._current_capital * (self.config.trading.max_size_per_trade_percent / 100)
        if opportunity.trade_size_usd > max_trade_size:
            return False, f"Trade size ${opportunity.trade_size_usd:.2f} exceeds max ${max_trade_size:.2f}"

        # Check consecutive losses circuit breaker (halt after 5 consecutive losses)
        if self._consecutive_losses >= 5:
            self._is_halted = True
            self._halt_reason = "5 consecutive losses - manual review required"
            return False, self._halt_reason

        # Check consecutive partials circuit breaker
        if self._consecutive_partials >= self.config.trading.max_consecutive_partials:
            self._is_halted = True
            self._halt_reason = (
                f"{self._consecutive_partials} consecutive partial fills - "
                f"pause for {self.config.trading.partial_pause_duration}s"
            )
            return False, self._halt_reason

        # Check daily loss limit (halt if down 3% in a day - more conservative)
        daily_loss_limit = self._current_capital * 0.03
        if self._daily_pnl < -daily_loss_limit:
            self._is_halted = True
            self._halt_reason = f"Daily loss limit (3%) exceeded: ${self._daily_pnl:.2f}"
            return False, self._halt_reason

        # Check max drawdown (halt if down 10% from peak)
        drawdown = (self._peak_capital - self._current_capital) / self._peak_capital if self._peak_capital > 0 else 0
        if drawdown > 0.10:
            self._is_halted = True
            self._halt_reason = f"Max drawdown exceeded: {drawdown*100:.1f}%"
            return False, self._halt_reason

        # Check minimum capital
        if self._current_capital < 100:  # Minimum $100 to trade
            return False, f"Insufficient capital: ${self._current_capital:.2f}"

        return True, "Trade allowed"

    def calculate_position_size(
        self,
        opportunity: ArbOpportunity,
        orderbooks_depth: Dict[str, float],
        market_volume_24h: float = 0.0,
        is_live_event: bool = False,
        latency_ms: float = 80.0
    ) -> float:
        """
        Calculate optimal position size for an opportunity.

        Uses gas-aware sizing: only trade sizes where expected profit > gas cost.

        Per Grok Round 7: Integrated edge model expectancy for smarter sizing.
        Hot markets get reduced sizing due to lower fill probability.

        Args:
            opportunity: The arbitrage opportunity.
            orderbooks_depth: Available depth at best prices by token_id.
            market_volume_24h: 24-hour volume for market heat estimation.
            is_live_event: True if this is a live sports event.
            latency_ms: Our estimated latency in milliseconds.

        Returns:
            Recommended position size in USD.
        """
        # Start with the minimum available depth across all outcomes
        min_depth = min(orderbooks_depth.values()) if orderbooks_depth else 0

        # Apply safety multiplier
        safe_size = min_depth * self.config.trading.depth_safety_multiplier

        # Cap at max percentage of capital
        max_size = self._current_capital * (self.config.trading.max_size_per_trade_percent / 100)
        position_size = min(safe_size, max_size)

        # ABSOLUTE cap regardless of capital (per audit: $200 max)
        absolute_max = self.config.trading.absolute_max_trade_size_usd
        position_size = min(position_size, absolute_max)

        # Per Grok Round 7: Apply edge model expectancy adjustment
        # Reduce size in hot markets where fill probability is lower
        if self._edge_model is not None:
            position_size = self._apply_expectancy_sizing(
                position_size,
                opportunity.profit_margin,
                min_depth,
                market_volume_24h,
                is_live_event,
                latency_ms
            )

        # Reduce size based on consecutive losses (risk scaling)
        if self._consecutive_losses > 0:
            scale_factor = 1.0 / (1 + self._consecutive_losses * 0.2)
            position_size *= scale_factor
            logger.debug(f"Scaled position by {scale_factor:.2f} due to {self._consecutive_losses} consecutive losses")

        # Reduce size based on consecutive partials
        if self._consecutive_partials > 0:
            scale_factor = 1.0 / (1 + self._consecutive_partials * 0.3)
            position_size *= scale_factor
            logger.debug(f"Scaled position by {scale_factor:.2f} due to {self._consecutive_partials} consecutive partials")

        # Gas-aware minimum sizing
        # Ensure expected profit > total gas cost
        num_outcomes = len(opportunity.market.outcomes)
        total_gas = self.config.trading.gas_buffer_usd * num_outcomes
        min_profitable_size = total_gas / opportunity.profit_margin if opportunity.profit_margin > 0 else float('inf')

        if position_size < min_profitable_size:
            logger.debug(
                f"Position ${position_size:.2f} below gas-profitable minimum ${min_profitable_size:.2f}"
            )
            return 0

        # Minimum viable trade size ($10)
        if position_size < 10:
            return 0

        return position_size

    def _apply_expectancy_sizing(
        self,
        base_size: float,
        edge_pct: float,
        available_depth: float,
        volume_24h: float,
        is_live_event: bool,
        latency_ms: float
    ) -> float:
        """
        Per Grok Round 7: Apply edge model expectancy adjustment to sizing.

        Reduces size in hot markets where fill probability is lower.
        Uses the edge model's optimal sizing calculation.

        Args:
            base_size: Initial position size.
            edge_pct: Arbitrage edge as decimal.
            available_depth: Available depth at best price.
            volume_24h: 24-hour market volume.
            is_live_event: True if live sports event.
            latency_ms: Our latency estimate.

        Returns:
            Adjusted position size.
        """
        if self._edge_model is None:
            return base_size

        try:
            from edge_model import MarketHeat

            # Estimate market heat
            heat = self._edge_model.estimate_market_heat(
                volume_24h=volume_24h,
                is_live_event=is_live_event,
                recent_price_volatility=0.0  # Could be enhanced with actual volatility
            )

            # Get optimal size from edge model
            estimate = self._edge_model.calculate_optimal_size(
                raw_edge_pct=edge_pct,
                max_size_usd=base_size,
                available_depth_usd=available_depth,
                latency_ms=latency_ms,
                heat=heat,
                capital_usd=self._current_capital,
                use_websocket=True  # Assume WebSocket usage
            )

            optimal_size = estimate.optimal_size_usd

            # If edge model suggests smaller size, use it
            if optimal_size < base_size and optimal_size > 0:
                logger.debug(
                    f"Expectancy sizing: ${base_size:.2f} -> ${optimal_size:.2f} "
                    f"(heat={heat.value}, fill_prob={estimate.fill_probability:.1%})"
                )
                return optimal_size

            # If edge model suggests zero (expected profit negative), return 0
            if optimal_size == 0 and estimate.fill_probability < 0.05:
                logger.debug(
                    f"Expectancy sizing: SKIP (fill_prob={estimate.fill_probability:.1%} too low)"
                )
                return 0

            return base_size

        except Exception as e:
            logger.debug(f"Edge model sizing failed: {e}, using base size")
            return base_size

    def record_fill_attempt(self, latency_ms: float, edge_pct: float, filled: bool, heat_str: str = "warm"):
        """
        Per Grok Round 7: Record fill attempt for edge model calibration.

        Args:
            latency_ms: Latency of the fill attempt.
            edge_pct: Edge of the opportunity.
            filled: Whether the order filled.
            heat_str: Market heat level as string.
        """
        if self._edge_model is None:
            return

        try:
            from edge_model import MarketHeat

            heat_map = {
                "cold": MarketHeat.COLD,
                "warm": MarketHeat.WARM,
                "hot": MarketHeat.HOT,
                "blazing": MarketHeat.BLAZING,
            }
            heat = heat_map.get(heat_str.lower(), MarketHeat.WARM)

            self._edge_model.record_fill_attempt(latency_ms, edge_pct, filled, heat)

        except Exception:
            pass

    def calculate_geometric_mean_return(self) -> float:
        """
        Calculate geometric mean return for compounding analysis.

        Returns:
            Geometric mean return as a decimal.
        """
        n = len(self._trade_history)
        if n == 0:
            return 0.0

        # Geometric mean = exp(mean of log returns)
        mean_log_return = self._log_returns_sum / n
        return math.exp(mean_log_return) - 1

    def calculate_total_return(self) -> float:
        """
        Calculate total return since start.

        Returns:
            Total return as percentage.
        """
        if self._initial_capital <= 0:
            return 0.0
        return ((self._current_capital - self._initial_capital) / self._initial_capital) * 100

    def estimate_compound_growth(self, trades_per_day: int = 20, days: int = 30) -> float:
        """
        Estimate compound growth based on current geometric mean.

        Args:
            trades_per_day: Expected trades per day.
            days: Number of days to project.

        Returns:
            Projected capital multiple.
        """
        g = self.calculate_geometric_mean_return()
        if g <= 0:
            return 1.0

        total_trades = trades_per_day * days
        return math.pow(1 + g, total_trades)

    def validate_depth(
        self,
        opportunity: ArbOpportunity,
        min_depth: float
    ) -> bool:
        """
        Validate that orderbook depth is sufficient.

        Args:
            opportunity: The opportunity to validate.
            min_depth: Minimum required depth in USD.

        Returns:
            True if depth is sufficient.
        """
        for ob in opportunity.orderbooks.all_orderbooks:
            if opportunity.arb_type == ArbType.BUY_ARB:
                depth_usd = ob.best_ask_size * ob.best_ask_price if ob.best_ask else 0
            else:
                depth_usd = ob.best_bid_size * ob.best_bid_price if ob.best_bid else 0

            if depth_usd < min_depth:
                logger.debug(f"Insufficient depth for {ob.outcome_name}: ${depth_usd:.2f} < ${min_depth:.2f}")
                return False

        return True

    def get_metrics(self) -> RiskMetrics:
        """Get current risk metrics."""
        self._reset_daily_stats()

        metrics = RiskMetrics()
        metrics.daily_pnl_usd = self._daily_pnl
        metrics.daily_trades = len(self._daily_trades)
        metrics.consecutive_losses = self._consecutive_losses

        # Calculate win rate
        if self._trade_history:
            wins = sum(1 for t in self._trade_history if t.profit_usd > 0)
            metrics.win_rate = wins / len(self._trade_history)

        # Calculate drawdown
        if self._peak_capital > 0:
            metrics.max_drawdown_pct = (self._peak_capital - self._current_capital) / self._peak_capital * 100

        # Capital at risk
        metrics.capital_at_risk_pct = (
            sum(t.trade_size_usd for t in self._daily_trades) / self._current_capital * 100
            if self._current_capital > 0 else 0
        )

        # Geometric mean return for compounding analysis
        metrics.geometric_mean_return = self.calculate_geometric_mean_return()
        metrics.total_return_pct = self.calculate_total_return()

        return metrics

    def update_unrealized_pnl(self, positions: List[Position]) -> None:
        """
        Update unrealized PnL from open positions.

        Args:
            positions: List of current open positions.
        """
        self._unrealized_pnl = sum(p.unrealized_pnl_usd for p in positions)

    def check_force_hedge_needed(self, positions: List[Position]) -> tuple[bool, List[Position]]:
        """
        Check if unrealized drawdown exceeds threshold and force hedge is needed.

        Triggers when unrealized loss > 5% of current capital.

        Args:
            positions: List of current open positions.

        Returns:
            Tuple of (force_hedge_needed, positions_to_hedge).
        """
        self.update_unrealized_pnl(positions)

        # Calculate unrealized drawdown as % of capital
        if self._current_capital <= 0:
            return False, []

        unrealized_drawdown_pct = abs(min(0, self._unrealized_pnl)) / self._current_capital * 100

        if unrealized_drawdown_pct >= self.FORCE_HEDGE_THRESHOLD_PCT:
            self._force_hedge_triggered = True
            logger.warning(
                f"FORCE HEDGE TRIGGERED: Unrealized drawdown {unrealized_drawdown_pct:.2f}% "
                f"exceeds threshold {self.FORCE_HEDGE_THRESHOLD_PCT}%. "
                f"Unrealized PnL: ${self._unrealized_pnl:.2f}"
            )

            # Return all positions with negative unrealized PnL
            positions_to_hedge = [p for p in positions if p.unrealized_pnl_usd < 0]
            return True, positions_to_hedge

        self._force_hedge_triggered = False
        return False, []

    def get_unrealized_drawdown_pct(self) -> float:
        """Get current unrealized drawdown as percentage of capital."""
        if self._current_capital <= 0:
            return 0.0
        return abs(min(0, self._unrealized_pnl)) / self._current_capital * 100

    def resume_trading(self):
        """Resume trading after a halt (manual action)."""
        self._is_halted = False
        self._halt_reason = None
        self._consecutive_losses = 0
        self._force_hedge_triggered = False
        self._current_unhedged_exposure = 0.0  # Reset exposure on resume
        logger.info("Trading resumed manually")

    def record_fill_type(self, is_maker: bool):
        """
        Record whether a fill was maker or taker for ratio tracking.

        Args:
            is_maker: True if fill was maker (post-only), False if taker.
        """
        if is_maker:
            self._maker_fills += 1
        else:
            self._taker_fills += 1

    def get_maker_ratio(self) -> float:
        """
        Get current maker fill ratio.

        Returns:
            Ratio of maker fills to total fills (0-1).
        """
        total = self._maker_fills + self._taker_fills
        if total == 0:
            return 1.0  # No fills yet, assume good
        return self._maker_fills / total

    def check_maker_ratio_ok(self) -> tuple[bool, str]:
        """
        Check if maker ratio is above target threshold.

        Per audit: rn1 preferred maker orders for fee rebates.
        Pause taker orders if ratio drops below target.

        Returns:
            Tuple of (ok, reason).
        """
        ratio = self.get_maker_ratio()
        target = self.config.trading.maker_ratio_target

        # Need at least 10 fills to have meaningful ratio
        total_fills = self._maker_fills + self._taker_fills
        if total_fills < 10:
            return True, f"Maker ratio: {ratio*100:.0f}% (insufficient data)"

        if ratio < target:
            if not self._maker_ratio_warning_shown:
                logger.warning(
                    f"Maker ratio {ratio*100:.0f}% below target {target*100:.0f}%. "
                    f"Consider using more post-only orders."
                )
                self._maker_ratio_warning_shown = True
            return False, f"Maker ratio {ratio*100:.0f}% below target {target*100:.0f}%"

        self._maker_ratio_warning_shown = False
        return True, f"Maker ratio: {ratio*100:.0f}%"

    def update_unhedged_exposure(self, exposure_usd: float):
        """
        Update current unhedged exposure from partial fills.

        Args:
            exposure_usd: Current net unhedged exposure in USD.
        """
        self._current_unhedged_exposure = exposure_usd

    def add_unhedged_exposure(self, amount_usd: float):
        """
        Add to unhedged exposure (from partial fill).

        Args:
            amount_usd: Amount to add to exposure.
        """
        self._current_unhedged_exposure += amount_usd
        logger.debug(f"Unhedged exposure now: ${self._current_unhedged_exposure:.2f}")

    def reduce_unhedged_exposure(self, amount_usd: float):
        """
        Reduce unhedged exposure (from successful hedge).

        Args:
            amount_usd: Amount to reduce from exposure.
        """
        self._current_unhedged_exposure = max(0, self._current_unhedged_exposure - amount_usd)
        logger.debug(f"Unhedged exposure reduced to: ${self._current_unhedged_exposure:.2f}")

    def check_unhedged_exposure_ok(self) -> tuple[bool, str]:
        """
        Check if unhedged exposure is below maximum threshold.

        Per audit: Pause new arbs if exposure from partial fills exceeds limit.

        Returns:
            Tuple of (ok, reason).
        """
        max_exposure = self.config.trading.max_unhedged_exposure_usd

        if self._current_unhedged_exposure > max_exposure:
            return False, (
                f"Unhedged exposure ${self._current_unhedged_exposure:.2f} "
                f"exceeds max ${max_exposure:.2f}. Hedge positions before new arbs."
            )

        return True, f"Unhedged exposure: ${self._current_unhedged_exposure:.2f}"

    def get_unhedged_exposure(self) -> float:
        """Get current unhedged exposure in USD."""
        return self._current_unhedged_exposure

    # =========================================================================
    # TRASH MODE: Volume Farming (Per Grok Round 8)
    # =========================================================================

    def is_trash_mode_enabled(self) -> bool:
        """
        Per Grok Round 8: Check if trash mode is enabled.

        Trash mode is for volume farming - buying near-certain losers (<$0.03)
        purely for volume metrics (airdrop speculation).

        Returns:
            True if TRASH_MODE=true in env.
        """
        return self._trash_mode_enabled

    def is_trash_trade_candidate(self, price: float, is_near_resolution: bool = False) -> bool:
        """
        Per Grok Round 8: Check if a price qualifies for trash volume trading.

        Trash trades are:
        - Priced at <$0.03 (near-certain losers)
        - Near resolution (within 24h) - price won't recover

        Args:
            price: The ask price for the outcome.
            is_near_resolution: True if market is <24h from resolution.

        Returns:
            True if this qualifies as a trash trade candidate.
        """
        if not self._trash_mode_enabled:
            return False

        if price > self._trash_mode_max_price:
            return False

        # Prefer markets near resolution (price won't recover)
        # But allow any <$0.03 if trash mode is on
        return True

    def get_trash_trade_size(self, price: float, available_depth_usd: float) -> float:
        """
        Per Grok Round 8: Calculate size for a trash volume trade.

        Trash trades are capped at a small fixed amount to minimize losses
        while maximizing volume for airdrop farming.

        Args:
            price: The ask price for the outcome.
            available_depth_usd: Available depth at this price.

        Returns:
            Trade size in USD (0 if not a valid trash trade).
        """
        if not self.is_trash_trade_candidate(price):
            return 0.0

        # Cap at trash mode max size
        size = min(self._trash_mode_max_size_usd, available_depth_usd)

        # Minimum viable size ($1)
        if size < 1.0:
            return 0.0

        return size

    def record_trash_trade(self, size_usd: float, price: float):
        """
        Per Grok Round 8: Record a trash volume trade.

        Args:
            size_usd: Trade size in USD.
            price: Price paid per share.
        """
        self._trash_mode_trades += 1
        self._trash_mode_volume_usd += size_usd
        logger.info(
            f"TRASH TRADE #{self._trash_mode_trades}: ${size_usd:.2f} @ ${price:.4f} "
            f"(total trash volume: ${self._trash_mode_volume_usd:.2f})"
        )

    def get_trash_mode_stats(self) -> Dict[str, Any]:
        """
        Per Grok Round 8/9: Get trash mode statistics including airdrop projection.

        Returns:
            Dict with trash mode stats.
        """
        # Per Grok Round 9: Calculate projected airdrop equity
        # Formula: trash_volume * (allocation_per_1k / 1000)
        # Example: $10k volume * ($0.03 / $1k) = $0.30 projected airdrop
        projected_airdrop_equity = (
            self._trash_mode_volume_usd * self._airdrop_allocation_per_1k_volume / 1000
        )

        return {
            "trash_mode_enabled": self._trash_mode_enabled,
            "trash_mode_trades": self._trash_mode_trades,
            "trash_mode_volume_usd": self._trash_mode_volume_usd,
            "trash_mode_max_price": self._trash_mode_max_price,
            "trash_mode_max_size_usd": self._trash_mode_max_size_usd,
            # Per Grok Round 9: Speculative airdrop projection
            # HIGHLY SPECULATIVE - rumored $0.01-0.05 per $1k volume
            "projected_airdrop_equity_usd": projected_airdrop_equity,
            "airdrop_allocation_per_1k_volume": self._airdrop_allocation_per_1k_volume,
        }

    def check_position_age_risk(self, positions: List[Position]) -> List[Position]:
        """
        Check for positions approaching resolution that may need early exit.

        Per Grok audit: UMA disputes can lock funds. Positions nearing resolution
        should be flagged for potential early exit to avoid dispute risk.

        Args:
            positions: List of current open positions.

        Returns:
            List of positions that are at risk (>24h old or near resolution).
        """
        at_risk = []
        now = datetime.now(timezone.utc)

        for pos in positions:
            # Check if position is old (>24 hours) - higher dispute risk
            if hasattr(pos, 'opened_at') and pos.opened_at:
                age_hours = (now - pos.opened_at).total_seconds() / 3600
                if age_hours > 24:
                    logger.warning(
                        f"Position {pos.token_id[:16]}... is {age_hours:.1f}h old - "
                        f"consider exiting to avoid resolution/dispute risk"
                    )
                    at_risk.append(pos)
                    continue

            # Check if market is near resolution (if end_date available)
            if hasattr(pos, 'market_end_date') and pos.market_end_date:
                hours_to_end = (pos.market_end_date - now).total_seconds() / 3600
                if 0 < hours_to_end < 6:  # Within 6 hours of resolution
                    logger.warning(
                        f"Position {pos.token_id[:16]}... is {hours_to_end:.1f}h from resolution - "
                        f"exit immediately to avoid UMA dispute lock"
                    )
                    at_risk.append(pos)

        return at_risk

    def should_auto_hedge_aged_positions(self, positions: List[Position]) -> List[Position]:
        """
        Get positions that should be auto-hedged due to age risk.

        Per Grok Round 3: Instead of just flagging, provide actionable list
        for auto-hedge. Positions >24h old or <6h from resolution should be
        automatically hedged to avoid UMA dispute lock.

        Args:
            positions: List of current open positions.

        Returns:
            List of positions that should be auto-hedged immediately.
        """
        at_risk = self.check_position_age_risk(positions)

        # Filter to only positions with significant value (avoid gas waste on dust)
        min_value_to_hedge = 5.0  # $5 minimum to bother hedging
        actionable = []

        for pos in at_risk:
            pos_value = getattr(pos, 'current_value_usd', 0) or getattr(pos, 'size', 0)
            if pos_value >= min_value_to_hedge:
                actionable.append(pos)
                logger.info(
                    f"AUTO-HEDGE TRIGGERED: Position {pos.token_id[:16]}... "
                    f"value ${pos_value:.2f} queued for hedge"
                )

        if actionable:
            logger.warning(
                f"Per Grok Round 3: {len(actionable)} positions flagged for auto-hedge "
                f"due to age/resolution risk"
            )

        return actionable

    def is_halted(self) -> tuple[bool, Optional[str]]:
        """Check if trading is halted."""
        return self._is_halted, self._halt_reason

    def is_force_hedge_active(self) -> bool:
        """Check if force hedge was triggered."""
        return self._force_hedge_triggered

    def get_stats(self) -> Dict[str, Any]:
        """Get risk management statistics."""
        metrics = self.get_metrics()
        stats = {
            "initial_capital_usd": self._initial_capital,
            "current_capital_usd": self._current_capital,
            "peak_capital_usd": self._peak_capital,
            "daily_pnl_usd": metrics.daily_pnl_usd,
            "daily_trades": metrics.daily_trades,
            "total_trades": len(self._trade_history),
            "consecutive_losses": metrics.consecutive_losses,
            "consecutive_partials": self._consecutive_partials,
            "win_rate_pct": metrics.win_rate * 100,
            "max_drawdown_pct": metrics.max_drawdown_pct,
            "unrealized_pnl_usd": self._unrealized_pnl,
            "unrealized_drawdown_pct": self.get_unrealized_drawdown_pct(),
            "force_hedge_triggered": self._force_hedge_triggered,
            "geometric_mean_return_pct": metrics.geometric_mean_return * 100,
            "total_return_pct": metrics.total_return_pct,
            "projected_30d_multiple": self.estimate_compound_growth(20, 30),
            "is_halted": self._is_halted,
            "halt_reason": self._halt_reason,
            # New audit metrics
            "maker_fills": self._maker_fills,
            "taker_fills": self._taker_fills,
            "maker_ratio_pct": self.get_maker_ratio() * 100,
            "unhedged_exposure_usd": self._current_unhedged_exposure,
        }
        # Per Grok Round 8: Include trash mode stats
        stats.update(self.get_trash_mode_stats())
        return stats


class DepthValidator:
    """Validates orderbook depth to prevent partial fills and rugs."""

    def __init__(self, config: BotConfig):
        """
        Initialize the depth validator.

        Args:
            config: Bot configuration.
        """
        self.config = config

    def validate_opportunity(self, opportunity: ArbOpportunity) -> tuple[bool, str]:
        """
        Validate that an opportunity has sufficient depth.

        Args:
            opportunity: The opportunity to validate.

        Returns:
            Tuple of (valid, reason).
        """
        min_depth = self.config.trading.min_depth_usd
        orderbooks = opportunity.orderbooks

        # Check each outcome has sufficient depth
        for ob in orderbooks.all_orderbooks:
            if opportunity.arb_type == ArbType.BUY_ARB:
                if not ob.best_ask:
                    return False, f"No ask for {ob.outcome_name}"

                depth_usd = ob.best_ask_size * ob.best_ask_price
                if depth_usd < min_depth:
                    return False, f"Insufficient ask depth for {ob.outcome_name}: ${depth_usd:.2f}"

            else:  # SELL_ARB
                if not ob.best_bid:
                    return False, f"No bid for {ob.outcome_name}"

                depth_usd = ob.best_bid_size * ob.best_bid_price
                if depth_usd < min_depth:
                    return False, f"Insufficient bid depth for {ob.outcome_name}: ${depth_usd:.2f}"

        # Check spread is reasonable (not a stale/manipulated book)
        for ob in orderbooks.all_orderbooks:
            if ob.best_bid and ob.best_ask:
                spread = ob.best_ask_price - ob.best_bid_price
                if spread > 0.20:  # 20% spread is suspicious
                    return False, f"Suspicious spread for {ob.outcome_name}: {spread*100:.1f}%"

        return True, "Depth validated"

    def get_safe_trade_size(self, opportunity: ArbOpportunity) -> float:
        """
        Calculate the safe trade size based on available depth.

        Args:
            opportunity: The opportunity.

        Returns:
            Safe trade size in USD.
        """
        min_depth = float('inf')

        for ob in opportunity.orderbooks.all_orderbooks:
            if opportunity.arb_type == ArbType.BUY_ARB:
                if ob.best_ask:
                    depth = ob.best_ask_size * ob.best_ask_price
                    min_depth = min(min_depth, depth)
            else:
                if ob.best_bid:
                    depth = ob.best_bid_size * ob.best_bid_price
                    min_depth = min(min_depth, depth)

        if min_depth == float('inf'):
            return 0

        # Apply safety multiplier
        return min_depth * self.config.trading.depth_safety_multiplier
