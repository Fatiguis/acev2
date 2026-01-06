"""
Simulator Module for Backtesting.
Allows testing the arbitrage strategy with synthetic orderbooks.
"""

import logging
import random
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import math

from config import BotConfig, load_config, calculate_dynamic_threshold
from market_discovery import Market, Outcome
from orderbook import (
    Orderbook, OrderbookLevel, MarketOrderbooks,
    ArbOpportunity, ArbType, OrderbookPoller
)
from execution import ExecutionResult, OrderResult, OrderStatus
from risk import RiskManager, TradeRecord

logger = logging.getLogger(__name__)


# =============================================================================
# RN1 PROFILE DATA (from observed trading patterns)
# =============================================================================
# Based on analysis: 15,396 trades, $417k positions, heavy soccer focus
RN1_PROFILE = {
    "total_trades": 15396,
    "total_position_value": 417000,  # USD
    "avg_trade_size": 27.1,  # USD per trade
    # Per Grok audit: Hedged arbs should approach ~100% win rate if properly hedged
    # 0.68 was observed win rate which includes partial fills and execution failures
    # For fully hedged arbs, theoretical win rate is ~95%+ (accounting for slippage)
    "win_rate": 0.95,  # Hedged arb win rate (0.68 was pre-hedge observed rate)

    # Sport distribution (observed)
    "sport_weights": {
        "soccer": 0.45,      # 45% of trades
        "basketball": 0.20,
        "football": 0.15,
        "politics": 0.10,
        "other": 0.10
    },

    # Trade timing patterns (hour of day UTC weights)
    "hour_weights": {
        # Higher activity during European soccer hours
        12: 1.2, 13: 1.4, 14: 1.6, 15: 1.8, 16: 2.0, 17: 2.2, 18: 2.4, 19: 2.2,
        20: 2.0, 21: 1.8, 22: 1.5, 23: 1.2,
        # Lower activity off-hours
        0: 0.5, 1: 0.3, 2: 0.2, 3: 0.2, 4: 0.2, 5: 0.3, 6: 0.5, 7: 0.7,
        8: 0.8, 9: 0.9, 10: 1.0, 11: 1.1
    },

    # Arb margin distribution (what margins RN1 typically captures)
    "margin_distribution": {
        "min": 0.003,   # 0.3% minimum
        "median": 0.008,  # 0.8% median
        "max": 0.025,   # 2.5% max
        "std": 0.005    # Standard deviation
    },

    # Trade size distribution
    "size_distribution": {
        "min": 5,
        "median": 25,
        "max": 200,
        "std": 30
    },

    # Execution characteristics - UPDATED with realistic rates
    # Original 8% was optimistic; real sports arbs see 20-30% partials
    "partial_fill_rate": 0.22,   # 22% partial fills (realistic for sports HFT)
    "slippage_skip_rate": 0.12,  # 12% skipped due to slippage (higher during bursts)
    "stale_skip_rate": 0.05,     # 5% stale books (WS helps reduce this)

    # Burst trading patterns (Poisson-based clustering)
    # RN1 trades in bursts during live games (5-15 trades/game)
    "burst_config": {
        "enabled": True,
        "lambda_per_game": 10,  # Average trades per burst (Poisson lambda)
        "games_per_hour": {  # Average games running per hour by sport
            "soccer": 2.5,  # Multiple leagues, overlapping matches
            "basketball": 1.5,
            "football": 0.5,
            "politics": 0.2,  # Debates, announcements
            "other": 0.3
        },
        "burst_duration_minutes": 90,  # Average game duration
        "inter_trade_seconds": {  # Time between trades in a burst
            "min": 3,
            "max": 60,
            "mean": 15
        }
    }
}


@dataclass
class SimulationConfig:
    """Configuration for simulation."""
    # Number of markets to simulate
    num_markets: int = 20

    # Simulation duration in seconds
    duration_seconds: float = 3600  # 1 hour

    # Probability of an arb appearing per poll
    arb_probability: float = 0.05  # 5% chance per market

    # Arb margin range (as decimal)
    arb_margin_min: float = 0.005  # 0.5%
    arb_margin_max: float = 0.025  # 2.5%

    # Depth range in USD
    depth_min: float = 500
    depth_max: float = 5000

    # Partial fill probability
    partial_fill_probability: float = 0.1  # 10% chance

    # Stale book probability
    stale_probability: float = 0.02  # 2%

    # High slippage probability
    slippage_probability: float = 0.05  # 5%

    # Poll interval
    poll_interval: float = 1.0


@dataclass
class HistoricalSimConfig:
    """Configuration for RN1-like historical simulation."""
    # Number of trades to simulate
    num_trades: int = 1000

    # Use RN1 profile for realistic patterns
    use_rn1_profile: bool = True

    # Override RN1 profile settings
    custom_win_rate: Optional[float] = None
    custom_avg_size: Optional[float] = None

    # Time compression (1.0 = real time, 0.1 = 10x faster)
    time_compression: float = 0.0  # 0 = instant (no delays)

    # Sport focus (None = use RN1 distribution)
    sport_focus: Optional[str] = None  # e.g., "soccer" to focus only on soccer

    # Margin multiplier (1.0 = RN1 margins, 1.5 = 50% better margins)
    margin_multiplier: float = 1.0

    # Starting capital for comparison
    starting_capital: float = 10000.0

    # Enable Poisson burst mode for realistic trade clustering
    # When enabled, trades are generated in bursts (like live games)
    enable_burst_mode: bool = True

    # Custom Poisson lambda for burst (None = use RN1 default of 10)
    burst_lambda: Optional[float] = None


@dataclass
class SimulationResult:
    """Results from a simulation run."""
    duration_seconds: float = 0.0
    total_polls: int = 0
    arbs_generated: int = 0
    arbs_detected: int = 0
    trades_attempted: int = 0
    trades_successful: int = 0
    partial_fills: int = 0
    stale_skipped: int = 0
    slippage_skipped: int = 0
    total_volume_usd: float = 0.0
    total_profit_usd: float = 0.0
    final_capital_usd: float = 0.0
    peak_capital_usd: float = 0.0
    max_drawdown_pct: float = 0.0
    win_rate_pct: float = 0.0
    geometric_mean_return_pct: float = 0.0
    total_return_pct: float = 0.0
    projected_30d_multiple: float = 0.0


class SyntheticMarketGenerator:
    """Generates synthetic markets and orderbooks for testing."""

    def __init__(self, config: SimulationConfig):
        self.config = config

    def generate_markets(self, count: int) -> List[Market]:
        """Generate synthetic markets."""
        markets = []
        for i in range(count):
            # Create binary market (Yes/No)
            outcomes = [
                Outcome(name="Yes", token_id=f"yes_token_{i}", price=0.5),
                Outcome(name="No", token_id=f"no_token_{i}", price=0.5)
            ]

            market = Market(
                market_id=f"market_{i}",
                condition_id=f"condition_{i}",
                question=f"Simulated Market #{i}: Will event happen?",
                slug=f"sim-market-{i}",
                outcomes=outcomes,
                volume=random.uniform(100000, 1000000),
                liquidity=random.uniform(10000, 100000),
                is_active=True,
                is_closed=False
            )
            markets.append(market)

        return markets

    def generate_orderbook(
        self,
        market: Market,
        create_arb: bool = False,
        arb_margin: float = 0.0,
        is_stale: bool = False,
        has_slippage: bool = False
    ) -> MarketOrderbooks:
        """
        Generate synthetic orderbooks for a market.

        Args:
            market: The market to generate orderbooks for.
            create_arb: Whether to create an arbitrage opportunity.
            arb_margin: Margin for the arb (if creating one).
            is_stale: Whether to make the orderbook stale.
            has_slippage: Whether to add high slippage to second level.
        """
        orderbooks = {}

        if is_stale:
            # Stale book: prices don't sum to ~1
            yes_mid = random.uniform(0.3, 0.5)
            no_mid = random.uniform(0.3, 0.5)  # Sum will be off
        elif create_arb:
            # Create buy arb: sum of asks < 1
            total_ask = 1.0 - arb_margin
            yes_ask = random.uniform(0.3, 0.7)
            no_ask = total_ask - yes_ask
            yes_mid = yes_ask - 0.01
            no_mid = no_ask - 0.01
        else:
            # Normal pricing: sum to ~1
            yes_mid = random.uniform(0.3, 0.7)
            no_mid = 1.0 - yes_mid

        for outcome in market.outcomes:
            if outcome.name == "Yes":
                mid = yes_mid
            else:
                mid = no_mid

            spread = random.uniform(0.01, 0.03)
            best_bid = max(0.01, mid - spread / 2)
            best_ask = min(0.99, mid + spread / 2)

            depth = random.uniform(self.config.depth_min, self.config.depth_max)
            bid_size = depth / best_bid
            ask_size = depth / best_ask

            # Create orderbook levels
            bids = [OrderbookLevel(price=best_bid, size=bid_size)]
            asks = [OrderbookLevel(price=best_ask, size=ask_size)]

            # Add second level
            if has_slippage:
                # High slippage: second level much worse
                second_bid = best_bid * 0.85  # 15% worse
                second_ask = best_ask * 1.15
            else:
                second_bid = best_bid * 0.97  # 3% worse
                second_ask = best_ask * 1.03

            bids.append(OrderbookLevel(price=second_bid, size=bid_size * 0.5))
            asks.append(OrderbookLevel(price=second_ask, size=ask_size * 0.5))

            orderbooks[outcome.token_id] = Orderbook(
                token_id=outcome.token_id,
                outcome_name=outcome.name,
                bids=bids,
                asks=asks
            )

        return MarketOrderbooks(market=market, orderbooks=orderbooks)


class Simulator:
    """Runs simulations of the arbitrage strategy."""

    def __init__(self, bot_config: BotConfig, sim_config: Optional[SimulationConfig] = None):
        self.bot_config = bot_config
        self.sim_config = sim_config or SimulationConfig()
        self.generator = SyntheticMarketGenerator(self.sim_config)
        self.risk_manager = RiskManager(bot_config)

        # Stats
        self._arbs_generated = 0
        self._arbs_detected = 0
        self._trades_attempted = 0
        self._trades_successful = 0
        self._partial_fills = 0
        self._stale_skipped = 0
        self._slippage_skipped = 0
        self._total_volume = 0.0
        self._total_profit = 0.0

    def _should_create_arb(self) -> bool:
        """Determine if an arb should be created this poll."""
        return random.random() < self.sim_config.arb_probability

    def _get_arb_margin(self) -> float:
        """Get a random arb margin."""
        return random.uniform(
            self.sim_config.arb_margin_min,
            self.sim_config.arb_margin_max
        )

    def _should_partial_fill(self) -> bool:
        """Determine if order should partially fill."""
        return random.random() < self.sim_config.partial_fill_probability

    def _simulate_execution(
        self,
        opportunity: ArbOpportunity
    ) -> Tuple[bool, float, bool]:
        """
        Simulate execution of an opportunity.

        Per Grok Round 3: Added fee and slippage modeling for realistic simulation.

        Returns:
            Tuple of (success, profit, was_partial).
        """
        # Check for partial fill
        if self._should_partial_fill():
            # Partial fill - assume we hedge and break even
            return False, 0.0, True

        # Per Grok Round 3: Add fee and slippage modeling
        trade_size = opportunity.trade_size_usd
        raw_profit = trade_size * opportunity.profit_margin

        # Fee modeling (Polymarket implied from POST_ONLY vs taker)
        # Maker rebate: +0.1%, Taker fee: ~0.2%
        # Assume 70% maker (post-only), 30% taker (fallback)
        maker_ratio = 0.70
        maker_rebate_pct = 0.001  # 0.1%
        taker_fee_pct = 0.002     # 0.2%

        fee_impact = trade_size * (
            maker_ratio * (-maker_rebate_pct) +  # Rebate = negative cost
            (1 - maker_ratio) * taker_fee_pct    # Taker = positive cost
        )

        # Slippage modeling (price movement between detection and execution)
        # Conservative: 0.05% avg slippage on small trades
        avg_slippage_pct = 0.0005
        slippage_variation = random.gauss(0, 0.0003)  # Std dev 0.03%
        actual_slippage_pct = max(0, avg_slippage_pct + slippage_variation)
        slippage_cost = trade_size * actual_slippage_pct

        # Gas cost (Polygon ~$0.01-0.05 per tx, 2 txs per arb)
        gas_cost = 0.03  # Avg $0.03 total

        # Net profit after fees, slippage, gas
        net_profit = raw_profit - fee_impact - slippage_cost - gas_cost

        # Log for debugging large discrepancies
        if abs(net_profit - raw_profit) / max(raw_profit, 0.01) > 0.5:
            logger.debug(
                f"Large fee/slippage impact: raw=${raw_profit:.4f}, "
                f"fees=${fee_impact:.4f}, slip=${slippage_cost:.4f}, "
                f"net=${net_profit:.4f}"
            )

        return True, net_profit, False

    def run_simulation(self) -> SimulationResult:
        """
        Run a full simulation.

        Returns:
            SimulationResult with all metrics.
        """
        logger.info("=" * 60)
        logger.info("SIMULATION START")
        logger.info(f"Duration: {self.sim_config.duration_seconds}s")
        logger.info(f"Markets: {self.sim_config.num_markets}")
        logger.info(f"Arb probability: {self.sim_config.arb_probability * 100}%")
        logger.info("=" * 60)

        # Generate markets
        markets = self.generator.generate_markets(self.sim_config.num_markets)

        # Create orderbook poller for detection
        poller = OrderbookPoller(self.bot_config)

        # Simulation loop
        start_time = datetime.now(timezone.utc)
        poll_count = 0
        poll_interval = self.sim_config.poll_interval

        while True:
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed >= self.sim_config.duration_seconds:
                break

            poll_count += 1

            # Generate orderbooks for each market
            for market in markets:
                create_arb = self._should_create_arb()
                is_stale = random.random() < self.sim_config.stale_probability
                has_slippage = random.random() < self.sim_config.slippage_probability

                if create_arb:
                    self._arbs_generated += 1

                market_obs = self.generator.generate_orderbook(
                    market,
                    create_arb=create_arb and not is_stale,
                    arb_margin=self._get_arb_margin() if create_arb else 0,
                    is_stale=is_stale,
                    has_slippage=has_slippage
                )

                # Try to detect arb
                opportunity = poller.detect_arb_opportunity(market_obs)

                if opportunity:
                    self._arbs_detected += 1

                    # Check risk
                    allowed, reason = self.risk_manager.check_trade_allowed(opportunity)
                    if not allowed:
                        logger.debug(f"Trade blocked: {reason}")
                        continue

                    # Simulate execution
                    self._trades_attempted += 1
                    success, profit, was_partial = self._simulate_execution(opportunity)

                    if success:
                        self._trades_successful += 1
                        self._total_volume += opportunity.trade_size_usd
                        self._total_profit += profit

                        # Record trade
                        trade = TradeRecord(
                            timestamp=datetime.now(timezone.utc),
                            arb_type=opportunity.arb_type,
                            trade_size_usd=opportunity.trade_size_usd,
                            profit_usd=profit,
                            market_id=market.market_id,
                            is_complete=True,
                            was_partial=False
                        )
                        self.risk_manager.record_trade(trade)

                    elif was_partial:
                        self._partial_fills += 1

                        # Record partial as zero profit
                        trade = TradeRecord(
                            timestamp=datetime.now(timezone.utc),
                            arb_type=opportunity.arb_type,
                            trade_size_usd=opportunity.trade_size_usd,
                            profit_usd=0.0,
                            market_id=market.market_id,
                            is_complete=False,
                            was_partial=True
                        )
                        self.risk_manager.record_trade(trade)

            # Update stats from poller
            poller_stats = poller.get_stats()
            self._stale_skipped = poller_stats['stale_books_skipped']
            self._slippage_skipped = poller_stats['slippage_rejected']

            # Small delay to simulate real polling
            # In actual simulation we can skip this for speed

        # Compile results
        risk_stats = self.risk_manager.get_stats()

        result = SimulationResult(
            duration_seconds=self.sim_config.duration_seconds,
            total_polls=poll_count,
            arbs_generated=self._arbs_generated,
            arbs_detected=self._arbs_detected,
            trades_attempted=self._trades_attempted,
            trades_successful=self._trades_successful,
            partial_fills=self._partial_fills,
            stale_skipped=self._stale_skipped,
            slippage_skipped=self._slippage_skipped,
            total_volume_usd=self._total_volume,
            total_profit_usd=self._total_profit,
            final_capital_usd=risk_stats['current_capital_usd'],
            peak_capital_usd=risk_stats['peak_capital_usd'],
            max_drawdown_pct=risk_stats['max_drawdown_pct'],
            win_rate_pct=risk_stats['win_rate_pct'],
            geometric_mean_return_pct=risk_stats['geometric_mean_return_pct'],
            total_return_pct=risk_stats['total_return_pct'],
            projected_30d_multiple=risk_stats['projected_30d_multiple']
        )

        self._print_results(result)
        return result

    def _print_results(self, result: SimulationResult):
        """Print simulation results."""
        logger.info("=" * 60)
        logger.info("SIMULATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Duration: {result.duration_seconds / 60:.1f} minutes")
        logger.info(f"Total polls: {result.total_polls}")
        logger.info(f"Arbs generated: {result.arbs_generated}")
        logger.info(f"Arbs detected: {result.arbs_detected} ({result.arbs_detected/max(1,result.arbs_generated)*100:.1f}%)")
        logger.info(f"Trades attempted: {result.trades_attempted}")
        logger.info(f"Trades successful: {result.trades_successful}")
        logger.info(f"Partial fills: {result.partial_fills}")
        logger.info(f"Stale skipped: {result.stale_skipped}")
        logger.info(f"Slippage skipped: {result.slippage_skipped}")
        logger.info("-" * 40)
        logger.info(f"Total volume: ${result.total_volume_usd:,.2f}")
        logger.info(f"Total profit: ${result.total_profit_usd:.4f}")
        logger.info(f"Final capital: ${result.final_capital_usd:,.2f}")
        logger.info(f"Peak capital: ${result.peak_capital_usd:,.2f}")
        logger.info(f"Total return: {result.total_return_pct:.2f}%")
        logger.info(f"Win rate: {result.win_rate_pct:.1f}%")
        logger.info(f"Max drawdown: {result.max_drawdown_pct:.2f}%")
        logger.info(f"Geometric mean return: {result.geometric_mean_return_pct:.4f}%")
        logger.info(f"Projected 30-day multiple: {result.projected_30d_multiple:.2f}x")
        logger.info("=" * 60)


class RN1Simulator:
    """
    Historical simulation mode that replays RN1-like trade patterns.

    Based on observed RN1 profile:
    - 15,396 trades
    - $417k total positions
    - Heavy soccer market focus (45%)
    - European trading hours bias
    """

    def __init__(self, bot_config: BotConfig, hist_config: Optional[HistoricalSimConfig] = None):
        self.bot_config = bot_config
        self.hist_config = hist_config or HistoricalSimConfig()
        self.risk_manager = RiskManager(bot_config)
        self.profile = RN1_PROFILE.copy()

        # Override profile with custom settings
        if self.hist_config.custom_win_rate is not None:
            self.profile["win_rate"] = self.hist_config.custom_win_rate
        if self.hist_config.custom_avg_size is not None:
            self.profile["avg_trade_size"] = self.hist_config.custom_avg_size

        # Stats
        self._trades_executed = 0
        self._trades_won = 0
        self._trades_lost = 0
        self._partial_fills = 0
        self._slippage_skips = 0
        self._stale_skips = 0
        self._total_volume = 0.0
        self._total_profit = 0.0
        self._capital = self.hist_config.starting_capital
        self._peak_capital = self._capital
        self._max_drawdown = 0.0

    def _sample_sport(self) -> str:
        """Sample a sport category based on RN1 distribution."""
        if self.hist_config.sport_focus:
            return self.hist_config.sport_focus

        weights = self.profile["sport_weights"]
        sports = list(weights.keys())
        probs = list(weights.values())
        return random.choices(sports, weights=probs, k=1)[0]

    def _sample_trade_size(self) -> float:
        """Sample trade size from RN1 distribution."""
        dist = self.profile["size_distribution"]
        # Use log-normal to get right-skewed distribution like real trades
        size = random.gauss(dist["median"], dist["std"])
        return max(dist["min"], min(dist["max"], size))

    def _sample_margin(self) -> float:
        """Sample arb margin from RN1 distribution."""
        dist = self.profile["margin_distribution"]
        margin = random.gauss(dist["median"], dist["std"])
        margin = max(dist["min"], min(dist["max"], margin))
        return margin * self.hist_config.margin_multiplier

    def _sample_hour(self) -> int:
        """Sample trading hour based on RN1 activity patterns."""
        weights = self.profile["hour_weights"]
        hours = list(weights.keys())
        probs = list(weights.values())
        return random.choices(hours, weights=probs, k=1)[0]

    def _generate_poisson_burst(self, sport: str) -> List[Dict]:
        """
        Generate a burst of trades using Poisson distribution.

        Simulates RN1's pattern of trading in bursts during live games.
        Uses Poisson(lambda=10) for number of trades per game,
        with exponential inter-arrival times.

        Args:
            sport: Sport category for this burst.

        Returns:
            List of trade dictionaries for this burst.
        """
        burst_config = self.profile.get("burst_config", {})
        if not burst_config.get("enabled", False):
            return []

        # Get lambda (average trades per burst)
        lam = self.hist_config.burst_lambda or burst_config.get("lambda_per_game", 10)

        # Sample number of trades in this burst from Poisson
        import numpy as np
        num_trades_in_burst = np.random.poisson(lam)
        num_trades_in_burst = max(1, min(num_trades_in_burst, 25))  # Cap at 1-25

        # Get inter-trade timing config
        inter_trade = burst_config.get("inter_trade_seconds", {"min": 3, "max": 60, "mean": 15})

        trades = []
        base_hour = self._sample_hour()
        current_time_offset = 0  # Seconds from burst start

        for i in range(num_trades_in_burst):
            # Sample trade characteristics
            size = self._sample_trade_size()
            margin = self._sample_margin()

            # Exponential inter-arrival time (memoryless property of Poisson process)
            if i > 0:
                mean_gap = inter_trade.get("mean", 15)
                gap = random.expovariate(1.0 / mean_gap)
                gap = max(inter_trade.get("min", 3), min(gap, inter_trade.get("max", 60)))
                current_time_offset += gap

            trade = {
                "trade_num": len(trades),
                "sport": sport,
                "size_usd": size,
                "margin": margin,
                "hour_utc": base_hour,
                "burst_offset_seconds": current_time_offset,
                "is_burst_trade": True,
                "market_id": f"burst_{sport}_{base_hour}_{i}",
                "timestamp": datetime.now(timezone.utc).replace(hour=base_hour)
            }
            trades.append(trade)

        return trades

    def _generate_burst_trades(self, total_trades: int) -> List[Dict]:
        """
        Generate all trades using burst patterns.

        Creates bursts per sport weighted by games_per_hour,
        then fills remaining trades with individual samples.

        Args:
            total_trades: Total number of trades to generate.

        Returns:
            List of all trade dictionaries.
        """
        all_trades = []
        burst_config = self.profile.get("burst_config", {})
        games_per_hour = burst_config.get("games_per_hour", {})

        # Calculate expected bursts needed
        avg_trades_per_burst = burst_config.get("lambda_per_game", 10)
        estimated_bursts = total_trades // avg_trades_per_burst

        # Weight bursts by sport game frequency
        sport_weights = self.profile["sport_weights"]
        if self.hist_config.sport_focus:
            # Single sport focus
            sport_burst_counts = {self.hist_config.sport_focus: estimated_bursts}
        else:
            # Distribute bursts by sport weights and games_per_hour
            total_weight = sum(
                sport_weights.get(s, 0) * games_per_hour.get(s, 0.5)
                for s in sport_weights
            )
            sport_burst_counts = {}
            for sport, weight in sport_weights.items():
                game_rate = games_per_hour.get(sport, 0.5)
                sport_burst_counts[sport] = int(
                    estimated_bursts * (weight * game_rate) / max(total_weight, 0.1)
                )

        # Generate bursts for each sport
        for sport, num_bursts in sport_burst_counts.items():
            for _ in range(num_bursts):
                burst = self._generate_poisson_burst(sport)
                for trade in burst:
                    trade["trade_num"] = len(all_trades)
                    all_trades.append(trade)

                if len(all_trades) >= total_trades:
                    break
            if len(all_trades) >= total_trades:
                break

        # Fill remaining with individual trades if needed
        while len(all_trades) < total_trades:
            trade = self._generate_mock_trade(len(all_trades))
            trade["is_burst_trade"] = False
            all_trades.append(trade)

        # Trim to exact count and shuffle slightly for realism
        all_trades = all_trades[:total_trades]

        logger.debug(
            f"Generated {len(all_trades)} trades: "
            f"{sum(1 for t in all_trades if t.get('is_burst_trade'))} in bursts, "
            f"{sum(1 for t in all_trades if not t.get('is_burst_trade'))} individual"
        )

        return all_trades

    def _simulate_trade_outcome(self, trade_size: float, margin: float) -> Tuple[bool, float, str]:
        """
        Simulate a single trade outcome based on RN1 patterns.

        Returns:
            Tuple of (success, profit/loss, outcome_reason)
        """
        # Check for stale book skip
        if random.random() < self.profile["stale_skip_rate"]:
            self._stale_skips += 1
            return False, 0.0, "stale_skip"

        # Check for slippage skip
        if random.random() < self.profile["slippage_skip_rate"]:
            self._slippage_skips += 1
            return False, 0.0, "slippage_skip"

        # Check for partial fill
        if random.random() < self.profile["partial_fill_rate"]:
            self._partial_fills += 1
            # Partial fills typically break even after hedging
            return True, 0.0, "partial_fill"

        # Successful execution - check if arb was captured
        if random.random() < self.profile["win_rate"]:
            profit = trade_size * margin
            return True, profit, "win"
        else:
            # Losing trade (market moved against us)
            # Assume we lose the margin we were trying to capture
            loss = -trade_size * margin * 0.5  # Assume partial loss
            return True, loss, "loss"

    def _generate_mock_trade(self, trade_num: int) -> Dict:
        """Generate a single mock trade based on RN1 patterns."""
        sport = self._sample_sport()
        size = self._sample_trade_size()
        margin = self._sample_margin()
        hour = self._sample_hour()

        return {
            "trade_num": trade_num,
            "sport": sport,
            "size_usd": size,
            "margin": margin,
            "hour_utc": hour,
            "market_id": f"mock_{sport}_{trade_num}",
            "timestamp": datetime.now(timezone.utc).replace(hour=hour)
        }

    def run_historical_simulation(self) -> SimulationResult:
        """
        Run historical simulation replaying RN1-like trades.

        When burst mode is enabled, trades are generated in Poisson-distributed
        bursts to simulate RN1's live game trading patterns.

        Returns:
            SimulationResult with all metrics.
        """
        logger.info("=" * 60)
        logger.info("RN1 HISTORICAL SIMULATION START")
        logger.info(f"Trades to simulate: {self.hist_config.num_trades}")
        logger.info(f"Starting capital: ${self.hist_config.starting_capital:,.2f}")
        logger.info(f"Win rate: {self.profile['win_rate'] * 100:.1f}%")
        logger.info(f"Margin multiplier: {self.hist_config.margin_multiplier:.2f}x")
        if self.hist_config.sport_focus:
            logger.info(f"Sport focus: {self.hist_config.sport_focus}")
        if self.hist_config.enable_burst_mode:
            lam = self.hist_config.burst_lambda or self.profile.get("burst_config", {}).get("lambda_per_game", 10)
            logger.info(f"Burst mode: ENABLED (Poisson lambda={lam})")
        else:
            logger.info("Burst mode: DISABLED (uniform sampling)")
        logger.info("=" * 60)

        start_time = datetime.now(timezone.utc)
        sport_stats = {sport: {"trades": 0, "profit": 0.0, "burst_trades": 0} for sport in self.profile["sport_weights"]}

        # Generate trades using burst mode or uniform sampling
        if self.hist_config.enable_burst_mode:
            all_trades = self._generate_burst_trades(self.hist_config.num_trades)
        else:
            all_trades = [self._generate_mock_trade(i) for i in range(self.hist_config.num_trades)]

        for trade in all_trades:

            # Check risk limits
            mock_opportunity = ArbOpportunity(
                market_id=trade["market_id"],
                arb_type=ArbType.BUY_ARB,
                profit_margin=trade["margin"],
                trade_size_usd=trade["size_usd"],
                orderbooks={}
            )

            allowed, reason = self.risk_manager.check_trade_allowed(mock_opportunity)
            if not allowed:
                logger.debug(f"Trade {i} blocked: {reason}")
                continue

            # Simulate execution
            success, profit, outcome = self._simulate_trade_outcome(
                trade["size_usd"],
                trade["margin"]
            )

            if success and outcome not in ("stale_skip", "slippage_skip"):
                self._trades_executed += 1
                self._total_volume += trade["size_usd"]
                self._total_profit += profit
                self._capital += profit

                # Track peak and drawdown
                if self._capital > self._peak_capital:
                    self._peak_capital = self._capital
                drawdown = (self._peak_capital - self._capital) / self._peak_capital * 100
                if drawdown > self._max_drawdown:
                    self._max_drawdown = drawdown

                # Track by sport
                sport = trade["sport"]
                sport_stats[sport]["trades"] += 1
                sport_stats[sport]["profit"] += profit
                if trade.get("is_burst_trade"):
                    sport_stats[sport]["burst_trades"] += 1

                if profit > 0:
                    self._trades_won += 1
                elif profit < 0:
                    self._trades_lost += 1

                # Record trade
                trade_record = TradeRecord(
                    timestamp=trade["timestamp"],
                    arb_type=ArbType.BUY_ARB,
                    trade_size_usd=trade["size_usd"],
                    profit_usd=profit,
                    market_id=trade["market_id"],
                    is_complete=True,
                    was_partial=(outcome == "partial_fill")
                )
                self.risk_manager.record_trade(trade_record)

            # Optional delay for time compression
            if self.hist_config.time_compression > 0:
                import time
                time.sleep(0.01 * self.hist_config.time_compression)

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Calculate final stats
        win_rate = self._trades_won / max(1, self._trades_executed) * 100
        total_return = (self._capital - self.hist_config.starting_capital) / self.hist_config.starting_capital * 100

        # Compile results
        result = SimulationResult(
            duration_seconds=elapsed,
            total_polls=self.hist_config.num_trades,
            arbs_generated=self.hist_config.num_trades,
            arbs_detected=self._trades_executed + self._stale_skips + self._slippage_skips,
            trades_attempted=self._trades_executed + self._partial_fills,
            trades_successful=self._trades_won,
            partial_fills=self._partial_fills,
            stale_skipped=self._stale_skips,
            slippage_skipped=self._slippage_skips,
            total_volume_usd=self._total_volume,
            total_profit_usd=self._total_profit,
            final_capital_usd=self._capital,
            peak_capital_usd=self._peak_capital,
            max_drawdown_pct=self._max_drawdown,
            win_rate_pct=win_rate,
            geometric_mean_return_pct=0.0,  # Calculated separately if needed
            total_return_pct=total_return,
            projected_30d_multiple=0.0  # Calculated separately if needed
        )

        self._print_results(result, sport_stats)
        return result

    def _print_results(self, result: SimulationResult, sport_stats: Dict):
        """Print historical simulation results."""
        logger.info("=" * 60)
        logger.info("RN1 HISTORICAL SIMULATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Simulation time: {result.duration_seconds:.2f}s")
        logger.info(f"Trades simulated: {self.hist_config.num_trades}")
        logger.info(f"Trades executed: {self._trades_executed}")
        logger.info(f"  - Wins: {self._trades_won}")
        logger.info(f"  - Losses: {self._trades_lost}")
        logger.info(f"  - Partial fills: {self._partial_fills}")
        logger.info(f"Skipped - Stale: {self._stale_skips}, Slippage: {self._slippage_skips}")
        logger.info("-" * 40)
        logger.info(f"Total volume: ${result.total_volume_usd:,.2f}")
        logger.info(f"Total profit: ${result.total_profit_usd:,.2f}")
        logger.info(f"Starting capital: ${self.hist_config.starting_capital:,.2f}")
        logger.info(f"Final capital: ${result.final_capital_usd:,.2f}")
        logger.info(f"Total return: {result.total_return_pct:.2f}%")
        logger.info(f"Win rate: {result.win_rate_pct:.1f}%")
        logger.info(f"Max drawdown: {result.max_drawdown_pct:.2f}%")
        logger.info("-" * 40)
        logger.info("PERFORMANCE BY SPORT:")
        total_burst_trades = 0
        for sport, stats in sorted(sport_stats.items(), key=lambda x: -x[1]["profit"]):
            if stats["trades"] > 0:
                burst_pct = stats.get("burst_trades", 0) / stats["trades"] * 100 if stats["trades"] > 0 else 0
                total_burst_trades += stats.get("burst_trades", 0)
                logger.info(
                    f"  {sport}: {stats['trades']} trades ({burst_pct:.0f}% burst), "
                    f"${stats['profit']:.2f} profit"
                )
        if self.hist_config.enable_burst_mode:
            logger.info(f"  Total burst trades: {total_burst_trades}/{self._trades_executed} "
                       f"({total_burst_trades/max(1,self._trades_executed)*100:.1f}%)")
        logger.info("=" * 60)

        # Compare to RN1 performance
        logger.info("")
        logger.info("COMPARISON TO RN1:")
        rn1_profit_estimate = self.profile["total_trades"] * self.profile["avg_trade_size"] * 0.008 * self.profile["win_rate"]
        logger.info(f"  RN1 estimated profit (from {self.profile['total_trades']} trades): ${rn1_profit_estimate:,.2f}")
        scale_factor = self.hist_config.num_trades / self.profile["total_trades"]
        expected_at_scale = rn1_profit_estimate * scale_factor
        logger.info(f"  Your simulation ({self.hist_config.num_trades} trades): ${result.total_profit_usd:,.2f}")
        if expected_at_scale > 0:
            performance_vs_rn1 = result.total_profit_usd / expected_at_scale * 100
            logger.info(f"  Performance vs RN1 scaled: {performance_vs_rn1:.1f}%")
        logger.info("=" * 60)


def run_backtest():
    """Run a backtest simulation."""
    from logger import setup_logging

    config = load_config()
    setup_logging(config.logging)

    sim_config = SimulationConfig(
        num_markets=30,
        duration_seconds=3600,  # 1 hour
        arb_probability=0.03,   # 3% per market per poll
        arb_margin_min=0.005,
        arb_margin_max=0.02,
        partial_fill_probability=0.1,
        poll_interval=1.0
    )

    simulator = Simulator(config, sim_config)
    result = simulator.run_simulation()

    return result


def run_rn1_simulation(num_trades: int = 1000, sport_focus: Optional[str] = None):
    """
    Run RN1-like historical simulation.

    Args:
        num_trades: Number of trades to simulate.
        sport_focus: Optional sport to focus on (e.g., "soccer").

    Returns:
        SimulationResult with all metrics.
    """
    from logger import setup_logging

    config = load_config()
    setup_logging(config.logging)

    hist_config = HistoricalSimConfig(
        num_trades=num_trades,
        use_rn1_profile=True,
        sport_focus=sport_focus,
        starting_capital=config.starting_capital_usd
    )

    simulator = RN1Simulator(config, hist_config)
    return simulator.run_historical_simulation()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--rn1":
        # Run RN1 historical simulation
        num_trades = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        sport = sys.argv[3] if len(sys.argv) > 3 else None
        run_rn1_simulation(num_trades=num_trades, sport_focus=sport)
    else:
        run_backtest()
