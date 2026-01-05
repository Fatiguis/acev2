"""
Latency-Adjusted Edge Expectancy Model.

Provides mathematical models for:
1. Fill probability as function of latency (exponential decay)
2. Expected profit accounting for partial fills and front-running
3. Optimal sizing given latency and competition
4. rn1-style volume capture estimation

Key formula:
E[profit] = edge% × P(fill|latency) × P(not_frontrun) × size - fees

Where:
- P(fill|latency) ≈ e^(-λt), λ = arb decay rate (~5-10 for hot markets)
- P(not_frontrun) = f(depth_rank, competitor_count)
"""

import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class MarketHeat(Enum):
    """Market activity level affecting arb decay rate."""
    COLD = "cold"      # Low volume, slow decay (λ ≈ 1)
    WARM = "warm"      # Medium volume (λ ≈ 3)
    HOT = "hot"        # High volume, fast decay (λ ≈ 7)
    BLAZING = "blazing"  # Live sports, news events (λ ≈ 15)


@dataclass
class EdgeModelConfig:
    """Configuration for edge expectancy model."""
    # Arb decay rates by market heat (λ in e^(-λt))
    decay_rate_cold: float = 1.0      # Cold markets: 63% fill prob at 1s latency
    decay_rate_warm: float = 3.0      # Warm markets: 5% fill prob at 1s latency
    decay_rate_hot: float = 7.0       # Hot markets: 0.1% fill prob at 1s latency
    decay_rate_blazing: float = 15.0  # Blazing: essentially 0% at 1s

    # Front-running probability factors
    # Base probability of being front-run at each depth rank
    frontrun_base_prob: float = 0.3   # 30% base chance of being front-run
    frontrun_per_competitor: float = 0.05  # +5% per estimated competitor

    # Competitor estimation
    estimated_competitors_cold: int = 2
    estimated_competitors_warm: int = 5
    estimated_competitors_hot: int = 10
    estimated_competitors_blazing: int = 20

    # Volume capture thresholds (vs rn1-tier bots)
    rn1_typical_size_usd: float = 10000.0  # rn1 average trade size
    rn1_latency_ms: float = 50.0           # rn1 estimated latency
    our_latency_ms_http: float = 800.0     # Our HTTP polling latency
    our_latency_ms_ws: float = 80.0        # Our WebSocket latency

    # Fee structure (Polymarket)
    maker_rebate_pct: float = 0.001  # 0.1% maker rebate
    taker_fee_pct: float = 0.002     # 0.2% taker fee
    gas_cost_usd: float = 0.01       # Average gas per tx


@dataclass
class EdgeEstimate:
    """Estimated edge and fill probability for an opportunity."""
    raw_edge_pct: float           # Raw arbitrage edge
    fill_probability: float       # P(fill|latency)
    frontrun_probability: float   # P(being front-run)
    expected_edge_pct: float      # After accounting for fill prob
    expected_profit_usd: float    # E[profit] for given size
    optimal_size_usd: float       # Size that maximizes E[profit]
    latency_ms: float             # Latency used in calculation
    market_heat: MarketHeat       # Market activity level
    capture_rate_vs_rn1: float    # Our capture rate vs rn1-tier bots


class EdgeExpectancyModel:
    """
    Mathematical model for edge expectancy with latency adjustment.

    Based on the observation that arb opportunities decay exponentially:
    - The probability of filling an arb decreases exponentially with latency
    - Faster bots (like rn1) capture opportunities before slower bots
    - Expected profit must account for fill probability and front-running

    Model:
    P(fill|t) = e^(-λt) where:
    - t = latency in seconds
    - λ = decay rate (higher for hotter markets)

    E[profit] = edge × P(fill|t) × (1 - P(frontrun)) × size - fees
    """

    def __init__(self, config: Optional[EdgeModelConfig] = None):
        self.config = config or EdgeModelConfig()

        # Track historical fill rates for calibration
        self._fill_attempts = 0
        self._successful_fills = 0
        self._fill_history: List[Tuple[float, float, bool]] = []  # (latency, edge, filled)

    def get_decay_rate(self, heat: MarketHeat) -> float:
        """Get arb decay rate for market heat level."""
        rates = {
            MarketHeat.COLD: self.config.decay_rate_cold,
            MarketHeat.WARM: self.config.decay_rate_warm,
            MarketHeat.HOT: self.config.decay_rate_hot,
            MarketHeat.BLAZING: self.config.decay_rate_blazing,
        }
        return rates.get(heat, self.config.decay_rate_warm)

    def estimate_market_heat(
        self,
        volume_24h: float,
        is_live_event: bool = False,
        recent_price_volatility: float = 0.0
    ) -> MarketHeat:
        """
        Estimate market heat based on observables.

        Args:
            volume_24h: 24-hour trading volume in USD.
            is_live_event: True if this is a live event (sports game in progress).
            recent_price_volatility: Price change in last hour.

        Returns:
            Estimated MarketHeat level.
        """
        # Live events are always blazing hot
        if is_live_event:
            return MarketHeat.BLAZING

        # High volatility indicates news event
        if recent_price_volatility > 0.10:  # >10% move
            return MarketHeat.BLAZING
        elif recent_price_volatility > 0.05:  # >5% move
            return MarketHeat.HOT

        # Volume-based classification
        if volume_24h > 500000:  # >$500k
            return MarketHeat.HOT
        elif volume_24h > 100000:  # >$100k
            return MarketHeat.WARM
        else:
            return MarketHeat.COLD

    def calculate_fill_probability(
        self,
        latency_seconds: float,
        heat: MarketHeat
    ) -> float:
        """
        Calculate probability of filling an arb given latency.

        Uses exponential decay model: P(fill) = e^(-λt)

        Args:
            latency_seconds: Total latency (detection + execution) in seconds.
            heat: Market heat level.

        Returns:
            Fill probability between 0 and 1.
        """
        decay_rate = self.get_decay_rate(heat)
        return math.exp(-decay_rate * latency_seconds)

    def calculate_frontrun_probability(
        self,
        heat: MarketHeat,
        our_depth_rank: int = 1
    ) -> float:
        """
        Calculate probability of being front-run by faster bots.

        Args:
            heat: Market heat level (determines competitor count).
            our_depth_rank: Our position in the queue (1 = first).

        Returns:
            Probability of being front-run.
        """
        competitors = {
            MarketHeat.COLD: self.config.estimated_competitors_cold,
            MarketHeat.WARM: self.config.estimated_competitors_warm,
            MarketHeat.HOT: self.config.estimated_competitors_hot,
            MarketHeat.BLAZING: self.config.estimated_competitors_blazing,
        }[heat]

        # Each competitor ahead of us has a chance to take the arb
        # Probability increases with more competitors
        prob = self.config.frontrun_base_prob
        prob += self.config.frontrun_per_competitor * competitors
        prob *= (our_depth_rank - 1) / max(competitors, 1)  # Lower if we're first

        return min(prob, 0.95)  # Cap at 95%

    def calculate_expected_edge(
        self,
        raw_edge_pct: float,
        latency_ms: float,
        heat: MarketHeat,
        use_websocket: bool = True
    ) -> EdgeEstimate:
        """
        Calculate expected edge after accounting for latency and competition.

        Args:
            raw_edge_pct: Raw arbitrage edge (e.g., 0.01 for 1%).
            latency_ms: Our latency in milliseconds.
            heat: Market heat level.
            use_websocket: True if using websocket (affects latency estimate).

        Returns:
            EdgeEstimate with all calculated values.
        """
        # Adjust latency based on data source
        if use_websocket:
            effective_latency_ms = latency_ms + self.config.our_latency_ms_ws
        else:
            effective_latency_ms = latency_ms + self.config.our_latency_ms_http

        latency_seconds = effective_latency_ms / 1000.0

        # Calculate probabilities
        fill_prob = self.calculate_fill_probability(latency_seconds, heat)
        frontrun_prob = self.calculate_frontrun_probability(heat)

        # Expected edge = raw_edge × P(fill) × P(not_frontrun)
        expected_edge = raw_edge_pct * fill_prob * (1 - frontrun_prob)

        # Calculate capture rate vs rn1
        rn1_fill_prob = self.calculate_fill_probability(
            self.config.rn1_latency_ms / 1000.0, heat
        )
        capture_rate = fill_prob / rn1_fill_prob if rn1_fill_prob > 0 else 0

        return EdgeEstimate(
            raw_edge_pct=raw_edge_pct,
            fill_probability=fill_prob,
            frontrun_probability=frontrun_prob,
            expected_edge_pct=expected_edge,
            expected_profit_usd=0.0,  # Calculated with size
            optimal_size_usd=0.0,     # Calculated below
            latency_ms=effective_latency_ms,
            market_heat=heat,
            capture_rate_vs_rn1=capture_rate,
        )

    def calculate_expected_profit(
        self,
        raw_edge_pct: float,
        size_usd: float,
        latency_ms: float,
        heat: MarketHeat,
        use_websocket: bool = True,
        is_maker: bool = False
    ) -> EdgeEstimate:
        """
        Calculate expected profit for a given opportunity and size.

        Full formula:
        E[profit] = edge × P(fill) × (1 - P(frontrun)) × size
                    - taker_fee × size × P(fill)
                    + maker_rebate × size × P(fill)  [if maker]
                    - gas_cost

        Args:
            raw_edge_pct: Raw arbitrage edge.
            size_usd: Trade size in USD.
            latency_ms: Latency in milliseconds.
            heat: Market heat level.
            use_websocket: True if using websocket.
            is_maker: True if placing post-only orders.

        Returns:
            EdgeEstimate with expected profit.
        """
        estimate = self.calculate_expected_edge(
            raw_edge_pct, latency_ms, heat, use_websocket
        )

        # Base expected profit
        base_profit = estimate.expected_edge_pct * size_usd

        # Fees (only charged on filled orders)
        if is_maker:
            # Maker gets rebate
            fee_impact = -self.config.maker_rebate_pct * size_usd * estimate.fill_probability
        else:
            # Taker pays fee
            fee_impact = self.config.taker_fee_pct * size_usd * estimate.fill_probability

        # Gas cost (fixed per transaction attempt)
        gas_cost = self.config.gas_cost_usd

        # Final expected profit
        expected_profit = base_profit - fee_impact - gas_cost

        estimate.expected_profit_usd = expected_profit

        return estimate

    def calculate_optimal_size(
        self,
        raw_edge_pct: float,
        max_size_usd: float,
        available_depth_usd: float,
        latency_ms: float,
        heat: MarketHeat,
        capital_usd: float,
        use_websocket: bool = True
    ) -> EdgeEstimate:
        """
        Calculate optimal trade size that maximizes expected profit.

        Considers:
        - Kelly criterion for sizing based on edge
        - Fill probability decay with size (larger orders harder to fill)
        - Depth constraints
        - Capital constraints

        Args:
            raw_edge_pct: Raw arbitrage edge.
            max_size_usd: Maximum size from depth validator.
            available_depth_usd: Available depth at best price.
            latency_ms: Latency in milliseconds.
            heat: Market heat level.
            capital_usd: Available capital.
            use_websocket: True if using websocket.

        Returns:
            EdgeEstimate with optimal size.
        """
        estimate = self.calculate_expected_edge(
            raw_edge_pct, latency_ms, heat, use_websocket
        )

        # If expected edge is negative or fill probability too low, size = 0
        min_fill_prob = 0.05  # Don't trade if <5% fill probability
        if estimate.expected_edge_pct <= 0 or estimate.fill_probability < min_fill_prob:
            estimate.optimal_size_usd = 0
            estimate.expected_profit_usd = 0
            return estimate

        # Kelly criterion: f* = edge / variance
        # For binary arb, variance ≈ 1, so f* ≈ edge
        # But we use fractional Kelly (0.25-0.5) for safety
        kelly_fraction = 0.25
        kelly_size = capital_usd * estimate.expected_edge_pct * kelly_fraction

        # Size decreases fill probability (larger orders compete more)
        # Scale down for larger sizes relative to depth
        if available_depth_usd > 0:
            depth_ratio = kelly_size / available_depth_usd
            if depth_ratio > 0.5:  # Taking >50% of depth
                # Reduce size to cap at 30% of depth for better fill prob
                kelly_size = min(kelly_size, available_depth_usd * 0.3)

        # Apply constraints
        optimal_size = min(
            kelly_size,
            max_size_usd,
            available_depth_usd * 0.5,  # Never take >50% of depth
            capital_usd * 0.10,  # Never risk >10% of capital
        )

        # Round to reasonable precision
        optimal_size = max(0, round(optimal_size, 2))

        estimate.optimal_size_usd = optimal_size

        # Calculate expected profit at optimal size
        if optimal_size > 0:
            profit_estimate = self.calculate_expected_profit(
                raw_edge_pct, optimal_size, latency_ms, heat, use_websocket
            )
            estimate.expected_profit_usd = profit_estimate.expected_profit_usd

        return estimate

    def estimate_rn1_capture_rate(
        self,
        our_latency_ms: float,
        heat: MarketHeat,
        our_size_usd: float
    ) -> Dict[str, float]:
        """
        Estimate what percentage of rn1-volume opportunities we can capture.

        rn1 characteristics (from public data):
        - Average trade size: $10k-$100k
        - Latency: <100ms (likely 30-50ms)
        - Fill rate: ~80-90% on hot markets
        - Edge captured: 0.5-1.5%

        Args:
            our_latency_ms: Our latency.
            heat: Market heat level.
            our_size_usd: Our trade size.

        Returns:
            Dict with capture rate analysis.
        """
        # rn1 fill probability at their latency
        rn1_latency_s = self.config.rn1_latency_ms / 1000.0
        rn1_fill_prob = self.calculate_fill_probability(rn1_latency_s, heat)

        # Our fill probability
        our_latency_s = our_latency_ms / 1000.0
        our_fill_prob = self.calculate_fill_probability(our_latency_s, heat)

        # Capture rate = our_fill_prob / rn1_fill_prob
        # This represents what fraction of rn1-capturable arbs we can also capture
        if rn1_fill_prob > 0:
            relative_capture = our_fill_prob / rn1_fill_prob
        else:
            relative_capture = 0

        # Size-adjusted capture
        # rn1 takes $10k+, we take $100-$1000
        # We can only capture the "scraps" they leave behind
        size_ratio = our_size_usd / self.config.rn1_typical_size_usd

        # Opportunities rn1 passes on (too small, or partial fills)
        # Estimate ~10-20% of opportunities are "scraps"
        scraps_rate = 0.15

        # Our effective capture of total addressable arbs
        effective_capture = relative_capture * max(size_ratio, scraps_rate)

        return {
            "rn1_fill_prob": rn1_fill_prob,
            "our_fill_prob": our_fill_prob,
            "relative_capture_rate": relative_capture,
            "size_ratio": size_ratio,
            "scraps_available_pct": scraps_rate * 100,
            "effective_capture_pct": effective_capture * 100,
            "projected_daily_arbs": effective_capture * 50,  # Assuming 50 arbs/day total
        }

    def project_roi_timeline(
        self,
        starting_capital: float,
        target_multiple: float,
        avg_edge_pct: float,
        arbs_per_day: float,
        avg_size_pct: float,
        heat: MarketHeat,
        latency_ms: float
    ) -> Dict[str, float]:
        """
        Project time to reach target ROI.

        Args:
            starting_capital: Initial capital.
            target_multiple: Target multiple (e.g., 10 for 10x).
            avg_edge_pct: Average arb edge.
            arbs_per_day: Number of arbs captured per day.
            avg_size_pct: Average size as % of capital.
            heat: Typical market heat.
            latency_ms: Our latency.

        Returns:
            Dict with ROI projections.
        """
        # Calculate expected edge after fill probability
        estimate = self.calculate_expected_edge(avg_edge_pct, latency_ms, heat)

        # Expected profit per arb
        avg_size = starting_capital * avg_size_pct
        profit_per_arb = estimate.expected_edge_pct * avg_size

        # Daily profit
        daily_profit = profit_per_arb * arbs_per_day

        # Daily return %
        daily_return_pct = (daily_profit / starting_capital) * 100

        # Time to target (compound growth)
        if daily_return_pct > 0:
            # Using log: ln(target) / ln(1 + daily_return)
            daily_multiplier = 1 + (daily_return_pct / 100)
            days_to_target = math.log(target_multiple) / math.log(daily_multiplier)
        else:
            days_to_target = float('inf')

        return {
            "starting_capital": starting_capital,
            "target_multiple": target_multiple,
            "target_value": starting_capital * target_multiple,
            "fill_probability": estimate.fill_probability,
            "expected_edge_pct": estimate.expected_edge_pct * 100,
            "profit_per_arb_usd": profit_per_arb,
            "arbs_per_day": arbs_per_day,
            "daily_profit_usd": daily_profit,
            "daily_return_pct": daily_return_pct,
            "days_to_target": days_to_target,
            "months_to_target": days_to_target / 30 if days_to_target < float('inf') else float('inf'),
            "annualized_return_pct": daily_return_pct * 365,
        }

    def record_fill_attempt(self, latency_ms: float, edge_pct: float, filled: bool):
        """Record a fill attempt for model calibration."""
        self._fill_attempts += 1
        if filled:
            self._successful_fills += 1
        self._fill_history.append((latency_ms, edge_pct, filled))

        # Keep only last 1000 samples
        if len(self._fill_history) > 1000:
            self._fill_history = self._fill_history[-1000:]

    def get_calibration_stats(self) -> Dict[str, float]:
        """Get calibration statistics from recorded fills."""
        if not self._fill_history:
            return {
                "total_attempts": 0,
                "successful_fills": 0,
                "observed_fill_rate": 0,
                "avg_latency_ms": 0,
                "avg_edge_pct": 0,
            }

        successful = [f for f in self._fill_history if f[2]]
        failed = [f for f in self._fill_history if not f[2]]

        return {
            "total_attempts": len(self._fill_history),
            "successful_fills": len(successful),
            "observed_fill_rate": len(successful) / len(self._fill_history),
            "avg_latency_ms_success": sum(f[0] for f in successful) / len(successful) if successful else 0,
            "avg_latency_ms_fail": sum(f[0] for f in failed) / len(failed) if failed else 0,
            "avg_edge_pct_success": sum(f[1] for f in successful) / len(successful) * 100 if successful else 0,
            "avg_edge_pct_fail": sum(f[1] for f in failed) / len(failed) * 100 if failed else 0,
        }


def print_analysis(capital: float = 1000.0, latency_http: float = 800.0, latency_ws: float = 80.0):
    """Print analysis comparing HTTP vs WebSocket latency impact."""
    model = EdgeExpectancyModel()

    print("=" * 70)
    print("LATENCY-ADJUSTED EDGE EXPECTANCY ANALYSIS")
    print("=" * 70)
    print(f"\nStarting Capital: ${capital:,.0f}")
    print(f"HTTP Latency: {latency_http}ms | WebSocket Latency: {latency_ws}ms")
    print()

    # Analyze different edge scenarios
    edges = [0.005, 0.01, 0.015, 0.02]  # 0.5%, 1%, 1.5%, 2%
    heats = [MarketHeat.WARM, MarketHeat.HOT, MarketHeat.BLAZING]

    print("Fill Probability by Latency and Market Heat:")
    print("-" * 70)
    print(f"{'Edge':<8} {'Heat':<10} {'HTTP Fill%':<12} {'WS Fill%':<12} {'Improvement':<12}")
    print("-" * 70)

    for edge in edges:
        for heat in heats:
            http_est = model.calculate_expected_edge(edge, latency_http, heat, use_websocket=False)
            ws_est = model.calculate_expected_edge(edge, latency_ws, heat, use_websocket=True)

            improvement = ws_est.fill_probability / http_est.fill_probability if http_est.fill_probability > 0 else float('inf')

            print(
                f"{edge*100:.1f}%     "
                f"{heat.value:<10} "
                f"{http_est.fill_probability*100:>8.1f}%    "
                f"{ws_est.fill_probability*100:>8.1f}%    "
                f"{improvement:>8.1f}x"
            )

    print()
    print("=" * 70)
    print("ROI PROJECTIONS (1% avg edge, 10 arbs/day, 5% avg size)")
    print("=" * 70)

    for heat in heats:
        http_proj = model.project_roi_timeline(
            starting_capital=capital,
            target_multiple=10,
            avg_edge_pct=0.01,
            arbs_per_day=10,
            avg_size_pct=0.05,
            heat=heat,
            latency_ms=latency_http
        )

        ws_proj = model.project_roi_timeline(
            starting_capital=capital,
            target_multiple=10,
            avg_edge_pct=0.01,
            arbs_per_day=10,
            avg_size_pct=0.05,
            heat=heat,
            latency_ms=latency_ws
        )

        print(f"\n{heat.value.upper()} Market:")
        print(f"  HTTP: {http_proj['days_to_target']:.0f} days to 10x | {http_proj['daily_return_pct']:.3f}% daily")
        print(f"  WS:   {ws_proj['days_to_target']:.0f} days to 10x | {ws_proj['daily_return_pct']:.3f}% daily")

    print()
    print("=" * 70)
    print("rn1 CAPTURE RATE ANALYSIS")
    print("=" * 70)

    for heat in heats:
        analysis = model.estimate_rn1_capture_rate(
            our_latency_ms=latency_ws,  # With websocket
            heat=heat,
            our_size_usd=100
        )
        print(f"\n{heat.value.upper()} Market (with WebSocket):")
        print(f"  rn1 Fill Prob: {analysis['rn1_fill_prob']*100:.1f}%")
        print(f"  Our Fill Prob: {analysis['our_fill_prob']*100:.1f}%")
        print(f"  Relative Capture: {analysis['relative_capture_rate']*100:.1f}%")
        print(f"  Effective Capture (after size): {analysis['effective_capture_pct']:.2f}%")
        print(f"  Projected Daily Arbs: {analysis['projected_daily_arbs']:.1f}")


if __name__ == "__main__":
    print_analysis()
