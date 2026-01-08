"""
Volatility Harvester - RN1 Strategy Implementation.

This module implements the TRUE RN1 strategy: Volatility Harvesting.

STRATEGY MATH:
==============
In binary markets: YES + NO = $1.00 guaranteed

RN1 doesn't wait for instant arb (sum_asks < 1).
Instead, they ACCUMULATE both sides during live game volatility:

1. DETECT: One side becomes cheap (ask < threshold)
2. BUY: That side immediately (no need for both sides to be cheap)
3. WAIT: For the other side to become cheap
4. BUY: The other side when it's cheap
5. PROFIT: Combined cost < $1.00 = guaranteed profit

Example from live RN1 data (Atalanta game):
- Bought YES at avg $0.70 (193 trades)
- Bought NO at avg $0.28 (172 trades)
- Combined: $0.98 (2% edge)
- Matched: 1,527 shares = $30.54 guaranteed profit

The key insight: You don't race against HFT bots.
You accumulate over minutes/hours during live game volatility.
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from config import BotConfig
from market_discovery import Market, Outcome
from position_tracker import PositionTracker, get_position_tracker, MarketPosition

logger = logging.getLogger(__name__)


@dataclass
class PriceHistory:
    """Track price history for an outcome to detect good entry points."""
    token_id: str
    outcome_name: str
    prices: List[Tuple[datetime, float]] = field(default_factory=list)
    max_history_minutes: int = 30

    @property
    def avg_price(self) -> float:
        """Average price over history."""
        if not self.prices:
            return 0.5
        return sum(p[1] for p in self.prices) / len(self.prices)

    @property
    def min_price(self) -> float:
        """Minimum price seen."""
        if not self.prices:
            return 1.0
        return min(p[1] for p in self.prices)

    @property
    def max_price(self) -> float:
        """Maximum price seen."""
        if not self.prices:
            return 0.0
        return max(p[1] for p in self.prices)

    @property
    def volatility(self) -> float:
        """Price volatility (max - min)."""
        return self.max_price - self.min_price

    def add_price(self, price: float):
        """Add a price observation."""
        now = datetime.now(timezone.utc)
        self.prices.append((now, price))

        # Cleanup old prices
        cutoff = now - timedelta(minutes=self.max_history_minutes)
        self.prices = [(t, p) for t, p in self.prices if t > cutoff]

    def is_cheap(self, current_price: float, threshold_pct: float = 0.10) -> bool:
        """
        Check if current price is cheap relative to history.

        Cheap = below average OR near historical minimum.
        """
        if not self.prices or len(self.prices) < 5:
            # Not enough history, use absolute threshold
            return current_price < 0.45

        # Below average by threshold
        if current_price < self.avg_price * (1 - threshold_pct):
            return True

        # Near historical minimum (within 5%)
        if current_price <= self.min_price * 1.05:
            return True

        return False


@dataclass
class HarvestOpportunity:
    """A single-leg buying opportunity for volatility harvesting."""
    market: Market
    token_id: str
    outcome_name: str
    price: float
    size_usd: float
    reason: str
    combined_price: float  # What combined avg would be after this trade
    current_edge: float    # Current edge if we have existing position
    projected_edge: float  # Projected edge after this trade
    priority: float = 0.0  # Higher = more urgent


class VolatilityHarvester:
    """
    Implements RN1's volatility harvesting strategy.

    Key differences from instant arb:
    - Buys ONE SIDE at a time
    - No atomic requirement
    - Accumulates over time
    - Uses price history to detect "cheap"
    - Targets live games with volatility
    """

    def __init__(self, config: BotConfig, position_tracker: Optional[PositionTracker] = None):
        self.config = config
        self.position_tracker = position_tracker or get_position_tracker()

        # Price history per token
        self.price_history: Dict[str, PriceHistory] = {}

        # Configuration
        self.min_edge_to_start = 0.02        # 2% edge to start new position
        self.min_edge_to_add = 0.01          # 1% improvement to add to position
        self.max_combined_price = 0.98       # Never pay more than 98c combined
        self.min_trade_size_usd = 5.0        # Polymarket minimum
        self.max_trade_size_usd = 200.0      # Per-trade cap
        self.rebalance_threshold = 1.5       # Rebalance if ratio > 1.5x

        # Stats
        self.opportunities_found = 0
        self.trades_executed = 0
        self.total_volume = 0.0

    def update_price(self, token_id: str, outcome_name: str, price: float):
        """Update price history for an outcome."""
        if token_id not in self.price_history:
            self.price_history[token_id] = PriceHistory(
                token_id=token_id,
                outcome_name=outcome_name
            )
        self.price_history[token_id].add_price(price)

    def detect_harvest_opportunities(
        self,
        market: Market,
        orderbooks: Dict[str, dict]  # token_id -> {best_ask, best_bid, outcome_name, ...}
    ) -> List[HarvestOpportunity]:
        """
        Detect volatility harvesting opportunities.

        RN1 STRATEGY - THE REAL APPROACH:
        ==================================
        During live games, prices swing wildly. RN1 buys BOTH sides over time:
        - When YES dips → buy YES
        - When NO dips → buy NO
        - Goal: combined average < $1.00

        Example from RN1 data (Counter-Strike):
        - 252 trades over 40 minutes
        - Bought CSDIILIT at avg $0.59
        - Bought Washington at avg $0.30
        - Combined: $0.89 = 11% edge = $1,298 profit

        The key insight: We DON'T wait for instant arb.
        We accumulate BOTH sides when each becomes cheap.

        Args:
            market: The market to analyze
            orderbooks: Orderbook data for each outcome

        Returns:
            List of HarvestOpportunity for single-leg trades
        """
        opportunities = []

        if not market.is_binary or len(orderbooks) != 2:
            # Focus on binary markets (RN1's main arena)
            return opportunities

        # Calculate current combined ask price
        combined_ask = sum(ob.get('best_ask', 1.0) for ob in orderbooks.values())

        # DEBUG: Log markets with potential (combined < 0.99)
        if combined_ask < 0.99:
            logger.info(
                f"[HARVEST SCAN] {market.question[:40]}... | Combined: ${combined_ask:.4f} | "
                f"Edge: {(1 - combined_ask) * 100:.2f}%"
            )

        # Update price history for volatility tracking
        for token_id, ob in orderbooks.items():
            ask_price = ob.get('best_ask', 1.0)
            outcome_name = ob.get('outcome_name', 'Unknown')
            self.update_price(token_id, outcome_name, ask_price)

        # Get existing position if any
        position = self.position_tracker.get_position(market.condition_id)

        if position is None:
            # === NO POSITION: Start accumulating BOTH sides ===
            # RN1 STRATEGY: Buy EITHER side when it dips below threshold
            # We don't need combined < $1.00 to start - we accumulate over time

            for token_id, ob in orderbooks.items():
                ask_price = ob.get('best_ask', 1.0)
                outcome_name = ob.get('outcome_name', 'Unknown')
                other_ask = [ob2.get('best_ask', 1.0) for tid, ob2 in orderbooks.items() if tid != token_id][0]
                projected_combined = ask_price + other_ask
                projected_edge = max(0, 1.0 - projected_combined)

                # ONLY TRADE WHEN COMBINED < $1.00 (INSTANT PROFIT)
                # Wait for volatility to create the opportunity, then execute immediately
                #
                # RN1 Strategy: During live games, prices swing. When combined asks < $1.00:
                # - Buy BOTH sides instantly
                # - Lock in guaranteed profit
                #
                # Example: YES=$0.45, NO=$0.52 → Combined=$0.97 → 3% edge → EXECUTE!

                # Skip if no profit opportunity
                if projected_combined >= 1.00:
                    continue  # No profit - wait for prices to swing

                # Skip near-resolved markets (no volatility)
                if ask_price <= 0.05 or ask_price >= 0.95:
                    continue
                if other_ask <= 0.05 or other_ask >= 0.95:
                    continue

                # PROFIT OPPORTUNITY FOUND! Combined < $1.00
                edge_percent = (1.0 - projected_combined) * 100

                # Size based on edge - bigger edge = bigger size
                size_usd = min(
                    self.max_trade_size_usd,
                    max(self.min_trade_size_usd, edge_percent * 50)  # $50 per 1% edge
                )

                opportunities.append(HarvestOpportunity(
                    market=market,
                    token_id=token_id,
                    outcome_name=outcome_name,
                    price=ask_price,
                    size_usd=size_usd,
                    reason=f"PROFIT: ${ask_price:.2f}+${other_ask:.2f}=${projected_combined:.2f} edge={edge_percent:.1f}%",
                    combined_price=projected_combined,
                    current_edge=0.0,
                    projected_edge=projected_edge,
                    priority=edge_percent * 100  # Higher edge = higher priority
                ))
                self.opportunities_found += 1
        else:
            # === EXISTING POSITION: Complete the pair ONLY if profitable ===
            # If we own one side, buy the other ONLY when combined < $1.00
            current_combined = position.combined_avg_price
            current_edge = position.edge

            for token_id, ob in orderbooks.items():
                ask_price = ob.get('best_ask', 1.0)
                outcome_name = ob.get('outcome_name', 'Unknown')
                other_ask = [ob2.get('best_ask', 1.0) for tid, ob2 in orderbooks.items() if tid != token_id][0]

                outcome_pos = position.get_outcome_position(token_id)

                # Skip near-resolved markets
                if ask_price <= 0.05 or ask_price >= 0.95:
                    continue
                if other_ask <= 0.05 or other_ask >= 0.95:
                    continue

                # === COMPLETE PAIR: Buy the side we DON'T have yet ===
                if outcome_pos is None:
                    existing_outcomes = list(position.outcomes.values())
                    if existing_outcomes:
                        existing_avg = existing_outcomes[0].avg_price
                        new_combined = existing_avg + ask_price
                        new_edge = 1.0 - new_combined

                        # ONLY buy if it creates profit (combined < $1.00)
                        if new_combined < 1.00:
                            edge_percent = new_edge * 100
                            size_usd = min(
                                self.max_trade_size_usd,
                                max(self.min_trade_size_usd, edge_percent * 50)
                            )
                            # Match existing position size for balanced pair
                            target_shares = existing_outcomes[0].shares
                            size_usd = min(size_usd, target_shares * ask_price)
                            size_usd = max(self.min_trade_size_usd, size_usd)

                            opportunities.append(HarvestOpportunity(
                                market=market,
                                token_id=token_id,
                                outcome_name=outcome_name,
                                price=ask_price,
                                size_usd=size_usd,
                                reason=f"COMPLETE PAIR: {existing_outcomes[0].outcome_name}@${existing_avg:.2f}+{outcome_name}@${ask_price:.2f}=${new_combined:.2f} edge={edge_percent:.1f}%",
                                combined_price=new_combined,
                                current_edge=current_edge,
                                projected_edge=new_edge,
                                priority=edge_percent * 100
                            ))
                            self.opportunities_found += 1

        # Sort by priority (highest first)
        opportunities.sort(key=lambda x: -x.priority)

        return opportunities

    def get_best_opportunity(
        self,
        markets: List[Market],
        all_orderbooks: Dict[str, Dict[str, dict]]  # condition_id -> {token_id -> orderbook}
    ) -> Optional[HarvestOpportunity]:
        """
        Get the single best opportunity across all markets.

        This is called in the main loop to find what to trade next.
        """
        all_opportunities = []

        for market in markets:
            if market.condition_id in all_orderbooks:
                orderbooks = all_orderbooks[market.condition_id]
                opportunities = self.detect_harvest_opportunities(market, orderbooks)
                all_opportunities.extend(opportunities)

        if not all_opportunities:
            return None

        # Return highest priority
        return max(all_opportunities, key=lambda x: x.priority)

    def record_trade(self, opportunity: HarvestOpportunity, shares_filled: float, fill_price: float):
        """Record a completed trade."""
        self.trades_executed += 1
        self.total_volume += shares_filled * fill_price

        # Record in position tracker
        self.position_tracker.record_fill(
            condition_id=opportunity.market.condition_id,
            market_question=opportunity.market.question,
            token_id=opportunity.token_id,
            outcome_name=opportunity.outcome_name,
            shares=shares_filled,
            price=fill_price
        )

        logger.info(
            f"HARVEST TRADE: {opportunity.outcome_name} | "
            f"{shares_filled:.2f} shares @ ${fill_price:.4f} | "
            f"Reason: {opportunity.reason}"
        )

    def get_stats(self) -> dict:
        """Get harvester statistics."""
        position_stats = self.position_tracker.get_stats()
        return {
            "opportunities_found": self.opportunities_found,
            "trades_executed": self.trades_executed,
            "total_volume": self.total_volume,
            "price_histories_tracked": len(self.price_history),
            "positions": position_stats
        }

    def log_status(self):
        """Log current status."""
        stats = self.get_stats()
        logger.info(
            f"HARVESTER: {stats['opportunities_found']} opportunities, "
            f"{stats['trades_executed']} trades, ${stats['total_volume']:.2f} volume"
        )
        self.position_tracker.log_status()


# Global instance
_harvester: Optional[VolatilityHarvester] = None


def get_volatility_harvester(config: Optional[BotConfig] = None) -> VolatilityHarvester:
    """Get or create global volatility harvester."""
    global _harvester
    if _harvester is None:
        if config is None:
            from config import BotConfig
            config = BotConfig()
        _harvester = VolatilityHarvester(config)
    return _harvester
