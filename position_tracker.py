"""
Position Tracker for RN1-Style Volatility Harvesting Strategy.

RN1 STRATEGY MATH:
==================
In binary markets: YES + NO = $1.00 guaranteed payout

If you accumulate:
- X shares of YES at average price P_yes
- Y shares of NO at average price P_no

Your guaranteed profit = min(X, Y) * (1 - P_yes_avg - P_no_avg)

RN1 doesn't wait for instant arb (sum_asks < 1).
Instead, they ACCUMULATE both sides during live game volatility,
achieving a good combined average over many trades.

Example from Counter-Strike match:
- Bought CSDIILIT at prices ranging $0.33 to $0.89, avg $0.59
- Bought Washington at prices ranging $0.05 to $0.75, avg $0.30
- Combined average: $0.89 (11% edge!)
- 252 trades over 40 minutes
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class OutcomePosition:
    """Position in a single outcome."""
    token_id: str
    outcome_name: str
    shares: float = 0.0
    total_cost: float = 0.0  # Total USD spent
    num_trades: int = 0
    first_trade_time: Optional[datetime] = None
    last_trade_time: Optional[datetime] = None

    @property
    def avg_price(self) -> float:
        """Average price per share."""
        if self.shares > 0:
            return self.total_cost / self.shares
        return 0.0

    def add_fill(self, shares: float, price: float):
        """Record a fill."""
        cost = shares * price
        self.shares += shares
        self.total_cost += cost
        self.num_trades += 1
        now = datetime.now(timezone.utc)
        if self.first_trade_time is None:
            self.first_trade_time = now
        self.last_trade_time = now

    def to_dict(self) -> dict:
        return {
            "token_id": self.token_id,
            "outcome_name": self.outcome_name,
            "shares": self.shares,
            "total_cost": self.total_cost,
            "avg_price": self.avg_price,
            "num_trades": self.num_trades
        }


@dataclass
class MarketPosition:
    """
    Position across all outcomes in a market.

    For binary markets: tracks YES and NO positions.
    Calculates matched pairs and guaranteed profit.
    """
    condition_id: str
    market_question: str
    outcomes: Dict[str, OutcomePosition] = field(default_factory=dict)  # token_id -> position
    target_shares: float = 0.0  # Target shares per side (0 = no target)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def total_invested(self) -> float:
        """Total USD invested across all outcomes."""
        return sum(p.total_cost for p in self.outcomes.values())

    @property
    def total_shares(self) -> float:
        """Total shares across all outcomes."""
        return sum(p.shares for p in self.outcomes.values())

    @property
    def matched_shares(self) -> float:
        """Number of matched share pairs (min across outcomes)."""
        if not self.outcomes:
            return 0.0
        shares_list = [p.shares for p in self.outcomes.values()]
        return min(shares_list) if shares_list else 0.0

    @property
    def combined_avg_price(self) -> float:
        """Sum of average prices across outcomes."""
        return sum(p.avg_price for p in self.outcomes.values())

    @property
    def edge(self) -> float:
        """Current edge as decimal (1 - combined_avg_price)."""
        return 1.0 - self.combined_avg_price

    @property
    def guaranteed_profit(self) -> float:
        """
        Guaranteed profit from matched pairs.

        Formula: matched_shares * $1 - total_cost_for_matched
        Or equivalently: matched_shares * edge
        """
        if self.matched_shares <= 0:
            return 0.0
        # Guaranteed payout for matched shares
        payout = self.matched_shares  # $1 per matched pair
        # Cost for matched shares (proportional)
        cost_for_matched = self.matched_shares * self.combined_avg_price
        return payout - cost_for_matched

    @property
    def unmatched_exposure(self) -> Tuple[str, float, float]:
        """
        Get unmatched exposure.

        Returns: (outcome_name, excess_shares, excess_cost)
        """
        if not self.outcomes or len(self.outcomes) < 2:
            return ("", 0.0, 0.0)

        positions = list(self.outcomes.values())
        max_pos = max(positions, key=lambda p: p.shares)
        min_shares = self.matched_shares

        excess_shares = max_pos.shares - min_shares
        excess_cost = excess_shares * max_pos.avg_price

        return (max_pos.outcome_name, excess_shares, excess_cost)

    @property
    def is_balanced(self) -> bool:
        """Check if position is roughly balanced (within 10%)."""
        if len(self.outcomes) < 2:
            return False
        shares_list = [p.shares for p in self.outcomes.values()]
        if min(shares_list) == 0:
            return False
        ratio = max(shares_list) / min(shares_list)
        return ratio <= 1.1

    @property
    def num_trades(self) -> int:
        """Total number of trades across outcomes."""
        return sum(p.num_trades for p in self.outcomes.values())

    def get_outcome_position(self, token_id: str) -> Optional[OutcomePosition]:
        """Get position for specific outcome."""
        return self.outcomes.get(token_id)

    def add_fill(self, token_id: str, outcome_name: str, shares: float, price: float):
        """Record a fill for an outcome."""
        if token_id not in self.outcomes:
            self.outcomes[token_id] = OutcomePosition(
                token_id=token_id,
                outcome_name=outcome_name
            )
        self.outcomes[token_id].add_fill(shares, price)

    def should_buy_more(self, token_id: str, current_price: float) -> Tuple[bool, float, str]:
        """
        Determine if we should buy more of this outcome.

        RN1 Strategy:
        1. If we have no position, buy if combined price < threshold
        2. If we have position, buy if it improves our average
        3. Never exceed target shares

        Returns: (should_buy, suggested_size_usd, reason)
        """
        if token_id not in self.outcomes:
            # New position - check if price is good
            combined_current = current_price + sum(
                p.avg_price for tid, p in self.outcomes.items() if tid != token_id
            )
            if combined_current < 0.98:  # 2% edge threshold
                size = min(100.0, (0.98 - combined_current) * 1000)  # Scale size with edge
                return (True, size, f"New position, combined={combined_current:.4f}")
            return (False, 0.0, f"Combined price {combined_current:.4f} too high")

        position = self.outcomes[token_id]

        # Check target
        if self.target_shares > 0 and position.shares >= self.target_shares:
            return (False, 0.0, f"Target {self.target_shares} reached")

        # Would buying improve our average?
        if current_price < position.avg_price:
            improvement = position.avg_price - current_price
            size = min(200.0, improvement * 2000)  # More size for bigger improvement
            return (True, size, f"Improves avg from {position.avg_price:.4f} to lower")

        # Would it still maintain good combined edge?
        new_combined = self.combined_avg_price + (current_price - position.avg_price) * 0.1
        if new_combined < 0.95:  # Still 5% edge
            return (True, 50.0, f"Still good edge at {1 - new_combined:.2%}")

        return (False, 0.0, f"Price {current_price:.4f} doesn't improve position")

    def to_dict(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "market_question": self.market_question,
            "total_invested": self.total_invested,
            "matched_shares": self.matched_shares,
            "combined_avg_price": self.combined_avg_price,
            "edge": self.edge,
            "guaranteed_profit": self.guaranteed_profit,
            "num_trades": self.num_trades,
            "outcomes": {tid: p.to_dict() for tid, p in self.outcomes.items()}
        }


class PositionTracker:
    """
    Tracks all positions for volatility harvesting strategy.

    This replaces the instant-arb approach with accumulation:
    - Track positions across markets
    - Determine when to add to positions
    - Calculate guaranteed profit from matched pairs
    """

    def __init__(self, max_markets: int = 20, max_position_age_hours: float = 4.0):
        """
        Initialize tracker.

        Args:
            max_markets: Maximum concurrent market positions
            max_position_age_hours: Close positions older than this
        """
        self.positions: Dict[str, MarketPosition] = {}  # condition_id -> position
        self.max_markets = max_markets
        self.max_position_age = timedelta(hours=max_position_age_hours)

        # Stats
        self.total_trades_executed = 0
        self.total_profit_realized = 0.0

    def get_position(self, condition_id: str) -> Optional[MarketPosition]:
        """Get position for a market."""
        return self.positions.get(condition_id)

    def has_position(self, condition_id: str) -> bool:
        """Check if we have a position in market."""
        return condition_id in self.positions

    def create_position(
        self,
        condition_id: str,
        market_question: str,
        target_shares: float = 0.0
    ) -> MarketPosition:
        """Create a new market position."""
        if len(self.positions) >= self.max_markets:
            # Remove oldest position
            oldest = min(self.positions.values(), key=lambda p: p.created_at)
            logger.warning(f"Max positions reached, removing {oldest.market_question[:40]}...")
            del self.positions[oldest.condition_id]

        position = MarketPosition(
            condition_id=condition_id,
            market_question=market_question,
            target_shares=target_shares
        )
        self.positions[condition_id] = position
        return position

    def record_fill(
        self,
        condition_id: str,
        market_question: str,
        token_id: str,
        outcome_name: str,
        shares: float,
        price: float
    ):
        """Record a fill for tracking."""
        if condition_id not in self.positions:
            self.create_position(condition_id, market_question)

        self.positions[condition_id].add_fill(token_id, outcome_name, shares, price)
        self.total_trades_executed += 1

        position = self.positions[condition_id]
        logger.info(
            f"FILL RECORDED: {outcome_name} {shares:.2f} @ ${price:.4f} | "
            f"Position: {position.matched_shares:.2f} matched, "
            f"edge={position.edge*100:.2f}%, profit=${position.guaranteed_profit:.2f}"
        )

    def get_accumulation_opportunities(
        self,
        orderbooks: Dict[str, dict],  # token_id -> {best_ask, best_bid, ...}
        condition_id: str,
        market_question: str
    ) -> List[Dict]:
        """
        Determine what to buy for volatility harvesting.

        This is the CORE of RN1 strategy:
        1. Check current prices vs our position
        2. Buy if it improves combined average
        3. Buy both sides to maintain balance

        Returns list of {token_id, outcome_name, size_usd, price, reason}
        """
        opportunities = []

        # Get or create position
        if condition_id not in self.positions:
            # New market - check if instant arb exists
            combined = sum(ob.get('best_ask', 1.0) for ob in orderbooks.values())
            if combined < 0.98:  # 2% edge
                # Start new position on both sides
                for token_id, ob in orderbooks.items():
                    ask = ob.get('best_ask', 1.0)
                    if ask < 0.95:  # Don't buy extremely high prices
                        edge_contribution = (0.98 - combined) / len(orderbooks)
                        size = min(200.0, max(25.0, edge_contribution * 5000))
                        opportunities.append({
                            'token_id': token_id,
                            'outcome_name': ob.get('outcome_name', 'Unknown'),
                            'size_usd': size,
                            'price': ask,
                            'reason': f'New arb position, combined={combined:.4f}'
                        })
            return opportunities

        position = self.positions[condition_id]

        # Existing position - check each outcome
        for token_id, ob in orderbooks.items():
            current_ask = ob.get('best_ask', 1.0)
            outcome_name = ob.get('outcome_name', 'Unknown')

            should_buy, size, reason = position.should_buy_more(token_id, current_ask)

            if should_buy and size > 5.0:  # Minimum $5
                opportunities.append({
                    'token_id': token_id,
                    'outcome_name': outcome_name,
                    'size_usd': size,
                    'price': current_ask,
                    'reason': reason
                })

        # Balance check - if one side is much heavier, add to lighter side
        if len(position.outcomes) == 2:
            positions_list = list(position.outcomes.values())
            if positions_list[0].shares > 0 and positions_list[1].shares > 0:
                ratio = max(p.shares for p in positions_list) / min(p.shares for p in positions_list)
                if ratio > 1.5:  # More than 50% imbalanced
                    lighter = min(positions_list, key=lambda p: p.shares)
                    if lighter.token_id in orderbooks:
                        current_ask = orderbooks[lighter.token_id].get('best_ask', 1.0)
                        if current_ask < lighter.avg_price * 1.1:  # Within 10% of avg
                            imbalance = max(p.shares for p in positions_list) - lighter.shares
                            size = min(imbalance * current_ask * 0.5, 500.0)
                            opportunities.append({
                                'token_id': lighter.token_id,
                                'outcome_name': lighter.outcome_name,
                                'size_usd': size,
                                'price': current_ask,
                                'reason': f'Rebalancing, ratio={ratio:.2f}'
                            })

        return opportunities

    def get_profitable_positions(self, min_profit: float = 1.0) -> List[MarketPosition]:
        """Get positions with guaranteed profit above threshold."""
        return [p for p in self.positions.values() if p.guaranteed_profit >= min_profit]

    def get_stale_positions(self) -> List[MarketPosition]:
        """Get positions older than max age."""
        now = datetime.now(timezone.utc)
        return [p for p in self.positions.values() if now - p.created_at > self.max_position_age]

    def cleanup_closed_positions(self, closed_condition_ids: List[str]):
        """Remove positions for markets that have settled."""
        for cid in closed_condition_ids:
            if cid in self.positions:
                position = self.positions[cid]
                self.total_profit_realized += position.guaranteed_profit
                logger.info(
                    f"Position closed: {position.market_question[:40]}... "
                    f"Profit: ${position.guaranteed_profit:.2f}"
                )
                del self.positions[cid]

    def get_stats(self) -> dict:
        """Get tracker statistics."""
        return {
            "active_positions": len(self.positions),
            "total_invested": sum(p.total_invested for p in self.positions.values()),
            "total_matched_shares": sum(p.matched_shares for p in self.positions.values()),
            "total_guaranteed_profit": sum(p.guaranteed_profit for p in self.positions.values()),
            "total_trades": self.total_trades_executed,
            "realized_profit": self.total_profit_realized,
            "positions": [
                {
                    "market": p.market_question[:50],
                    "invested": p.total_invested,
                    "matched": p.matched_shares,
                    "edge": f"{p.edge*100:.2f}%",
                    "profit": p.guaranteed_profit,
                    "trades": p.num_trades
                }
                for p in sorted(self.positions.values(), key=lambda x: -x.guaranteed_profit)
            ]
        }

    def log_status(self):
        """Log current position status."""
        stats = self.get_stats()
        logger.info(
            f"POSITION STATUS: {stats['active_positions']} markets, "
            f"${stats['total_invested']:.2f} invested, "
            f"${stats['total_guaranteed_profit']:.2f} guaranteed profit"
        )
        for p in stats['positions'][:5]:
            logger.info(
                f"  {p['market']}: ${p['invested']:.2f} invested, "
                f"{p['matched']:.2f} matched, {p['edge']} edge, ${p['profit']:.2f} profit"
            )


# Global instance
_position_tracker: Optional[PositionTracker] = None


def get_position_tracker() -> PositionTracker:
    """Get or create global position tracker."""
    global _position_tracker
    if _position_tracker is None:
        _position_tracker = PositionTracker()
    return _position_tracker
