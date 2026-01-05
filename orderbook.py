"""
Orderbook Polling & Arbitrage Detection Module.
Asynchronously polls orderbooks and detects arbitrage opportunities.
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from enum import Enum
import aiohttp

from config import BotConfig, calculate_dynamic_threshold
from market_discovery import Market, Outcome
from rate_limiter import get_global_limiter, handle_response_status

logger = logging.getLogger(__name__)


class ArbType(Enum):
    """Type of arbitrage opportunity."""
    BUY_ARB = "buy_arb"   # Sum of asks < 1 - threshold
    SELL_ARB = "sell_arb"  # Sum of bids > 1 + threshold


@dataclass
class OrderbookLevel:
    """Single price level in orderbook."""
    price: float
    size: float  # In shares


@dataclass
class Orderbook:
    """Orderbook for a single outcome."""
    token_id: str
    outcome_name: str
    bids: List[OrderbookLevel] = field(default_factory=list)
    asks: List[OrderbookLevel] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def best_bid(self) -> Optional[OrderbookLevel]:
        """Get best bid (highest buy price)."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[OrderbookLevel]:
        """Get best ask (lowest sell price)."""
        return self.asks[0] if self.asks else None

    @property
    def best_bid_price(self) -> float:
        """Get best bid price or 0."""
        return self.best_bid.price if self.best_bid else 0.0

    @property
    def best_ask_price(self) -> float:
        """Get best ask price or 1.0."""
        return self.best_ask.price if self.best_ask else 1.0

    @property
    def best_bid_size(self) -> float:
        """Get best bid size or 0."""
        return self.best_bid.size if self.best_bid else 0.0

    @property
    def best_ask_size(self) -> float:
        """Get best ask size or 0."""
        return self.best_ask.size if self.best_ask else 0.0

    def get_depth_at_price(self, side: str, max_levels: int = 5) -> float:
        """Get total depth up to N levels."""
        levels = self.bids if side == "bid" else self.asks
        return sum(l.size for l in levels[:max_levels])

    def get_second_level_price(self, side: str) -> Optional[float]:
        """Get second-best price level for slippage check."""
        levels = self.bids if side == "bid" else self.asks
        return levels[1].price if len(levels) > 1 else None

    def get_spread(self) -> float:
        """Get bid-ask spread."""
        if self.best_bid and self.best_ask:
            return self.best_ask_price - self.best_bid_price
        return 1.0  # Max spread if one side missing

    @property
    def mid_price(self) -> float:
        """Get mid price."""
        if self.best_bid and self.best_ask:
            return (self.best_bid_price + self.best_ask_price) / 2
        return self.best_bid_price or self.best_ask_price or 0.5


@dataclass
class MarketOrderbooks:
    """Collection of orderbooks for all outcomes in a market."""
    market: Market
    orderbooks: Dict[str, Orderbook] = field(default_factory=dict)  # token_id -> Orderbook
    fetch_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def get_orderbook(self, token_id: str) -> Optional[Orderbook]:
        """Get orderbook for specific outcome."""
        return self.orderbooks.get(token_id)

    @property
    def all_orderbooks(self) -> List[Orderbook]:
        """Get all orderbooks."""
        return list(self.orderbooks.values())

    @property
    def is_complete(self) -> bool:
        """Check if we have orderbooks for all outcomes."""
        return len(self.orderbooks) == len(self.market.outcomes)


@dataclass
class ArbOpportunity:
    """Represents a detected arbitrage opportunity."""
    market: Market
    arb_type: ArbType
    profit_margin: float  # As decimal (0.02 = 2%)
    orderbooks: MarketOrderbooks
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Computed trade details
    trade_size_usd: float = 0.0
    expected_profit_usd: float = 0.0
    outcome_sizes: Dict[str, float] = field(default_factory=dict)  # token_id -> size in USD

    @property
    def is_valid(self) -> bool:
        """Check if opportunity is still valid based on timing."""
        age_seconds = (datetime.now(timezone.utc) - self.detected_at).total_seconds()
        return age_seconds < 5.0  # Opportunities expire in 5 seconds

    def __repr__(self) -> str:
        return (
            f"ArbOpportunity({self.arb_type.value}, "
            f"margin={self.profit_margin*100:.3f}%, "
            f"size=${self.trade_size_usd:.2f}, "
            f"profit=${self.expected_profit_usd:.2f})"
        )


class OrderbookPoller:
    """Polls orderbooks and detects arbitrage opportunities."""

    def __init__(self, config: BotConfig):
        """
        Initialize the orderbook poller.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self.clob_endpoint = config.network.clob_endpoint
        self._session: Optional[aiohttp.ClientSession] = None

        # Stats
        self._polls_count = 0
        self._arbs_found = 0
        self._stale_books_skipped = 0
        self._slippage_rejected = 0
        self._big_arb_alerts = 0
        self._batch_fetches = 0
        self._batch_fetch_errors = 0
        self._last_poll_time: Optional[datetime] = None

        # Burst mode tracking
        self._market_depth_history: Dict[str, List[float]] = {}  # market_id -> last N depths
        self._market_price_history: Dict[str, List[float]] = {}  # market_id -> last N mid prices
        self._market_volume_history: Dict[str, List[float]] = {}  # market_id -> last N volumes
        self._market_arb_frequency: Dict[str, int] = {}  # market_id -> arbs found in last window
        self._burst_markets: set = set()  # Markets currently in burst mode
        self._depth_history_size = 5  # Track last 5 polls for spike detection
        self._burst_depth_multiplier = 1.8  # Depth spike threshold (1.8x = 80% increase)
        self._burst_price_jump_pct = 0.03  # Price jump threshold (3% = potential news event)
        self._burst_volume_spike_mult = 2.0  # Volume spike threshold (2x = unusual activity)
        self._burst_poll_interval = 0.4  # Fast polling during bursts (400ms)

        # Slippage prediction with EMA (addresses 22% partial fill rate)
        self._spread_ema: Dict[str, float] = {}  # token_id -> EMA of spread
        self._depth_ema: Dict[str, float] = {}   # token_id -> EMA of depth
        self._volatility_ema: Dict[str, float] = {}  # token_id -> EMA of price volatility
        self._last_prices: Dict[str, float] = {}  # token_id -> last mid price
        self._ema_alpha = 0.3  # EMA smoothing factor (0.3 = responsive to recent changes)
        self._slippage_predictions: Dict[str, float] = {}  # token_id -> predicted slippage %

    def update_slippage_prediction(self, orderbook: Orderbook):
        """
        Update slippage prediction EMA for an orderbook.

        Uses exponential moving averages of:
        - Spread (wider spread = higher slippage risk)
        - Depth (lower depth = higher slippage risk)
        - Price volatility (higher volatility = higher slippage risk)

        Predicted slippage formula:
        slippage_pct = base_spread + volatility_factor + depth_penalty

        Args:
            orderbook: The orderbook to update predictions for.
        """
        token_id = orderbook.token_id
        alpha = self._ema_alpha

        # Update spread EMA
        spread = orderbook.get_spread()
        if token_id in self._spread_ema:
            self._spread_ema[token_id] = alpha * spread + (1 - alpha) * self._spread_ema[token_id]
        else:
            self._spread_ema[token_id] = spread

        # Update depth EMA (use L1 depth)
        depth = orderbook.best_ask_size + orderbook.best_bid_size
        if token_id in self._depth_ema:
            self._depth_ema[token_id] = alpha * depth + (1 - alpha) * self._depth_ema[token_id]
        else:
            self._depth_ema[token_id] = depth

        # Update volatility EMA (price change from last)
        mid = orderbook.mid_price
        if token_id in self._last_prices:
            price_change = abs(mid - self._last_prices[token_id]) / self._last_prices[token_id] if self._last_prices[token_id] > 0 else 0
            if token_id in self._volatility_ema:
                self._volatility_ema[token_id] = alpha * price_change + (1 - alpha) * self._volatility_ema[token_id]
            else:
                self._volatility_ema[token_id] = price_change
        self._last_prices[token_id] = mid

        # Calculate predicted slippage
        # Formula: spread contributes directly, volatility amplifies, low depth penalizes
        spread_component = self._spread_ema.get(token_id, 0.05)
        volatility_component = self._volatility_ema.get(token_id, 0) * 2  # 2x multiplier for volatility
        depth_value = self._depth_ema.get(token_id, 100)

        # Depth penalty: higher slippage when depth is low
        # At depth=100, penalty=0; at depth=10, penalty=0.01 (1%)
        depth_penalty = max(0, 0.1 / max(depth_value, 1) - 0.001)

        predicted_slippage = spread_component + volatility_component + depth_penalty
        self._slippage_predictions[token_id] = predicted_slippage

    def get_predicted_slippage(self, token_id: str) -> float:
        """
        Get predicted slippage for a token.

        Args:
            token_id: The token ID.

        Returns:
            Predicted slippage as decimal (e.g., 0.02 = 2%).
        """
        return self._slippage_predictions.get(token_id, 0.02)  # Default 2%

    def should_skip_due_to_slippage(self, token_ids: List[str], edge_pct: float) -> Tuple[bool, str]:
        """
        Check if predicted slippage exceeds the edge.

        Args:
            token_ids: List of token IDs in the arb.
            edge_pct: The arbitrage edge as decimal.

        Returns:
            Tuple of (should_skip, reason).
        """
        total_slippage = 0
        for tid in token_ids:
            total_slippage += self.get_predicted_slippage(tid)

        # Average slippage across outcomes
        avg_slippage = total_slippage / len(token_ids) if token_ids else 0

        # Skip if predicted slippage > 50% of edge
        if avg_slippage > edge_pct * 0.5:
            return True, f"Predicted slippage {avg_slippage*100:.2f}% > 50% of edge {edge_pct*100:.2f}%"

        return False, ""

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_orderbook(self, token_id: str, outcome_name: str = "") -> Optional[Orderbook]:
        """
        Fetch orderbook for a single token/outcome.

        Args:
            token_id: The token ID to fetch.
            outcome_name: Name of the outcome for logging.

        Returns:
            Orderbook object or None on error.
        """
        session = await self._get_session()
        url = f"{self.clob_endpoint}/book"
        params = {"token_id": token_id}

        try:
            # Check global rate limiter before request
            limiter = get_global_limiter()
            if await limiter.wait_if_limited():
                logger.debug(f"Waited for rate limit before fetching {token_id}")

            async with session.get(url, params=params) as response:
                if response.status == 200:
                    await limiter.record_success()
                    data = await response.json()
                    return self._parse_orderbook(data, token_id, outcome_name)
                elif response.status == 429:
                    # Use global rate limiter with exponential backoff
                    await handle_response_status(429, f"orderbook/{token_id}")
                    return None
                else:
                    logger.debug(f"Failed to fetch orderbook for {token_id}: HTTP {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching orderbook for {token_id}")
            return None
        except Exception as e:
            logger.debug(f"Error fetching orderbook for {token_id}: {e}")
            return None

    async def fetch_orderbooks_batch(
        self,
        token_ids: List[str],
        token_id_to_outcome: Dict[str, str]
    ) -> Dict[str, Orderbook]:
        """
        Fetch multiple orderbooks in a single batch request.

        Uses the CLOB /books endpoint which supports batch fetching.
        Much more efficient than individual /book calls - stays well under rate limits.

        Args:
            token_ids: List of token IDs to fetch.
            token_id_to_outcome: Mapping of token_id -> outcome_name for logging.

        Returns:
            Dict mapping token_id -> Orderbook for successfully fetched books.
        """
        if not token_ids:
            return {}

        session = await self._get_session()
        url = f"{self.clob_endpoint}/books"

        # Build batch params - CLOB API expects comma-separated token_ids
        # or array format depending on endpoint version
        results: Dict[str, Orderbook] = {}

        # Split into batches if needed
        batch_size = self.config.trading.orderbook_batch_size
        batches = [token_ids[i:i + batch_size] for i in range(0, len(token_ids), batch_size)]

        headers = {"Content-Type": "application/json"}

        for batch_idx, batch in enumerate(batches):
            try:
                # POST with JSON body per Polymarket docs
                payload = [{"token_id": tid} for tid in batch]

                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self._batch_fetches += 1

                        # Response is list of orderbooks in same order as request
                        if isinstance(data, list):
                            for i, book_data in enumerate(data):
                                if i < len(batch):
                                    tid = batch[i]
                                    outcome_name = token_id_to_outcome.get(tid, "")
                                    ob = self._parse_orderbook(book_data, tid, outcome_name)
                                    results[tid] = ob
                        # Response might be dict with token_id keys
                        elif isinstance(data, dict):
                            for tid, book_data in data.items():
                                if tid in batch:
                                    outcome_name = token_id_to_outcome.get(tid, "")
                                    ob = self._parse_orderbook(book_data, tid, outcome_name)
                                    results[tid] = ob

                        logger.debug(f"Batch {batch_idx+1}/{len(batches)}: fetched {len(batch)} orderbooks")

                    elif response.status == 429:
                        logger.warning(f"Rate limited on batch fetch ({len(batch)} tokens)")
                        self._batch_fetch_errors += 1
                        await asyncio.sleep(self.config.trading.rate_limit_backoff_base * 2)
                    elif response.status == 403 or response.status == 1015:
                        # Cloudflare block - wait longer
                        logger.warning(f"Cloudflare block on batch fetch, waiting 30s...")
                        self._batch_fetch_errors += 1
                        await asyncio.sleep(30)
                    else:
                        text = await response.text()
                        logger.warning(f"Batch fetch failed: HTTP {response.status} - {text[:200]}")
                        self._batch_fetch_errors += 1

            except asyncio.TimeoutError:
                logger.debug(f"Timeout on batch fetch ({len(batch)} tokens)")
                self._batch_fetch_errors += 1
            except Exception as e:
                logger.warning(f"Error in batch fetch: {e}")
                self._batch_fetch_errors += 1

            # Small delay between batches to avoid rate limits
            if batch_idx < len(batches) - 1:
                await asyncio.sleep(0.1)

        return results

    def _parse_orderbook(self, data: Dict, token_id: str, outcome_name: str) -> Orderbook:
        """Parse orderbook data from API response."""
        bids = []
        asks = []

        # Parse bids (sorted highest first)
        for bid in data.get("bids", []):
            try:
                price = float(bid.get("price", 0))
                size = float(bid.get("size", 0))
                if price > 0 and size > 0:
                    bids.append(OrderbookLevel(price=price, size=size))
            except (ValueError, TypeError):
                continue

        # Parse asks (sorted lowest first)
        for ask in data.get("asks", []):
            try:
                price = float(ask.get("price", 0))
                size = float(ask.get("size", 0))
                if price > 0 and size > 0:
                    asks.append(OrderbookLevel(price=price, size=size))
            except (ValueError, TypeError):
                continue

        # Sort properly
        bids.sort(key=lambda x: x.price, reverse=True)  # Highest first
        asks.sort(key=lambda x: x.price)  # Lowest first

        return Orderbook(
            token_id=token_id,
            outcome_name=outcome_name,
            bids=bids,
            asks=asks
        )

    async def fetch_market_orderbooks(self, market: Market) -> MarketOrderbooks:
        """
        Fetch orderbooks for all outcomes in a market concurrently.

        Args:
            market: The market to fetch orderbooks for.

        Returns:
            MarketOrderbooks containing all outcome orderbooks.
        """
        tasks = []
        for outcome in market.outcomes:
            tasks.append(self.fetch_orderbook(outcome.token_id, outcome.name))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        orderbooks = {}
        for i, result in enumerate(results):
            if isinstance(result, Orderbook):
                orderbooks[market.outcomes[i].token_id] = result
            elif isinstance(result, Exception):
                logger.debug(f"Error fetching orderbook: {result}")

        self._polls_count += 1
        self._last_poll_time = datetime.now(timezone.utc)

        return MarketOrderbooks(market=market, orderbooks=orderbooks)

    def _is_stale_orderbook(self, orderbooks: List[Orderbook]) -> Tuple[bool, str]:
        """
        Check if orderbooks appear stale (prices don't sum to ~1).

        Args:
            orderbooks: List of orderbooks to check.

        Returns:
            Tuple of (is_stale, reason).
        """
        stale_threshold = self.config.trading.stale_book_threshold

        # Check mid-price sum (should be close to 1 for fair market)
        mid_sum = sum(ob.mid_price for ob in orderbooks)
        if abs(mid_sum - 1.0) > stale_threshold:
            return True, f"Mid prices sum to {mid_sum:.3f}, expected ~1.0"

        # Check for suspicious spreads (>20% on any outcome = likely stale)
        for ob in orderbooks:
            spread = ob.get_spread()
            if spread > 0.20:
                return True, f"Suspicious spread {spread*100:.1f}% on {ob.outcome_name}"

        return False, ""

    def _send_big_arb_alert(
        self,
        arb_type: ArbType,
        market_question: str,
        profit_margin: float,
        trade_size_usd: float,
        expected_profit_usd: float
    ):
        """
        Send alert for big arbitrage opportunity (>2% margin).

        Logs prominently and schedules webhook notification (non-blocking).

        Args:
            arb_type: Type of arbitrage (buy/sell).
            market_question: Market question text.
            profit_margin: Profit margin as decimal (e.g., 0.025 = 2.5%).
            trade_size_usd: Trade size in USD.
            expected_profit_usd: Expected profit in USD.
        """
        margin_pct = profit_margin * 100
        self._big_arb_alerts += 1

        # Prominent log alert (sync - immediate)
        logger.warning("=" * 60)
        logger.warning(f"BIG ARB ALERT: {margin_pct:.2f}% margin detected!")
        logger.warning(f"Type: {arb_type.value.upper()}")
        logger.warning(f"Market: {market_question[:80]}...")
        logger.warning(f"Trade Size: ${trade_size_usd:.2f}")
        logger.warning(f"Expected Profit: ${expected_profit_usd:.4f}")
        logger.warning(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
        logger.warning("=" * 60)

        # Schedule webhook if configured (non-blocking)
        webhook_url = self.config.trading.big_arb_webhook_url
        if webhook_url:
            try:
                loop = asyncio.get_event_loop()
                loop.create_task(self._send_webhook_alert(
                    webhook_url,
                    arb_type,
                    market_question,
                    margin_pct,
                    trade_size_usd,
                    expected_profit_usd
                ))
            except RuntimeError:
                # No event loop running - skip webhook
                logger.debug("No event loop for webhook, skipping")

    async def _send_webhook_alert(
        self,
        webhook_url: str,
        arb_type: ArbType,
        market_question: str,
        margin_pct: float,
        trade_size_usd: float,
        expected_profit_usd: float
    ):
        """
        Send webhook notification for big arb alert.

        Supports Slack/Discord-style webhook payloads.

        Args:
            webhook_url: Webhook URL to POST to.
            arb_type: Type of arbitrage.
            market_question: Market question text.
            margin_pct: Profit margin as percentage.
            trade_size_usd: Trade size in USD.
            expected_profit_usd: Expected profit in USD.
        """
        try:
            session = await self._get_session()

            # Slack/Discord compatible payload
            payload = {
                "text": f"BIG ARB ALERT: {margin_pct:.2f}% margin!",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"BIG ARB: {margin_pct:.2f}% Margin"
                        }
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Type:* {arb_type.value.upper()}"},
                            {"type": "mrkdwn", "text": f"*Margin:* {margin_pct:.2f}%"},
                            {"type": "mrkdwn", "text": f"*Size:* ${trade_size_usd:.2f}"},
                            {"type": "mrkdwn", "text": f"*Profit:* ${expected_profit_usd:.4f}"}
                        ]
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Market:* {market_question[:100]}"
                        }
                    }
                ],
                # Discord compatibility
                "embeds": [
                    {
                        "title": f"BIG ARB ALERT: {margin_pct:.2f}%",
                        "color": 0x00FF00,  # Green
                        "fields": [
                            {"name": "Type", "value": arb_type.value.upper(), "inline": True},
                            {"name": "Margin", "value": f"{margin_pct:.2f}%", "inline": True},
                            {"name": "Size", "value": f"${trade_size_usd:.2f}", "inline": True},
                            {"name": "Profit", "value": f"${expected_profit_usd:.4f}", "inline": True},
                            {"name": "Market", "value": market_question[:100]}
                        ],
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                ]
            }

            async with session.post(webhook_url, json=payload) as response:
                if response.status in (200, 204):
                    logger.debug(f"Big arb webhook sent successfully")
                else:
                    logger.warning(f"Webhook returned status {response.status}")

        except Exception as e:
            logger.debug(f"Failed to send big arb webhook: {e}")
            # Don't raise - webhook failure shouldn't block trading

    def _check_slippage(self, orderbooks: List[Orderbook], arb_type: ArbType) -> Tuple[bool, str, float]:
        """
        Check if next orderbook level has acceptable slippage.

        Instead of skipping entirely, returns a size reduction factor
        to limit trade to L1 depth when L2 has high slippage.

        For sparse orderbooks (<2 levels), returns factor=1.0 to use L1 depth
        safely (no L2 means we can't assess slippage, but L1 is still valid).

        Args:
            orderbooks: List of orderbooks.
            arb_type: Type of arbitrage.

        Returns:
            Tuple of (acceptable, reason, size_reduction_factor).
            size_reduction_factor: 1.0 = full size, <1.0 = reduce to L1 only.
        """
        slippage_threshold = self.config.trading.slippage_threshold_percent / 100
        max_slippage_found = 0.0
        high_slippage_outcomes = []
        sparse_orderbooks = 0

        for ob in orderbooks:
            if arb_type == ArbType.BUY_ARB:
                best = ob.best_ask_price
                second = ob.get_second_level_price("ask")
                has_l2 = len(ob.asks) >= 2 if ob.asks else False
            else:
                best = ob.best_bid_price
                second = ob.get_second_level_price("bid")
                has_l2 = len(ob.bids) >= 2 if ob.bids else False

            # Handle sparse orderbooks (<2 levels): treat as L1-only, factor=1.0
            # This is NOT high slippage - we simply don't have L2 data
            if not has_l2:
                sparse_orderbooks += 1
                logger.debug(f"Sparse orderbook for {ob.outcome_name}: <2 levels, using L1 only")
                continue

            if second is not None and best > 0:
                if arb_type == ArbType.BUY_ARB:
                    slippage = (second - best) / best
                else:
                    slippage = (best - second) / best

                max_slippage_found = max(max_slippage_found, slippage)

                if slippage > slippage_threshold:
                    high_slippage_outcomes.append((ob.outcome_name, slippage))

        if high_slippage_outcomes:
            # Instead of rejecting, reduce size to L1 depth only
            # Return a factor that limits to L1 depth
            worst = max(high_slippage_outcomes, key=lambda x: x[1])
            logger.debug(
                f"High slippage detected on {worst[0]} ({worst[1]*100:.1f}%), "
                f"limiting to L1 depth"
            )
            # Return factor of 0.5 to reduce size significantly when L2 is bad
            # This means we'll only use ~50% of L1 depth as safe trade size
            return True, f"L2 slippage {worst[1]*100:.1f}%", 0.5

        # If all orderbooks are sparse (<2 levels), use L1 fully (factor=1.0)
        # This is safe because we're only trading at L1 prices
        if sparse_orderbooks == len(orderbooks):
            logger.debug("All orderbooks sparse (<2 levels), using L1 depth at factor=1.0")

        return True, "", 1.0

    def _get_l1_only_depth(self, orderbooks: List[Orderbook], arb_type: ArbType) -> float:
        """
        Get the minimum L1-only depth across all orderbooks.

        Args:
            orderbooks: List of orderbooks.
            arb_type: Type of arbitrage.

        Returns:
            Minimum L1 depth in USD.
        """
        min_depth = float('inf')

        for ob in orderbooks:
            if arb_type == ArbType.BUY_ARB:
                if ob.best_ask:
                    depth = ob.best_ask_size * ob.best_ask_price
                    min_depth = min(min_depth, depth)
            else:
                if ob.best_bid:
                    depth = ob.best_bid_size * ob.best_bid_price
                    min_depth = min(min_depth, depth)

        return min_depth if min_depth != float('inf') else 0.0

    def _check_orderbook_freshness(self, orderbooks: List[Orderbook]) -> tuple[bool, str]:
        """
        Check if orderbooks are fresh enough for trading.

        Per audit: Skip books older than 200ms to avoid executing on stale data.

        Args:
            orderbooks: List of orderbooks to check.

        Returns:
            Tuple of (is_fresh, reason).
        """
        max_age_ms = self.config.trading.orderbook_max_age_ms
        now = datetime.now(timezone.utc)

        for ob in orderbooks:
            age_ms = (now - ob.timestamp).total_seconds() * 1000
            if age_ms > max_age_ms:
                return False, f"Orderbook for {ob.outcome_name} is {age_ms:.0f}ms old (max {max_age_ms}ms)"

        return True, ""

    def _get_weighted_price_across_levels(
        self,
        orderbook: Orderbook,
        side: str,
        num_levels: int = 3,
        target_size_usd: float = 50.0
    ) -> tuple[float, float]:
        """
        Get volume-weighted average price across multiple levels.

        Per audit: Sum depth across top 3-5 levels for more realistic edge detection.

        Args:
            orderbook: The orderbook to analyze.
            side: "ask" or "bid".
            num_levels: Number of levels to consider.
            target_size_usd: Target trade size for weighting.

        Returns:
            Tuple of (weighted_avg_price, total_depth_usd).
        """
        levels = orderbook.asks if side == "ask" else orderbook.bids
        if not levels:
            return (1.0 if side == "ask" else 0.0), 0.0

        total_size = 0.0
        weighted_sum = 0.0
        total_depth_usd = 0.0

        for i, level in enumerate(levels[:num_levels]):
            level_usd = level.size * level.price
            total_depth_usd += level_usd
            weighted_sum += level.price * level.size
            total_size += level.size

        if total_size == 0:
            return (1.0 if side == "ask" else 0.0), 0.0

        weighted_avg = weighted_sum / total_size
        return weighted_avg, total_depth_usd

    def detect_arb_opportunity(
        self,
        market_orderbooks: MarketOrderbooks
    ) -> Optional[ArbOpportunity]:
        """
        Detect arbitrage opportunity in market orderbooks.

        For a complete market:
        - Buy arb: sum(best_asks) < 1 - dynamic_threshold
        - Sell arb: sum(best_bids) > 1 + dynamic_threshold

        Uses dynamic threshold based on trade size and gas costs.
        Includes stale book detection, freshness check, and slippage checks.
        Per audit: Also considers depth across multiple levels for realistic edge.

        Args:
            market_orderbooks: Orderbooks for all market outcomes.

        Returns:
            ArbOpportunity if found, None otherwise.
        """
        if not market_orderbooks.is_complete:
            logger.debug(f"Incomplete orderbooks for {market_orderbooks.market.question}")
            return None

        market = market_orderbooks.market
        orderbooks = market_orderbooks.all_orderbooks
        num_outcomes = len(orderbooks)

        min_depth = self.config.trading.min_depth_usd

        # Check orderbook freshness (per audit: skip books >200ms old)
        is_fresh, freshness_reason = self._check_orderbook_freshness(orderbooks)
        if not is_fresh:
            logger.debug(f"Stale timestamp for {market.question[:40]}: {freshness_reason}")
            self._stale_books_skipped += 1
            return None

        # Check for stale orderbooks (price sanity check)
        is_stale, stale_reason = self._is_stale_orderbook(orderbooks)
        if is_stale:
            logger.debug(f"Stale orderbook detected for {market.question[:40]}: {stale_reason}")
            self._stale_books_skipped += 1
            return None

        # Calculate sum of best asks and best bids (L1)
        sum_asks = sum(ob.best_ask_price for ob in orderbooks)
        sum_bids = sum(ob.best_bid_price for ob in orderbooks)

        # Also calculate weighted price across 3 levels (per audit)
        # This gives more realistic edge when L1 is thin
        sum_asks_weighted = 0.0
        sum_bids_weighted = 0.0
        min_depth_across_levels = float('inf')

        for ob in orderbooks:
            ask_weighted, ask_depth = self._get_weighted_price_across_levels(ob, "ask", num_levels=3)
            bid_weighted, bid_depth = self._get_weighted_price_across_levels(ob, "bid", num_levels=3)
            sum_asks_weighted += ask_weighted
            sum_bids_weighted += bid_weighted
            min_depth_across_levels = min(min_depth_across_levels, ask_depth, bid_depth)

        # Check minimum depth at best levels
        min_ask_depth_usd = min(
            ob.best_ask_size * ob.best_ask_price for ob in orderbooks
            if ob.best_ask
        ) if all(ob.best_ask for ob in orderbooks) else 0

        min_bid_depth_usd = min(
            ob.best_bid_size * ob.best_bid_price for ob in orderbooks
            if ob.best_bid
        ) if all(ob.best_bid for ob in orderbooks) else 0

        # Calculate dynamic threshold based on expected trade size
        estimated_trade_size = min(min_ask_depth_usd, min_bid_depth_usd)
        dynamic_threshold = calculate_dynamic_threshold(
            estimated_trade_size, num_outcomes, self.config.trading
        )

        # Check for buy arbitrage (buy all outcomes cheaper than $1)
        if sum_asks < (1.0 - dynamic_threshold) and min_ask_depth_usd >= min_depth:
            # Per audit: Also validate using weighted prices across 3 levels
            # L1 might show an edge, but deeper levels might not
            weighted_margin = 1.0 - sum_asks_weighted
            if weighted_margin < dynamic_threshold * 0.5:
                logger.debug(
                    f"L1 edge {(1.0 - sum_asks)*100:.2f}% but weighted edge only "
                    f"{weighted_margin*100:.2f}% - likely thin L1, skipping"
                )
                return None

            # Check slippage - now returns size reduction factor instead of rejecting
            slippage_ok, slippage_reason, size_factor = self._check_slippage(orderbooks, ArbType.BUY_ARB)
            if not slippage_ok:
                # This shouldn't happen with new logic, but handle gracefully
                logger.debug(f"Slippage check failed: {slippage_reason}")
                self._slippage_rejected += 1
                return None

            profit_margin = 1.0 - sum_asks
            opportunity = ArbOpportunity(
                market=market,
                arb_type=ArbType.BUY_ARB,
                profit_margin=profit_margin,
                orderbooks=market_orderbooks
            )
            self._calculate_trade_sizes(opportunity)

            # Apply size reduction if L2 has high slippage
            if size_factor < 1.0:
                l1_depth = self._get_l1_only_depth(orderbooks, ArbType.BUY_ARB)
                opportunity.trade_size_usd = min(opportunity.trade_size_usd, l1_depth * size_factor)
                opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin
                logger.debug(f"Reduced trade size to ${opportunity.trade_size_usd:.2f} due to L2 slippage")

            # Re-check with actual trade size
            final_threshold = calculate_dynamic_threshold(
                opportunity.trade_size_usd, num_outcomes, self.config.trading
            )
            if profit_margin < final_threshold:
                logger.debug(
                    f"Margin {profit_margin*100:.3f}% below dynamic threshold "
                    f"{final_threshold*100:.3f}% for size ${opportunity.trade_size_usd:.2f}"
                )
                return None

            self._arbs_found += 1
            logger.info(
                f"BUY ARB FOUND: {market.question[:50]}... "
                f"| Sum asks: {sum_asks:.4f} | Margin: {profit_margin*100:.3f}% "
                f"| Threshold: {final_threshold*100:.3f}% | Depth: ${min_ask_depth_usd:.2f}"
            )

            # Big arb alert if margin exceeds threshold (default 2%)
            if profit_margin >= self.config.trading.big_arb_alert_threshold:
                self._send_big_arb_alert(
                    arb_type=ArbType.BUY_ARB,
                    market_question=market.question,
                    profit_margin=profit_margin,
                    trade_size_usd=opportunity.trade_size_usd,
                    expected_profit_usd=opportunity.expected_profit_usd
                )

            return opportunity

        # Check for sell arbitrage (sell all outcomes for more than $1)
        if sum_bids > (1.0 + dynamic_threshold) and min_bid_depth_usd >= min_depth:
            # Per audit: Also validate using weighted prices across 3 levels
            # L1 might show an edge, but deeper levels might not
            weighted_margin = sum_bids_weighted - 1.0
            if weighted_margin < dynamic_threshold * 0.5:
                logger.debug(
                    f"L1 edge {(sum_bids - 1.0)*100:.2f}% but weighted edge only "
                    f"{weighted_margin*100:.2f}% - likely thin L1, skipping"
                )
                return None

            # Check slippage - now returns size reduction factor instead of rejecting
            slippage_ok, slippage_reason, size_factor = self._check_slippage(orderbooks, ArbType.SELL_ARB)
            if not slippage_ok:
                # This shouldn't happen with new logic, but handle gracefully
                logger.debug(f"Slippage check failed: {slippage_reason}")
                self._slippage_rejected += 1
                return None

            profit_margin = sum_bids - 1.0
            opportunity = ArbOpportunity(
                market=market,
                arb_type=ArbType.SELL_ARB,
                profit_margin=profit_margin,
                orderbooks=market_orderbooks
            )
            self._calculate_trade_sizes(opportunity)

            # Apply size reduction if L2 has high slippage
            if size_factor < 1.0:
                l1_depth = self._get_l1_only_depth(orderbooks, ArbType.SELL_ARB)
                opportunity.trade_size_usd = min(opportunity.trade_size_usd, l1_depth * size_factor)
                opportunity.expected_profit_usd = opportunity.trade_size_usd * opportunity.profit_margin
                logger.debug(f"Reduced trade size to ${opportunity.trade_size_usd:.2f} due to L2 slippage")

            # Re-check with actual trade size
            final_threshold = calculate_dynamic_threshold(
                opportunity.trade_size_usd, num_outcomes, self.config.trading
            )
            if profit_margin < final_threshold:
                logger.debug(
                    f"Margin {profit_margin*100:.3f}% below dynamic threshold "
                    f"{final_threshold*100:.3f}% for size ${opportunity.trade_size_usd:.2f}"
                )
                return None

            self._arbs_found += 1
            logger.info(
                f"SELL ARB FOUND: {market.question[:50]}... "
                f"| Sum bids: {sum_bids:.4f} | Margin: {profit_margin*100:.3f}% "
                f"| Threshold: {final_threshold*100:.3f}% | Depth: ${min_bid_depth_usd:.2f}"
            )

            # Big arb alert if margin exceeds threshold (default 2%)
            if profit_margin >= self.config.trading.big_arb_alert_threshold:
                self._send_big_arb_alert(
                    arb_type=ArbType.SELL_ARB,
                    market_question=market.question,
                    profit_margin=profit_margin,
                    trade_size_usd=opportunity.trade_size_usd,
                    expected_profit_usd=opportunity.expected_profit_usd
                )

            return opportunity

        return None

    def _calculate_trade_sizes(self, opportunity: ArbOpportunity):
        """
        Calculate optimal trade sizes for an arbitrage opportunity.

        Args:
            opportunity: The opportunity to calculate sizes for.
        """
        orderbooks = opportunity.orderbooks
        market = opportunity.market
        safety_mult = self.config.trading.depth_safety_multiplier
        max_pct = self.config.trading.max_size_per_trade_percent / 100

        if opportunity.arb_type == ArbType.BUY_ARB:
            # For buy arb, we buy all outcomes at their ask prices
            # Size limited by minimum available depth across all outcomes
            min_depth_shares = float('inf')
            for ob in orderbooks.all_orderbooks:
                if ob.best_ask:
                    depth_shares = ob.best_ask_size
                    min_depth_shares = min(min_depth_shares, depth_shares)

            # Convert to USD using average ask price
            avg_ask = sum(ob.best_ask_price for ob in orderbooks.all_orderbooks) / len(orderbooks.all_orderbooks)
            trade_size_usd = min_depth_shares * avg_ask * safety_mult

            # Cap at max percentage of capital
            max_size = self.config.starting_capital_usd * max_pct
            trade_size_usd = min(trade_size_usd, max_size)

            # Calculate per-outcome sizes
            outcome_sizes = {}
            for ob in orderbooks.all_orderbooks:
                # Each outcome gets proportional share based on its price
                outcome_usd = trade_size_usd * ob.best_ask_price / sum(
                    o.best_ask_price for o in orderbooks.all_orderbooks
                )
                outcome_sizes[ob.token_id] = outcome_usd

            opportunity.trade_size_usd = trade_size_usd
            opportunity.outcome_sizes = outcome_sizes
            opportunity.expected_profit_usd = trade_size_usd * opportunity.profit_margin

        else:  # SELL_ARB
            # For sell arb, we sell all outcomes at their bid prices
            min_depth_shares = float('inf')
            for ob in orderbooks.all_orderbooks:
                if ob.best_bid:
                    depth_shares = ob.best_bid_size
                    min_depth_shares = min(min_depth_shares, depth_shares)

            avg_bid = sum(ob.best_bid_price for ob in orderbooks.all_orderbooks) / len(orderbooks.all_orderbooks)
            trade_size_usd = min_depth_shares * avg_bid * safety_mult

            max_size = self.config.starting_capital_usd * max_pct
            trade_size_usd = min(trade_size_usd, max_size)

            outcome_sizes = {}
            for ob in orderbooks.all_orderbooks:
                outcome_usd = trade_size_usd * ob.best_bid_price / sum(
                    o.best_bid_price for o in orderbooks.all_orderbooks
                )
                outcome_sizes[ob.token_id] = outcome_usd

            opportunity.trade_size_usd = trade_size_usd
            opportunity.outcome_sizes = outcome_sizes
            opportunity.expected_profit_usd = trade_size_usd * opportunity.profit_margin

    async def poll_markets_for_arbs(
        self,
        markets: List[Market]
    ) -> List[ArbOpportunity]:
        """
        Poll multiple markets for arbitrage opportunities using batch orderbook fetch.

        Collects all token_ids from target markets and fetches them in a single
        batch request (or few batches if >200 ids). This eliminates rate limiting
        and improves detection reliability.

        Args:
            markets: List of markets to poll.

        Returns:
            List of detected arbitrage opportunities.
        """
        if not markets:
            return []

        # Collect all unique token_ids and build mappings
        all_token_ids: List[str] = []
        token_id_to_outcome: Dict[str, str] = {}
        token_id_to_market: Dict[str, Market] = {}

        for market in markets:
            for outcome in market.outcomes:
                tid = outcome.token_id
                all_token_ids.append(tid)
                token_id_to_outcome[tid] = outcome.name
                token_id_to_market[tid] = market

        logger.debug(f"Batch fetching {len(all_token_ids)} orderbooks for {len(markets)} markets")

        # Fetch all orderbooks in batch (1-2 requests instead of 100+)
        orderbooks_by_token = await self.fetch_orderbooks_batch(
            all_token_ids,
            token_id_to_outcome
        )

        # Group orderbooks by market
        market_orderbooks_map: Dict[str, MarketOrderbooks] = {}

        for market in markets:
            market_key = market.condition_id
            if market_key not in market_orderbooks_map:
                market_orderbooks_map[market_key] = MarketOrderbooks(market=market)

            for outcome in market.outcomes:
                tid = outcome.token_id
                if tid in orderbooks_by_token:
                    market_orderbooks_map[market_key].orderbooks[tid] = orderbooks_by_token[tid]

        # Update stats
        self._polls_count += 1
        self._last_poll_time = datetime.now(timezone.utc)

        # Detect arbitrage opportunities
        opportunities = []
        for market_orderbooks in market_orderbooks_map.values():
            opp = self.detect_arb_opportunity(market_orderbooks)
            if opp:
                opportunities.append(opp)

        return opportunities

    def _update_depth_history(self, market_id: str, total_depth: float):
        """
        Update depth history for burst detection.

        Args:
            market_id: Market identifier.
            total_depth: Current total depth across all outcomes.
        """
        if market_id not in self._market_depth_history:
            self._market_depth_history[market_id] = []

        history = self._market_depth_history[market_id]
        history.append(total_depth)

        # Keep only last N entries
        if len(history) > self._depth_history_size:
            self._market_depth_history[market_id] = history[-self._depth_history_size:]

    def _update_price_history(self, market_id: str, mid_price: float):
        """
        Update price history for price jump detection.

        Args:
            market_id: Market identifier.
            mid_price: Current mid price (average of best bid/ask).
        """
        if market_id not in self._market_price_history:
            self._market_price_history[market_id] = []

        history = self._market_price_history[market_id]
        history.append(mid_price)

        if len(history) > self._depth_history_size:
            self._market_price_history[market_id] = history[-self._depth_history_size:]

    def _update_volume_history(self, market_id: str, volume: float):
        """
        Update volume history for volume spike detection.

        Args:
            market_id: Market identifier.
            volume: Current market volume.
        """
        if market_id not in self._market_volume_history:
            self._market_volume_history[market_id] = []

        history = self._market_volume_history[market_id]
        history.append(volume)

        if len(history) > self._depth_history_size:
            self._market_volume_history[market_id] = history[-self._depth_history_size:]

    def _detect_depth_spike(self, market_id: str, current_depth: float) -> bool:
        """
        Detect if current depth is a spike compared to recent history.

        Args:
            market_id: Market identifier.
            current_depth: Current total depth.

        Returns:
            True if depth spike detected (possible incoming activity).
        """
        history = self._market_depth_history.get(market_id, [])
        if len(history) < 2:
            return False

        # Compare to average of previous depths
        avg_depth = sum(history[:-1]) / len(history[:-1]) if history[:-1] else 0
        if avg_depth <= 0:
            return False

        return current_depth >= avg_depth * self._burst_depth_multiplier

    def _detect_price_jump(self, market_id: str) -> bool:
        """
        Detect if price has jumped significantly (potential news event).

        Price jumps indicate breaking news or significant information,
        which often creates temporary arb opportunities.

        Args:
            market_id: Market identifier.

        Returns:
            True if price jump detected.
        """
        history = self._market_price_history.get(market_id, [])
        if len(history) < 2:
            return False

        # Calculate price change
        current_price = history[-1]
        prev_price = history[-2]

        if prev_price <= 0:
            return False

        price_change_pct = abs(current_price - prev_price) / prev_price

        return price_change_pct >= self._burst_price_jump_pct

    def _detect_volume_spike(self, market_id: str) -> bool:
        """
        Detect if volume has spiked significantly (unusual trading activity).

        Volume spikes indicate increased interest and often precede
        price moves and arb opportunities.

        Args:
            market_id: Market identifier.

        Returns:
            True if volume spike detected.
        """
        history = self._market_volume_history.get(market_id, [])
        if len(history) < 3:  # Need more history for volume comparison
            return False

        # Compare current volume to average of previous periods
        current_volume = history[-1]
        prev_volumes = history[:-1]
        avg_volume = sum(prev_volumes) / len(prev_volumes) if prev_volumes else 0

        if avg_volume <= 0:
            return False

        return current_volume >= avg_volume * self._burst_volume_spike_mult

    def check_burst_conditions(self, market_id: str, mid_price: float, volume: float) -> bool:
        """
        Check all burst conditions for a market.

        Triggers burst mode if ANY of:
        1. Depth spike detected
        2. Price jump detected (>3%)
        3. Volume spike detected (>2x)

        Args:
            market_id: Market identifier.
            mid_price: Current mid price.
            volume: Current market volume.

        Returns:
            True if any burst condition triggered.
        """
        # Update histories
        self._update_price_history(market_id, mid_price)
        self._update_volume_history(market_id, volume)

        # Check each condition
        is_burst = False

        if self._detect_price_jump(market_id):
            logger.debug(f"Price jump detected for {market_id[:8]}... triggering burst mode")
            is_burst = True

        if self._detect_volume_spike(market_id):
            logger.debug(f"Volume spike detected for {market_id[:8]}... triggering burst mode")
            is_burst = True

        # Depth spike is checked separately when we have orderbook data

        if is_burst:
            self._burst_markets.add(market_id)

        return is_burst

    def record_arb_found(self, market_id: str):
        """
        Record an arb found for frequency tracking.

        Args:
            market_id: Market identifier.
        """
        self._market_arb_frequency[market_id] = self._market_arb_frequency.get(market_id, 0) + 1

        # Add to burst markets if high frequency
        if self._market_arb_frequency.get(market_id, 0) >= 2:
            self._burst_markets.add(market_id)

    def is_burst_market(self, market_id: str) -> bool:
        """Check if market is currently in burst mode."""
        return market_id in self._burst_markets

    def get_burst_poll_interval(self) -> float:
        """Get faster poll interval for burst mode."""
        return self._burst_poll_interval

    def get_normal_poll_interval(self) -> float:
        """Get normal poll interval."""
        return self.config.trading.poll_interval_seconds

    def prioritize_markets(self, markets: List[Market]) -> List[Market]:
        """
        Prioritize markets for polling based on arb frequency and neg_risk.

        High-priority markets:
        1. Markets with recent arb finds (burst mode)
        2. Neg-risk markets (more capital efficient, like RN1)
        3. Higher volume markets

        Args:
            markets: List of markets to prioritize.

        Returns:
            Sorted list with highest priority first.
        """
        def market_priority(m: Market) -> tuple:
            # Higher values = higher priority
            arb_freq = self._market_arb_frequency.get(m.condition_id, 0)
            is_burst = 1 if m.condition_id in self._burst_markets else 0
            # Neg-risk gets 3x weighting (more capital efficient, better liquidation)
            is_neg_risk = 3 if getattr(m, 'neg_risk', False) else 0
            volume_score = min(m.volume / 100000, 1.0)  # Normalize to 0-1

            return (is_burst, arb_freq, is_neg_risk, volume_score)

        return sorted(markets, key=market_priority, reverse=True)

    def reset_arb_frequency(self):
        """Reset arb frequency tracking (call periodically, e.g., every 5 minutes)."""
        self._market_arb_frequency.clear()
        # Keep some burst markets active for a bit longer
        # Remove markets that had low frequency
        self._burst_markets = {
            m for m in self._burst_markets
            if self._market_arb_frequency.get(m, 0) >= 1
        }

    def get_stats(self) -> Dict:
        """Get polling statistics."""
        return {
            "polls_count": self._polls_count,
            "arbs_found": self._arbs_found,
            "stale_books_skipped": self._stale_books_skipped,
            "slippage_rejected": self._slippage_rejected,
            "big_arb_alerts": self._big_arb_alerts,
            "batch_fetches": self._batch_fetches,
            "batch_fetch_errors": self._batch_fetch_errors,
            "burst_markets": len(self._burst_markets),
            "last_poll": self._last_poll_time.isoformat() if self._last_poll_time else None
        }
