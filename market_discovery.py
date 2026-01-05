"""
Market Discovery Module.
Fetches and filters active sports markets from Polymarket's Gamma API.
Prioritizes high-volume, in-play markets suitable for arbitrage.
"""

import logging
import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
import aiohttp

from config import BotConfig
from rate_limiter import get_global_limiter, handle_response_status

logger = logging.getLogger(__name__)


@dataclass
class Outcome:
    """Represents a single outcome in a market."""
    name: str
    token_id: str
    price: float = 0.0


@dataclass
class Market:
    """Represents a Polymarket market with all relevant data."""
    market_id: str
    condition_id: str
    question: str
    slug: str
    outcomes: List[Outcome]
    volume: float
    liquidity: float
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: bool = True
    is_closed: bool = False
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade_price: Optional[float] = None
    event_id: Optional[str] = None
    event_title: Optional[str] = None
    series_id: Optional[int] = None
    neg_risk: bool = False  # True for winner-take-all markets (can short without holding tokens)

    @property
    def is_binary(self) -> bool:
        """Check if market is a binary (Yes/No) market."""
        return len(self.outcomes) == 2

    @property
    def outcome_count(self) -> int:
        """Get the number of outcomes."""
        return len(self.outcomes)

    def get_token_ids(self) -> List[str]:
        """Get all token IDs for this market."""
        return [o.token_id for o in self.outcomes]


@dataclass
class Event:
    """Represents a Polymarket event containing multiple markets."""
    event_id: str
    title: str
    slug: str
    markets: List[Market] = field(default_factory=list)
    volume: float = 0.0
    liquidity: float = 0.0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    is_active: bool = True
    is_closed: bool = False
    series_id: Optional[int] = None
    tags: List[str] = field(default_factory=list)


class MarketDiscovery:
    """Discovers and filters markets suitable for arbitrage trading."""

    def __init__(self, config: BotConfig):
        """
        Initialize the market discovery module.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self.gamma_endpoint = config.network.gamma_endpoint
        self.clob_endpoint = config.network.clob_endpoint
        self._session: Optional[aiohttp.ClientSession] = None
        self._cached_markets: List[Market] = []
        self._last_refresh: Optional[datetime] = None

        # Resilient caching for API failures
        self._fallback_cache: List[Market] = []  # Longer-term cache for complete API failure
        self._fallback_cache_time: Optional[datetime] = None
        self._fallback_cache_ttl_seconds = 259200  # 72h aggressive fallback cache per audit
        self._consecutive_gamma_failures = 0
        self._max_gamma_failures_before_fallback = 3

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch_json_with_retry(
        self,
        url: str,
        params: Optional[Dict] = None,
        max_retries: Optional[int] = None
    ) -> Optional[Any]:
        """
        Fetch JSON from a URL with exponential backoff retry.

        Uses global rate limiter for coordinated backoff across all HTTP clients.

        Args:
            url: URL to fetch.
            params: Query parameters.
            max_retries: Maximum retry attempts (defaults to config value).

        Returns:
            Parsed JSON response or None on error.
        """
        if max_retries is None:
            max_retries = self.config.trading.max_rate_limit_retries

        session = await self._get_session()
        limiter = get_global_limiter()

        # Check if globally rate limited before making request
        if await limiter.wait_if_limited():
            logger.debug(f"Waited for global rate limit before {url}")

        backoff = self.config.trading.rate_limit_backoff_base
        max_backoff = self.config.trading.rate_limit_backoff_max

        for attempt in range(max_retries + 1):
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        await limiter.record_success()
                        return await response.json()
                    elif response.status == 429:
                        # Use global rate limiter for coordinated backoff
                        await handle_response_status(429, url)
                        if attempt < max_retries:
                            continue  # Retry after global backoff
                        else:
                            logger.error(f"Rate limit exhausted after {max_retries} retries: {url}")
                            return None
                    elif response.status >= 500:
                        # Server error, retry with backoff
                        if attempt < max_retries:
                            sleep_time = min(backoff * (2 ** attempt), max_backoff)
                            logger.warning(
                                f"Server error {response.status} on {url}, "
                                f"retrying in {sleep_time:.1f}s"
                            )
                            await asyncio.sleep(sleep_time)
                            continue
                        else:
                            logger.error(f"Server error persisted after {max_retries} retries: {url}")
                            return None
                    else:
                        logger.error(f"Failed to fetch {url}: HTTP {response.status}")
                        return None

            except asyncio.TimeoutError:
                if attempt < max_retries:
                    sleep_time = min(backoff * (2 ** attempt), max_backoff)
                    logger.warning(f"Timeout on {url}, retrying in {sleep_time:.1f}s")
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"Timeout exhausted after {max_retries} retries: {url}")
                    return None

            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    sleep_time = min(backoff * (2 ** attempt), max_backoff)
                    logger.warning(f"Client error on {url}: {e}, retrying in {sleep_time:.1f}s")
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"Client error exhausted after {max_retries} retries: {url}")
                    return None

            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                return None

        return None

    async def _fetch_json(self, url: str, params: Optional[Dict] = None) -> Optional[Any]:
        """
        Fetch JSON from a URL (wrapper for backward compatibility).

        Args:
            url: URL to fetch.
            params: Query parameters.

        Returns:
            Parsed JSON response or None on error.
        """
        return await self._fetch_json_with_retry(url, params)

    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse a datetime string to datetime object."""
        if not dt_str:
            return None
        try:
            # Handle various datetime formats
            if dt_str.endswith('Z'):
                dt_str = dt_str[:-1] + '+00:00'
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except ValueError:
            return None

    def _parse_market(self, market_data: Dict, event_data: Optional[Dict] = None) -> Optional[Market]:
        """
        Parse market data from API response.

        Args:
            market_data: Raw market data from API.
            event_data: Parent event data if available.

        Returns:
            Parsed Market object or None if invalid.
        """
        try:
            # Extract outcomes and token IDs
            # API returns JSON-encoded strings for these fields
            outcomes_raw = market_data.get("outcomes", "[]")
            outcome_prices_raw = market_data.get("outcomePrices", "[]")
            clob_token_ids_raw = market_data.get("clobTokenIds", "[]")

            # Parse JSON strings to lists
            try:
                outcomes_names = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            except (json.JSONDecodeError, TypeError):
                outcomes_names = []

            try:
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            except (json.JSONDecodeError, TypeError):
                outcome_prices = []

            try:
                clob_token_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else clob_token_ids_raw
            except (json.JSONDecodeError, TypeError):
                clob_token_ids = []

            if not outcomes_names or not clob_token_ids:
                return None

            # Parse prices safely
            prices = []
            for p in outcome_prices:
                try:
                    prices.append(float(p) if p else 0.0)
                except (ValueError, TypeError):
                    prices.append(0.0)

            # Build outcomes list
            outcomes = []
            for i, name in enumerate(outcomes_names):
                token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
                price = prices[i] if i < len(prices) else 0.0
                if token_id:
                    outcomes.append(Outcome(name=name, token_id=token_id, price=price))

            if not outcomes:
                return None

            # Parse volume
            volume = 0.0
            volume_val = market_data.get("volume") or market_data.get("volumeNum") or 0
            try:
                volume = float(volume_val)
            except (ValueError, TypeError):
                pass

            # Parse liquidity
            liquidity = 0.0
            liquidity_val = market_data.get("liquidity") or 0
            try:
                liquidity = float(liquidity_val)
            except (ValueError, TypeError):
                pass

            # Parse negRisk field (winner-take-all markets)
            neg_risk = market_data.get("negRisk", False)
            if isinstance(neg_risk, str):
                neg_risk = neg_risk.lower() == "true"

            market = Market(
                market_id=market_data.get("id", ""),
                condition_id=market_data.get("conditionId", ""),
                question=market_data.get("question", ""),
                slug=market_data.get("slug", ""),
                outcomes=outcomes,
                volume=volume,
                liquidity=liquidity,
                start_date=self._parse_datetime(market_data.get("startDate")),
                end_date=self._parse_datetime(market_data.get("endDate")),
                is_active=market_data.get("active", True),
                is_closed=market_data.get("closed", False),
                best_bid=float(market_data["bestBid"]) if market_data.get("bestBid") else None,
                best_ask=float(market_data["bestAsk"]) if market_data.get("bestAsk") else None,
                last_trade_price=float(market_data["lastTradePrice"]) if market_data.get("lastTradePrice") else None,
                neg_risk=neg_risk,
            )

            # Add event context if available
            if event_data:
                market.event_id = event_data.get("id")
                market.event_title = event_data.get("title")

            return market

        except Exception as e:
            logger.debug(f"Failed to parse market: {e}")
            return None

    async def fetch_sports_events(self, series_id: Optional[int] = None) -> List[Event]:
        """
        Fetch active sports events from Gamma API.

        Args:
            series_id: Optional specific series ID to filter by.

        Returns:
            List of Event objects.
        """
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
        }

        if series_id:
            params["series_id"] = str(series_id)

        url = f"{self.gamma_endpoint}/events"
        data = await self._fetch_json(url, params)

        if not data:
            return []

        events = []
        for event_data in data:
            try:
                markets = []
                for market_data in event_data.get("markets", []):
                    market = self._parse_market(market_data, event_data)
                    if market:
                        markets.append(market)

                if markets:  # Only include events with valid markets
                    event = Event(
                        event_id=event_data.get("id", ""),
                        title=event_data.get("title", ""),
                        slug=event_data.get("slug", ""),
                        markets=markets,
                        volume=float(event_data.get("volume", 0) or 0),
                        liquidity=float(event_data.get("liquidity", 0) or 0),
                        start_date=self._parse_datetime(event_data.get("startDate")),
                        end_date=self._parse_datetime(event_data.get("endDate")),
                        is_active=event_data.get("active", True),
                        is_closed=event_data.get("closed", False),
                        series_id=series_id,
                        tags=[t.get("label", "") for t in event_data.get("tags", [])],
                    )
                    events.append(event)

            except Exception as e:
                logger.debug(f"Failed to parse event: {e}")
                continue

        return events

    async def fetch_all_priority_sports(self) -> List[Event]:
        """
        Fetch events from all priority sports series in parallel.

        Returns:
            Combined list of events from all priority sports.
        """
        tasks = []
        for sport_code, series_id in self.config.sports.priority_series.items():
            tasks.append(self._fetch_series_with_code(sport_code, series_id))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching series: {result}")
            elif result:
                all_events.extend(result)

        logger.info(f"Fetched {len(all_events)} total events from priority sports")
        return all_events

    async def _fetch_series_with_code(self, sport_code: str, series_id: int) -> List[Event]:
        """Fetch events for a series and tag with sport code."""
        events = await self.fetch_sports_events(series_id)
        for event in events:
            event.series_id = series_id
        logger.debug(f"Fetched {len(events)} events for {sport_code} (series {series_id})")
        return events

    def filter_high_volume_markets(self, events: List[Event]) -> List[Market]:
        """
        Filter markets by volume threshold.

        Args:
            events: List of events to filter.

        Returns:
            List of markets meeting volume criteria.
        """
        min_volume = self.config.trading.min_volume_usd
        filtered_markets = []

        for event in events:
            for market in event.markets:
                if market.volume >= min_volume and market.is_active and not market.is_closed:
                    market.event_id = event.event_id
                    market.event_title = event.title
                    market.series_id = event.series_id
                    filtered_markets.append(market)

        logger.info(f"Filtered to {len(filtered_markets)} markets with volume >= ${min_volume:,.0f}")
        return filtered_markets

    def estimate_in_play(self, market: Market) -> bool:
        """
        Estimate if a market is currently in-play using multiple signals.

        Enhanced detection per Round 3 audit:
        1. neg_risk markets (winner-take-all) are more likely in-play tradeable
        2. Time-based heuristics (start_date <= now < end_date + buffer)
        3. Active + volume + bid/ask spread present
        4. Spread tightness (in-play sports have tight spreads due to liquidity)

        Uses game_durations from SportsConfig for sport-specific timing.

        Args:
            market: Market to check.

        Returns:
            True if market appears to be in-play.
        """
        now = datetime.now(timezone.utc)

        # Quick reject: not active or closed
        if not market.is_active or market.is_closed:
            return False

        from datetime import timedelta

        # Priority 1: neg_risk markets with active trading
        # Winner-take-all markets (neg_risk=True) are typically live sports
        # and are the primary target for rn1-style trading
        if market.neg_risk and market.is_active:
            # neg_risk + active + has quotes = likely in-play
            if market.best_bid is not None and market.best_ask is not None:
                # Tight spread suggests active in-play market
                spread = market.best_ask - market.best_bid
                if spread < 0.10:  # Less than 10 cents spread
                    return True

        # Priority 2: Time-based heuristics with start/end dates
        if market.start_date:
            if market.start_date > now:
                return False  # Event hasn't started

            # Get estimated duration from config or use sport detection
            estimated_duration_minutes = self._get_sport_duration(market)
            estimated_end = market.start_date + timedelta(minutes=estimated_duration_minutes)

            # Check if within game window
            if now < estimated_end:
                return True

            # Past estimated end - check extended window for overtime/delays
            if market.is_active and not market.is_closed:
                extended_end = estimated_end + timedelta(minutes=60)
                if now < extended_end:
                    return True

        # Priority 3: End date check (if available)
        if market.end_date:
            # If end_date is in future and start_date is in past (or not set)
            if market.end_date > now:
                if market.start_date is None or market.start_date <= now:
                    # Within the event window
                    return True

        # Priority 4: Active market with trading activity
        # If no timing info, use activity signals
        if market.is_active and market.volume > 0:
            # Has both bid and ask = active market maker presence
            if market.best_ask is not None and market.best_bid is not None:
                return True

        return False

    def get_in_play_score(self, market: Market) -> float:
        """
        Get in-play confidence score for prioritization.

        Higher scores = more confident the market is actively in-play.
        Used for sorting/prioritizing markets.

        Enhanced scoring per Round 3 audit:
        - neg_risk markets get significant boost (rn1 focus)
        - Tight spreads indicate active in-play
        - Time-into-game affects score

        Args:
            market: Market to score.

        Returns:
            Score from 0.0 (not in-play) to 1.0 (definitely in-play).
        """
        if not self.estimate_in_play(market):
            return 0.0

        score = 0.5  # Base score if we think it's in-play

        now = datetime.now(timezone.utc)

        # SIGNIFICANT boost for neg_risk markets (rn1 primary target)
        # Winner-take-all markets are key for sports arb
        if market.neg_risk:
            score += 0.15  # Raised from 0.05

        # Boost for tight spreads (indicates active in-play trading)
        if market.best_bid is not None and market.best_ask is not None:
            spread = market.best_ask - market.best_bid
            if spread < 0.03:  # Very tight (<3 cents)
                score += 0.15
            elif spread < 0.05:  # Tight (<5 cents)
                score += 0.10
            elif spread < 0.10:  # Moderate (<10 cents)
                score += 0.05

        # Boost score based on time into game
        if market.start_date:
            minutes_into_game = (now - market.start_date).total_seconds() / 60
            duration = self._get_sport_duration(market)

            # Peak score in middle of game (most volatility)
            game_progress = minutes_into_game / duration
            if 0.2 <= game_progress <= 0.8:
                score += 0.2  # In the meat of the game
            elif 0.8 < game_progress <= 1.0:
                score += 0.15  # Late game (still good for arbs)
            elif 0 < game_progress < 0.2:
                score += 0.1  # Early game

        # Boost for high volume (more liquidity = better arb execution)
        if market.volume >= 50000:
            score += 0.1
        elif market.volume >= 20000:
            score += 0.05

        return min(1.0, score)

    def _get_sport_duration(self, market: Market) -> int:
        """
        Get estimated game duration in minutes for a market.

        Uses series_id to identify sport, falls back to title matching.

        Args:
            market: Market to get duration for.

        Returns:
            Estimated duration in minutes.
        """
        game_durations = self.config.sports.game_durations
        priority_series = self.config.sports.priority_series

        # Try to match by series_id first (most accurate)
        if market.series_id:
            for sport_code, series_id in priority_series.items():
                if market.series_id == series_id:
                    # Map sport code to duration key
                    if sport_code in ["epl", "lal", "bun", "fl1", "sea", "ucl", "uel", "mls", "acn"]:
                        return game_durations.get("soccer", 120)
                    elif sport_code in ["nba", "ncaab"]:
                        return game_durations.get("basketball", 150)
                    elif sport_code == "nfl":
                        return game_durations.get("football", 210)
                    elif sport_code == "nhl":
                        return game_durations.get("hockey", 150)
                    elif sport_code in ["atp", "wta"]:
                        return game_durations.get("tennis", 180)
                    elif sport_code in ["cs2", "dota2", "lol", "val"]:
                        return game_durations.get("esports", 90)
                    elif sport_code == "mma":
                        return game_durations.get("mma", 30)

        # Fall back to title matching
        title_lower = (market.event_title or "").lower() + " " + (market.question or "").lower()

        if any(s in title_lower for s in ["soccer", "premier", "serie a", "la liga", "bundesliga", "champions league"]):
            return game_durations.get("soccer", 120)
        elif any(s in title_lower for s in ["nba", "basketball", "ncaa"]):
            return game_durations.get("basketball", 150)
        elif any(s in title_lower for s in ["nfl", "super bowl"]):
            return game_durations.get("football", 210)
        elif any(s in title_lower for s in ["nhl", "hockey"]):
            return game_durations.get("hockey", 150)
        elif any(s in title_lower for s in ["tennis", "atp", "wta", "wimbledon", "us open"]):
            return game_durations.get("tennis", 180)
        elif any(s in title_lower for s in ["ufc", "mma", "fight", "bellator"]):
            return game_durations.get("mma", 30)
        elif any(s in title_lower for s in ["csgo", "cs2", "dota", "league of legends", "valorant", "esport"]):
            return game_durations.get("esports", 90)

        # Default to 3 hours for unknown sports
        return 180

    def filter_in_play_markets(self, markets: List[Market]) -> List[Market]:
        """
        Filter to only in-play markets.

        Args:
            markets: List of markets to filter.

        Returns:
            List of markets currently in-play.
        """
        in_play = [m for m in markets if self.estimate_in_play(m)]
        logger.info(f"Identified {len(in_play)} in-play markets")
        return in_play

    async def fetch_markets_from_clob(self) -> List[Market]:
        """
        PRIMARY: Fetch active markets directly from CLOB API.

        Per Round 4 audit: CLOB /markets is the RELIABLE source for active markets.
        Gamma API is flaky with undocumented rate limits and outages.
        rn1 dominates live sports by always having fresh active market list.

        Features:
        - 15 retries with 15s exponential backoff (vs 2 before)
        - Prioritizes neg_risk markets (rn1 focus)
        - Parses all market data including open_interest

        Returns:
            List of Market objects from CLOB.
        """
        try:
            url = f"{self.clob_endpoint}/markets"
            params = {"active": "true", "closed": "false"}

            # 15 retries with aggressive backoff per audit
            data = await self._fetch_json_with_retry(url, params, max_retries=15)
            if not data:
                logger.warning("CLOB /markets returned no data after retries")
                return []

            markets = []
            neg_risk_count = 0

            for market_data in data:
                try:
                    # CLOB market format
                    condition_id = market_data.get("condition_id", "")
                    if not condition_id:
                        continue

                    # Parse tokens
                    tokens = market_data.get("tokens", [])
                    if len(tokens) < 2:
                        continue

                    outcomes = []
                    for token in tokens:
                        token_id = token.get("token_id", "")
                        outcome_name = token.get("outcome", "")
                        price = float(token.get("price", 0.5))
                        if token_id:
                            outcomes.append(Outcome(name=outcome_name, token_id=token_id, price=price))

                    if not outcomes:
                        continue

                    # Parse neg_risk (critical for rn1-style trading)
                    neg_risk = market_data.get("neg_risk", False)
                    if isinstance(neg_risk, str):
                        neg_risk = neg_risk.lower() == "true"

                    if neg_risk:
                        neg_risk_count += 1

                    # Parse additional fields from CLOB
                    open_interest = float(market_data.get("open_interest", 0) or 0)

                    market = Market(
                        market_id=market_data.get("market_id", condition_id),
                        condition_id=condition_id,
                        question=market_data.get("question", "Unknown"),
                        slug=market_data.get("market_slug", ""),
                        outcomes=outcomes,
                        volume=float(market_data.get("volume", 0) or 0),
                        liquidity=float(market_data.get("liquidity", 0) or 0) + open_interest,
                        is_active=market_data.get("active", True),
                        is_closed=market_data.get("closed", False),
                        neg_risk=neg_risk,
                    )
                    markets.append(market)

                except Exception as e:
                    logger.debug(f"Failed to parse CLOB market: {e}")
                    continue

            logger.info(f"CLOB PRIMARY: fetched {len(markets)} markets ({neg_risk_count} neg_risk)")
            return markets

        except Exception as e:
            logger.error(f"CLOB primary fetch failed: {e}")
            return []

    def _is_in_play_from_status(self, market_data: Dict) -> bool:
        """
        Check if market is in-play using API status fields.

        Looks for explicit in-play indicators from the API:
        - enableOrderBook: True (active trading)
        - game_start_time: Past but game not ended
        - Tags containing 'in-play', 'live', etc.

        Args:
            market_data: Raw market data from API.

        Returns:
            True if market appears to be in-play based on status.
        """
        # Check for explicit in-play tag
        tags = market_data.get("tags", [])
        if isinstance(tags, list):
            tag_labels = [t.get("label", "").lower() if isinstance(t, dict) else str(t).lower() for t in tags]
            if any(label in ["in-play", "live", "in play", "in_play"] for label in tag_labels):
                return True

        # Check if orderbook is enabled (live trading = likely in-play)
        if market_data.get("enableOrderBook") is True:
            # Also check it's not closed/resolved
            if not market_data.get("closed", False) and market_data.get("active", True):
                # Check game_start_time if available
                game_start = market_data.get("game_start_time") or market_data.get("startDate")
                if game_start:
                    start_dt = self._parse_datetime(str(game_start))
                    if start_dt and start_dt <= datetime.now(timezone.utc):
                        return True

        return False

    def _parse_event(self, event_data: Dict) -> Optional[Event]:
        """
        Parse event data including its markets.

        Args:
            event_data: Raw event data from API.

        Returns:
            Event object or None if parsing fails.
        """
        try:
            event = Event(
                event_id=event_data.get("id", ""),
                title=event_data.get("title", ""),
                slug=event_data.get("slug", ""),
                is_active=event_data.get("active", True),
                is_closed=event_data.get("closed", False),
                start_date=self._parse_datetime(event_data.get("startDate")),
                end_date=self._parse_datetime(event_data.get("endDate")),
            )

            # Parse markets in this event
            markets_data = event_data.get("markets", [])
            for market_data in markets_data:
                market = self._parse_market(market_data, event_data)
                if market:
                    event.markets.append(market)
                    event.volume += market.volume

            return event if event.markets else None

        except Exception as e:
            logger.debug(f"Failed to parse event: {e}")
            return None

    async def discover_target_markets(self, include_pre_match: bool = True) -> List[Market]:
        """
        Main discovery method: fetch and filter target markets.

        Per Round 4 audit - TRUE CLOB-PRIMARY approach:
        1. PRIMARY: CLOB /markets API (reliable, 15 retries, has all active markets)
        2. PARALLEL ENRICH: Gamma API for question/slug/series (fire-and-forget cache)
        3. AGGRESSIVE CACHE: 72h fallback if all APIs fail

        rn1 dominates live sports by ALWAYS having fresh market list.
        Gamma is known flaky - never depend on it for primary discovery.

        Args:
            include_pre_match: Whether to include pre-match markets (not just in-play).

        Returns:
            List of target markets for arbitrage scanning.
        """
        markets = []
        min_volume = self.config.trading.min_volume_usd

        # PRIMARY: CLOB /markets API - ALWAYS try this first
        # This is the RELIABLE source per audit
        clob_success = False
        try:
            clob_markets = await self.fetch_markets_from_clob()
            if clob_markets:
                # Filter by volume and active status
                markets = [m for m in clob_markets if m.volume >= min_volume and m.is_active]

                # PRIORITIZE neg_risk markets (rn1 focus on winner-take-all sports)
                neg_risk_markets = [m for m in markets if m.neg_risk]
                non_neg_risk = [m for m in markets if not m.neg_risk]
                markets = neg_risk_markets + non_neg_risk  # neg_risk first

                clob_success = True
                logger.info(f"CLOB PRIMARY: {len(markets)} markets ({len(neg_risk_markets)} neg_risk priority)")
        except Exception as e:
            logger.error(f"CLOB PRIMARY failed: {e}")

        # PARALLEL ENRICH: Fire-and-forget Gamma enrichment (don't block on it)
        # Only for question/slug/series data - NOT for market discovery
        if clob_success and len(markets) > 0:
            # Launch Gamma enrichment as background task (don't await)
            try:
                asyncio.create_task(self._enrich_markets_from_gamma(markets))
            except Exception:
                pass  # Enrichment failure is non-fatal

        # FALLBACK: Only if CLOB completely failed, try Gamma
        if not clob_success or len(markets) == 0:
            logger.warning("CLOB failed - falling back to Gamma API (less reliable)")
            try:
                events = await self.fetch_all_priority_sports()
                if events:
                    gamma_markets = self.filter_high_volume_markets(events)
                    # Merge: add Gamma markets not already in CLOB list
                    existing_ids = {m.condition_id for m in markets}
                    for gm in gamma_markets:
                        if gm.condition_id not in existing_ids:
                            markets.append(gm)
                    self._consecutive_gamma_failures = 0
                    logger.debug(f"Gamma supplement: total {len(markets)} markets")
                else:
                    self._consecutive_gamma_failures += 1
            except Exception as e:
                self._consecutive_gamma_failures += 1
                logger.debug(f"Gamma supplement failed: {e}")

        # If still no markets, use fallback cache
        if not markets:
            if self._fallback_cache and self._fallback_cache_time:
                cache_age = (datetime.now(timezone.utc) - self._fallback_cache_time).total_seconds()
                if cache_age < self._fallback_cache_ttl_seconds:
                    logger.warning(f"Using fallback cache ({len(self._fallback_cache)} markets, {cache_age:.0f}s old)")
                    markets = self._fallback_cache
                else:
                    logger.error(f"Fallback cache expired ({cache_age:.0f}s old), no markets available")
            else:
                logger.error("No markets available and no fallback cache")

        # Optionally filter to in-play only
        if markets and not include_pre_match:
            markets = self.filter_in_play_markets(markets)

        # Prioritize in-play markets (rn1 pattern: heavy in-play focus)
        # Sort by: in-play score (higher = better), then by volume
        if markets:
            markets.sort(key=lambda m: (-self.get_in_play_score(m), -m.volume))

        # Limit to max concurrent markets
        max_markets = self.config.trading.max_concurrent_markets
        if len(markets) > max_markets:
            markets = markets[:max_markets]
            logger.info(f"Limited to top {max_markets} markets (in-play prioritized)")

        # Update caches
        if markets:
            self._cached_markets = markets
            self._last_refresh = datetime.now(timezone.utc)

            # Update fallback cache (always update if we got fresh data)
            self._fallback_cache = markets.copy()
            self._fallback_cache_time = datetime.now(timezone.utc)

        # Log in-play breakdown
        in_play_count = sum(1 for m in markets if self.estimate_in_play(m))
        logger.info(f"Discovered {len(markets)} markets ({in_play_count} in-play, {len(markets) - in_play_count} pre-match)")

        return markets

    async def _enrich_markets_from_gamma(self, markets: List[Market]):
        """
        Fire-and-forget enrichment of CLOB markets with Gamma data.

        This runs in background to add question/slug/series/tags to markets
        without blocking the main discovery flow.

        Args:
            markets: List of markets to enrich (modified in place).
        """
        try:
            # Fetch Gamma events in parallel (don't block main flow)
            events = await self.fetch_all_priority_sports()
            if not events:
                return

            # Build lookup by condition_id
            gamma_lookup = {}
            for event in events:
                for market in event.markets:
                    gamma_lookup[market.condition_id] = {
                        "question": market.question,
                        "slug": market.slug,
                        "series_id": market.series_id,
                        "start_date": market.start_date,
                        "end_date": market.end_date,
                    }

            # Enrich CLOB markets with Gamma data
            enriched = 0
            for market in markets:
                gamma_data = gamma_lookup.get(market.condition_id)
                if gamma_data:
                    if not market.question or market.question == "Unknown":
                        market.question = gamma_data.get("question", market.question)
                    if not market.slug:
                        market.slug = gamma_data.get("slug", "")
                    if not market.series_id:
                        market.series_id = gamma_data.get("series_id", "")
                    if not market.start_date:
                        market.start_date = gamma_data.get("start_date")
                    if not market.end_date:
                        market.end_date = gamma_data.get("end_date")
                    enriched += 1

            if enriched > 0:
                logger.debug(f"Gamma enriched {enriched}/{len(markets)} markets")

        except Exception as e:
            # Enrichment failure is non-fatal - log and continue
            logger.debug(f"Gamma enrichment failed (non-fatal): {e}")

    def get_cached_markets(self) -> List[Market]:
        """Get cached markets from last discovery."""
        return self._cached_markets

    def needs_refresh(self) -> bool:
        """Check if market list needs refresh."""
        if not self._last_refresh:
            return True

        from datetime import timedelta
        refresh_interval = timedelta(seconds=self.config.trading.market_refresh_interval_seconds)
        return datetime.now(timezone.utc) - self._last_refresh > refresh_interval
