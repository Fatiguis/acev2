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
        self._session: Optional[aiohttp.ClientSession] = None
        self._cached_markets: List[Market] = []
        self._last_refresh: Optional[datetime] = None

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
        backoff = self.config.trading.rate_limit_backoff_base
        max_backoff = self.config.trading.rate_limit_backoff_max

        for attempt in range(max_retries + 1):
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        if attempt < max_retries:
                            sleep_time = min(backoff * (2 ** attempt), max_backoff)
                            logger.warning(
                                f"Rate limited on {url}, "
                                f"backing off {sleep_time:.1f}s (attempt {attempt + 1}/{max_retries})"
                            )
                            await asyncio.sleep(sleep_time)
                            continue
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
        Estimate if a market is currently in-play using config durations.

        Uses game_durations from SportsConfig for precise sport-specific timing.

        Args:
            market: Market to check.

        Returns:
            True if market appears to be in-play.
        """
        now = datetime.now(timezone.utc)

        if market.start_date:
            if market.start_date > now:
                return False  # Event hasn't started

            # Get estimated duration from config or use sport detection
            estimated_duration_minutes = self._get_sport_duration(market)

            from datetime import timedelta
            estimated_end = market.start_date + timedelta(minutes=estimated_duration_minutes)

            if now < estimated_end:
                return True

        return False

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

    async def discover_target_markets(self, include_pre_match: bool = True) -> List[Market]:
        """
        Main discovery method: fetch and filter target markets.

        Args:
            include_pre_match: Whether to include pre-match markets (not just in-play).

        Returns:
            List of target markets for arbitrage scanning.
        """
        # Fetch all priority sports events
        events = await self.fetch_all_priority_sports()

        # Filter by volume
        markets = self.filter_high_volume_markets(events)

        # Optionally filter to in-play only
        if not include_pre_match:
            markets = self.filter_in_play_markets(markets)

        # Limit to max concurrent markets
        max_markets = self.config.trading.max_concurrent_markets
        if len(markets) > max_markets:
            # Sort by volume and take top N
            markets.sort(key=lambda m: m.volume, reverse=True)
            markets = markets[:max_markets]
            logger.info(f"Limited to top {max_markets} markets by volume")

        # Cache results
        self._cached_markets = markets
        self._last_refresh = datetime.now(timezone.utc)

        logger.info(f"Discovered {len(markets)} target markets for arbitrage scanning")
        return markets

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
