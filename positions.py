"""
Position Monitoring & Auto-Claim Module.
Monitors positions and automatically claims winnings on market resolution.
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Callable, TypeVar
from enum import Enum
import json

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from web3.exceptions import TransactionNotFound
from eth_account import Account
import aiohttp

from config import BotConfig

logger = logging.getLogger(__name__)

T = TypeVar('T')


def _exponential_backoff(attempt: int, base: float = 1.0, max_backoff: float = 30.0) -> float:
    """Calculate exponential backoff delay."""
    return min(base * (2 ** attempt), max_backoff)


async def _retry_rpc_with_backoff(
    func: Callable[[], T],
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    operation_name: str = "RPC call"
) -> T:
    """
    Retry an RPC call with exponential backoff.

    Args:
        func: Sync function to retry (Web3 calls are sync).
        max_retries: Maximum number of retries.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds.
        operation_name: Name for logging.

    Returns:
        Result of the function.

    Raises:
        Exception: If all retries fail.
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                delay = _exponential_backoff(attempt, base_delay, max_delay)
                logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(f"{operation_name} failed after {max_retries + 1} attempts: {e}")

    raise last_exception


# ConditionalTokens ABI (minimal for redeeming)
CONDITIONAL_TOKENS_ABI = json.loads('''
[
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"}
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"}
        ],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]
''')


class PositionStatus(Enum):
    """Status of a position."""
    OPEN = "open"
    WINNING = "winning"
    LOSING = "losing"
    CLAIMED = "claimed"
    DISPUTED = "disputed"  # Market is disputed (payoutDenominator == 0 after resolution)
    UNKNOWN = "unknown"


@dataclass
class Position:
    """Represents a position in a market outcome."""
    token_id: str
    condition_id: str
    outcome_name: str
    market_question: str
    size_shares: float
    avg_price: float
    cost_basis_usd: float
    current_price: float = 0.0
    current_value_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    status: PositionStatus = PositionStatus.OPEN
    is_resolved: bool = False
    is_winner: bool = False
    claimable_amount_usd: float = 0.0
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class PositionSummary:
    """Summary of all positions."""
    total_positions: int = 0
    total_open_value_usd: float = 0.0
    total_unrealized_pnl_usd: float = 0.0
    total_claimable_usd: float = 0.0
    winning_positions: int = 0
    positions: List[Position] = field(default_factory=list)


class PositionMonitor:
    """Monitors positions and handles auto-claiming of winnings."""

    def __init__(self, config: BotConfig):
        """
        Initialize the position monitor.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self._web3: Optional[Web3] = None
        self._contract: Optional[Any] = None
        self._wallet_address: Optional[str] = None
        self._account: Optional[Account] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Stats
        self._claims_count = 0
        self._total_claimed_usd = 0.0

    def initialize(self):
        """Initialize web3 connection and contract."""
        if self.config.dry_run:
            logger.info("Position monitor in dry run mode - no web3 initialization")
            return

        # Initialize Web3
        self._web3 = Web3(Web3.HTTPProvider(self.config.network.polygon_rpc))
        self._web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

        if not self._web3.is_connected():
            raise ConnectionError("Failed to connect to Polygon RPC")

        # Initialize account
        if self.config.wallet.private_key:
            self._account = Account.from_key(self.config.wallet.private_key)
            self._wallet_address = self._account.address
            logger.info(f"Position monitor initialized for wallet: {self._wallet_address}")

        # Initialize contract
        self._contract = self._web3.eth.contract(
            address=Web3.to_checksum_address(self.config.network.conditional_tokens_address),
            abi=CONDITIONAL_TOKENS_ABI
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        """Close resources."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def fetch_positions(self) -> List[Position]:
        """
        Fetch current positions from the CLOB API.

        Returns:
            List of current positions.
        """
        if self.config.dry_run:
            logger.debug("Fetching positions in dry run mode - returning empty list")
            return []

        session = await self._get_session()
        url = f"{self.config.network.clob_endpoint}/positions"

        # Note: This endpoint requires L2 authentication headers
        # The actual implementation would use the authenticated client
        # For now, we'll use the data available from the client

        try:
            # In practice, use the CLOB client's get_positions or similar method
            # This is a placeholder that would be replaced with actual API call
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_positions(data)
                else:
                    logger.debug(f"Failed to fetch positions: HTTP {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []

    def _parse_positions(self, data: List[Dict]) -> List[Position]:
        """Parse positions from API response."""
        positions = []
        for pos_data in data:
            try:
                position = Position(
                    token_id=pos_data.get("asset", ""),
                    condition_id=pos_data.get("conditionId", ""),
                    outcome_name=pos_data.get("outcome", ""),
                    market_question=pos_data.get("question", ""),
                    size_shares=float(pos_data.get("size", 0)),
                    avg_price=float(pos_data.get("avgPrice", 0)),
                    cost_basis_usd=float(pos_data.get("costBasis", 0)),
                    current_price=float(pos_data.get("currentPrice", 0)),
                    current_value_usd=float(pos_data.get("currentValue", 0)),
                    unrealized_pnl_usd=float(pos_data.get("unrealizedPnl", 0)),
                )
                positions.append(position)
            except Exception as e:
                logger.debug(f"Failed to parse position: {e}")
                continue
        return positions

    async def check_resolved_markets(self, positions: List[Position]) -> List[Position]:
        """
        Check which positions are in resolved markets.

        Handles disputed markets: payoutDenominator == 0 indicates an
        invalid/disputed market where redeem will fail. These are logged
        and skipped.

        Args:
            positions: List of positions to check.

        Returns:
            List of positions in resolved markets with claimable amounts.
        """
        resolved = []
        disputed_count = 0

        for position in positions:
            if not position.condition_id:
                continue

            try:
                # Check if market is resolved by checking payoutDenominator
                if self._contract and self._web3:
                    condition_id_bytes = bytes.fromhex(position.condition_id.replace("0x", ""))
                    payout_denominator = self._contract.functions.payoutDenominator(
                        condition_id_bytes
                    ).call()

                    # payoutDenominator == 0 means disputed/invalid market
                    # Redeem will fail on these - skip and log
                    if payout_denominator == 0:
                        position.status = PositionStatus.DISPUTED
                        position.is_resolved = False
                        disputed_count += 1
                        logger.warning(
                            f"DISPUTED market detected (payoutDenominator=0): "
                            f"{position.market_question[:40]}... | "
                            f"condition_id: {position.condition_id[:16]}... | "
                            f"Skipping claim (would fail)"
                        )
                        continue

                    if payout_denominator > 0:
                        position.is_resolved = True
                        # If we have shares and it's resolved, we have claimable amount
                        if position.size_shares > 0:
                            position.claimable_amount_usd = position.size_shares
                            position.status = PositionStatus.WINNING
                            resolved.append(position)
                            logger.info(
                                f"Found resolved position: {position.market_question[:40]}... "
                                f"Claimable: ${position.claimable_amount_usd:.2f}"
                            )

            except Exception as e:
                logger.debug(f"Error checking resolution for {position.condition_id}: {e}")
                continue

        if disputed_count > 0:
            logger.info(f"Skipped {disputed_count} disputed markets (payoutDenominator=0)")

        return resolved

    async def claim_winnings(self, position: Position, max_retries: int = 3) -> bool:
        """
        Claim winnings for a resolved position with exponential backoff retry.

        Args:
            position: The position to claim.
            max_retries: Maximum claim attempts.

        Returns:
            True if claim was successful.
        """
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would claim ${position.claimable_amount_usd:.2f} from {position.market_question[:40]}...")
            return True

        if not self._contract or not self._web3 or not self._account:
            logger.error("Web3 not initialized for claiming")
            return False

        for attempt in range(max_retries):
            try:
                # Prepare the redeem transaction
                condition_id_bytes = bytes.fromhex(position.condition_id.replace("0x", ""))

                # Index sets for binary markets (1 for Yes, 2 for No)
                index_sets = [1, 2]  # Claim both to be safe

                # Get nonce with retry - use 'pending' to include pending txs
                # This prevents nonce reuse on RPC lag (web3 best practice)
                nonce = await _retry_rpc_with_backoff(
                    lambda: self._web3.eth.get_transaction_count(
                        self._wallet_address,
                        block_identifier='pending'
                    ),
                    max_retries=3,
                    operation_name="get_nonce"
                )

                # Get gas price with retry
                gas_price = await _retry_rpc_with_backoff(
                    lambda: self._web3.eth.gas_price,
                    max_retries=3,
                    operation_name="get_gas_price"
                )

                # Build transaction
                tx = self._contract.functions.redeemPositions(
                    Web3.to_checksum_address(self.config.network.usdc_address),
                    bytes(32),  # parentCollectionId (0 for root)
                    condition_id_bytes,
                    index_sets
                ).build_transaction({
                    'from': self._wallet_address,
                    'nonce': nonce,
                    'gas': 200000,
                    'gasPrice': gas_price,
                    'chainId': self.config.network.chain_id
                })

                # Sign and send
                signed_tx = self._account.sign_transaction(tx)
                tx_hash = self._web3.eth.send_raw_transaction(signed_tx.raw_transaction)

                # Wait for confirmation with retry logic
                receipt = await _retry_rpc_with_backoff(
                    lambda: self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120),
                    max_retries=3,
                    base_delay=2.0,
                    operation_name="wait_for_receipt"
                )

                if receipt.status == 1:
                    self._claims_count += 1
                    self._total_claimed_usd += position.claimable_amount_usd
                    position.status = PositionStatus.CLAIMED
                    logger.info(
                        f"Successfully claimed ${position.claimable_amount_usd:.2f} "
                        f"| TX: {tx_hash.hex()}"
                    )
                    return True
                else:
                    logger.error(f"Claim transaction failed: {tx_hash.hex()}")
                    return False

            except Exception as e:
                backoff_delay = _exponential_backoff(attempt, base=2.0, max_backoff=30.0)
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Claim attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {backoff_delay:.1f}s..."
                    )
                    await asyncio.sleep(backoff_delay)
                else:
                    logger.error(f"Failed to claim winnings after {max_retries} attempts: {e}")
                    return False

        return False

    async def auto_claim_all(self) -> int:
        """
        Automatically claim all available winnings.

        Returns:
            Number of successful claims.
        """
        positions = await self.fetch_positions()
        resolved = await self.check_resolved_markets(positions)

        claims = 0
        for position in resolved:
            if position.claimable_amount_usd > 0:
                if await self.claim_winnings(position):
                    claims += 1
                # Small delay between claims
                await asyncio.sleep(1)

        if claims > 0:
            logger.info(f"Auto-claimed {claims} positions")

        return claims

    def get_position_summary(self, positions: List[Position]) -> PositionSummary:
        """
        Get summary of all positions.

        Args:
            positions: List of positions.

        Returns:
            PositionSummary with aggregated data.
        """
        summary = PositionSummary(positions=positions)
        summary.total_positions = len(positions)

        for pos in positions:
            summary.total_open_value_usd += pos.current_value_usd
            summary.total_unrealized_pnl_usd += pos.unrealized_pnl_usd
            summary.total_claimable_usd += pos.claimable_amount_usd
            if pos.status == PositionStatus.WINNING:
                summary.winning_positions += 1

        return summary

    def get_stats(self) -> Dict[str, Any]:
        """Get claiming statistics."""
        return {
            "claims_count": self._claims_count,
            "total_claimed_usd": self._total_claimed_usd
        }


class ProfitLocker:
    """Monitors positions for profit-locking opportunities (negative exposure)."""

    def __init__(self, config: BotConfig):
        """
        Initialize the profit locker.

        Args:
            config: Bot configuration.
        """
        self.config = config
        self._locked_profits = 0.0

    async def check_lock_opportunities(
        self,
        positions: List[Position],
        current_prices: Dict[str, float]
    ) -> List[Dict]:
        """
        Check for profit-locking opportunities.

        When a position has moved favorably by more than the threshold,
        we can add opposing exposure to lock in profits.

        Args:
            positions: Current positions.
            current_prices: Current market prices by token_id.

        Returns:
            List of lock opportunities with details.
        """
        threshold = self.config.trading.profit_lock_threshold_percent / 100
        opportunities = []

        for position in positions:
            if position.token_id not in current_prices:
                continue

            current_price = current_prices[position.token_id]
            entry_price = position.avg_price

            if entry_price <= 0:
                continue

            # Calculate price movement
            price_change = (current_price - entry_price) / entry_price

            # Check if we should lock profits
            if price_change >= threshold:
                lock_size = position.size_shares * current_price * 0.5  # Lock 50%
                expected_profit = position.size_shares * (current_price - entry_price) * 0.5

                opportunities.append({
                    "position": position,
                    "current_price": current_price,
                    "entry_price": entry_price,
                    "price_change_pct": price_change * 100,
                    "lock_size_usd": lock_size,
                    "expected_locked_profit": expected_profit
                })

                logger.info(
                    f"Profit lock opportunity: {position.outcome_name} "
                    f"moved {price_change*100:.1f}% | Lock ${lock_size:.2f}"
                )

        return opportunities

    def record_locked_profit(self, amount: float):
        """Record a locked profit amount."""
        self._locked_profits += amount

    def get_stats(self) -> Dict[str, Any]:
        """Get profit locking statistics."""
        return {
            "total_locked_profits_usd": self._locked_profits
        }
