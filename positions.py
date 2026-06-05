"""
Position Monitoring & Auto-Claim Module.
Monitors positions and automatically claims winnings on market resolution.
"""

import logginggag
import asyncio
import secrets
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
from rate_limiter import get_global_limiter
from clob_client_patch import run_sync_in_thread

logger = logging.getLogger(__name__)

T = TypeVar('T')


def _exponential_backoff(attempt: int, base: float = 1.0, max_backoff: float = 30.0) -> float:
    """Calculate exponential backoff delay."""
    return min(base * (2 ** attempt), max_backoff)


def generate_dvm_salt() -> int:
    """
    Generate a cryptographically secure random salt for DVM voting.

    Per Grok Round 6 optimization: UMA recommends unique per-vote salts for privacy.
    Using secrets module ensures cryptographic randomness (vs random module).

    The salt is used in commit-reveal scheme:
    - Commit: hash(price + salt) - hides your vote
    - Reveal: price + salt - proves your commit

    Same salt reuse across votes could leak voting patterns.

    Returns:
        256-bit random integer suitable for int256 salt parameter.
    """
    # Generate 32 bytes (256 bits) of cryptographic randomness
    # Convert to int, ensuring it fits in int256 (signed, so max is 2^255-1)
    random_bytes = secrets.token_bytes(32)
    salt = int.from_bytes(random_bytes, byteorder='big')
    # Ensure positive and within int256 range
    return salt % (2 ** 255)


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


# ConditionalTokens ABI (extended per Grok audit - was truncated)
# Includes redeemPositions, balanceOf, payoutDenominator, payoutNumerators, getConditionId
# These are needed for proper dispute handling and position status checking
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
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "index", "type": "uint256"}
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "oracle", "type": "address"},
            {"name": "questionId", "type": "bytes32"},
            {"name": "outcomeSlotCount", "type": "uint256"}
        ],
        "name": "getConditionId",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "pure",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"}
        ],
        "name": "getOutcomeSlotCount",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]
''')


class PositionStatus(Enum):
    """
    Status of a position.

    Per Grok audit: Added detailed dispute handling for UMA Optimistic Oracle.
    Polymarket uses UMA for resolution - disputes can invalidate outcomes.

    Per Grok Round 3: UMA dispute flow:
    1. Market resolves via Optimistic Oracle (72h window)
    2. If disputed, escalates to UMA DVM (Data Verification Mechanism)
    3. DVM voting takes ~48h additional
    4. During dispute, funds are LOCKED - cannot claim or trade

    Action: If DISPUTE_PENDING, wait for DVM resolution. Do NOT hedge
    (position is locked). Monitor UMA DVM for outcome.
    """
    OPEN = "open"
    WINNING = "winning"
    LOSING = "losing"
    CLAIMED = "claimed"
    DISPUTED = "disputed"  # Market is disputed (payoutDenominator == 0 after resolution)
    DISPUTE_PENDING = "dispute_pending"  # Dispute raised, awaiting DVM resolution
    DVM_VOTING = "dvm_voting"  # Per Grok Round 3: Escalated to DVM, voting in progress
    UNKNOWN = "unknown"


# Per Grok Round 3: UMA DVM escalation info
DVM_ESCALATION_INFO = """
UMA Optimistic Oracle Dispute Resolution Flow (per uma-ctf-adapter):

1. INITIAL RESOLUTION (72 hours)
   - Oracle proposes outcome
   - Disputers can challenge within window

2. IF DISPUTED -> DVM ESCALATION
   - Dispute bond required (significant USDC)
   - Escalates to UMA DVM for token-holder vote
   - Voting takes ~48 hours

3. DVM OUTCOME
   - If original correct: disputer loses bond
   - If original wrong: proposer loses bond, outcome reversed

ACTION FOR BOT:
- If position is DISPUTE_PENDING or DVM_VOTING: DO NOT TRADE
- Funds are locked until resolution
- Monitor UMA subgraph for dispute status
- Exit positions BEFORE 6h from resolution to avoid lock risk
"""

# Per Grok Round 4: UMA DVM Subgraph endpoint for voting status
# Used to check if a market is in DVM voting phase
UMA_DVM_SUBGRAPH = "https://api.thegraph.com/subgraphs/name/umaprotocol/uma-voting"

# Per Grok Round 5: UMA Voting contract on Polygon (for DVM participation)
# Required to vote on disputed markets and unlock funds
UMA_VOTING_CONTRACT = "0x8d51bcc80bF62c1CB23C1a6a91C7FfE1d4e12c27"

# Query to check DVM voting status for a price request
DVM_VOTING_QUERY = """
query GetPriceRequest($identifier: String!, $timestamp: BigInt!) {
  priceRequests(where: {identifier: $identifier, time: $timestamp}) {
    id
    identifier
    time
    resolvedPrice
    isResolved
    votingRound {
      roundId
      startTime
      endTime
    }
  }
}
"""

# Per Grok Round 6: Query to get current voting phase timing
DVM_PHASE_QUERY = """
query GetCurrentVotingPhase {
  votingRounds(first: 1, orderBy: roundId, orderDirection: desc) {
    roundId
    startTime
    endTime
    commitEndTime
    revealEndTime
  }
}
"""

# UMA Voting ABI (extended for commit-reveal with phase monitoring - Grok Round 6)
UMA_VOTING_ABI = json.loads('''
[
    {
        "inputs": [
            {"name": "identifier", "type": "bytes32"},
            {"name": "time", "type": "uint256"},
            {"name": "ancillaryData", "type": "bytes"},
            {"name": "hash", "type": "bytes32"}
        ],
        "name": "commitVote",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "identifier", "type": "bytes32"},
            {"name": "time", "type": "uint256"},
            {"name": "ancillaryData", "type": "bytes"},
            {"name": "price", "type": "int256"},
            {"name": "salt", "type": "int256"}
        ],
        "name": "revealVote",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "identifier", "type": "bytes32"},
            {"name": "time", "type": "uint256"},
            {"name": "ancillaryData", "type": "bytes"}
        ],
        "name": "hasVoted",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "getCurrentRoundId",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "getCurrentTime",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "getVotePhase",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function"
    }
]
''')


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
class PendingDVMReveal:
    """
    Per Grok Round 6: Track pending DVM vote commits awaiting reveal phase.

    UMA DVM uses commit-reveal voting:
    1. Commit phase: Submit hash(price + salt) - keeps vote secret
    2. Reveal phase: Submit actual price + salt - verifies commit

    Missing reveal = vote doesn't count = funds stay locked!
    This dataclass tracks commits so we can auto-reveal in reveal phase.
    """
    identifier: bytes
    timestamp: int
    ancillary_data: bytes
    vote_price: int
    salt: int
    commit_tx_hash: str
    commit_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    revealed: bool = False
    reveal_tx_hash: Optional[str] = None


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

        # Per Grok Round 6: Track pending DVM reveals for auto-reveal scheduler
        self._pending_reveals: List[PendingDVMReveal] = []
        self._voting_contract: Optional[Any] = None

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

        Uses global rate limiter for coordinated backoff across all HTTP clients.

        Returns:
            List of current positions.
        """
        if self.config.dry_run:
            logger.debug("Fetching positions in dry run mode - returning empty list")
            return []

        session = await self._get_session()
        url = f"{self.config.network.clob_endpoint}/positions"
        limiter = get_global_limiter()

        # Check if globally rate limited before making request
        if await limiter.wait_if_limited():
            logger.debug("Waited for global rate limit before fetching positions")

        # Note: This endpoint requires L2 authentication headers
        # The actual implementation would use the authenticated client
        # For now, we'll use the data available from the client

        try:
            # In practice, use the CLOB client's get_positions or similar method
            # This is a placeholder that would be replaced with actual API call
            async with session.get(url) as response:
                if response.status == 200:
                    await limiter.record_success()
                    data = await response.json()
                    return self._parse_positions(data)
                elif response.status == 429:
                    await limiter.record_429("positions")
                    logger.warning("Rate limited fetching positions")
                    return []
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
                # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
                if self._contract and self._web3:
                    condition_id_bytes = bytes.fromhex(position.condition_id.replace("0x", ""))
                    payout_denominator = await run_sync_in_thread(
                        self._contract.functions.payoutDenominator(condition_id_bytes).call
                    )

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
                # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
                signed_tx = self._account.sign_transaction(tx)
                tx_hash = await run_sync_in_thread(
                    self._web3.eth.send_raw_transaction, signed_tx.raw_transaction
                )

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

    async def check_dvm_voting_status(self, condition_id: str) -> Optional[PositionStatus]:
        """
        Check if a market is in DVM voting phase.

        Per Grok Round 5: Queries UMA DVM subgraph to check voting status.
        When a market is disputed, it escalates to DVM voting which takes ~48h.
        During this period, positions are LOCKED and should not be traded.

        Args:
            condition_id: The condition ID of the market to check.

        Returns:
            PositionStatus.DVM_VOTING if in voting, None if not disputed or error.
        """
        session = await self._get_session()

        try:
            # Query UMA subgraph for active voting rounds
            # Note: condition_id mapping to UMA identifier is market-specific
            # This implementation checks for any active voting that might affect our position
            payload = {
                "query": DVM_VOTING_QUERY,
                "variables": {
                    "identifier": f"0x{condition_id[:32]}",  # Simplified mapping
                    "timestamp": "0"  # Check all timestamps
                }
            }

            async with session.post(UMA_DVM_SUBGRAPH, json=payload, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    price_requests = data.get("data", {}).get("priceRequests", [])

                    for req in price_requests:
                        voting_round = req.get("votingRound")
                        is_resolved = req.get("isResolved", True)

                        if voting_round and not is_resolved:
                            logger.warning(
                                f"DVM voting ACTIVE for {condition_id[:16]}... "
                                f"Round: {voting_round.get('roundId')} - POSITION LOCKED"
                            )
                            return PositionStatus.DVM_VOTING

        except asyncio.TimeoutError:
            logger.debug(f"DVM subgraph timeout for {condition_id[:16]}...")
        except Exception as e:
            logger.debug(f"DVM voting check failed: {e}")

        return None

    async def submit_dvm_vote(
        self,
        identifier: bytes,
        timestamp: int,
        ancillary_data: bytes,
        vote_price: int,
        salt: Optional[int] = None
    ) -> Optional[str]:
        """
        Submit a vote to UMA DVM for a disputed market.

        Per Grok Round 5 (uma-ctf-adapter): To unlock funds from a disputed market,
        you MUST participate in DVM voting. This is a two-phase process:
        1. Commit phase: Submit hash of your vote (commitVote)
        2. Reveal phase: Reveal your actual vote (revealVote)

        WARNING: This requires UMA tokens staked to vote. If you don't have
        staked UMA, you cannot participate in DVM voting.

        Per Grok Round 6 optimization: Salt is now auto-generated if not provided,
        using cryptographically secure randomness for privacy.

        Args:
            identifier: The price request identifier (bytes32).
            timestamp: The price request timestamp.
            ancillary_data: Additional data for the request.
            vote_price: Your vote (e.g., 1e18 for YES, 0 for NO).
            salt: Random salt for commit-reveal scheme. If None, auto-generated
                  using cryptographically secure randomness.

        Returns:
            Transaction hash if successful, None otherwise.
        """
        # Per Grok Round 6: Auto-generate secure salt if not provided
        if salt is None:
            salt = generate_dvm_salt()
            logger.debug(f"Generated secure DVM salt: {salt % 1000}... (truncated)")
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would submit DVM vote for identifier {identifier.hex()[:16]}...")
            return "0xdryrun"

        if not self._web3 or not self._account:
            logger.error("Web3 not initialized for DVM voting")
            return None

        try:
            voting_contract = self._web3.eth.contract(
                address=Web3.to_checksum_address(UMA_VOTING_CONTRACT),
                abi=UMA_VOTING_ABI
            )

            # Phase 1: Commit vote (hash of price + salt)
            # The hash commits you to a specific vote without revealing it
            import hashlib
            commit_hash = Web3.keccak(
                Web3.to_bytes(vote_price) + Web3.to_bytes(salt)
            )

            # Check if already voted
            # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
            has_voted = await run_sync_in_thread(
                voting_contract.functions.hasVoted(identifier, timestamp, ancillary_data).call
            )

            if has_voted:
                logger.info(f"Already voted on this price request")
                return None

            # Get nonce
            nonce = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.get_transaction_count(
                    self._wallet_address, 'pending'
                ),
                operation_name="get_nonce_for_vote"
            )

            gas_price = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.gas_price,
                operation_name="get_gas_price_for_vote"
            )

            # Build commit transaction
            tx = voting_contract.functions.commitVote(
                identifier,
                timestamp,
                ancillary_data,
                commit_hash
            ).build_transaction({
                'from': self._wallet_address,
                'nonce': nonce,
                'gas': 150000,
                'gasPrice': gas_price,
                'chainId': self.config.network.chain_id
            })

            # Sign and send
            # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
            signed_tx = self._account.sign_transaction(tx)
            tx_hash = await run_sync_in_thread(
                self._web3.eth.send_raw_transaction, signed_tx.raw_transaction
            )

            logger.info(f"DVM vote commit submitted: {tx_hash.hex()}")

            # Wait for confirmation
            receipt = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120),
                operation_name="wait_for_vote_receipt"
            )

            if receipt.status == 1:
                logger.info(f"DVM vote commit confirmed! TX: {tx_hash.hex()}")

                # Per Grok Round 6: Track for auto-reveal in reveal phase
                pending = PendingDVMReveal(
                    identifier=identifier,
                    timestamp=timestamp,
                    ancillary_data=ancillary_data,
                    vote_price=vote_price,
                    salt=salt,
                    commit_tx_hash=tx_hash.hex()
                )
                self._pending_reveals.append(pending)
                logger.info(
                    f"Commit tracked for auto-reveal. Pending reveals: {len(self._pending_reveals)}"
                )
                return tx_hash.hex()
            else:
                logger.error(f"DVM vote commit failed: {tx_hash.hex()}")
                return None

        except Exception as e:
            logger.error(f"DVM vote submission failed: {e}")
            return None

    async def get_current_vote_phase(self) -> Optional[int]:
        """
        Per Grok Round 6: Get current UMA voting phase.

        Returns:
            0 = Commit phase (submit hash)
            1 = Reveal phase (submit actual vote)
            None = Error or not in voting round
        """
        if not self._web3:
            return None

        try:
            if not self._voting_contract:
                self._voting_contract = self._web3.eth.contract(
                    address=Web3.to_checksum_address(UMA_VOTING_CONTRACT),
                    abi=UMA_VOTING_ABI
                )

            # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
            phase = await run_sync_in_thread(
                self._voting_contract.functions.getVotePhase().call
            )
            return phase

        except Exception as e:
            logger.debug(f"Failed to get vote phase: {e}")
            return None

    async def reveal_dvm_vote(self, pending: PendingDVMReveal) -> Optional[str]:
        """
        Per Grok Round 6: Reveal a committed DVM vote.

        Must be called during reveal phase to finalize vote.
        If not revealed, commit is wasted and funds stay locked!

        Args:
            pending: The pending reveal with commit data.

        Returns:
            Transaction hash if successful, None otherwise.
        """
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would reveal DVM vote for {pending.identifier.hex()[:16]}...")
            pending.revealed = True
            return "0xdryrun_reveal"

        if not self._web3 or not self._account:
            logger.error("Web3 not initialized for DVM reveal")
            return None

        try:
            if not self._voting_contract:
                self._voting_contract = self._web3.eth.contract(
                    address=Web3.to_checksum_address(UMA_VOTING_CONTRACT),
                    abi=UMA_VOTING_ABI
                )

            # Check we're in reveal phase
            phase = await self.get_current_vote_phase()
            if phase != 1:  # 1 = reveal phase
                logger.warning(f"Not in reveal phase (current: {phase}), cannot reveal yet")
                return None

            # Get nonce
            nonce = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.get_transaction_count(
                    self._wallet_address, 'pending'
                ),
                operation_name="get_nonce_for_reveal"
            )

            gas_price = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.gas_price,
                operation_name="get_gas_price_for_reveal"
            )

            # Build reveal transaction
            tx = self._voting_contract.functions.revealVote(
                pending.identifier,
                pending.timestamp,
                pending.ancillary_data,
                pending.vote_price,
                pending.salt
            ).build_transaction({
                'from': self._wallet_address,
                'nonce': nonce,
                'gas': 150000,
                'gasPrice': gas_price,
                'chainId': self.config.network.chain_id
            })

            # Sign and send
            # Per Grok Round 6: Wrap sync Web3 call in thread to avoid blocking
            signed_tx = self._account.sign_transaction(tx)
            tx_hash = await run_sync_in_thread(
                self._web3.eth.send_raw_transaction, signed_tx.raw_transaction
            )

            logger.info(f"DVM vote reveal submitted: {tx_hash.hex()}")

            # Wait for confirmation
            receipt = await _retry_rpc_with_backoff(
                lambda: self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120),
                operation_name="wait_for_reveal_receipt"
            )

            if receipt.status == 1:
                pending.revealed = True
                pending.reveal_tx_hash = tx_hash.hex()
                logger.info(f"DVM vote reveal confirmed! TX: {tx_hash.hex()}")
                logger.info("Vote finalized - funds will unlock after resolution")
                return tx_hash.hex()
            else:
                logger.error(f"DVM vote reveal failed: {tx_hash.hex()}")
                return None

        except Exception as e:
            logger.error(f"DVM reveal failed: {e}")
            return None

    async def run_reveal_scheduler(self) -> int:
        """
        Per Grok Round 6: Automated DVM reveal scheduler.

        Checks for pending commits that need revealing and submits reveals
        when in reveal phase. Should be called periodically (e.g., every 5 min).

        Returns:
            Number of successful reveals.
        """
        if not self._pending_reveals:
            return 0

        # Check current phase
        phase = await self.get_current_vote_phase()
        if phase != 1:  # Not in reveal phase
            unrevealed = [p for p in self._pending_reveals if not p.revealed]
            if unrevealed:
                logger.debug(
                    f"{len(unrevealed)} commits awaiting reveal phase (current phase: {phase})"
                )
            return 0

        # In reveal phase - process pending reveals
        reveals_done = 0
        for pending in self._pending_reveals:
            if pending.revealed:
                continue

            logger.info(f"Auto-revealing commit from {pending.commit_time}")
            tx_hash = await self.reveal_dvm_vote(pending)

            if tx_hash:
                reveals_done += 1
                logger.info(f"Auto-reveal successful: {tx_hash}")
            else:
                logger.warning(f"Auto-reveal failed for commit {pending.commit_tx_hash[:16]}...")

            # Rate limit between reveals
            await asyncio.sleep(2)

        # Clean up old revealed entries (keep for 24h)
        cutoff = datetime.now(timezone.utc).timestamp() - (24 * 3600)
        self._pending_reveals = [
            p for p in self._pending_reveals
            if not p.revealed or p.commit_time.timestamp() > cutoff
        ]

        if reveals_done > 0:
            logger.info(f"Reveal scheduler completed: {reveals_done} reveals")

        return reveals_done

    def get_pending_reveals_count(self) -> int:
        """Get count of pending reveals awaiting reveal phase."""
        return len([p for p in self._pending_reveals if not p.revealed])

    def is_position_locked(self, position: Position) -> bool:
        """
        Check if a position is locked due to dispute/DVM voting.

        Per Grok Round 4: Locked positions should not be traded or hedged.

        Args:
            position: The position to check.

        Returns:
            True if position is locked (dispute pending or DVM voting).
        """
        locked_statuses = {
            PositionStatus.DISPUTED,
            PositionStatus.DISPUTE_PENDING,
            PositionStatus.DVM_VOTING
        }
        return position.status in locked_statuses


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
