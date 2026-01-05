"""
Multi-Wallet Manager Module.
Handles wallet rotation, load balancing, and parallel execution across multiple wallets.
Enables unlimited scaling and rate limit avoidance through wallet distribution.

Supports both:
- EOA wallets (private key signing)
- Proxy wallets (API credentials from Magic/email login, no private key needed)
"""

import logging
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from config import BotConfig, ProxyApiCreds

logger = logging.getLogger(__name__)

# Minimal ERC20 ABI for balance check
ERC20_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    }
]


@dataclass
class WalletState:
    """State tracking for a single wallet (EOA or proxy)."""
    wallet_index: int
    # Private key (only for EOA wallets, empty for proxy-only)
    private_key: str
    # Derived address (from private key for EOA, from funder_address for proxy-only)
    address: str
    # For proxy wallets, this is where funds are held (Magic wallet address)
    funder_address: str
    # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE
    signature_type: int

    # CLOB client for this wallet
    client: Optional[ClobClient] = None
    api_creds: Optional[ApiCreds] = None

    # Pre-loaded API credentials for proxy-only wallets (no private key)
    proxy_creds: Optional[ProxyApiCreds] = None

    # Balance tracking (cached, refreshed periodically)
    usdc_balance: float = 0.0
    last_balance_check: Optional[datetime] = None

    # Volume tracking for rotation decisions
    session_volume_usd: float = 0.0
    total_executions: int = 0
    consecutive_failures: int = 0

    # Rate limit tracking
    last_request_time: Optional[datetime] = None
    requests_this_minute: int = 0

    # Status
    is_initialized: bool = False
    is_available: bool = True  # False if temporarily rate limited or erroring
    cooldown_until: Optional[datetime] = None

    @property
    def effective_address(self) -> str:
        """Get the address where funds are held (funder for proxy, self for EOA)."""
        return self.funder_address or self.address

    @property
    def is_proxy_wallet(self) -> bool:
        """Check if this is a proxy wallet (relayer handles gas)."""
        return self.signature_type == 1

    @property
    def is_proxy_only(self) -> bool:
        """Check if this is a proxy-only wallet (no private key, API creds only)."""
        return not self.private_key and self.proxy_creds is not None

    @property
    def is_on_cooldown(self) -> bool:
        """Check if wallet is currently on cooldown."""
        if self.cooldown_until is None:
            return False
        return datetime.now(timezone.utc) < self.cooldown_until

    @property
    def free_balance_ratio(self) -> float:
        """Get ratio of free balance to session volume (higher = less used)."""
        if self.session_volume_usd == 0:
            return float('inf')  # Unused wallet
        return self.usdc_balance / self.session_volume_usd


@dataclass
class WalletPoolStats:
    """Aggregate statistics for the wallet pool."""
    total_wallets: int = 0
    available_wallets: int = 0
    total_usdc_balance: float = 0.0
    total_session_volume: float = 0.0
    total_executions: int = 0
    wallets_on_cooldown: int = 0


class WalletManager:
    """
    Manages a pool of wallets for parallel execution and rate limit avoidance.

    Features:
    - Round-robin wallet selection for load distribution
    - Balance-based selection (prefer wallets with more free USDC)
    - Automatic cooldown on rate limits or errors
    - Volume tracking for rotation thresholds
    - Parallel execution support across multiple wallets
    """

    def __init__(self, config: BotConfig):
        """
        Initialize the wallet manager.

        Args:
            config: Bot configuration containing wallet settings.
        """
        self.config = config
        self._wallets: List[WalletState] = []
        self._current_index = 0
        self._lock = asyncio.Lock()
        self._web3: Optional[Web3] = None
        self._usdc_contract = None

        # Balance cache duration in seconds
        self._balance_cache_seconds = 60

        # Rate limit settings (per wallet)
        self._max_requests_per_minute = 90  # Conservative, CLOB is ~100/min
        self._cooldown_duration_seconds = 30

    def _init_web3(self) -> bool:
        """Initialize Web3 for balance checks."""
        if self._web3 is not None:
            return True

        try:
            self._web3 = Web3(Web3.HTTPProvider(self.config.network.polygon_rpc))
            self._web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if self._web3.is_connected():
                self._usdc_contract = self._web3.eth.contract(
                    address=Web3.to_checksum_address(self.config.network.usdc_address),
                    abi=ERC20_BALANCE_ABI
                )
                return True
            else:
                logger.warning("Failed to connect to Polygon RPC")
                return False
        except Exception as e:
            logger.error(f"Error initializing Web3: {e}")
            return False

    async def initialize(self) -> int:
        """
        Initialize all wallets in the pool.

        Supports both:
        - EOA wallets (from private keys)
        - Proxy wallets (from API credentials, no private key needed)

        Returns:
            Number of successfully initialized wallets.
        """
        all_keys = self.config.wallet.all_private_keys
        all_api_creds = self.config.wallet.all_api_creds

        total_wallets = len(all_keys) + len(all_api_creds)

        if total_wallets == 0:
            if self.config.dry_run:
                logger.info("Dry run mode - no wallets needed")
                return 0
            raise ValueError("No wallets configured (need PRIVATE_KEY or API credentials)")

        logger.info(f"Initializing {total_wallets} wallet(s) ({len(all_keys)} EOA, {len(all_api_creds)} proxy)...")

        # Initialize Web3 for balance checks
        self._init_web3()

        wallet_index = 0

        # Create wallet states for EOA wallets (private key based)
        for pk in all_keys:
            try:
                account = Account.from_key(pk)

                # For first wallet, use configured funder; others use their own address
                if wallet_index == 0:
                    funder = self.config.wallet.funder_address or account.address
                    sig_type = self.config.wallet.signature_type
                else:
                    funder = account.address  # Additional EOA wallets assumed pure EOA
                    sig_type = 0

                wallet = WalletState(
                    wallet_index=wallet_index,
                    private_key=pk,
                    address=account.address,
                    funder_address=funder,
                    signature_type=sig_type
                )

                self._wallets.append(wallet)
                logger.info(f"  Wallet {wallet_index} (EOA): {account.address[:10]}...{account.address[-6:]}")
                wallet_index += 1

            except Exception as e:
                logger.error(f"Failed to initialize EOA wallet {wallet_index}: {e}")

        # Create wallet states for proxy-only wallets (API credentials based)
        for creds in all_api_creds:
            try:
                # For proxy-only wallets, address = funder_address (Magic wallet)
                wallet = WalletState(
                    wallet_index=wallet_index,
                    private_key="",  # No private key for proxy-only
                    address=creds.funder_address,
                    funder_address=creds.funder_address,
                    signature_type=1,  # POLY_PROXY
                    proxy_creds=creds
                )

                self._wallets.append(wallet)
                addr = creds.funder_address
                logger.info(f"  Wallet {wallet_index} (Proxy): {addr[:10]}...{addr[-6:]}")
                wallet_index += 1

            except Exception as e:
                logger.error(f"Failed to initialize proxy wallet {wallet_index}: {e}")

        # Initialize CLOB clients for each wallet
        initialized_count = 0
        for wallet in self._wallets:
            try:
                await self._initialize_wallet_client(wallet)
                initialized_count += 1
            except Exception as e:
                logger.error(f"Failed to initialize client for wallet {wallet.wallet_index}: {e}")
                wallet.is_available = False

        logger.info(f"Successfully initialized {initialized_count}/{len(self._wallets)} wallets")
        return initialized_count

    async def _initialize_wallet_client(self, wallet: WalletState):
        """Initialize CLOB client for a specific wallet (EOA or proxy)."""
        if self.config.dry_run:
            wallet.is_initialized = True
            wallet.usdc_balance = self.config.starting_capital_usd / max(len(self._wallets), 1)
            return

        if wallet.is_proxy_only:
            # Proxy-only wallet: use pre-loaded API credentials
            await self._initialize_proxy_only_client(wallet)
        else:
            # EOA or proxy with private key: derive credentials
            await self._initialize_eoa_client(wallet)

        # Fetch initial balance
        await self._refresh_wallet_balance(wallet)

        wallet_type = "Proxy" if wallet.is_proxy_only else "EOA"
        logger.debug(
            f"Wallet {wallet.wallet_index} ({wallet_type}) initialized: "
            f"balance=${wallet.usdc_balance:.2f}"
        )

    async def _initialize_eoa_client(self, wallet: WalletState):
        """Initialize CLOB client for EOA wallet (has private key)."""
        # Determine funder based on signature type
        if wallet.signature_type == 0:
            funder = wallet.address
        else:
            funder = wallet.funder_address

        client = ClobClient(
            host=self.config.network.clob_endpoint,
            key=wallet.private_key,
            chain_id=self.config.network.chain_id,
            signature_type=wallet.signature_type,
            funder=funder
        )

        # Verify connection
        ok = client.get_ok()
        if not ok:
            raise ConnectionError(f"CLOB API not OK for wallet {wallet.wallet_index}")

        # Derive API credentials from private key
        wallet.api_creds = client.create_or_derive_api_creds()
        client.set_api_creds(wallet.api_creds)

        wallet.client = client
        wallet.is_initialized = True

    async def _initialize_proxy_only_client(self, wallet: WalletState):
        """
        Initialize CLOB client for proxy-only wallet (no private key).

        Uses pre-existing API credentials obtained from Magic/email login.
        No signature derivation needed - relayer handles all signing.
        """
        if not wallet.proxy_creds:
            raise ValueError(f"Wallet {wallet.wallet_index} has no proxy credentials")

        # Create client without private key for API-only operations
        # For proxy-only, we create a minimal client and set creds directly
        client = ClobClient(
            host=self.config.network.clob_endpoint,
            chain_id=self.config.network.chain_id
        )

        # Verify connection
        ok = client.get_ok()
        if not ok:
            raise ConnectionError(f"CLOB API not OK for proxy wallet {wallet.wallet_index}")

        # Set pre-loaded API credentials (from Magic login)
        wallet.api_creds = ApiCreds(
            api_key=wallet.proxy_creds.api_key,
            api_secret=wallet.proxy_creds.api_secret,
            api_passphrase=wallet.proxy_creds.api_passphrase
        )
        client.set_api_creds(wallet.api_creds)

        wallet.client = client
        wallet.is_initialized = True

        logger.debug(
            f"Proxy wallet {wallet.wallet_index} initialized with API creds "
            f"(funder: {wallet.funder_address[:10]}...)"
        )

    async def _refresh_wallet_balance(self, wallet: WalletState):
        """Refresh USDC balance for a wallet."""
        if self.config.dry_run:
            return

        if not self._init_web3() or self._usdc_contract is None:
            return

        try:
            check_address = Web3.to_checksum_address(wallet.effective_address)
            raw_balance = self._usdc_contract.functions.balanceOf(check_address).call()
            wallet.usdc_balance = raw_balance / 1e6
            wallet.last_balance_check = datetime.now(timezone.utc)
        except Exception as e:
            logger.debug(f"Failed to fetch balance for wallet {wallet.wallet_index}: {e}")

    async def _should_refresh_balance(self, wallet: WalletState) -> bool:
        """Check if wallet balance should be refreshed."""
        if wallet.last_balance_check is None:
            return True

        age = (datetime.now(timezone.utc) - wallet.last_balance_check).total_seconds()
        return age > self._balance_cache_seconds

    def _is_rate_limited(self, wallet: WalletState) -> bool:
        """Check if wallet is rate limited."""
        if wallet.last_request_time is None:
            return False

        now = datetime.now(timezone.utc)
        elapsed = (now - wallet.last_request_time).total_seconds()

        # Reset counter after 60 seconds
        if elapsed > 60:
            wallet.requests_this_minute = 0
            return False

        return wallet.requests_this_minute >= self._max_requests_per_minute

    def _record_request(self, wallet: WalletState):
        """Record an API request for rate limiting."""
        now = datetime.now(timezone.utc)

        if wallet.last_request_time is None:
            wallet.requests_this_minute = 1
        else:
            elapsed = (now - wallet.last_request_time).total_seconds()
            if elapsed > 60:
                wallet.requests_this_minute = 1
            else:
                wallet.requests_this_minute += 1

        wallet.last_request_time = now

    def _put_on_cooldown(self, wallet: WalletState, reason: str):
        """Put a wallet on temporary cooldown."""
        from datetime import timedelta
        wallet.cooldown_until = datetime.now(timezone.utc) + timedelta(
            seconds=self._cooldown_duration_seconds
        )
        wallet.is_available = False
        logger.warning(
            f"Wallet {wallet.wallet_index} on cooldown for {self._cooldown_duration_seconds}s: {reason}"
        )

    def _clear_cooldown(self, wallet: WalletState):
        """Clear cooldown if expired."""
        if wallet.cooldown_until and datetime.now(timezone.utc) >= wallet.cooldown_until:
            wallet.cooldown_until = None
            wallet.is_available = True
            wallet.consecutive_failures = 0
            logger.debug(f"Wallet {wallet.wallet_index} cooldown cleared")

    async def get_next_wallet(
        self,
        min_balance: float = 0.0,
        selection_mode: str = "smart"
    ) -> Optional[WalletState]:
        """
        Get the next available wallet for execution.

        Args:
            min_balance: Minimum USDC balance required.
            selection_mode: Wallet selection strategy:
                - "smart": Prefer lowest volume + highest free balance ratio (default)
                - "balance": Prefer wallet with highest absolute balance
                - "round_robin": Simple rotation through available wallets
                - "lowest_volume": Prefer wallet with lowest session volume

        Returns:
            Available WalletState or None if no wallet available.
        """
        async with self._lock:
            # Clear expired cooldowns
            for wallet in self._wallets:
                self._clear_cooldown(wallet)

            # Get available wallets
            available = [
                w for w in self._wallets
                if w.is_initialized
                and w.is_available
                and not w.is_on_cooldown
                and not self._is_rate_limited(w)
                and w.usdc_balance >= min_balance
            ]

            if not available:
                return None

            if selection_mode == "smart":
                # Smart selection: composite score of low volume + high free ratio
                # Prioritizes: 1) unused wallets, 2) lowest volume, 3) highest balance
                def smart_score(w: WalletState) -> tuple:
                    # Primary: lowest session volume (spread trades across wallets)
                    # Secondary: highest free balance ratio
                    # Tertiary: highest absolute balance
                    return (-w.session_volume_usd, w.free_balance_ratio, w.usdc_balance)

                available.sort(key=smart_score, reverse=True)
                return available[0]

            elif selection_mode == "balance":
                # Sort by absolute balance (highest first)
                available.sort(key=lambda w: w.usdc_balance, reverse=True)
                return available[0]

            elif selection_mode == "lowest_volume":
                # Sort by session volume (lowest first)
                available.sort(key=lambda w: w.session_volume_usd)
                return available[0]

            else:  # round_robin
                # Simple rotation
                self._current_index = (self._current_index + 1) % len(available)
                return available[self._current_index % len(available)]

    async def get_wallet_by_balance(self, min_balance: float) -> Optional[WalletState]:
        """
        Get a wallet with at least the specified balance.

        Args:
            min_balance: Minimum required USDC balance.

        Returns:
            Wallet meeting balance requirement or None.
        """
        return await self.get_next_wallet(min_balance=min_balance, selection_mode="smart")

    async def get_all_available_wallets(self, min_balance: float = 0.0) -> List[WalletState]:
        """
        Get all currently available wallets.

        Useful for parallel execution across multiple wallets.

        Args:
            min_balance: Minimum USDC balance required.

        Returns:
            List of available wallets.
        """
        async with self._lock:
            for wallet in self._wallets:
                self._clear_cooldown(wallet)

            return [
                w for w in self._wallets
                if w.is_initialized
                and w.is_available
                and not w.is_on_cooldown
                and not self._is_rate_limited(w)
                and w.usdc_balance >= min_balance
            ]

    async def get_top_wallets_for_burst(
        self,
        count: int = 2,
        min_balance: float = 0.0
    ) -> List[WalletState]:
        """
        Get top N wallets for parallel burst execution.

        Prioritizes wallets with:
        1. Highest free USDC percentage (most available capital)
        2. Lowest recent volume (least used this session)

        Used for parallelizing deep arbs (>2x min_depth) across multiple wallets
        during burst mode to maximize fill probability.

        Args:
            count: Number of wallets to return (default 2).
            min_balance: Minimum USDC balance required per wallet.

        Returns:
            List of top wallets for burst parallelization, sorted by priority.
        """
        async with self._lock:
            # Clear expired cooldowns
            for wallet in self._wallets:
                self._clear_cooldown(wallet)

            # Get available wallets
            available = [
                w for w in self._wallets
                if w.is_initialized
                and w.is_available
                and not w.is_on_cooldown
                and not self._is_rate_limited(w)
                and w.usdc_balance >= min_balance
            ]

            if len(available) <= 1:
                return available

            # Sort by burst priority: highest free % + lowest volume
            def burst_score(w: WalletState) -> tuple:
                # Primary: highest free balance ratio (most capital available %)
                # Secondary: lowest session volume (least used)
                # Tertiary: highest absolute balance
                return (w.free_balance_ratio, -w.session_volume_usd, w.usdc_balance)

            available.sort(key=burst_score, reverse=True)

            return available[:count]

    def record_execution(
        self,
        wallet: WalletState,
        volume_usd: float,
        success: bool
    ):
        """
        Record an execution result for a wallet.

        Args:
            wallet: The wallet that executed.
            volume_usd: Volume executed in USD.
            success: Whether execution was successful.
        """
        self._record_request(wallet)

        if success:
            wallet.session_volume_usd += volume_usd
            wallet.total_executions += 1
            wallet.consecutive_failures = 0
        else:
            wallet.consecutive_failures += 1

            # Put on cooldown after multiple consecutive failures
            if wallet.consecutive_failures >= 3:
                self._put_on_cooldown(wallet, "3+ consecutive failures")

    def record_rate_limit(self, wallet: WalletState):
        """Record a rate limit hit for a wallet."""
        self._put_on_cooldown(wallet, "rate limit hit")

    async def refresh_all_balances(self):
        """Refresh USDC balances for all wallets."""
        for wallet in self._wallets:
            try:
                await self._refresh_wallet_balance(wallet)
            except Exception as e:
                logger.debug(f"Failed to refresh wallet {wallet.wallet_index} balance: {e}")

    def get_pool_stats(self) -> WalletPoolStats:
        """Get aggregate statistics for the wallet pool."""
        available = sum(
            1 for w in self._wallets
            if w.is_available and not w.is_on_cooldown
        )
        on_cooldown = sum(1 for w in self._wallets if w.is_on_cooldown)

        return WalletPoolStats(
            total_wallets=len(self._wallets),
            available_wallets=available,
            total_usdc_balance=sum(w.usdc_balance for w in self._wallets),
            total_session_volume=sum(w.session_volume_usd for w in self._wallets),
            total_executions=sum(w.total_executions for w in self._wallets),
            wallets_on_cooldown=on_cooldown
        )

    def get_total_available_balance(self) -> float:
        """Get total USDC balance across all available wallets."""
        return sum(
            w.usdc_balance for w in self._wallets
            if w.is_available and not w.is_on_cooldown
        )

    def get_wallet_count(self) -> int:
        """Get total number of wallets in the pool."""
        return len(self._wallets)

    def get_primary_wallet(self) -> Optional[WalletState]:
        """Get the primary (first) wallet."""
        return self._wallets[0] if self._wallets else None

    def get_wallet(self, index: int) -> Optional[WalletState]:
        """Get a specific wallet by index."""
        if 0 <= index < len(self._wallets):
            return self._wallets[index]
        return None
