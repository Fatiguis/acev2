"""
Setup & Authentication Module.
Handles wallet initialization, CLOB client setup, and API credential management.
"""

import logging
import json
import time
from typing import Optional, Tuple, Callable, TypeVar
from eth_account import Account
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from config import BotConfig

logger = logging.getLogger(__name__)

# Minimum MATIC balance required for gas (in MATIC)
# Per Grok Round 23 CRITICAL: Raised to 2.0 MATIC for rn1-style burst trading
# Math: rn1's burst pattern (10-25 tx in 3min during live games) hits gas spikes
# Polygon gas can spike 10x during congestion - 2.0 MATIC provides safety buffer
# 1.0 was causing mid-arb tx failures during high-volume sports events
MIN_MATIC_FOR_GAS = 2.0

# Warning threshold - alert when approaching minimum (triggers webhook)
# Per Grok Round 23: Raised to 3.0 to give time for manual top-up
MATIC_WARNING_THRESHOLD = 3.0

T = TypeVar('T')


def _exponential_backoff(attempt: int, base: float = 1.0, max_backoff: float = 30.0) -> float:
    """Calculate exponential backoff delay."""
    return min(base * (2 ** attempt), max_backoff)


def _retry_rpc_sync(
    func: Callable[[], T],
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    operation_name: str = "RPC call"
) -> T:
    """
    Retry a synchronous RPC call with exponential backoff.

    Per Grok Round 5: Uses time.sleep() which blocks the thread.
    Use ONLY in:
    - CLI scripts (cancel_orders.py, close_positions.py)
    - Initialization code (AuthManager.initialize - runs before event loop)
    - Balance checks from CLI

    For async contexts (main bot loop, execution, positions), use
    _retry_rpc_async() instead which uses asyncio.sleep().

    Args:
        func: Sync function to retry.
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
                time.sleep(delay)
            else:
                logger.error(f"{operation_name} failed after {max_retries + 1} attempts: {e}")

    raise last_exception


async def _retry_rpc_async(
    func: Callable[[], T],
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    operation_name: str = "RPC call"
) -> T:
    """
    Retry a synchronous RPC call with exponential backoff (async-compatible).

    Per Grok audit: time.sleep() blocks the event loop in async contexts.
    This version uses asyncio.sleep() for non-blocking waits while still
    calling synchronous web3 functions.

    Args:
        func: Sync function to retry (web3 calls are sync).
        max_retries: Maximum number of retries.
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds.
        operation_name: Name for logging.

    Returns:
        Result of the function.

    Raises:
        Exception: If all retries fail.
    """
    import asyncio
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            # Web3 calls are sync - run in executor to not block event loop
            # For simple calls, direct call is fine; for heavy calls, use run_in_executor
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                delay = _exponential_backoff(attempt, base_delay, max_delay)
                logger.warning(
                    f"{operation_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)  # Non-blocking sleep
            else:
                logger.error(f"{operation_name} failed after {max_retries + 1} attempts: {e}")

    raise last_exception

# ERC20 ABI for balance check and approvals
ERC20_ABI = json.loads('''
[
    {
        "constant": true,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"}
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"}
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function"
    }
]
''')

# Per Grok Round 19 CRITICAL FIX: CAPPED approvals instead of unlimited
# Unlimited (2**256-1) is a security risk - if contract is compromised, all funds at risk
# Cap at reasonable trading amount + buffer (50% margin for batch trades)
# For USDC (6 decimals): 50,000 USDC = 50_000 * 1e6 raw units
CAPPED_APPROVAL_USDC = 50_000 * 10**6  # $50k cap - sufficient for rn1-style micro-arbs

# Minimum allowance threshold before re-approving (in raw units)
# Re-approve when we drop below 20% of cap (i.e., below $10k)
MIN_ALLOWANCE_USDC = 10_000 * 10**6


class AuthManager:
    """Manages authentication and client initialization for Polymarket CLOB."""

    def __init__(self, config: BotConfig):
        """
        Initialize the authentication manager.

        Args:
            config: Bot configuration containing wallet and network settings.
        """
        self.config = config
        self._client: Optional[ClobClient] = None
        self._api_creds: Optional[ApiCreds] = None
        self._wallet_address: Optional[str] = None
        self._web3: Optional[Web3] = None
        self._usdc_contract = None

    def initialize(self) -> ClobClient:
        """
        Initialize and return an authenticated CLOB client.

        Returns:
            Authenticated ClobClient instance ready for trading.

        Raises:
            ValueError: If required configuration is missing.
            ConnectionError: If unable to connect to CLOB API.
        """
        if self.config.dry_run:
            logger.info("Initializing in DRY RUN mode - no authentication required")
            return self._init_read_only_client()

        return self._init_authenticated_client()

    def _init_read_only_client(self) -> ClobClient:
        """Initialize a read-only client for dry run mode."""
        client = ClobClient(self.config.network.clob_endpoint)

        # Verify connection
        try:
            ok = client.get_ok()
            if ok:
                logger.info("Read-only CLOB client initialized successfully")
            else:
                raise ConnectionError("CLOB API returned not OK status")
        except Exception as e:
            logger.error(f"Failed to connect to CLOB API: {e}")
            raise ConnectionError(f"Unable to connect to CLOB API: {e}")

        self._client = client
        return client

    def _init_authenticated_client(self) -> ClobClient:
        """Initialize a fully authenticated client for live trading."""
        private_key = self.config.wallet.private_key
        if not private_key:
            raise ValueError("Private key is required for authenticated client")

        # Derive wallet address from private key
        account = Account.from_key(private_key)
        self._wallet_address = account.address

        signature_type = self.config.wallet.signature_type

        # Determine funder address based on signature type
        # For EOA (type 0): funder should be the wallet address from private key
        # For POLY_PROXY (type 1): funder is the proxy address, signer is the private key owner
        if signature_type == 0:
            # EOA mode - funder must be the wallet derived from private key
            funder = self._wallet_address
            if self.config.wallet.funder_address and self.config.wallet.funder_address != self._wallet_address:
                logger.warning(
                    f"EOA mode (SIGNATURE_TYPE=0): Ignoring FUNDER_ADDRESS={self.config.wallet.funder_address}. "
                    f"Using wallet address {self._wallet_address} instead."
                )
        else:
            # Proxy mode (1 or 2) - use specified funder address
            funder = self.config.wallet.funder_address
            if not funder:
                raise ValueError(
                    f"SIGNATURE_TYPE={signature_type} requires FUNDER_ADDRESS to be set. "
                    "Set FUNDER_ADDRESS to your Polymarket proxy wallet address."
                )

        logger.info(f"Initializing authenticated client for wallet: {self._wallet_address}")
        logger.info(f"Funder address: {funder}")
        logger.info(f"Signature type: {signature_type}")

        # Initialize client with authentication
        client = ClobClient(
            host=self.config.network.clob_endpoint,
            key=private_key,
            chain_id=self.config.network.chain_id,
            signature_type=self.config.wallet.signature_type,
            funder=funder
        )

        # Verify connection first
        try:
            ok = client.get_ok()
            if not ok:
                raise ConnectionError("CLOB API returned not OK status")
        except Exception as e:
            logger.error(f"Failed to connect to CLOB API: {e}")
            raise ConnectionError(f"Unable to connect to CLOB API: {e}")

        # Create or derive API credentials (L2 authentication)
        try:
            self._api_creds = client.create_or_derive_api_creds()
            client.set_api_creds(self._api_creds)
            logger.info("API credentials derived successfully")

            # Log credential details (redacted) for debugging
            # FIX: Check for None/empty strings before slicing to avoid crashes
            if self._api_creds:
                key = self._api_creds.api_key or ""
                secret = self._api_creds.api_secret or ""
                passphrase = self._api_creds.api_passphrase or ""
                if len(key) >= 12:
                    logger.debug(f"API Key: {key[:8]}...{key[-4:]}")
                if len(secret) >= 8:
                    logger.debug(f"API Secret: {secret[:4]}...{secret[-4:]}")
                if len(passphrase) >= 4:
                    logger.debug(f"API Passphrase: {passphrase[:4]}...")

        except Exception as e:
            logger.error(f"Failed to derive API credentials: {e}")
            logger.error(f"Wallet: {self._wallet_address}")
            logger.error(f"Funder: {funder}")
            logger.error(f"Signature type: {self.config.wallet.signature_type}")
            raise ValueError(f"Unable to derive API credentials: {e}")

        # Verify credentials work by testing a simple authenticated call
        try:
            self._verify_api_creds(client)
        except Exception as e:
            logger.warning(f"Credential verification warning: {e}")

        self._client = client
        logger.info("Authenticated CLOB client initialized successfully")

        # For EOA wallets (signature_type=0), ensure token approvals are set
        # py-clob-client docs: EOA wallets require USDC + ConditionalTokens approvals BEFORE trading
        if self.config.wallet.signature_type == 0:
            self._ensure_eoa_approvals(client)

        return client

    def _ensure_eoa_approvals(self, client: ClobClient):
        """
        Ensure EOA wallet has CAPPED token approvals for trading.

        Per Grok Round 19 CRITICAL FIX: Use capped approvals instead of unlimited.
        Unlimited (uint256.max) is a security risk - if the exchange contract is
        compromised, attacker can drain all approved tokens.

        EOA wallets (signature_type=0) require:
        1. USDC approval for the CTF Exchange (spending collateral)
        2. ConditionalTokens approval for the CTF Exchange (trading positions)

        Without these, post_order() fails with "not enough balance / allowance" (400 error).
        This is py-clob-client GitHub issue #109.

        Uses CAPPED approval ($50k) - re-approves when balance drops below $10k.
        This limits exposure while avoiding frequent approval txs.

        Args:
            client: The authenticated CLOB client.
        """
        logger.info("EOA wallet detected - checking/setting CAPPED token approvals...")

        # Initialize Web3 for approval transactions
        self._init_web3()
        if self._web3 is None or not self._web3.is_connected():
            logger.error("Cannot set approvals - Web3 not connected")
            return

        # Get exchange address (spender for approvals)
        try:
            exchange_address = client.get_exchange_address()
            exchange_address = Web3.to_checksum_address(exchange_address)
            logger.info(f"Exchange address: {exchange_address}")
        except Exception as e:
            logger.error(f"Failed to get exchange address: {e}")
            return

        # Get conditional tokens address
        try:
            conditional_address = client.get_conditional_address()
            conditional_address = Web3.to_checksum_address(conditional_address)
            logger.info(f"ConditionalTokens address: {conditional_address}")
        except Exception as e:
            logger.error(f"Failed to get conditional tokens address: {e}")
            conditional_address = None

        wallet_address = Web3.to_checksum_address(self._wallet_address)
        private_key = self.config.wallet.private_key

        # 1. Check and set USDC approval
        usdc_address = Web3.to_checksum_address(self.config.network.usdc_address)
        usdc_contract = self._web3.eth.contract(address=usdc_address, abi=ERC20_ABI)

        try:
            current_allowance = usdc_contract.functions.allowance(
                wallet_address, exchange_address
            ).call()

            logger.info(f"Current USDC allowance: {current_allowance / 1e6:,.2f} USDC")

            if current_allowance < MIN_ALLOWANCE_USDC:
                # Per Grok Round 19: Use CAPPED approval, not unlimited
                logger.info(f"USDC allowance insufficient (${current_allowance/1e6:,.2f}) - setting CAPPED approval (${CAPPED_APPROVAL_USDC/1e6:,.0f})...")
                self._send_approval_tx(
                    usdc_contract, exchange_address, CAPPED_APPROVAL_USDC,
                    wallet_address, private_key, "USDC"
                )
            else:
                logger.info(f"USDC allowance sufficient: ${current_allowance/1e6:,.2f}")

        except Exception as e:
            logger.error(f"USDC approval check/set failed: {e}")

        # 2. Check and set ConditionalTokens approval (ERC1155 - uses setApprovalForAll)
        if conditional_address:
            try:
                # ConditionalTokens is ERC1155, needs setApprovalForAll
                ct_abi = json.loads('''[
                    {
                        "constant": true,
                        "inputs": [
                            {"name": "account", "type": "address"},
                            {"name": "operator", "type": "address"}
                        ],
                        "name": "isApprovedForAll",
                        "outputs": [{"name": "", "type": "bool"}],
                        "type": "function"
                    },
                    {
                        "constant": false,
                        "inputs": [
                            {"name": "operator", "type": "address"},
                            {"name": "approved", "type": "bool"}
                        ],
                        "name": "setApprovalForAll",
                        "outputs": [],
                        "type": "function"
                    }
                ]''')

                ct_contract = self._web3.eth.contract(address=conditional_address, abi=ct_abi)

                is_approved = ct_contract.functions.isApprovedForAll(
                    wallet_address, exchange_address
                ).call()

                if not is_approved:
                    logger.info("ConditionalTokens not approved - setting approval...")
                    self._send_ct_approval_tx(
                        ct_contract, exchange_address, wallet_address, private_key
                    )
                else:
                    logger.info("ConditionalTokens already approved for exchange")

            except Exception as e:
                logger.error(f"ConditionalTokens approval check/set failed: {e}")

        logger.info("EOA token approvals check complete")

    def _send_approval_tx(
        self,
        contract,
        spender: str,
        amount: int,
        wallet_address: str,
        private_key: str,
        token_name: str
    ):
        """
        Send an ERC20 approve transaction with exponential backoff retry.

        Per Grok Round 21: Polygon gas spikes are common. Single-attempt approvals
        can block trading indefinitely. Loop with exponential backoff + alert on
        persistent failure.
        """
        MAX_RETRIES = 10  # Per Grok Round 21: Aggressive retry for production
        ALERT_THRESHOLD = 5  # Alert webhook after 5 failures

        for attempt in range(MAX_RETRIES):
            try:
                # Build transaction with dynamic gas price (bump on retries)
                nonce = self._web3.eth.get_transaction_count(wallet_address, 'pending')
                base_gas_price = self._web3.eth.gas_price

                # Per Grok Round 21: Bump gas price on retries (10% per attempt)
                gas_multiplier = 1.0 + (attempt * 0.1)
                gas_price = int(base_gas_price * gas_multiplier)

                tx = contract.functions.approve(spender, amount).build_transaction({
                    'from': wallet_address,
                    'nonce': nonce,
                    'gas': 100000,
                    'gasPrice': gas_price,
                    'chainId': self.config.network.chain_id
                })

                # Sign and send
                account = Account.from_key(private_key)
                signed_tx = account.sign_transaction(tx)
                tx_hash = self._web3.eth.send_raw_transaction(signed_tx.raw_transaction)

                logger.info(f"{token_name} approval tx sent: {tx_hash.hex()} (attempt {attempt + 1})")

                # Wait for confirmation
                receipt = self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

                if receipt.status == 1:
                    logger.info(f"{token_name} CAPPED approval confirmed (${CAPPED_APPROVAL_USDC/1e6:,.0f} limit)")
                    return True  # Success
                else:
                    logger.error(f"{token_name} approval tx failed (reverted)")

            except Exception as e:
                logger.warning(f"{token_name} approval attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")

                # Per Grok Round 21: Alert webhook after ALERT_THRESHOLD failures
                if attempt + 1 == ALERT_THRESHOLD:
                    logger.error(
                        f"APPROVAL ALERT: {token_name} approval failed {ALERT_THRESHOLD} times! "
                        f"Trading may be blocked. Check Polygon gas prices and wallet MATIC balance."
                    )
                    # TODO: Add webhook notification here when webhook system is implemented

                if attempt < MAX_RETRIES - 1:
                    # Exponential backoff: 2^attempt seconds (2, 4, 8, 16, 32, ...)
                    delay = min(2 ** attempt, 60)  # Cap at 60s
                    logger.info(f"Retrying {token_name} approval in {delay}s...")
                    import time
                    time.sleep(delay)

        # All retries exhausted
        logger.error(
            f"CRITICAL: {token_name} approval failed after {MAX_RETRIES} attempts! "
            f"Trading will be blocked until manually resolved."
        )
        return False

    def _send_ct_approval_tx(
        self,
        contract,
        operator: str,
        wallet_address: str,
        private_key: str
    ):
        """
        Send an ERC1155 setApprovalForAll transaction with exponential backoff retry.

        Per Grok Round 21: Same retry logic as USDC approval.
        """
        MAX_RETRIES = 10
        ALERT_THRESHOLD = 5

        for attempt in range(MAX_RETRIES):
            try:
                nonce = self._web3.eth.get_transaction_count(wallet_address, 'pending')
                base_gas_price = self._web3.eth.gas_price

                # Bump gas price on retries
                gas_multiplier = 1.0 + (attempt * 0.1)
                gas_price = int(base_gas_price * gas_multiplier)

                tx = contract.functions.setApprovalForAll(operator, True).build_transaction({
                    'from': wallet_address,
                    'nonce': nonce,
                    'gas': 100000,
                    'gasPrice': gas_price,
                    'chainId': self.config.network.chain_id
                })

                account = Account.from_key(private_key)
                signed_tx = account.sign_transaction(tx)
                tx_hash = self._web3.eth.send_raw_transaction(signed_tx.raw_transaction)

                logger.info(f"ConditionalTokens approval tx sent: {tx_hash.hex()} (attempt {attempt + 1})")

                receipt = self._web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

                if receipt.status == 1:
                    logger.info("ConditionalTokens approval confirmed!")
                    return True
                else:
                    logger.error("ConditionalTokens approval tx failed (reverted)")

            except Exception as e:
                logger.warning(f"ConditionalTokens approval attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")

                if attempt + 1 == ALERT_THRESHOLD:
                    logger.error(
                        f"APPROVAL ALERT: ConditionalTokens approval failed {ALERT_THRESHOLD} times! "
                        f"Trading may be blocked."
                    )

                if attempt < MAX_RETRIES - 1:
                    delay = min(2 ** attempt, 60)
                    logger.info(f"Retrying ConditionalTokens approval in {delay}s...")
                    import time
                    time.sleep(delay)

        logger.error(
            f"CRITICAL: ConditionalTokens approval failed after {MAX_RETRIES} attempts!"
        )
        return False

    def _verify_api_creds(self, client: ClobClient) -> bool:
        """
        Verify API credentials work by testing an authenticated endpoint.

        Args:
            client: The ClobClient to verify.

        Returns:
            True if credentials are valid.

        Raises:
            Exception: If credentials verification fails.
        """
        try:
            # Try to get API keys - this validates our credentials
            api_keys = client.get_api_keys()
            if api_keys:
                logger.info(f"Credentials verified: {len(api_keys)} API key(s) found")
                return True
            else:
                logger.warning("No API keys returned - credentials may not be properly set up")
                return False
        except Exception as e:
            error_msg = str(e).lower()
            if "invalid signature" in error_msg or "unauthorized" in error_msg:
                logger.error(f"Credential verification failed - invalid signature: {e}")
                logger.error("Check that:")
                logger.error("  1. PRIVATE_KEY is the correct key for trading")
                logger.error("  2. SIGNATURE_TYPE matches your wallet type (0=EOA, 1=POLY_PROXY)")
                logger.error("  3. For EOA (SIGNATURE_TYPE=0): FUNDER_ADDRESS should be empty or match wallet")
                logger.error("  4. For POLY_PROXY (SIGNATURE_TYPE=1): FUNDER_ADDRESS = your Polymarket proxy address")
                raise
            else:
                # Other errors (network, etc.) - log but don't fail
                logger.debug(f"Credential verification skipped: {e}")
                return True

    def get_client(self) -> ClobClient:
        """
        Get the initialized CLOB client.

        Returns:
            The initialized ClobClient instance.

        Raises:
            RuntimeError: If client has not been initialized.
        """
        if self._client is None:
            raise RuntimeError("Client not initialized. Call initialize() first.")
        return self._client

    def get_wallet_address(self) -> Optional[str]:
        """
        Get the wallet address.

        Returns:
            The wallet address or None if not initialized.
        """
        return self._wallet_address

    def get_api_creds(self) -> Optional[ApiCreds]:
        """
        Get the API credentials.

        Returns:
            The API credentials or None if in dry run mode.
        """
        return self._api_creds

    def verify_connection(self) -> Tuple[bool, str]:
        """
        Verify the connection to the CLOB API.

        Returns:
            Tuple of (success, message).
        """
        try:
            client = self.get_client()
            server_time = client.get_server_time()
            return True, f"Connected. Server time: {server_time}"
        except Exception as e:
            return False, f"Connection failed: {e}"

    def _init_web3(self):
        """Initialize Web3 connection for balance checks."""
        if self._web3 is None:
            self._web3 = Web3(Web3.HTTPProvider(self.config.network.polygon_rpc))
            self._web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if self._web3.is_connected():
                # Initialize USDC contract
                self._usdc_contract = self._web3.eth.contract(
                    address=Web3.to_checksum_address(self.config.network.usdc_address),
                    abi=ERC20_ABI
                )
                logger.debug("Web3 initialized for balance checks")
            else:
                logger.warning("Failed to connect to Polygon RPC for balance checks")

    def get_usdc_balance(self, max_retries: int = 3) -> Optional[float]:
        """
        Get USDC balance from the blockchain with retry logic.

        Args:
            max_retries: Maximum number of RPC retries.

        Returns:
            USDC balance in USD or None if unable to fetch.
        """
        if self.config.dry_run:
            # Return simulated balance for dry run
            return self.config.starting_capital_usd

        if not self._wallet_address:
            logger.warning("Wallet address not initialized")
            return None

        try:
            self._init_web3()

            if self._usdc_contract is None:
                return None

            # Use funder address if set (for proxy wallets)
            check_address = self.config.wallet.funder_address or self._wallet_address
            check_address = Web3.to_checksum_address(check_address)

            # Get balance with retry (USDC has 6 decimals)
            raw_balance = _retry_rpc_sync(
                lambda: self._usdc_contract.functions.balanceOf(check_address).call(),
                max_retries=max_retries,
                operation_name="get_usdc_balance"
            )
            balance = raw_balance / 1e6  # Convert from 6 decimals

            logger.info(f"USDC Balance: ${balance:,.2f}")
            return balance

        except Exception as e:
            logger.error(f"Failed to fetch USDC balance after retries: {e}")
            return None

    def verify_sufficient_balance(self, min_balance: Optional[float] = None) -> Tuple[bool, str]:
        """
        Verify the wallet has sufficient USDC balance for trading.

        Args:
            min_balance: Minimum required balance (defaults to min_capital_required).

        Returns:
            Tuple of (sufficient, message).
        """
        if self.config.dry_run:
            return True, f"Dry run mode - simulated balance ${self.config.starting_capital_usd:,.2f}"

        if min_balance is None:
            # Use min_capital_required ($900) as the hard floor
            min_balance = self.config.min_capital_required

        balance = self.get_usdc_balance()

        if balance is None:
            return False, "Unable to fetch USDC balance"

        if balance < min_balance:
            return False, f"Insufficient balance: ${balance:,.2f} < ${min_balance:,.2f} required"

        return True, f"Balance: ${balance:,.2f}"

    def get_matic_balance(self, address: Optional[str] = None, max_retries: int = 3) -> Optional[float]:
        """
        Get native MATIC balance for gas from the blockchain.

        Args:
            address: Address to check (defaults to wallet_address).
            max_retries: Maximum number of RPC retries.

        Returns:
            MATIC balance or None if unable to fetch.
        """
        if self.config.dry_run:
            return 1.0  # Simulated balance

        check_address = address or self._wallet_address
        if not check_address:
            logger.warning("Wallet address not initialized for MATIC check")
            return None

        try:
            self._init_web3()

            if self._web3 is None or not self._web3.is_connected():
                return None

            check_address = Web3.to_checksum_address(check_address)

            # Get native balance with retry
            raw_balance = _retry_rpc_sync(
                lambda: self._web3.eth.get_balance(check_address),
                max_retries=max_retries,
                operation_name="get_matic_balance"
            )
            balance = raw_balance / 1e18  # Convert from wei to MATIC

            logger.debug(f"MATIC Balance for {check_address[:10]}...: {balance:.6f}")
            return balance

        except Exception as e:
            logger.error(f"Failed to fetch MATIC balance after retries: {e}")
            return None

    def verify_gas_balance(self) -> Tuple[bool, str]:
        """
        Verify the signer wallet has enough MATIC for gas.

        For proxy wallets (SIGNATURE_TYPE=1), on-chain txs are relayed by Polymarket.
        But for EOA wallets, direct gas is needed.

        Returns:
            Tuple of (has_gas, message).
        """
        if self.config.dry_run:
            return True, "Dry run mode - gas check skipped"

        # For proxy wallets, Polymarket handles gas via relayer
        if self.config.wallet.signature_type == 1:
            logger.info("Proxy wallet mode - gas paid by Polymarket relayer")
            return True, "Proxy wallet - gas handled by relayer"

        # For EOA, check MATIC on signer wallet
        matic_balance = self.get_matic_balance()

        if matic_balance is None:
            return False, "Unable to check MATIC balance"

        if matic_balance < MIN_MATIC_FOR_GAS:
            msg = (
                f"INSUFFICIENT MATIC FOR GAS!\n"
                f"  Current: {matic_balance:.6f} MATIC\n"
                f"  Required: {MIN_MATIC_FOR_GAS:.4f} MATIC (raised for rn1-style burst trading)\n"
                f"  Wallet: {self._wallet_address}\n"
                f"  \n"
                f"  Fund your wallet with MATIC:\n"
                f"    - Bridge from Ethereum: https://wallet.polygon.technology/bridge\n"
                f"    - Buy on exchange and withdraw to Polygon\n"
                f"    - Use Polygon faucet (for small amounts)"
            )
            return False, msg

        # Warning if approaching minimum
        if matic_balance < MATIC_WARNING_THRESHOLD:
            logger.warning(
                f"MATIC balance low: {matic_balance:.4f} MATIC "
                f"(warning threshold: {MATIC_WARNING_THRESHOLD} MATIC)"
            )

        return True, f"MATIC balance OK: {matic_balance:.6f} MATIC"

    def get_balance(self) -> Optional[dict]:
        """
        Get the account balance information.

        Returns:
            Balance information dict or None if unable to fetch.
        """
        if self.config.dry_run:
            return {
                "status": "dry_run",
                "wallet": "simulated",
                "usdc_balance": self.config.starting_capital_usd,
                "matic_balance": 1.0
            }

        usdc_balance = self.get_usdc_balance()
        matic_balance = self.get_matic_balance()

        return {
            "status": "authenticated",
            "wallet": self._wallet_address,
            "funder": self.config.wallet.funder_address or self._wallet_address,
            "usdc_balance": usdc_balance,
            "matic_balance": matic_balance
        }

    def check_usdc_allowance(self, client: ClobClient, min_required: float = 100.0) -> Tuple[bool, float]:
        """
        Check current USDC allowance against exchange.

        This can be called before trading to detect approval issues early,
        rather than getting 400 "not enough balance / allowance" errors.

        Args:
            client: CLOB client to get exchange address.
            min_required: Minimum required allowance in USD.

        Returns:
            Tuple of (has_sufficient, current_allowance_usd).
        """
        if self.config.dry_run:
            return True, float('inf')

        if self.config.wallet.signature_type != 0:
            # Proxy wallets don't need manual approvals
            return True, float('inf')

        # Per Grok Round 19 HIGH FIX: No optimistic returns - fail safe, not fail open
        # If we can't verify allowance, block trading to prevent 400 errors
        self._init_web3()
        if self._web3 is None or not self._web3.is_connected():
            logger.error("BLOCKED: Cannot check allowance - Web3 not connected")
            raise ConnectionError("Web3 not connected - cannot verify USDC allowance for trading")

        try:
            # Get exchange address
            exchange_address = client.get_exchange_address()
            exchange_address = Web3.to_checksum_address(exchange_address)

            # Get USDC allowance with retry
            wallet_address = Web3.to_checksum_address(self._wallet_address)
            usdc_address = Web3.to_checksum_address(self.config.network.usdc_address)
            usdc_contract = self._web3.eth.contract(address=usdc_address, abi=ERC20_ABI)

            allowance_raw = _retry_rpc_sync(
                lambda: usdc_contract.functions.allowance(wallet_address, exchange_address).call(),
                max_retries=3,
                operation_name="check_usdc_allowance"
            )

            allowance_usd = allowance_raw / 1e6  # USDC has 6 decimals

            if allowance_usd < min_required:
                logger.warning(
                    f"USDC allowance LOW: ${allowance_usd:,.2f} < required ${min_required:,.2f}. "
                    f"Consider re-approving to avoid 400 errors."
                )
                return False, allowance_usd

            return True, allowance_usd

        except Exception as e:
            # Per Grok Round 19: Raise on failure instead of optimistic True
            # This blocks trading until allowance is confirmed - fail safe
            logger.error(f"BLOCKED: Error checking USDC allowance after retries: {e}")
            raise RuntimeError(f"Cannot verify USDC allowance - blocking trading: {e}")

    def ensure_sufficient_allowance(
        self,
        client: ClobClient,
        required_usd: float = 10000.0,
        current_capital: Optional[float] = None
    ) -> bool:
        """
        Ensure USDC allowance is sufficient, re-approve if needed.

        Per Grok Round 23: Allowance requirement scales with current capital.
        rn1's unlimited approvals scaled naturally; we use 2x capital buffer
        to avoid mid-trade approval failures during burst execution.

        Args:
            client: CLOB client to get exchange address.
            required_usd: Minimum required allowance in USD.
            current_capital: Current trading capital (used to scale requirement).

        Returns:
            True if allowance is now sufficient.
        """
        # Per Grok Round 23: Scale allowance requirement with capital
        # Use 2x capital buffer to handle burst trading (rn1's live game pattern)
        # Example: $50k capital → require $100k allowance
        if current_capital is not None and current_capital > 0:
            capital_scaled_requirement = current_capital * 2.0
            required_usd = max(required_usd, capital_scaled_requirement)
            logger.debug(
                f"Allowance requirement scaled to ${required_usd:,.2f} "
                f"(2x capital ${current_capital:,.2f})"
            )

        has_sufficient, current = self.check_usdc_allowance(client, required_usd)

        if has_sufficient:
            return True

        logger.info(f"USDC allowance ${current:,.2f} below required ${required_usd:,.2f}, re-approving...")

        # Re-run approval
        self._ensure_eoa_approvals(client)

        # Re-check
        has_sufficient, new_allowance = self.check_usdc_allowance(client, required_usd)
        if has_sufficient:
            logger.info(f"USDC allowance restored: ${new_allowance:,.2f}")
        else:
            logger.error(f"USDC allowance still insufficient after re-approval: ${new_allowance:,.2f}")

        return has_sufficient
