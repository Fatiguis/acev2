# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ace-Arb is a Polymarket arbitrage bot that exploits pricing inefficiencies in prediction markets. It detects opportunities where the sum of best asks < $1 (buy arb) or sum of best bids > $1 (sell arb), then executes trades to lock in risk-free profit.

The bot is modeled after "rn1" - a highly successful Polymarket trader with $2.6M+ profit on $113M volume.

## RN1 Strategy: Volatility Harvesting

### Core Insight
In a binary market (YES/NO), owning 1 YES share + 1 NO share = guaranteed $1.00 payout. One of them MUST win.

### How It Works
During live games, prices swing wildly based on events:

1. **Team A scores** → YES jumps to $0.65, NO drops to $0.38
   - RN1 buys NO at $0.38

2. **Team B scores** → YES drops to $0.45, NO jumps to $0.58
   - RN1 buys YES at $0.45

Result:
- 1 YES share (cost $0.45) + 1 NO share (cost $0.38)
- **Total cost: $0.83**
- **Guaranteed payout: $1.00**
- **Locked profit: $0.17 (20.5% return)**

### The Math
```
Guaranteed Profit = matched_shares × (1 - avg_YES_price - avg_NO_price)
```

If avg_YES = $0.52 and avg_NO = $0.41, combined = $0.93
Profit per matched pair = $1.00 - $0.93 = $0.07 (7.5% edge)

### vs Traditional Arbitrage

| Traditional Arb | RN1 Volatility Harvesting |
|----------------|---------------------------|
| Instant: buy both sides when sum < $1 | Accumulate over time during game |
| Rare opportunities | Many opportunities per game |
| Needs both sides cheap simultaneously | Needs each side cheap at different times |
| Small edges (1-2%) | Larger edges (5-15%) |

### Key RN1 Patterns
- Small frequent trades (~$27 avg, now scaling to $170)
- Buy-arb only (no shorting)
- 2-3% edge threshold
- Zero gas via proxy wallets (SIGNATURE_TYPE=1)
- FAK order type for partial fills
- ".11" signature pattern (350.11, 750.11 shares) - fixed sizing

### Position Tracking Module
`position_tracker.py` implements this strategy:
- `OutcomePosition`: Tracks shares, cost, avg price per outcome
- `MarketPosition`: Calculates matched shares and guaranteed profit
- `PositionTracker`: Global tracker integrated into main bot loop

### RN1 Target Markets (Live Data Jan 2026)

**Primary: Live Sports (97%+ of trades)**

| Category | Markets | Why |
|----------|---------|-----|
| **Soccer (Serie A, EPL)** | Napoli, Atalanta, Arsenal, Aston Villa | High volatility during matches |
| **Esports (CS2, LoL)** | Washington vs CSDIILIT, Brute vs HyperSpirit | Fast-paced, frequent score changes |
| **Tennis (ATP, WTA)** | Hong Kong Open, Brisbane International | Point-by-point volatility |
| **NBA Basketball** | Mavericks vs Kings, Jazz vs Thunder | In-game momentum swings |

**Market Types per Game:**
```
Same game → multiple markets:
├── Will Team A win? (YES/NO)
├── Will Team B win? (YES/NO)
├── Draw? (YES/NO)
├── O/U 2.5, 3.5, 4.5 goals
├── Spread: Team A -1.5
└── Both Teams to Score
```

**What RN1 Does NOT Trade:**
- Politics (only 1.2%)
- Crypto predictions
- Long-term events without live action

**Key Insight:** RN1 targets markets with in-game volatility where prices swing every few seconds based on game events.

## Commands

```bash
# Run the bot
python main.py

# Dry run mode (no real trades)
DRY_RUN=true python main.py

# Cancel all open orders
python cancel_orders.py

# Close all positions
python close_positions.py
```

## Architecture

### Core Data Flow
```
MarketDiscovery → OrderbookPoller/WebSocket → ArbDetection → RiskManager → ExecutionEngine
```

### Key Modules

**main.py** - `ArbBot` orchestrator. Runs hybrid WebSocket+HTTP polling loop, coordinates all modules, handles graceful shutdown.

**orderbook.py** - `OrderbookPoller` fetches via HTTP, `ArbOpportunity` detection. Detects buy_arb (sum asks < 1) and sell_arb (sum bids > 1) for binary and multi-outcome markets.

**websocket_client.py** - `PolymarketWebSocket` for <100ms latency updates, `HybridOrderbookManager` combines WS+HTTP. Falls back to HTTP on disconnect with latency penalty.

**execution.py** - `ExecutionEngine` places orders. Uses `run_sync_in_thread()` since py-clob-client is synchronous. Supports batch orders (up to 15/request), FAK for partial fills, atomic arb pattern with hedge on failure.

**risk.py** - `RiskManager` for position sizing via Kelly criterion, circuit breaker (>5 consecutive failures = 30min pause), exposure limits. `DepthValidator` verifies orderbook depth.

**edge_model.py** - `EdgeExpectancyModel` calculates fill probability based on latency (P(fill) ≈ e^(-λt)), market heat classification, front-running probability.

**config.py** - All configuration via environment variables and dataclasses. `TradingConfig`, `WalletConfig`, `NetworkConfig`.

**auth.py** - `AuthManager` handles CLOB client initialization, API credential management, USDC allowance checks.

**wallet_manager.py** - `WalletManager` for multi-wallet rotation, balance tracking. Supports both EOA (private key) and proxy (API creds) wallets.

### Async Pattern
py-clob-client is synchronous (requests-based). All CLOB calls go through `run_sync_in_thread()` in `clob_client_patch.py` to avoid blocking the asyncio event loop.

### Order Types
- **GTC** (Good-Til-Cancelled): Maker orders, earns rebates
- **FOK** (Fill-Or-Kill): All-or-nothing taker
- **FAK** (Fill-And-Kill): Partial fill allowed, cancel rest (recommended for arb)

## Critical Configuration

### Authentication Modes
```bash
# EOA Mode (you pay gas ~$0.01-0.05/tx)
SIGNATURE_TYPE=0
PRIVATE_KEY=0x...

# Proxy Mode (ZERO gas - recommended)
SIGNATURE_TYPE=1
API_KEY=...
API_SECRET=...
API_PASSPHRASE=...
FUNDER_ADDRESS=0x...  # Your Polymarket proxy wallet address
```

### Key Trading Parameters
- `ARB_THRESHOLD_BASE`: Minimum edge to trade (default 2%)
- `USE_BATCH_ORDERS`: Batch API for speed (default true)
- `USE_POST_ONLY`: Maker-first strategy (default false for speed)
- `MIN_CAPITAL_REQUIRED`: Minimum to start trading ($5)
- `MAX_TRADE_SIZE_USD`: Cap per trade ($20 default)

## Polymarket API Constraints
- Minimum order size: 5 shares
- Tick sizes: 0.01 (standard), 0.001 (fine, for prices <0.04 or >0.96)
- Rate limits: 3,500/10s burst for POST /order, 1,000/10s for batch
- Fees: 0% on most markets (only 15-min crypto has taker fees)
- Batch orders: Max 15 orders per POST /orders request

## WebSocket vs HTTP
- WebSocket: ~50ms latency, ~70% fill probability
- HTTP polling: ~800ms latency, ~10% fill probability
- Bot automatically falls back to HTTP on WS disconnect with fill probability penalty

## Polymarket Documentation Reference

### Documentation Structure
Polymarket docs (docs.polymarket.com) has three main sections:
- **Polymarket Learn**: User guide for beginners (deposits, trading, FAQs)
- **Developers**: Technical docs (CLOB, APIs, SDKs, market making)
- **Changelog**: Platform updates and API changes

### Key Developer Documentation URLs

| Category | URL |
|----------|-----|
| CLOB Introduction | https://docs.polymarket.com/developers/CLOB/introduction |
| Create Order | https://docs.polymarket.com/developers/CLOB/orders/create-order |
| Create Order Batch | https://docs.polymarket.com/developers/CLOB/orders/create-order-batch |
| Rate Limits | https://docs.polymarket.com/quickstart/introduction/rate-limits |
| WebSocket Migration | https://docs.polymarket.com/developers/CLOB/websocket/market-channel-migration-guide |
| Proxy Wallet | https://docs.polymarket.com/developers/proxy-wallet |
| Negative Risk | https://docs.polymarket.com/developers/neg-risk/overview |
| Fees | https://docs.polymarket.com/polymarket-learn/trading/fees |
| Maker Rebates | https://docs.polymarket.com/polymarket-learn/trading/maker-rebates-program |
| Gamma API | https://docs.polymarket.com/developers/gamma-markets-api/overview |
| Data API Positions | https://docs.polymarket.com/developers/misc-endpoints/data-api-get-positions |
| Changelog | https://docs.polymarket.com/changelog/changelog |

### API Rate Limits (from docs)
```
POST /order:   3,500/10s burst, 36,000/10min sustained
POST /orders:  1,000/10s burst, 15,000/10min sustained
DELETE /order: 3,000/10s burst, 30,000/10min sustained
/book:         1,500/10s
/books:        500/10s
```

### Order Types (from docs)
- **GTC**: Good-Til-Cancelled - limit order active until fulfilled/cancelled
- **GTD**: Good-Til-Date - active until specified UTC timestamp
- **FOK**: Fill-Or-Kill - must execute entirely or cancel completely
- **FAK**: Fill-And-Kill - execute available shares, cancel remainder

### WebSocket New Format (Sept 2025)
```json
{
  "price_changes": [
    {
      "asset_id": "token_id",
      "side": "BUY",
      "price": "0.50",
      "size": "100",
      "best_bid": "0.49",
      "best_ask": "0.51"
    }
  ]
}
```
Bot handles both old (`changes` array) and new (`price_changes` array) formats.

### External Resources
- GitHub CLOB Client: https://github.com/Polymarket/py-clob-client
- CLOB Audit: https://github.com/Polymarket/ctf-exchange/blob/main/audit/ChainSecurity_Polymarket_Exchange_audit.pdf
- Discord: https://discord.gg/polymarket

