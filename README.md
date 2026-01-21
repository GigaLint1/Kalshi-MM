# Kalshi Market Maker: Avellaneda-Stoikov Implementation

A Python-based market making system for [Kalshi](https://kalshi.com) prediction markets, implementing the **Avellaneda-Stoikov (2008)** optimal market making framework.

## Overview

This repository demonstrates the practical application of the seminal [Avellaneda & Stoikov (2008) paper](https://www.math.nyu.edu/faculty/avellane/HighFrequencyTrading.pdf) "High-frequency trading in a limit order book" to prediction markets. The implementation provides a hands-on learning tool for understanding how theoretical market making models work in practice.

### What This Does

The system continuously quotes bid and ask prices on Kalshi prediction markets, dynamically adjusting:
- **Spreads** based on market volatility and time remaining
- **Quote prices** based on inventory position (long/short)
- **Order sizes** to manage risk and prevent excessive exposure

The goal is to profit from the bid-ask spread while managing inventory risk through mathematically optimal price adjustments.

## The Avellaneda-Stoikov Framework

### Core Principles

The Avellaneda-Stoikov model solves the market maker's fundamental challenge: balancing profitability (wider spreads) against competitiveness (narrower spreads) while managing inventory risk.

#### 1. **Reservation Price**

The "fair value" adjusted for current inventory position:

```
r(s, q, t) = s - q * γ * σ² * (T - t)
```

Where:
- `s` = current mid-price
- `q` = current inventory (position)
- `γ` = risk aversion parameter
- `σ` = volatility
- `T - t` = time remaining

**Intuition**: When you're long (positive inventory), your reservation price is *lower* than mid-price, incentivizing you to sell. When short, it's higher, incentivizing buying.

#### 2. **Optimal Spread**

The theoretically optimal bid-ask spread:

```
δ = γ * σ² * (T - t) + (2/γ) * ln(1 + γ/k)
```

Where:
- `k` = order book depth parameter
- Other parameters as above

**Intuition**: Spreads widen when:
- More time remains (more uncertainty)
- Volatility is higher (more risk)
- Risk aversion is higher (more conservative)

#### 3. **Bid and Ask Prices**

```
bid = r - δ/2
ask = r + δ/2
```

The reservation price becomes the midpoint of your quotes, with the optimal spread determining the distance from this midpoint.

### Implementation Enhancements

This implementation extends the base model with:

#### **Dynamic Gamma** (Risk Aversion)
```python
γ_dynamic = γ_base * exp(-|q / q_max|)
```
Risk aversion increases exponentially as position approaches limits, widening spreads to slow down adverse inventory accumulation.

#### **Inventory Skew Factor**
```python
skew = q * inventory_skew_factor * s
r = s + skew - q * γ * σ² * (T - t)
```
Additional adjustment to push quotes away from accumulating more adverse inventory.

#### **Asymmetric Spread Adjustments**

When long (q > 0):
- Tighten bid spread (encourage buying out of position less)
- Widen ask spread (encourage selling out of position more)

When short (q < 0):
- Widen bid spread
- Tighten ask spread

#### **Position-Based Order Sizing**

Order quantities scale based on distance from position limits, becoming more conservative as exposure increases.

### Prerequisites

- Python 3.8+
- Kalshi API credentials (demo or production)
- RSA private key file for authentication

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/Kalshi-MM.git
cd Kalshi-MM
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Set up credentials**

Create a `.env` file:
```env
DEMO_KEYID=your-api-key-id
DEMO_KEYFILE=./gigalint.pem
KALSHI_ENVIRONMENT=DEMO  # or PROD
```

Place your RSA private key PEM file in the project directory.

### Configuration

Edit `config.yaml` to define your market making strategies:

```yaml
STRATEGY_NAME:
  api:
    market_ticker: "KXNCAAF-26-ORE"  # Kalshi market ticker
    trade_side: "yes"                 # "yes" or "no" side

  market_maker:
    # Position Management
    max_position: 5                   # Maximum contracts to hold
    position_limit_buffer: 0.1        # Safety margin (10%)

    # Avellaneda-Stoikov Parameters
    gamma: 0.1                        # Risk aversion (higher = wider spreads)
    k: 1.5                            # Order book depth parameter
    sigma: 0.001                      # Volatility estimate
    T: 28800                          # Time horizon (seconds, 8 hours)
    min_spread: 0.0                   # Minimum spread floor

    # Inventory Management
    inventory_skew_factor: 0.001      # How much inventory affects quotes

    # Order Management
    order_expiration: 28800           # Order lifetime (seconds)

  dt: 2.0                             # Time between iterations (seconds)
```

#### Parameter Tuning Guide

**Risk Aversion (`gamma`)**:
- Lower (0.01-0.1): Tighter spreads, more aggressive
- Higher (0.5-2.0): Wider spreads, more conservative
- Default: 0.1

**Volatility (`sigma`)**:
- Estimate from historical price movements
- Higher σ → wider spreads
- For prediction markets: typically 0.001-0.01

**Time Horizon (`T`)**:
- Should match your trading session duration
- Affects how quickly spreads converge as time passes
- Typical: 8 hours (28800 seconds)

**Order Book Depth (`k`)**:
- Higher k → tighter spreads (assuming more liquidity)
- Lower k → wider spreads (assuming less liquidity)
- Typical: 1.0-2.0

### Running

```bash
python runner.py
```

This will:
1. Load all strategies from `config.yaml`
2. Initialize Kalshi API connections
3. Start market making threads for each strategy
4. Log all activity to individual strategy log files

**Monitor logs:**
```bash
tail -f STRATEGY_NAME.log
```

## Example Output

```
2025-11-25 14:32:15 - Running Avellaneda market maker at 0.00
2025-11-25 14:32:15 - Current mid price for yes: 0.0700, Inventory: 0
2025-11-25 14:32:15 - Reservation price: 0.0700
2025-11-25 14:32:15 - Computed desired bid: 0.0635, ask: 0.0765
2025-11-25 14:32:15 - Placed new buy order. ID: 96f6..., Price: 0.0635, Size: 5
2025-11-25 14:32:15 - Placed new sell order. ID: 3899..., Price: 0.0765, Size: 1

[After buying 2 contracts]
2025-11-25 14:35:20 - Current mid price for yes: 0.0705, Inventory: 2
2025-11-25 14:35:20 - Reservation price: 0.0693  # Lower than mid!
2025-11-25 14:35:20 - Computed desired bid: 0.0620, ask: 0.0755
# Notice: quotes shifted down to encourage selling
```

### Built-in Safeguards

1. **Position Limits**: Hard caps on maximum exposure
2. **Position Buffers**: Order sizes reduce near limits
3. **Rate Limiting**: 100ms minimum between API calls
4. **Order Expiration**: All orders have maximum lifetime
5. **Dynamic Risk Aversion**: Gamma increases with extreme positions

### Kalshi API Documentation

- [Official API Docs](https://trading-api.readme.io/reference)
- Authentication: RSA-PSS signed requests
- Rate Limits: Be mindful of request frequency
- Market Data: Real-time bid/ask quotes
- Order Management: Create, cancel, query orders