import time
from typing import Dict, List, Tuple
import logging
import uuid
import math
from src.clients import KalshiHttpClient #Using Kalshi example API code


class AvellanedaMarketMaker:
    def __init__(
        self,
        logger: logging.Logger,
        client: KalshiHttpClient,
        market_ticker: str,
        gamma: float,
        k: float,
        sigma: float,
        T: float,
        max_position: int,
        order_expiration: int,
        min_spread: float = 0.01,
        position_limit_buffer: float = 0.1,
        inventory_skew_factor: float = 0.01,
        trade_side: str = "yes"
    ):
        self.client = client
        self.market_ticker = market_ticker
        self.logger = logger
        self.base_gamma = gamma
        self.k = k
        self.sigma = sigma
        self.T = T
        self.max_position = max_position
        self.order_expiration = order_expiration
        self.min_spread = min_spread
        self.position_limit_buffer = position_limit_buffer
        self.inventory_skew_factor = inventory_skew_factor
        self.trade_side = trade_side

    def get_mid_price(self) -> Dict[str, float]:
        """Get mid-market prices for yes/no sides.

        Returns:
            Dict with 'yes' and 'no' keys, values in dollars
        """
        data = self.client.get_market(self.market_ticker)
        market = data['market']

        # Extract bid/ask prices (API returns in cents)
        yes_bid = float(market['yes_bid']) / 100
        yes_ask = float(market['yes_ask']) / 100
        no_bid = float(market['no_bid']) / 100
        no_ask = float(market['no_ask']) / 100

        # Calculate mid-prices
        yes_mid = round((yes_bid + yes_ask) / 2, 2)
        no_mid = round((no_bid + no_ask) / 2, 2)

        return {"yes": yes_mid, "no": no_mid}

    def get_current_position(self) -> int:
        """Get current net position for this market.

        Returns:
            Net position as integer (positive = long, negative = short)
        """
        response = self.client.get_positions(
            ticker=self.market_ticker,
            settlement_status="unsettled"
        )

        positions = response.get('market_positions', [])

        # Sum up positions for this ticker
        total_position = 0
        for pos in positions:
            if pos['ticker'] == self.market_ticker:
                total_position += pos['position']

        return total_position

    def get_resting_orders(self) -> List[Dict]:
        """Get all resting orders for this market.

        Returns:
            List of order dictionaries
        """
        response = self.client.get_orders(
            ticker=self.market_ticker,
            status="resting"
        )

        return response.get('orders', [])

    def run(self, dt: float):
        start_time = time.time()
        while time.time() - start_time < self.T:
            current_time = time.time() - start_time
            self.logger.info(f"Running Avellaneda market maker at {current_time:.2f}")

            mid_prices = self.get_mid_price()
            mid_price = mid_prices[self.trade_side]
            inventory = self.get_current_position()
            self.logger.info(f"Current mid price for {self.trade_side}: {mid_price:.4f}, Inventory: {inventory}")

            reservation_price = self.calculate_reservation_price(mid_price, inventory, current_time)
            bid_price, ask_price = self.calculate_asymmetric_quotes(mid_price, inventory, current_time)
            buy_size, sell_size = self.calculate_order_sizes(inventory)

            self.logger.info(f"Reservation price: {reservation_price:.4f}")
            self.logger.info(f"Computed desired bid: {bid_price:.4f}, ask: {ask_price:.4f}")

            self.manage_orders(bid_price, ask_price, buy_size, sell_size)

            time.sleep(dt)

        self.logger.info("Avellaneda market maker finished running")

    def calculate_asymmetric_quotes(self, mid_price: float, inventory: int, t: float) -> Tuple[float, float]:
        reservation_price = self.calculate_reservation_price(mid_price, inventory, t)
        base_spread = self.calculate_optimal_spread(t, inventory)
        
        position_ratio = inventory / self.max_position
        spread_adjustment = base_spread * abs(position_ratio) * 3
        
        if inventory > 0:
            bid_spread = base_spread / 2 + spread_adjustment
            ask_spread = max(base_spread / 2 - spread_adjustment, self.min_spread / 2)
        else:
            bid_spread = max(base_spread / 2 - spread_adjustment, self.min_spread / 2)
            ask_spread = base_spread / 2 + spread_adjustment
        
        bid_price = max(0, min(mid_price, reservation_price - bid_spread))
        ask_price = min(1, max(mid_price, reservation_price + ask_spread))
        
        return bid_price, ask_price

    def calculate_reservation_price(self, mid_price: float, inventory: int, t: float) -> float:
        dynamic_gamma = self.calculate_dynamic_gamma(inventory)
        inventory_skew = inventory * self.inventory_skew_factor * mid_price
        return mid_price + inventory_skew - inventory * dynamic_gamma * (self.sigma**2) * (1 - t/self.T)

    def calculate_optimal_spread(self, t: float, inventory: int) -> float:
        dynamic_gamma = self.calculate_dynamic_gamma(inventory)
        base_spread = (dynamic_gamma * (self.sigma**2) * (1 - t/self.T) + 
                       (2 / dynamic_gamma) * math.log(1 + (dynamic_gamma / self.k)))
        position_ratio = abs(inventory) / self.max_position
        spread_adjustment = 1 - (position_ratio ** 2)
        return max(base_spread * spread_adjustment * 0.01, self.min_spread)

    def calculate_dynamic_gamma(self, inventory: int) -> float:
        position_ratio = inventory / self.max_position
        return self.base_gamma * math.exp(-abs(position_ratio))

    def calculate_order_sizes(self, inventory: int) -> Tuple[int, int]:
        remaining_capacity = self.max_position - abs(inventory)
        buffer_size = int(self.max_position * self.position_limit_buffer)
        
        if inventory > 0:
            buy_size = max(1, min(buffer_size, remaining_capacity))
            sell_size = max(1, self.max_position)
        else:
            buy_size = max(1, self.max_position)
            sell_size = max(1, min(buffer_size, remaining_capacity))
        
        return buy_size, sell_size

    def manage_orders(self, bid_price: float, ask_price: float, buy_size: int, sell_size: int):
        current_orders = self.get_resting_orders()
        self.logger.info(f"Retrieved {len(current_orders)} total orders")

        buy_orders = []
        sell_orders = []

        for order in current_orders:
            if order['side'] == self.trade_side:
                if order['action'] == 'buy':
                    buy_orders.append(order)
                elif order['action'] == 'sell':
                    sell_orders.append(order)

        self.logger.info(f"Current buy orders: {len(buy_orders)}")
        self.logger.info(f"Current sell orders: {len(sell_orders)}")

        # Handle buy orders
        self.handle_order_side('buy', buy_orders, bid_price, buy_size)

        # Handle sell orders
        self.handle_order_side('sell', sell_orders, ask_price, sell_size)

    def handle_order_side(self, action: str, orders: List[Dict], desired_price: float, desired_size: int):
        keep_order = None
        for order in orders:
            current_price = float(order['yes_price']) / 100 if self.trade_side == 'yes' else float(order['no_price']) / 100
            if keep_order is None and abs(current_price - desired_price) < 0.01 and order['remaining_count'] == desired_size:
                keep_order = order
                self.logger.info(f"Keeping existing {action} order. ID: {order['order_id']}, Price: {current_price:.4f}")
            else:
                self.logger.info(f"Cancelling extraneous {action} order. ID: {order['order_id']}, Price: {current_price:.4f}")
                try:
                    self.client.cancel_order(order['order_id'])
                except Exception as e:
                    self.logger.error(f"Failed to cancel order {order['order_id']}: {e}")

        current_price = self.get_mid_price()[self.trade_side]
        if keep_order is None:
            if (action == 'buy' and desired_price < current_price) or (action == 'sell' and desired_price > current_price):
                try:
                    # Convert price from dollars to cents
                    price_cents = int(desired_price * 100)

                    # Determine which price field based on side
                    yes_price = price_cents if self.trade_side == "yes" else None
                    no_price = price_cents if self.trade_side == "no" else None

                    # Generate unique client order ID
                    client_order_id = str(uuid.uuid4())

                    # Create order via HTTP client
                    response = self.client.create_order(
                        ticker=self.market_ticker,
                        client_order_id=client_order_id,
                        side=self.trade_side,
                        action=action,
                        count=desired_size,
                        type="limit",
                        yes_price=yes_price,
                        no_price=no_price,
                        expiration_ts=int(time.time()) + self.order_expiration
                    )

                    order_id = response['order']['order_id']
                    self.logger.info(f"Placed new {action} order. ID: {order_id}, Price: {desired_price:.4f}, Size: {desired_size}")
                except Exception as e:
                    self.logger.error(f"Failed to place {action} order: {str(e)}")
            else:
                self.logger.info(f"Skipped placing {action} order. Desired price {desired_price:.4f} does not improve on current price {current_price:.4f}")
