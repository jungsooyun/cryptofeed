'''
Copyright (C) 2018-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import pytz
from collections import defaultdict
from cryptofeed.symbols import Symbol, str_to_symbol
import logging
from decimal import Decimal
from typing import List, Dict
from datetime import datetime as dt

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, BYBIT, CANCELLED, CANCELLING, CANDLES, FAILED, FILLED, FUNDING, L2_BOOK, LIMIT, LIQUIDATIONS, MAKER, MARKET, OPEN, PARTIAL, SELL, SUBMITTING, TAKER, TRADES, OPEN_INTEREST, INDEX, ORDER_INFO, FILLS, FUTURES, PERPETUAL
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Index, OpenInterest, Funding, OrderInfo, Fill, Candle, Liquidation


LOG = logging.getLogger('feedhandler')


class Bybit(Feed):
    id = BYBIT

    websocket_endpoints = [WebsocketEndpoint('wss://stream.bybit.com/v5/public/linear', instrument_filter=('QUOTE', ('USDT',))),
                           WebsocketEndpoint('wss://stream.bybit.com/v5/public/inverse', instrument_filter=('QUOTE', ('USD',))),
                           # WebsocketEndpoint('wss://stream.bybit.com/v5/public/spot', ...),
                           # WebsocketEndpoint('wss://stream.bybit.com/v5/public/option', ...),
                           # WebsocketEndpoint('wss://stream.bybit.com/v5/private', ...),
                           # Private
                           ]

    # NOTE: (jerry) rest_endpoints only used for get symbols
    rest_endpoints = [RestEndpoint('https://api.bybit.com', routes=Routes(['/v5/market/instruments-info?category=linear', '/v5/market/instruments-info?category=inverse']))]

    websocket_channels = {
        L2_BOOK: 'orderbook.200',  # Lev200: 100ms, hard-coded level
        TRADES: 'publicTrade',  # real-time
        INDEX: 'tickers',  # 100ms
        OPEN_INTEREST: 'tickers',  # 100ms
        FUNDING: 'tickers',  # 100ms
        CANDLES: 'kline',  # need candle interval, push frequence: 1-60s
        LIQUIDATIONS: 'liquidation',  # real-time
        # FILLS (private?)
        # ORDER_INFO (private?)
    }

    valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '1w', '1M'}
    candle_interval_map = {'1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30',
                           '1h': '60', '2h': '120', '4h': '240', '6h': '360', '12h': 720,
                           '1d': 'D', '1w': 'W', '1M': 'M'}

    @classmethod
    def _parse_symbol_data(cls, data: List[Dict]):
        """Parse return data from rest /v5/market/instruments-info?category=linear or inverse"""
        ret = {}
        info = defaultdict(dict)

        linear_data, inverse_data = data[0], data[1]
        agg_symbols = linear_data['result']['list'] + inverse_data['result']['list']
        for symbol in agg_symbols:
            base = symbol['baseCoin']
            quote = symbol['quoteCoin']

            stype = PERPETUAL
            expiry = None
            if not symbol['symbol'].endswith(quote):
                stype = FUTURES
                expiry_timestamp = int(symbol['deliveryTime'])  # timestamp (ms)
                expiry = dt.fromtimestamp(expiry_timestamp / 1000, tz=pytz.timezone('America/Los_Angeles'))  # Bybit uses PST/PDT

            s = Symbol(base, quote, type=stype, expiry_date=expiry)

            ret[s.normalized] = symbol['symbol']
            info['tick_size'][s.normalized] = symbol['priceFilter']['tickSize']
            info['instrument_type'][s.normalized] = stype

        return ret, info

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if "success" in msg:
            if msg['success']:
                if msg['op'] == 'auth':
                    LOG.debug(f"{conn.uuid}: Authenticated")
                elif msg['op'] == 'subscribe':
                    LOG.debug(f"{conn.uuid}: Subscribed to channels")
                else:
                    LOG.warning(f"{conn.uuid}: Unexpected success message {msg}")
            else:
                LOG.error(f"{conn.uuid}: Error from exchange {msg}")

        elif msg['topic'].startswith('publicTrade'):
            await self._trade(msg, timestamp)
        elif msg['topic'].startswith('orderbook'):
            await self._book(msg, timestamp)
        elif msg['topic'].startswith('kline'):
            await self._candles(msg, timestamp)
        elif msg['topic'].startswith('tickers'):
            await self._tickers(msg, timestamp)
        elif msg['topic'].startswith('liquidation'):
            await self._liquidations(msg, timestamp)
        else:
            LOG.warning(f"{conn.uuid}: Invalid message type {msg}")

    async def subscribe(self, connection: AsyncConnection):
        self.__reset(connection)
        for chan in connection.subscription:
            if not self.is_authenticated_channel(self.exchange_channel_to_std(chan)):
                for pair in connection.subscription[chan]:
                    sym = str_to_symbol(self.exchange_symbol_to_std_symbol(pair))

                    if self.exchange_channel_to_std(chan) == CANDLES:
                        sub = [f"{chan}.{self.candle_interval_map[self.candle_interval]}.{pair}"]  # ex. kline.30.BTCUSDT
                    else:
                        sub = [f"{chan}.{pair}"]

                    await connection.write(json.dumps({"op": "subscribe", "args": sub}))
            else:
                await connection.write(json.dumps({"op": "subscribe", "args": [chan]}))

    def __reset(self, conn: AsyncConnection):
        # delete cached books, tickers
        if self.std_channel_to_exchange(L2_BOOK) in conn.subscription:
            for pair in conn.subscription[self.std_channel_to_exchange(L2_BOOK)]:
                std_pair = self.exchange_symbol_to_std_symbol(pair)

                if std_pair in self._l2_book:
                    del self._l2_book[std_pair]

        self._ticker_info_cache = {}

    async def _candles(self, msg: dict, timestamp: float):
        """
        {
            "topic": "kline.5.BTCUSDT",
            "data": [
                {
                    "start": 1672324800000,      // Start time of the candle
                    "end": 1672325099999,        // End time of the candle
                    "interval": "5",             // Interval of the candle
                    "open": "16649.5",
                    "close": "16677",
                    "high": "16677",
                    "low": "16608",
                    "volume": "2.081",
                    "turnover": "34666.4005",
                    "confirm": false,            // Whether the candle(tick) is ended or not
                    "timestamp": 1672324988882   // Timestamp(ms) of last matched order in candle
                }
            ],
            "ts": 1672324988882,                 // Timestamp(ms) that system generates
            "type": "snapshot"
        }
        """
        symbol = self.exchange_symbol_to_std_symbol(msg['topic'].split('.')[-1])
        ts = msg['ts'] / 1000

        for entry in msg['data']:
            if self.candle_closed_only and not entry['confirm']:
                continue
            c = Candle(self.id,
                       symbol,
                       entry['start'] / 1000,
                       entry['end'] / 1000,
                       self.candle_interval,
                       entry['confirm'],
                       Decimal(entry['open']),
                       Decimal(entry['close']),
                       Decimal(entry['high']),
                       Decimal(entry['low']),
                       Decimal(entry['volume']),
                       None,
                       ts,
                       raw=entry)
            await self.callback(CANDLES, c, timestamp)

    async def _liquidations(self, msg: dict, timestamp: float):
        """
        {
            "data": {
                "price": "0.03803",           // Bankruptcy price (Entry Price * (1 +- Initial Margin Rate))
                "side": "Buy",                // Buy, Sell. Buy means long position was liquidated
                "size": "1637",
                "symbol": "GALAUSDT",
                "updatedTime": 1673251091822
            },
            "topic": "liquidation.GALAUSDT",
            "ts": 1673251091822,
            "type": "snapshot"
        }
        """
        liq = Liquidation(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['data']['symbol']),
            BUY if msg['data']['side'] == 'Buy' else SELL,
            Decimal(msg['data']['size']),
            Decimal(msg['data']['price']),
            None,
            None,
            msg['data']['updatedTime'] / 1000,
            raw=msg
        )
        await self.callback(LIQUIDATIONS, liq, timestamp)

    async def _tickers(self, msg: dict, timestamp: float):
        """
        # snapshot type response
        {
            "topic": "tickers.BTCUSDT",
            "type": "snapshot",
            "data": {
                "symbol": "BTCUSDT",                    // Symbol name
                "tickDirection": "PlusTick",            // Tick direction of last tick: PlusTick,ZeroPlusTick,MinusTick,ZeroMinusTick
                "price24hPcnt": "0.017103",             // Percentage change of market price in the last 24 hours
                "lastPrice": "17216.00",                // Last traded price
                "prevPrice24h": "16926.50",             // Market price 24 hours ago
                "highPrice24h": "17281.50",
                "lowPrice24h": "16915.00",
                "prevPrice1h": "17238.00",
                "markPrice": "17217.33",
                "indexPrice": "17227.36",
                "openInterest": "68744.761",            // Open interest 'size'
                "openInterestValue": "1183601235.91",   // Open interest 'value'
                "turnover24h": "1570383121.943499",
                "volume24h": "91705.276",
                "nextFundingTime": "1673280000000",     // Next funding time(ms)
                "fundingRate": "-0.000212",
                "bid1Price": "17215.50",                // Best bid price
                "bid1Size": "84.489",
                "ask1Price": "17216.00",
                "ask1Size": "83.020"
            },
            "cs": 24987956059,                          // Cross sequence
            "ts": 1673272861686                         // Timestamp(ms) that system generate
        }

        # delta type response
        {
            'topic': 'tickers.XRPUSDT',
            'type': 'delta',
            'data':
            {
                'symbol': 'XRPUSDT',
                'price24hPcnt': '-0.054421',
                'turnover24h': '283081758.2100',
                'volume24h': '456137160.0000',
                'fundingRate': '0.0001',
                'bid1Price': '0.6115',
                'bid1Size': '29887',
                'ask1Price': '0.6116',
                'ask1Size': '37291'
            },
            'cs': 63126322518,
            'ts': 1700220529058
        }
        """

        update_type = msg['type']
        if update_type == 'snapshot':
            updates = [msg['data']]  # TODO: its single value, not list?
        else:
            updates = [msg['data']]

        # inmemory cache for not sending same update twice
        for info in updates:
            if msg['topic'] in self._ticker_info_cache and self._ticker_info_cache[msg['topic']] == updates:
                continue
            else:
                self._ticker_info_cache[msg['topic']] = updates

            ts = int(msg['ts'] / 1000)

            if 'openInterest' in info:
                oi = OpenInterest(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    Decimal(info['openInterest']),
                    ts,
                    raw=info
                )
                await self.callback(OPEN_INTEREST, oi, timestamp)

            if 'indexPrice' in info:
                i = Index(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    Decimal(info['indexPrice']),
                    ts,
                    raw=info
                )
                await self.callback(INDEX, i, timestamp)

            if 'fundingRate' in info:
                f = Funding(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    None,
                    Decimal(info['fundingRate']),
                    # mstimestamp to timestamp
                    int(info['nextFundingTime']) / 1000 if 'nextFundingTime' in info else None,
                    ts,
                    predicted_rate=None,  # it's missing in v5
                    raw=info
                )
                await self.callback(FUNDING, f, timestamp)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,                                  // Timestamp(ms) that system generate
            "data": [
                {
                    "T": 1672304486865,                           // Timestamp(ms) that order filled
                    "s": "BTCUSDT",
                    "S": "Buy",                                   // Side of taker
                    "v": "0.001",                                 // Quantity of filled order
                    "p": "16578.50",                              // Price of filled order
                    "L": "PlusTick",                              // Tick direction of last filled order
                    "i": "20f43950-d8dd-5b31-9112-a178eb6023af",  // Trade id
                    "BT": false                                   // Wheter it is a block trade
                }
            ]
        }
        """

        data = msg['data']
        for trade in data:
            ts = int(trade['T'])  # timestamp (ms)

        t = Trade(
            self.id,
            self.exchange_symbol_to_std_symbol(trade['s']),
            BUY if trade['S'] == 'Buy' else SELL,
            Decimal(trade['v']),
            Decimal(trade['p']),
            ts / 1000,  # mstimestamp to timestamp
            id=trade['i'],
            raw=trade
        )
        await self.callback(TRADES, t, timestamp)

    async def _book(self, msg: dict, timestamp: float):
        raise NotImplementedError

    async def authenticate(self, conn: AsyncConnection):
        # At this time, not needed
        pass
