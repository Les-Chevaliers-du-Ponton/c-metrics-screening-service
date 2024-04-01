import json
import os
from datetime import datetime as dt
from multiprocessing import Process

import pandas as pd
import pandas_ta as ta

import helpers
from indicators.technicals import FractalCandlestickPattern


class Strategy(FractalCandlestickPattern):

    def __init__(
        self,
        strat_name: str = None,
        strat_description: str = None,
        strat_parameters: list = None,
    ):
        # TODO: look into pandas_ta doc to create better custom strat
        if not strat_name:
            strat_name = "Default Strategy"
        if not strat_description:
            strat_description = "Default Strategy"
        if not strat_parameters:
            strat_parameters = [
                {"kind": "bbands", "length": 20},
                {"kind": "rsi"},
                {"kind": "macd", "fast": 8, "slow": 21},
            ]
        self.ta_strat = ta.Strategy(
            name=strat_name, description=strat_description, ta=strat_parameters
        )
        super().__init__()


class Initializer(Strategy):
    def __init__(self, exchange_name: str, pairs: list):
        self.redis_streams = None
        self.fractal_refresh_seconds_delay = 1
        self.verbose = os.getenv("VERBOSE")
        self.pairs = pairs
        self.ref_currency = os.getenv("REFERENCE_CURRENCY")
        if self.verbose:
            helpers.LOG.info(
                f"Initializing Screening Service for {exchange_name} | ref currency: {self.ref_currency}"
            )
        self.exchange_name = exchange_name.lower()
        self.exchange_object = helpers.get_exchange_object(self.exchange_name)
        self.data = dict()
        self.scores = pd.DataFrame(columns=["pair"])
        self.all_scores = pd.DataFrame(columns=["pair"])
        helpers.write_fractal_refresh_tmstmp(init=True)
        self.load_initial_data()
        super().__init__()

    def get_exchange_mapping(self):
        symbols = self.exchange_object.load_markets()
        for details in symbols.values():
            if self.is_pair_in_scope(details):
                pair = details["id"].replace("/", "-")
                self.data[pair] = details
        if self.verbose:
            helpers.LOG.info(f"Will screen {len(self.data)} pairs")

    def is_pair_in_scope(self, details: dict) -> bool:
        if not self.pairs or details["id"] in self.pairs:
            if not self.ref_currency or details["quote"] == self.ref_currency:
                return True
        return False

    def get_pair_ohlcv(self, pair: str):
        if pair not in self.data:
            self.data[pair] = dict()
        ohlc_data = self.exchange_object.fetch_ohlcv(
            symbol=pair, timeframe="1d", limit=300
        )
        if self.verbose:
            helpers.LOG.info(f"Downloading OHLCV data for {pair}")
            if not ohlc_data:
                helpers.LOG.warning(f"No OHLCV data for {pair}")
        df = pd.DataFrame(
            data=ohlc_data,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        self.data[pair]["ohlcv"] = df.reset_index(drop=True)

    def load_all_data(self):
        for pair in self.data:
            self.get_pair_ohlcv(pair)

    def load_initial_data(self):
        self.redis_streams = helpers.get_available_redis_streams()
        self.get_exchange_mapping()
        self.load_all_data()


class ExchangeScreener(Initializer):
    def __init__(self, exchange_name: str, pairs: list):
        super().__init__(exchange_name, pairs)

    def run_screening(self):
        self.get_scoring()
        self.screen_exchange()

    def add_technical_indicators(self, pair: str):
        if self.verbose:
            helpers.LOG.info(f"Computing technical indicators for {pair}")
        try:
            self.data[pair]["ohlcv"].ta.strategy(self.ta_strat)
        except Exception as e:
            helpers.LOG.warning(
                f"Could not compute all indicators for {pair}:\n \n {e}"
            )

    def live_refresh(self, message: dict):
        pair = self.read_message(message)
        self.get_scoring([pair])
        self.write_to_redis()

    def update_pair_ohlcv(self, pair: str, data: dict):
        ohlcv = self.data[pair]["ohlcv"]
        trade_timestamp = dt.utcfromtimestamp(float(data["timestamp"])).date()
        latest_ohlcv_timestamp = dt.utcfromtimestamp(
            ohlcv["timestamp"].iloc[-1] / 1000
        ).date()
        if trade_timestamp > latest_ohlcv_timestamp:
            new_row = pd.DataFrame(
                [
                    [
                        data["timestamp"],
                        data["price"],
                        data["price"],
                        data["price"],
                        data["price"],
                        data["amount"],
                    ]
                ],
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            ohlcv = pd.concat([ohlcv, new_row])
        else:
            idx = ohlcv.index[len(ohlcv) - 1]
            if "price" in data:
                price = float(data["price"])
                ohlcv.loc[idx, "high"] = max(ohlcv.loc[idx, "high"], price)
                ohlcv.loc[idx, "low"] = min(ohlcv.loc[idx, "low"], price)
                ohlcv.loc[idx, "close"] = price
                ohlcv.loc[idx, "volume"] += float(data["amount"])
        self.data[pair]["ohlcv"] = ohlcv

    def read_message(self, message: dict) -> str:
        pair = message["symbol"]
        if pair in self.data:
            self.update_pair_ohlcv(pair, message)
        return pair

    def handle_fractals(self, scoring: dict, pair: str) -> dict:
        fractal_refresh_tmstmp = helpers.REDIS_CON.xrevrange(
            "{fractal_refresh_tmstmp}", count=1
        )
        fractal_related_fields = (
            "next_support",
            "next_resistance",
            "potential_gain",
            "support_dist",
        )
        fractal_refresh_tmstmp = dt.fromisoformat(fractal_refresh_tmstmp[0][1]["last"])
        missing_keys = set(fractal_related_fields) - set(scoring.keys())
        # if (
        #     dt.now() - fractal_refresh_tmstmp
        # ).seconds > self.fractal_refresh_seconds_delay or missing_keys:
        helpers.write_fractal_refresh_tmstmp()
        fractals = self.get_fractals(self.data[pair]["ohlcv"])
        supports = [level for level in fractals if level < scoring["close"]]
        resistances = [level for level in fractals if level > scoring["close"]]
        scoring["next_support"] = float(max(supports)) if supports else None
        scoring["next_resistance"] = (
            float(min(resistances)) if resistances else None
        )
        scoring["potential_gain"] = (
            (scoring["next_resistance"] / scoring["next_support"]) - 1
            if supports and resistances
            else None
        )
        scoring["support_dist"] = (
            (scoring["close"] / scoring["next_support"]) - 1 if supports else None
        )
        # else:
        #     pair_score = self.all_scores[self.all_scores["pair"] == pair].squeeze()
        #     for field in fractal_related_fields:
        #         scoring[field] = pair_score[field]
        return scoring

    def technicals_score(self, pair: str) -> dict:
        scoring = dict()
        self.add_technical_indicators(pair)
        self.data[pair]["ohlcv"] = self.data[pair]["ohlcv"].astype(float)
        scoring["close"] = self.data[pair]["ohlcv"]["close"].iloc[-1]
        scoring["24h_change"] = (
            scoring["close"] / self.data[pair]["ohlcv"]["open"].iloc[-1] - 1
        )
        try:
            scoring["rsi"] = (
                int(self.data[pair]["ohlcv"]["RSI_14"].iloc[-1])
                if "RSI_14" in self.data[pair]["ohlcv"].columns
                else None
            )
        except ValueError:
            scoring["rsi"] = None
        try:
            scoring["bbl"] = (
                (scoring["close"] / self.data[pair]["ohlcv"]["BBL_20_2.0"].iloc[-1]) - 1
                if "BBL_20_2.0" in self.data[pair]["ohlcv"].columns.tolist()
                else None
            )
        except ValueError:
            scoring["bbl"] = None
        scoring = self.handle_fractals(scoring, pair)
        return scoring

    def score_pair(self, pair: str) -> dict:
        if pair not in self.data:
            self.data[pair] = dict()
        if self.data[pair].get("ohlcv") is not None and not self.data[pair]["ohlcv"]:
            scoring = self.technicals_score(pair)
            if scoring["support_dist"] and scoring["bbl"] and scoring["rsi"]:
                scoring["technicals_score"] = 1 / (
                    scoring["rsi"]
                    * (1 + scoring["bbl"])
                    * (1 + scoring["support_dist"])
                )
            else:
                scoring["technicals_score"] = 0
            scoring["pair"] = pair
            return scoring

    def get_scoring(self, pairs_to_screen: list = None):
        pairs_to_screen = pairs_to_screen if pairs_to_screen else list(self.data.keys())
        scores = list()
        for pair in pairs_to_screen:
            scores.append(self.score_pair(pair))
        self.scores = pd.DataFrame(scores)
        if not pairs_to_screen:
            self.all_scores = self.scores.copy()
        if self.verbose:
            self.log_scores()

    def log_scores(self, top_score_amount: int = 10):
        if self.scores is not None:
            top_scores = self.scores.head(top_score_amount)
            helpers.LOG.info(top_scores.to_string())

    def write_to_redis(self):
        # TODO: one screening stream per pair
        df = self.scores.copy()
        df = df.set_index("pair")
        if not df.empty:
            data = df.to_json(orient="index")
            data = {k: json.dumps(v) for k, v in json.loads(data).items()}
            helpers.REDIS_CON.xadd(
                "{screening}",
                data,
                maxlen=len(self.data),
                approximate=True,
            )

    def screen_exchange(self):
        while True:
            streams = {stream: "$" for stream in self.redis_streams}
            data = helpers.REDIS_CON.xread(streams=streams, block=0)
            message = data[0][1][0][1]
            Process(target=self.live_refresh, args=(message,)).start()


def run_screening(exchange_list: list = None, pairs: list = None):
    if not exchange_list:
        exchange_list = ["coinbase"]
    for exchange in exchange_list:
        # TODO: add multiprocessing
        screener = ExchangeScreener(exchange, pairs)
        screener.run_screening()


if __name__ == "__main__":
    run_screening()
