from datetime import datetime

import duckdb
import pandas as pd

from vnpy_evo.trader.constant import Exchange, Interval
from vnpy_evo.trader.object import BarData, TickData
from vnpy_evo.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    convert_tz,
    DB_TZ
)
from vnpy_evo.trader.setting import SETTINGS
from vnpy_evo.trader.utility import get_file_path

from .duckdb_sql import (
    CREATE_BAR_TABLE_QUERY,
    CREATE_BAROVERVIEW_TABLE_QUERY,
    SAVE_BAR_QUERY,
    SAVE_BAROVERVIEW_QUERY,
    LOAD_BAR_QUERY,
    LOAD_BAROVERVIEW_QUERY,
    LOAD_ALL_BAROVERVIEW_QUERY,
    COUNT_BAR_QUERY,
    DELETE_BAR_QUERY,
    DELETE_BAROVERVIEW_QUERY,
)


class DuckdbDatabase(BaseDatabase):
    """Database adpater for DuckDB"""

    def __init__(self) -> None:
        """"""
        self.db_name: str = SETTINGS["database.database"]
        self.db_path: str = str(get_file_path(self.db_name))

        # Open database connection
        self.connection: duckdb.DuckDBPyConnection = duckdb.connect(database=self.db_path)

        self.cursor: duckdb.BaseCursor = self.connection.cursor()

        # Create tables if necessary
        self.cursor.execute(CREATE_BAR_TABLE_QUERY)
        self.cursor.execute(CREATE_BAROVERVIEW_TABLE_QUERY)

    def save_bar_data(self, bars: list[BarData]) -> bool:
        """Save bar data"""
        # Save bars into db
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        records: list[dict] = []
        for bar in bars:
            record: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "datetime": convert_tz(bar.datetime),
                "volume": bar.volume,
                "turnover": bar.turnover,
                "open_interest": bar.open_interest,
                "open_price": bar.open_price,
                "high_price": bar.high_price,
                "low_price": bar.low_price,
                "close_price": bar.close_price,
            }
            records.append(record)

        df: pd.DataFrame = pd.DataFrame.from_records(records)  # noqa

        self.connection.execute(SAVE_BAR_QUERY)

        # Query bars overview
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value
        }

        self.execute(LOAD_BAROVERVIEW_QUERY, params)
        row: tuple = self.cursor.fetchone()

        # New contract
        if not row:
            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "start": records[0]["datetime"],
                "end": records[-1]["datetime"],
                "count": len(bars)
            }
        # Existing contract
        else:
            self.execute(COUNT_BAR_QUERY, params)
            count = self.cursor.fetchone()[0]

            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "start": min(records[0]["datetime"], row[4]),
                "end": max(records[-1]["datetime"], row[5]),
                "count": count
            }

        self.execute(SAVE_BAROVERVIEW_QUERY, data)

        return True

    def save_tick_data(self, ticks: list[TickData]) -> bool:
        """Save tick data"""
        return False

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """Load bar data"""
        # Load data from db
        params = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value,
            "start": str(start),
            "end": str(end)
        }

        self.execute(LOAD_BAR_QUERY, params)
        data: list[tuple] = self.cursor.fetchall()

        # Return BarData list
        bars: list[BarData] = []

        for row in data:
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=datetime.fromtimestamp(row[3].timestamp(), DB_TZ),
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                open_price=row[7],
                high_price=row[8],
                low_price=row[9],
                close_price=row[10],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """Load tick data"""
        return []

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """Delete bar data"""
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value,
        }

        # Query data count
        self.execute(COUNT_BAR_QUERY, params)
        count = self.cursor.fetchone()[0]

        # Remove bars
        self.execute(DELETE_BAR_QUERY, params)

        # Remove bar overview
        self.cursor.execute(DELETE_BAROVERVIEW_QUERY, params)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
    ) -> int:
        """Delete tick data"""
        return 0

    def get_bar_overview(self) -> list[BarOverview]:
        """Get bar overview"""
        self.execute(LOAD_ALL_BAROVERVIEW_QUERY)
        data: list[tuple] = self.cursor.fetchall()

        overviews: list[BarOverview] = []

        for row in data:
            overview = BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                interval=Interval(row[2]),
                count=row[3],
                start=row[4],
                end=row[5],
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """Get tick overview"""
        return []

    def execute(self, query: str, data: object = None) -> None:
        """Execute SQL query"""
        self.cursor.execute(query, data)
        self.connection.commit()
