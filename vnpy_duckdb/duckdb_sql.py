CREATE_BAR_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS bar_data(
   "symbol" VARCHAR,
   "exchange" VARCHAR,
   "interval" VARCHAR,
   "datetime" TIMESTAMP,
   "volume" FLOAT,
   "turnover" FLOAT,
   "open_interest" FLOAT,
   "open_price" FLOAT,
   "high_price" FLOAT,
   "low_price" FLOAT,
   "close_price" FLOAT,
   PRIMARY KEY (symbol, exchange, interval, datetime)
)
"""

CREATE_BAROVERVIEW_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS bar_overview
(
   "symbol" VARCHAR,
   "exchange" VARCHAR,
   "interval" VARCHAR,
   "count" INT,
   "start" TIMESTAMP,
   "end" TIMESTAMP,
   PRIMARY KEY (symbol, exchange, interval)
)
"""

SAVE_BAR_QUERY = "INSERT INTO bar_data SELECT * FROM df ON CONFLICT DO NOTHING"

SAVE_BAROVERVIEW_QUERY = """
INSERT INTO bar_overview VALUES
($symbol, $exchange, $interval, $count, $start, $end)
ON CONFLICT
DO UPDATE SET "count" = EXCLUDED.count, "start" = EXCLUDED.start, "end" = EXCLUDED.end
"""

LOAD_BAR_QUERY = """
SELECT * FROM bar_data
WHERE symbol = $symbol
AND exchange = $exchange
AND interval = $interval
AND datetime >= $start
AND datetime <= $end
ORDER BY datetime ASC
"""

LOAD_BAROVERVIEW_QUERY = """
SELECT * FROM bar_overview
WHERE symbol = $symbol
AND exchange = $exchange
AND interval = $interval
"""

COUNT_BAR_QUERY = """
SELECT COUNT(close_price) FROM bar_data
WHERE symbol = $symbol
AND exchange = $exchange
AND interval = $interval
"""

LOAD_ALL_BAROVERVIEW_QUERY = "SELECT * FROM bar_overview"

DELETE_BAR_QUERY = """
DELETE FROM bar_data
WHERE symbol = $symbol
AND exchange = $exchange
AND interval = $interval
"""

DELETE_BAROVERVIEW_QUERY = """
DELETE FROM bar_overview
WHERE symbol = $symbol
AND exchange = $exchange
AND interval = $interval
"""
