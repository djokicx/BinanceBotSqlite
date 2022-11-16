import os
import pandas as pd
import sqlalchemy
import asyncio
from binance import BinanceSocketManager, AsyncClient
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")


async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)
    await strategy(client, "LUNCBUSD", 0.001, 60, 40000)
    await client.close_connection()


# TrendFollowing
# If the asset was rising by x% -> Buy
# Exit when profit is above 0.15% or loss is crossing -0.15


async def strategy(client, pair, entry, lookback, qty, open_position=False):
    engineName = f"sqlite:///{pair}stream.db"
    engine = sqlalchemy.create_engine(engineName)

    # just requesting data from the db
    while True:
        frame = pd.read_sql(pair, engine)
        lookbackperiod = frame.iloc[-lookback:]
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1
        if not open_position:
            if cumret[cumret.last_valid_index()] > entry:
                order = await client.create_order(
                    symbol=pair, side="BUY", type="MARKET", quantity=qty
                )
                print(order)
                open_position = True
                break

    if open_position:
        while True:
            frame = pd.read_sql(pair, engine)
            sincebuy = frame.loc[
                frame.Time > pd.to_datetime(order["transactTime"], unit="ms")
            ]
            if len(sincebuy) > 1:
                sincebuyret = (sincebuy.Price.pct_change() + 1).cumprod() - 1
                last_entry = sincebuyret[sincebuyret.last_valid_index()]
                if last_entry > 0.0015 or last_entry < -0.0015:
                    order = await client.create_order(
                        symbol=pair, side="SELL", type="MARKET", quantity=qty
                    )
                    print(order)
                    break


if __name__ == "__main__":
    asyncio.run(main())
