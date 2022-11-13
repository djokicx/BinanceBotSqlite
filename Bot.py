import os
import pandas as pd
import sqlalchemy
import asyncio
from binance import BinanceSocketManager, AsyncClient
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
engine = sqlalchemy.create_engine("sqlite:///BTCUSDTstream.db")


async def main():
    await socket()


async def socket():
    client = await AsyncClient.create(API_KEY, API_SECRET)
    bsm = BinanceSocketManager(client)
    socket = bsm.trade_socket("BTCUSDT")
    while True:
        await socket.__aenter__()
        msg = await socket.recv()
        # dictionary => frame
        frame = await createframe(msg)
        frame.to_sql("BTCUSDT", engine, if_exists="append", index=False)
        print(frame)

    await client.close_connection()


async def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ["s", "E", "p"]]
    df.columns = ["Symbol", "Time", "Price"]
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit="ms")
    return df


if __name__ == "__main__":
    asyncio.run(main())
