import asyncio

# Central shared queue for raw trades from fed modules
trade_queue: asyncio.Queue = asyncio.Queue()

