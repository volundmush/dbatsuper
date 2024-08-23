#!/usr/bin/env python3

import asyncio
from dbat.core import Application


async def main():
    app = Application()
    await app.setup("localhost", 4000)
    await app.start()

if __name__ == "__main__":
    asyncio.run(main())