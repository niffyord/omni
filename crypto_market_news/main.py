import asyncio
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

from .manager import CryptoNewsManager


async def run_periodically(interval: timedelta) -> None:
    """Run the crypto news workflow on a schedule."""
    while True:
        await CryptoNewsManager().run()
        await asyncio.sleep(interval.total_seconds())


async def main() -> None:
    await run_periodically(timedelta(minutes=10))


if __name__ == "__main__":
    asyncio.run(main())
