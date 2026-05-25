"""
Solana DEX trade producer for Kafka.

Subscribes to logsSubscribe for a specific SPL token, classifies swaps as BUY/SELL,
and publishes to Kafka topics.

Topics are partitioned by TOKEN_MINT.

"""

import asyncio
import logging
import os
import sys
from dotenv import load_dotenv

from listener import start_listener
from kafka_client import get_producer

# Load .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Check all required environment variables are set
def validate_env():
    
    required = [
        'TOKEN_MINT',
        'SOLANA_RPC_WS_URL',
        'SOLANA_RPC_HTTP_URL',
        'KAFKA_BOOTSTRAP_SERVERS',
        'KAFKA_TOPIC_BUYS',
        'KAFKA_TOPIC_SELLS',
        'KAFKA_TOPIC_FAILED',
    ]
    
    missing = [var for var in required if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


async def main():
    validate_env()
    
    token_mint = os.getenv('TOKEN_MINT')
    logger.info(f"Starting producer for token: {token_mint}")
    
    # Initialize Kafka producer (reused across all publishes)
    producer = get_producer()
    
    try:
        # Start WebSocket listener (blocking, runs forever)
        await start_listener(token_mint, producer)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == '__main__':
    asyncio.run(main())
