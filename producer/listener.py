"""
WebSocket listener for Solana logsSubscribe.

Connects to Solana RPC WebSocket, subscribes to logs mentioning the target token,
and fires off a getTransaction calls for each notification.
"""

import asyncio
import json
import logging
import os
from websockets import connect, ConnectionClosed

from fetcher import fetch_transaction
from classifier import classify_and_publish

logger = logging.getLogger(__name__)

async def start_listener(token_mint: str, producer):
    """
    Subscribe to log notifications for the given token and create process_transaction task.
    
    Args:
        token_mint: SPL token address to monitor
        producer: Kafka producer instance (passed in so we reuse the same connection)
    """
    
    ws_url = os.getenv('SOLANA_RPC_WS_URL')
    
    while True:
        try:
            async with connect(ws_url) as websocket:
                logger.info(f"Connected to {ws_url}")
                
                # Subscribe to logs mentioning specified token
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [token_mint]},
                        {"commitment": "confirmed"}
                    ]
                }
                await websocket.send(json.dumps(subscribe_msg))
                
                # Wait for subscription confirmation
                response = await websocket.recv()
                response_data = json.loads(response)
                
                # Verify subscription
                if 'result' in response_data:
                    subscription_id = response_data['result']
                    logger.info(f"Subscribed to logs for {token_mint} (subscription ID: {subscription_id})")
                else:
                    logger.error(f"Failed to subscribe: {response_data}")
                    await asyncio.sleep(5)
                    continue
                
                # Listen for notifications
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        
                        # Skip non-notification messages
                        if data.get('method') != 'logsNotification':
                            continue
                        
                        # Extract
                        notification = data['params']
                        signature = notification['result']['value']['signature']
                        slot = notification['result']['context']['slot']
                        err = notification['result']['value']['err']
                        
                        # Skip failed transactions
                        if err is not None:
                            logger.debug(f"Skipping failed tx {signature}")
                            continue
                        
                        logger.info(f"Log notification: {signature} (slot {slot})")
                        
                        # Spawn task to fetch full transaction so we don't block main
                        asyncio.create_task(
                            process_transaction(signature, token_mint, producer)
                        )
                    
                    except Exception as e:
                        logger.error(f"Error processing notification: {e}")
                        continue
        
        except ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        
        except Exception as e:
            logger.error(f"Unexpected error in listener: {e}. Reconnecting in 10s...")
            await asyncio.sleep(10)


async def process_transaction(signature: str, token_mint: str, producer):
    """
    Fetch transaction details and classify BUY/SELL, then call classify_and_publish.
    
    Args:
        signature: Transaction signature
        token_mint: Token address we're tracking
        producer: Kafka producer instance
    """
    try:
        # Fetch transaction details -> dict
        tx_data = await fetch_transaction(signature)
        
        if tx_data is None:
            logger.warning(f"Could not fetch transaction {signature}")
            return
        
        # Classify and publish to Kafka (buys/sells/failed topics)
        classify_and_publish(tx_data, token_mint, producer)
    
    except Exception as e:
        logger.error(f"Error processing transaction {signature}: {e}")
