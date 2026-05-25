"""
Fetcher for Solana getTransaction RPC calls with retry/backoff.

Handles rate limits and transient failures gracefully.
"""

import asyncio
import json
import logging
import os
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
BASE_BACKOFF = 1.0  # seconds


async def fetch_transaction(signature: str) -> dict | None:
    """
    Fetch transaction details from Solana RPC with retry logic.
    
    Args:
        signature: Transaction signature to fetch
    
    Returns:
        Transaction data dict, or None if all retries failed
    """
    http_url = os.getenv('SOLANA_RPC_HTTP_URL')
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
                "encoding": "json"
            }
        ]
    }
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Synchronous HTTP request (run in thread pool to avoid blocking)
            response = await asyncio.to_thread(_http_post, http_url, payload)
            
            if 'result' in response:
                return response['result']
            
            if 'error' in response:
                error_msg = response['error'].get('message', 'Unknown error')
                logger.warning(f"RPC error for {signature}: {error_msg}")
                
                # Don't retry if transaction not found
                if 'not found' in error_msg.lower():
                    return None
            
            # For other errors, retry with backoff
            backoff = BASE_BACKOFF * (2 ** (attempt - 1))
            logger.warning(f"Retrying {signature} in {backoff}s (attempt {attempt}/{MAX_RETRIES})")
            await asyncio.sleep(backoff)
        
        except (HTTPError, URLError) as e:
            logger.warning(f"HTTP error fetching {signature}: {e}")
            
            # Rate limit or server error — back off
            backoff = BASE_BACKOFF * (2 ** (attempt - 1))
            logger.warning(f"Retrying in {backoff}s (attempt {attempt}/{MAX_RETRIES})")
            await asyncio.sleep(backoff)
        
        except Exception as e:
            logger.error(f"Unexpected error fetching {signature}: {e}")
            return None
    
    logger.error(f"Failed to fetch {signature} after {MAX_RETRIES} attempts")
    return None


def _http_post(url: str, payload: dict) -> dict:
    """
    Synchronous HTTP POST helper (called via asyncio.to_thread).
    
    Args:
        url: RPC endpoint
        payload: JSON-RPC request body
    
    Returns:
        Parsed JSON response
    """
    data = json.dumps(payload).encode('utf-8')
    
    req = Request(
        url,
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    
    with urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode('utf-8'))
