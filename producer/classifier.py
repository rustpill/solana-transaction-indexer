"""
Transaction classifier and Kafka publisher.

Parses token balance changes to determine if a swap was a BUY or SELL,
then publishes to the appropriate Kafka topic.
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

LAMPORTS_PER_SOL = 1_000_000_000


def classify_and_publish(tx_data: dict, token_mint: str, producer):
    """
    Classify transaction as BUY or SELL and publish to Kafka.
    
    Logic:
    - Find the token account for our target mint in pre/postTokenBalances
    - Compare amounts to determine direction
    - Extract SOL delta from pre/postBalances
    - Publish to sol.buys or sol.sells topic
    
    Args:
        tx_data: Full transaction response from getTransaction
        token_mint: The SPL token we're tracking
        producer: Kafka producer instance
    """
    try:
        meta = tx_data.get('meta')
        transaction = tx_data.get('transaction')
        
        if not meta or not transaction:
            logger.warning("Transaction missing meta or transaction field")
            return
        
        signature = transaction['signatures'][0]
        slot = tx_data.get('slot')
        block_time = tx_data.get('blockTime')
        
        # Find signer (first account key)
        account_keys = transaction['message']['accountKeys']
        signer = account_keys[0] if account_keys else 'unknown'
        
        # Extract token balance changes for our mint
        pre_token_balances = meta.get('preTokenBalances', [])
        post_token_balances = meta.get('postTokenBalances', [])
        
        token_delta = _calculate_token_delta(
            pre_token_balances,
            post_token_balances,
            token_mint
        )
        
        if token_delta is None:
            logger.debug(f"No token balance change for {token_mint} in {signature}")
            return
        
        # Extract SOL balance change for the signer (index 0)
        pre_balances = meta.get('preBalances', [])
        post_balances = meta.get('postBalances', [])
        
        sol_delta = 0.0
        if len(pre_balances) > 0 and len(post_balances) > 0:
            sol_delta_lamports = post_balances[0] - pre_balances[0]
            sol_delta = sol_delta_lamports / LAMPORTS_PER_SOL
        
        # Classify: positive token delta = BUY, negative = SELL
        if token_delta > 0:
            trade_type = 'BUY'
            topic = os.getenv('KAFKA_TOPIC_BUYS')
        else:
            trade_type = 'SELL'
            topic = os.getenv('KAFKA_TOPIC_SELLS')
        
        # Build event payload
        event = {
            'signature': signature,
            'signer': signer,
            'type': trade_type,
            'token_mint': token_mint,
            'token_amount': abs(token_delta),
            'sol_amount': abs(sol_delta),
            'slot': slot,
            'block_time': datetime.fromtimestamp(block_time).isoformat() if block_time else None,
        }
        
        # Publish to Kafka (partition by token_mint for ordering)
        _publish_to_kafka(producer, topic, token_mint, event)
        
        logger.info(
            f"{trade_type}: {event['token_amount']:.4f} {token_mint[:8]}... "
            f"for {event['sol_amount']:.4f} SOL | tx: {signature[:16]}..."
        )
    
    except Exception as e:
        logger.error(f"Error classifying transaction: {e}")
        _publish_to_dead_letter(producer, tx_data, str(e))


def _calculate_token_delta(pre_balances: list, post_balances: list, mint: str) -> float | None:
    """
    Calculate the net change in token balance for the given mint.
    
    Args:
        pre_balances: preTokenBalances array
        post_balances: postTokenBalances array
        mint: Token mint address
    
    Returns:
        Delta as float (positive = received, negative = sent), or None if not found
    """
    # Find balances for our mint
    pre_amount = 0.0
    post_amount = 0.0
    decimals = 0
    
    for balance in pre_balances:
        if balance.get('mint') == mint:
            pre_amount = float(balance['uiTokenAmount']['amount'])
            decimals = balance['uiTokenAmount']['decimals']
            break
    
    for balance in post_balances:
        if balance.get('mint') == mint:
            post_amount = float(balance['uiTokenAmount']['amount'])
            decimals = balance['uiTokenAmount']['decimals']
            break
    
    # If token not found in either, return None
    if pre_amount == 0 and post_amount == 0:
        return None
    
    # Calculate delta in token units (not raw amount)
    raw_delta = post_amount - pre_amount
    token_delta = raw_delta / (10 ** decimals)
    
    return token_delta


def _publish_to_kafka(producer, topic: str, key: str, value: dict):
    """
    Publish event to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        topic: Target topic
        key: Partition key (token_mint)
        value: Event payload (dict)
    """
    try:
        producer.send(
            topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8')
        )
        producer.flush()  # Ensure it's sent immediately
    
    except Exception as e:
        logger.error(f"Failed to publish to Kafka topic {topic}: {e}")


def _publish_to_dead_letter(producer, tx_data: dict, error_msg: str):
    """
    Publish failed transaction to dead letter topic.
    
    Args:
        producer: Kafka producer instance
        tx_data: Raw transaction data
        error_msg: Error description
    """
    try:
        topic = os.getenv('KAFKA_TOPIC_FAILED')
        
        event = {
            'signature': tx_data.get('transaction', {}).get('signatures', ['unknown'])[0],
            'raw_log': tx_data,
            'error_msg': error_msg,
        }
        
        producer.send(
            topic,
            value=json.dumps(event).encode('utf-8')
        )
        producer.flush()
        
        logger.info(f"Published failed tx to {topic}")
    
    except Exception as e:
        logger.error(f"Failed to publish to dead letter topic: {e}")
