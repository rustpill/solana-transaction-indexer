"""
Kafka producer client initialization.

Single producer instance reused across all publishes.
"""

import logging
import os
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

# Returns a Coonfigured Kafka producer instance
def get_producer() -> KafkaProducer:
    # Configure server in .env
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    
    logger.info(f"Initializing Kafka producer (bootstrap: {bootstrap_servers})")
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all', # Wait for all replicas
        retries=3, # Retry failed sends
        max_in_flight_requests_per_connection=5,
        compression_type='snappy', # Optional, saves bandwidth
    )
    
    logger.info("Kafka producer ready")
    return producer
