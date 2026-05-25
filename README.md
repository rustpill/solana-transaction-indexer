# Solana Transaction Indexer

Real-time data pipeline that streams Solana DEX trades through Kafka into Snowflake for analytics. Tracks SPL token swaps, classifies them as BUY/SELL, and partitions by token mint address.

## Architecture

```
Solana RPC WebSocket (logsSubscribe)
  ↓
Python Producers (1 per token, partition by TOKEN_MINT)
  ↓
Kafka KRaft (sol.buys, sol.sells, sol.failed topics)
  ↓
Kafka Connect + Snowflake Connector (Snowpipe Streaming)
  ↓
Snowflake (SOLANA_DEX.PUBLIC.{BUYS, SELLS, FAILED_TXS})
```
## Prerequisites

- Docker Desktop (for Kafka & Kafka Connect)
- Python 3.11+
- Snowflake account
- Solana RPC endpoint (Helius, QuickNode, or public RPC)

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/rustpill/solana-transaction-indexer.git
cd solana-transaction-indexer
```

### 2. Generate Snowflake Key Pair (PKCS8)

```bash
# Generate private key in PKCS8 format
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt

# Extract public key
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

### 3. Configure Snowflake

Run the setup script in Snowflake (as ACCOUNTADMIN):

```sql
-- Copy/paste contents of snowflake/setup.sql
```

Then register your public key:

```sql
-- Remove headers/footers and newlines from snowflake_key.pub, then run:
ALTER USER KAFKA_USER SET RSA_PUBLIC_KEY='';
```

### 4. Configure Environment

```bash
# Create `.env` in project root:
cp .env.example .env
```


### 5. Start Kafka Infrastructure

```bash
cd docker

# Generate Kafka cluster ID
docker run --rm confluentinc/cp-kafka:7.9.0 kafka-storage random-uuid
# Copy this ID to your .env file as KAFKA_CLUSTER_ID

# Start Kafka and Kafka Connect
docker compose -f docker-compose.yaml --env-file ../.env up -d

# Verify connectors are running (wait 30 seconds for startup)
# You should see: `"state":"RUNNING"` for both connector and tasks.
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/snowflake-buys-sink/status
```

### 6. Start Python Producer

```bash
cd producer

# Install dependencies
pip install -r requirements.txt

# Start producer
python main.py
```

You should see:
```
2026-05-25 19:41:49 [INFO] classifier: BUY: 1093.9096 2Pvw3pyx... for 3.0037 SOL
2026-05-25 19:41:50 [INFO] classifier: SELL: 279040.8710 2Pvw3pyx... for 0.0040 SOL
```

## Running Multiple Tokens

Track multiple tokens simultaneously by running multiple producer instances:

```bash
# Terminal 1 - BONK
cd producer
python main.py

# Terminal 2 - JUP
cd producer
TOKEN_MINT=JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN python main.py

# Terminal 3 - USDC
cd producer
TOKEN_MINT=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v python main.py
```


All producers share the same Kafka topics and Snowflake tables, partitioned by `TOKEN_MINT`.



## Monitoring

### Check Kafka Topics

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# View messages in buys topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sol.buys --from-beginning --max-messages 10

# Check topic offsets
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic sol.buys
```

### Check Connector Status

```bash
# List connectors
curl http://localhost:8083/connectors

# Check buys connector status
curl http://localhost:8083/connectors/snowflake-buys-sink/status

# Restart a connector
curl -X POST http://localhost:8083/connectors/snowflake-buys-sink/restart
```
