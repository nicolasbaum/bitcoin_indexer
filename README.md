# Bitcoin Blockchain Indexer 🌐

A high-performance Bitcoin blockchain indexer that stores structured blockchain data in MongoDB. Features async processing, automatic retries, data enrichment, and real-time monitoring of blocks, mempool transactions, and network peers.

## Features

- **Block Indexing**: Full block data with transaction details
- **Mempool Monitoring**: Real-time unconfirmed transaction tracking
- **Peer Network Analysis**: Connected node statistics and geo-data
- **Data Enrichment**:
  - Transaction fee calculation
  - Address aggregation
  - Input/output value tracking
- **Resilient Architecture**:
  - Async RPC calls with exponential backoff
  - Queue-based processing
  - MongoDB duplicate handling

## Prerequisites

- Python 3.12+
- MongoDB 6.0+
- Poetry dependency manager
- Bitcoin Core node with RPC access

## Installation

```bash
git clone https://github.com/yourusername/bitcoin-indexer.git
cd bitcoin-indexer
poetry install
```

## Configuration

Create *.env* file:
```ini
RPC_USER=your_bitcoin_rpc_user
RPC_PASSWORD=your_bitcoin_rpc_password
RPC_URL=http://your.node:8332
#ONLY for RPC nodes on different machines:
MONGO_URL=mongodb://localhost:27017/
```

## MongoDB Setup

Start MongoDB replica set:
```bash
make mongo-start
```

Initialize collections:
```bash
poetry run python db_initializer.py
```

## Running the Indexer

Start the main indexer:
```bash
make run-indexer
```

Control MongoDB:
```bash
make mongo-start   # Start MongoDB
make mongo-stop    # Stop MongoDB
make mongo-clean   # Reset database
```

## Project Structure

| File/Folder         | Description                              |
|-------------------|-----------------------------------------------------------|
| `bitcoin_indexer.py`	| Main indexer logic and async processors      |
| `db_initializer.py` 	| MongoDB collection setup and index creation  |
| `mongo_repset/` 		| MongoDB replica set data storage             |
| `*.sh`				| MongoDB management scripts                  |
| `Makefile` 			| Development workflow shortcuts              |

## Data Models

### Blocks Collection
```javascript
{
  "hash": "000000000000034a7...",
  "height": 812345,
  "tx": [...],
  "time": 1689341205,
  "difficulty": 48127.35
}
```

### Transactions Collection
```javascript
{
  "txid": "a1075db55d416d3c...",
  "block_height": 812345,
  "input_total": 1.5432,
  "output_total": 1.5428,
  "fee": 0.0004,
  "all_addresses": ["1A10zP1eP5QGefi2..."]
}
```

## Development

Format code:
```bash
make fix-imports
```

Run checks:
```bash
make pre-commit-run
```

Enter virtual environment:
```bash
poetry shell
```

## **👨‍💻 Author**
Created by **[Nicolas Baum](https://github.com/nicolasbaum)** (nicolasbaum@gmail.com)
For contributions, feel free to submit a **PR** or open an **issue**! 🚀