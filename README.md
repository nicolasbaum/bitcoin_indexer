# Bitcoin Indexer

## Overview

Bitcoin Indexer is a Python-based asynchronous application that indexes Bitcoin blockchain data. It fetches block data, mempool transactions, and peer information via Bitcoin RPC calls, enriches the transaction data (e.g., by deriving addresses and calculating fees), and stores the information in MongoDB. Caching is implemented with aiocache using Redis. The application is fully containerized with Docker and Docker Compose and can be deployed using Colima or on Umbrel Home.

## Features

- **Asynchronous Processing:** Uses `aiohttp` and `asyncio` for non-blocking RPC and database calls.
- **Blockchain Data Indexing:** Processes blocks, transactions, and peer information.
- **Data Enrichment:** Enhances transactions by deriving addresses, calculating input/output totals and fees.
- **Caching:** Utilizes Redis (via aiocache) for caching previous transaction lookups.
- **Containerized Deployment:** Includes a Dockerfile and a Docker Compose configuration.
- **Custom Volume Bind Mounts:** Easily configure where data (e.g., MongoDB files) is stored on your host.
- **Umbrel Home Deployment:** Ready for deployment on Umbrel Home via a community app package.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Colima](https://github.com/abiosoft/colima) (if running Docker on macOS/Linux)
- (Optional) Umbrel Home for self-hosting

## Setup

### Clone the Repository

```bash
git clone https://github.com/your_username/bitcoin-indexer.git
cd bitcoin-indexer

### Environment Configuration
Create a .env file in the project root. For example:
# Bitcoin RPC Settings
RPC_USER=umbrel
RPC_PASSWORD=your_rpc_password
RPC_URL=http://umbrel.local:8332

# MongoDB Settings
MONGO_URI=mongodb://mongodb:27017/bitcoin_db

# Redis Settings
REDIS_URL=redis://redis:6379/0

# Optional: Define home directory for bind mounts
HOME_DIR=${HOME}
```

### Docker Compose Configuration

Your docker-compose.yaml includes three services:
- bitcoin-indexer: The main application container.
- mongodb: MongoDB container (exposing an external port if desired).
- redis: Redis container for caching.

An example excerpt:

```Dockerfile
services:
  bitcoin-indexer:
    build: .
    container_name: bitcoin-indexer
    environment:
      - RPC_USER=${RPC_USER}
      - RPC_PASSWORD=${RPC_PASSWORD}
      - RPC_URL=${RPC_URL}
      - MONGO_URI=${MONGO_URI}
      - REDIS_URL=${REDIS_URL}
    depends_on:
      - mongodb
      - redis
    volumes:
      - "${HOME_DIR}/mongo_data:/data/db"
    restart: unless-stopped

  mongodb:
    image: mongo:6.0
    container_name: mongodb
    ports:
      - "27018:27017"
    volumes:
      - "${HOME_DIR}/mongo_data:/data/db"

  redis:
    image: redis:alpine
    container_name: redis
    ports:
      - "6379:6379"
    volumes:
      - "${HOME_DIR}/redis_data:/data"
    restart: unless-stopped
```

### Global Cache Configuration

To ensure that your aiocache decorators use the correct Redis host, add a global configuration in your main.py (or a startup module) before any cached functions are called:

```python
from aiocache import caches

caches.set_config({
    "default": {
        "cache": "aiocache.RedisCache",
        "endpoint": "redis",
        "port": 6379,
        "db": 0,
        "ttl": 3600,
        "serializer": {
            "class": "aiocache.serializers.JsonSerializer"
        }
    }
})
```

## Building and Running

Use Docker Compose to build and start your project:
```bash
docker-compose up --build
```
Your MongoDB instance will be accessible on your host at port 27018 using:

```bash
mongo "mongodb://localhost:27018/bitcoin_db"
```

## Troubleshooting
- DNS Issues: Ensure that within your containers, hostnames such as mongodb and redis are used instead of localhost.
- Cache Connection: The global aiocache configuration ensures that Redis is reached at the correct endpoint.
- Permissions: Verify that the bind mount paths (e.g., ~/mongo_data) have appropriate permissions for Docker to write data.

## Contributing
Contributions are welcome! Please fork the repository, create a feature branch, and submit a pull request.

#### Contact me
- nicolasbaum@gmail.com
- github.com/nicolasbaum