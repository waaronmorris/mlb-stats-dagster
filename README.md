# MLB Stats Dagster Project

<div align="center">
  <a target="_blank" href="https://dagster.io">
    <img alt="dagster logo" src="https://github.com/dagster-io/dagster/raw/master/docs/next/public/assets/logos/dagster.png" width="auto" height="120px">
  </a>
</div>

A comprehensive data pipeline project that processes MLB statistics and Ottoneu fantasy baseball data using Dagster.

## Data Flow Architecture

### High-Level System Architecture
```mermaid
flowchart TB
    subgraph External Sources
        MLB[MLB Stats API]
        OTT[Ottoneu API]
    end

    subgraph Storage & Processing
        subgraph "Data Lake"
            R2[Cloudflare R2]
        end
        
        subgraph "Data Processing"
            DUCK[DuckDB]
            DBT[DBT Transformations]
        end
    end

    subgraph "Dagster Infrastructure"
        PG[PostgreSQL<br/>Dagster Metadata]
        DAG[Dagster Orchestration]
    end

    MLB --> R2
    OTT --> R2
    R2 --> DUCK
    DUCK --> DBT
    DBT --> R2
    
    DAG --> PG
    DAG --> MLB
    DAG --> OTT
```

### Asset Lineage
```mermaid
flowchart LR
    subgraph "Raw Data (R2)"
        direction TB
        BS[Box Scores]
        SCH[Schedules]
        TRANS[Transactions]
        ROST[Rosters]
    end

    subgraph "Stage (DuckDB)"
        direction TB
        PL_BS[Player Box Scores]
        GM_SCH[Game Schedules]
        TM_TRANS[Team Transactions]
        PL_ROST[Player Rosters]
    end

    subgraph "Refined (R2)"
        direction TB
        STATS[Player Statistics]
        TEAMS[Team Analytics]
        PERF[Performance Metrics]
    end

    BS --> PL_BS
    SCH --> GM_SCH
    TRANS --> TM_TRANS
    ROST --> PL_ROST

    PL_BS --> STATS
    GM_SCH --> STATS
    TM_TRANS --> TEAMS
    PL_ROST --> TEAMS

    STATS --> PERF
    TEAMS --> PERF
```

### Data Processing Flow
```mermaid
sequenceDiagram
    participant MLB as MLB Stats API
    participant OTT as Ottoneu API
    participant DAG as Dagster
    participant PG as PostgreSQL<br/>(Metadata)
    participant R2 as Cloudflare R2
    participant DUCK as DuckDB
    participant DBT as DBT

    Note over DAG,PG: Stores job metadata<br/>and orchestration state
    
    DAG->>MLB: Fetch game data
    MLB-->>DAG: Raw game data
    DAG->>OTT: Fetch fantasy data
    OTT-->>DAG: Raw fantasy data
    DAG->>R2: Store raw data
    
    DAG->>DUCK: Load data for processing
    DUCK->>R2: Read raw data
    DUCK->>DBT: Process with DBT
    DBT->>DUCK: Transform data
    DUCK->>R2: Store processed data
    
    DAG->>PG: Update job status
```

## Features

- MLB Statistics Processing
  - Fetches data from MLB Stats API
  - Processes box scores and game schedules
  - Historical and real-time data processing
- Ottoneu Fantasy Baseball Integration
  - League transaction tracking
  - Player roster analysis
  - Team performance metrics
- Data Storage & Processing
  - Cloudflare R2 for data lake storage (raw and processed data)
  - DuckDB for efficient data processing and transformations
  - PostgreSQL for Dagster metadata and job orchestration
- DBT Integration
  - Modular data transformations
  - Incremental processing
  - Data quality tests

## Project Structure

```
mlb_stats-dagster/
├── mlb_stats/              # Main Python package
│   ├── assets/            # Dagster assets definitions
│   ├── jobs/             # Dagster job definitions
│   ├── resources/        # Custom resource implementations
│   ├── sensors/          # Event sensors
│   └── io/              # I/O management
├── mlb_stats_dbt/         # DBT transformations
├── env/                   # Environment configuration
│   ├── .env.schema       # Environment variable documentation
│   ├── .env.default      # Default development values
│   └── .env.production   # Production configuration (not in VCS)
├── config/               # Dagster configuration
└── tests/               # Test suite
```

## Prerequisites

- Python 3.10+
- Docker and Docker Compose
- MLB Stats API access (if required)
- Ottoneu API credentials
- Cloudflare R2 account (for data lake)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/mlb-stats-dagster.git
   cd mlb-stats-dagster
   ```

2. Set up the environment:
   ```bash
   # Install dependencies
   pip install poetry
   poetry install

   # Set up environment configuration
   cp env/.env.default env/.env
   ```

3. Configure your environment variables in `env/.env`

## Development Setup

### Local Development

1. Start the development environment:
   ```bash
   ENV=development dagster dev
   ```

2. Access the Dagster UI at http://localhost:3000

### Docker Development

1. Build and start the containers:
   ```bash
   docker-compose up --build
   ```

2. Access the Dagster UI at http://localhost:3000

## Environment Configuration

The project uses a flexible environment configuration system:

- `ENV=development` → Uses `.env.default`
- `ENV=production` → Uses `.env.production`
- `ENV=staging` → Uses `.env.staging`

See [Environment Configuration](env/README.md) for detailed setup instructions.

## Running Jobs

### CLI

```bash
# Run a specific job
dagster job execute -f mlb_stats/jobs/mlb_api.py

# Materialize assets
dagster asset materialize --select "*"
```

### Scheduling

Jobs can be scheduled through the Dagster UI or configured in `mlb_stats/schedules.py`.

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest mlb_stats_tests/test_assets.py
```

## Deployment

### Docker Production Deployment

1. Build the production image:
   ```bash
   docker build -t mlb-stats-dagster .
   ```

2. Run with production configuration:
   ```bash
   docker run -e ENV=production -p 3000:3000 mlb-stats-dagster
   ```

### Dagster Cloud Deployment

This project is compatible with Dagster Cloud. See the [Dagster Cloud Documentation](https://docs.dagster.cloud) for deployment instructions.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Dagster](https://dagster.io/) for the data orchestration framework
- [MLB Stats API](https://statsapi.mlb.com/) for baseball statistics
- [Ottoneu Fantasy Baseball](https://ottoneu.fangraphs.com/) for fantasy baseball data
