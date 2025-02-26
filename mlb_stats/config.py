import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Get the project root directory
ROOT_DIR = Path(__file__).parent.parent
ENV_DIR = ROOT_DIR / "env"


def load_environment(env: Optional[str] = None) -> None:
    """
    Load environment variables from the appropriate .env file based on ENV setting.
    
    Args:
        env: Optional environment name. If not provided, will use ENV environment variable
             or default to 'development'
    """
    # Determine which environment to load
    env = env or os.getenv("ENV", "development")
    
    # Map environment names to their corresponding .env files
    env_files = {
        "development": ".env.default",
        "production": ".env.production",
        # Add more environments as needed
        "staging": ".env.staging",
    }
    
    # Get the appropriate .env file
    env_file = env_files.get(env, ".env.default")
    env_path = ENV_DIR / env_file
    
    # Check if the environment file exists
    if not env_path.exists():
        print(f"Warning: Environment file {env_path} not found, using .env.default")
        env_path = ENV_DIR / ".env.default"
    
    # Load the environment file
    load_dotenv(env_path)
    
    # Set additional environment variables
    os.environ["ENV"] = env
    os.environ["ENV_FILE"] = str(env_path)
    
    print(f"Loaded environment from: {env_path}")


def get_required_env(key: str) -> str:
    """
    Get a required environment variable.
    
    Args:
        key: The environment variable key
        
    Returns:
        The environment variable value
        
    Raises:
        ValueError: If the environment variable is not set
    """
    value = os.getenv(key)
    if value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    return value


def get_optional_env(key: str, default: str = "") -> str:
    """
    Get an optional environment variable with a default value.
    
    Args:
        key: The environment variable key
        default: The default value if not set
        
    Returns:
        The environment variable value or default
    """
    return os.getenv(key, default)


# Environment configuration class
class EnvConfig:
    """Configuration class that loads values from environment variables."""
    
    def __init__(self):
        # Database configuration
        self.postgres_user = get_required_env("POSTGRES_USER")
        self.postgres_password = get_required_env("POSTGRES_PASSWORD")
        self.postgres_db = get_required_env("POSTGRES_DB")
        self.postgres_host = get_required_env("POSTGRES_HOST")
        self.postgres_port = int(get_required_env("POSTGRES_PORT"))
        
        # MLB Stats API configuration
        self.mlb_stats_api_key = get_optional_env("MLB_STATS_API_KEY")
        self.mlb_stats_base_url = get_required_env("MLB_STATS_BASE_URL")
        
        # Ottoneu API configuration
        self.ottoneu_api_key = get_required_env("OTTONEU_API_KEY")
        self.ottoneu_league_id = get_required_env("OTTONEU_LEAGUE_ID")
        
        # DuckDB configuration
        self.duckdb_path = get_required_env("DUCKDB_PATH")
        
        # Dagster configuration
        self.dagster_home = get_required_env("DAGSTER_HOME")
        self.dagster_port = int(get_required_env("DAGSTER_PORT"))
        self.dagster_host = get_required_env("DAGSTER_HOST")
        
        # AWS configuration
        self.aws_access_key_id = get_optional_env("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = get_optional_env("AWS_SECRET_ACCESS_KEY")
        self.aws_default_region = get_optional_env("AWS_DEFAULT_REGION", "us-east-1")
        self.s3_bucket = get_optional_env("S3_BUCKET")
        
        # Environment information
        self.env = os.getenv("ENV", "development")
        self.env_file = os.getenv("ENV_FILE", "")


# Create a global config instance
config = EnvConfig() 