from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_raw_topic: str = "events.raw"
    kafka_dlq_topic: str = "events.dlq"
    kafka_group: str = "event-processors"
    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_database: str = "events"
    mysql_user: str = "events"
    mysql_password: str = "events_password"
    redis_host: str = "redis"
    redis_port: int = 6379
    dedup_ttl_seconds: int = 86400
    dedup_processing_ttl_seconds: int = 300
    batch_size: int = 100
    batch_timeout_seconds: float = 1.0

    @property
    def mysql_url(self) -> str:
        return f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
