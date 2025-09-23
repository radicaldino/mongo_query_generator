from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    MONGO_URIS: str | None = None
    MONGO_DBS: str | None = None
    PORT:int = 7323
    OPENAI_MODEL: str = "gpt-4o-mini"
    OPENAI_API_KEY: str | None = None
    MDB_MCP_CONNECTION_STRING_INVENTORY: str | None = None
    MDB_MCP_CONNECTION_STRING_APP: str | None = None
    MDB_MCP_CONNECTION_STRING_COMPONENT: str | None = None
    MDB_MCP_CONNECTION_STRING_FULFILLMENT: str | None = None
    MDB_MCP_LOG_PATH: str | None = None

     # LangSmith settings
    LANGSMITH_API_KEY: str | None = None
    LANGSMITH_PROJECT: str = "mongo_query_generator"
    LANGSMITH_ENDPOINT: str = "https://api.smith.langchain.com"
    LANGCHAIN_TRACING_V2: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()