from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from supabase import Client, create_client

from app.config import Settings


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None
        self.supabase: Client | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=self._settings.supabase_database_url.get_secret_value(),
            min_size=1,
            max_size=self._settings.db_pool_size,
            command_timeout=60,
        )
        self.supabase = create_client(
            str(self._settings.supabase_url),
            self._settings.supabase_service_role_key.get_secret_value(),
        )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
        self.supabase = None

    @property
    def pool(self) -> asyncpg.Pool:
        if not self._pool:
            raise RuntimeError("Database is not connected.")
        return self._pool

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection:
            yield connection
