from dotenv import load_dotenv

import asyncpg
import os
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool = None

        # Ability to grab env database information
        load_dotenv()

    async def _initialize_pool(self) -> None:
        self.pool = await asyncpg.create_pool(
            # The password after the colon is not included since there is currently, no password:
            # {os.environ.get('POSTGRES_PASSWORD')}
            f"postgres://{os.environ.get('POSTGRES_USER')}@localhost/{os.environ.get('DATABASE_NAME')}",
            min_size = 4, 
            max_size = 6
        )
        logger.info(
                "Pool has been made"
            )
            
    
    async def _write_to_database(self, query:str, data:list[tuple]) -> None:
        """
        > ERROR HANDLING
        PostgresError
        └── PostgresConnectionError
            ├── ConnectionDoesNotExistError
            ├── ConnectionFailureError
            ├── ClientCannotConnectError
            └── other connection-related errors
        """

        # This could eventually be expensive since we're checking on every turn
        if not self.pool:
            await self._initialize_pool()

        try: 
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    await connection.executemany(
                        query, data
                    )
            logging.info("A transaction has completed with a pooled connection")
        
        except asyncpg.IntegrityConstraintViolationError as error:
        # Covers UniqueViolationError, CheckViolationError, NotNullViolationError, etc. these mean the DATA itself is malformed relative to the schema 
            logger.error(
                f"Database integrity violation: {error}",
                exc_info=True,
            )
            raise

        except (asyncpg.ConnectionDoesNotExistError, asyncpg.CannotConnectNowError,
                asyncpg.ConnectionFailureError) as error:
            # Covers the server being unreachable, refusing connections, or the connection and MAY succeed if retried later
            logger.error(
                f"Database connection issue: {error}",
                exc_info=True,
            )
            raise

        except asyncpg.PostgresError as error:
            # Catch-all for any other Postgres-side errors (still narrower than bare Exception)
            logger.error(
                f"Unhandled postgres error: {error}",
                exc_info=True,
            )
            raise

        except Exception as error:
            logger.error(
                f"Non-database error: {error}",
                exc_info=True,
            )
            raise