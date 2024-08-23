import asyncio
import uuid
import asyncpg


class EventHandler:
    """
    Abstract base class for dealing with events in the game.
    """
    
    class EventError(Exception):
        """Custom exception class for handling specific event errors."""
        pass
    
    def __init__(self, id: uuid.UUID, parameters: dict, db_pool: asyncpg.Pool):
        self.id = id
        self.parameters = parameters
        self.task = None
        self.db_pool = db_pool
    
    def start(self):
        self.task = asyncio.create_task(self._execute_event())
    
    async def validate_task(self):
        """
        This coroutine is called before the main task is started. It should be used to check that the Task can be started.
        This can include checking preconditions, validating parameters, etc.
        """
    
    async def _execute_event(self):
        try:
            await self.validate_task()
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    try:
                        await self.run(conn)
                        await self._mark_state(conn, "finished")
                    except (asyncio.CancelledError, self.EventError) as e:
                        raise
                    except Exception as e:
                        raise self.EventError(f"Unhandled exception during event processing: {e}")
        except asyncio.CancelledError:
            await self._mark_state("cancelled")
        except self.EventError:
            await self._mark_state("error")
        except Exception as e:
            # Handle any unexpected errors that should also abort the event
            print(f"Unexpected error in EventHandler {self.id}: {e}")
            await self._mark_failed("error")
    
    async def run(self, conn: asyncpg.Connection):
        """
        Main logic of the event goes here. This method should be overridden in subclasses.
        
        If this function raises an exception that reaches the calling method, the transaction will be aborted.
        If it reaches the end successfully, the transaction will be committed. 
        """
        pass
    
    async def _mark_state(self, new_state: str):
        """
        This method is called if the event fails.
        It sets the event state to 'failed' in the database.
        """
        async with self.db_pool.acquire() as conn:
            await conn.execute("UPDATE dbat.events SET current_state = $1 WHERE id = $2", new_state, self.id)