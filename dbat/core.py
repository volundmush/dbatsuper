import typing
import uuid
import asyncpg
import logging
import orjson
from rich.traceback import install as install_tb
from rich.logging import RichHandler
import asyncio

from .events.base import EventHandler
from .net import Server, CONNECTIONS

EVENT_HANDLERS: typing.Dict[str, typing.Type[EventHandler]] = dict()

EVENTS: typing.Dict[uuid.UUID, EventHandler] = dict()

class Application:
    def __init__(self):
        self.db_pool = None
        self.server = None
    
    def setup_logging(self):
        install_tb()
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler()]
        )

    def register_event_handlers(self):
        from .events import connection
        
        for c in connection.__all__:
            EVENT_HANDLERS[c] = getattr(connection, c)
    
    def generate_dsn(self):
        with open("dbconf.json") as f:
            config = orjson.loads(f.read())
            # it has dbname, user, password, and host fields.
        return f"postgresql://{config['user']}:{config['password']}@{config['host']}/{config['dbname']}"
    
    async def setup_db(self):
        dsn = self.generate_dsn()
        self.db_pool = await asyncpg.create_pool(dsn)
        
    async def setup(self, host: str, port: int):
        self.setup_logging()
        self.register_event_handlers()
        await self.setup_db()
        self.server = Server(host, port, self.db_pool)


    async def start(self):
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.server.start())
                tg.create_task(self.run())
        except asyncio.CancelledError:
            logging.info("Application cancelled.")
        except BaseExceptionGroup as e:
            logging.error("Unhandled errors in TaskGroup:")
            for exc in e.exceptions:
                logging.error(f"Caught exception: {exc.__class__.__name__}: {exc}")
        finally:
            logging.critical("Shutting down...")

    async def run(self):
        try:
            await self.cleanup_nonpersistent_events()
            while True:
                await self.execute_aborts()
                await self.cleanup_events()
                await self.execute_events()
                await asyncio.sleep(0.08)
        except asyncio.CancelledError:
            logging.info("Application run cancelled.")
    
    async def cleanup_nonpersistent_events(self):
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM dbat.events WHERE persistent = FALSE")
    
    async def execute_aborts(self):
        """
        For every event in dbat.events where state is 'aborted', get event from EVENTS and call ev.task.cancel()
        """
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor("SELECT id FROM dbat.events WHERE current_state = 'aborted'"):
                    if (ev := EVENTS.get(record['id'])):
                        ev.task.cancel()
    
    async def cleanup_events(self):
        """
        For every event marked finished, cancelled, or error, remove the event from EVENTS if it exists.
        Then delete it from the database.
        """
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor("SELECT id, current_state FROM dbat.events WHERE current_state IN ('finished', 'cancelled', 'error')"):
                    EVENTS.pop(record['id'], None)
                    await conn.execute("DELETE FROM dbat.events WHERE id = $1", record['id'])
    
    async def execute_events(self):
        """
        For every event in dbat.events where current_state is 'pending', retrieve the uuid, event_name, and parameters. Mark state as 'active'
        Then create instance of proper event handler and call start() on it.
        """
        new_events = list()
        bad_event_uuids = list()
        new_event_uuids = list()
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor("SELECT id, event_name, parameters FROM dbat.events WHERE current_state = 'pending'"):
                    if (handler_class := EVENT_HANDLERS.get(record['event_name'], None)):
                        uu = record['id']
                        ev = handler_class(uu, orjson.loads(record['parameters'].encode()), self.db_pool)
                        new_event_uuids.append(uu)
                        new_events.append(ev)
                    else:
                        logging.error(f"Event {record['event_name']} not found.")
                        bad_event_uuids.append(record['id'])
                    if bad_event_uuids:
                        await conn.execute("DELETE FROM dbat.events WHERE id = ANY($1)", bad_event_uuids)
                    if new_event_uuids:
                        await conn.execute("UPDATE dbat.events SET current_state = 'active' WHERE id = ANY($1)", new_event_uuids)
        
        # Now that the transaction's complete and the pool has been released, start 'em up.
        for ev in new_events:
            EVENTS[ev.id] = ev
            ev.start()
