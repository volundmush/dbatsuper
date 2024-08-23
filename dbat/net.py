import asyncio
import uuid
import typing
import orjson
import logging

class OutMessage:
    """
    Basic Class used to hold outgoing message. flags provide information to the async writer about how to handle the message.
    """
    
    __slots__ = ['data', 'flags']
    
    def __init__(self, data: bytes, flags: int = 0):
        self.data = data
        self.flags = flags

class Connection:
    """
    Basic class which represents a game connection.
    """
    
    __slots__ = ['server', 'conn_id', 'reader', 'writer', 'task', 'out_queue', 'pending_commands', 'closed_by_client', 'closed_by_server']
    
    def __init__(self, server, conn_id: uuid.UUID, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.server = server
        self.conn_id = conn_id
        self.reader = reader
        self.writer = writer
        self.task = None
        
        # Holds instances of OutMessages.
        self.out_queue = asyncio.Queue()
        
        # commands read from the incoming reader.
        self.pending_commands = []
        
        self.closed_by_client = False
        self.closed_by_server = False
    
    def start(self):
        self.task = asyncio.create_task(self.run())
    
    async def close(self):
        self.closed_by_server = True
        self.task.cancel()
    
    async def run(self):
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.run_reader())
                tg.create_task(self.run_writer())
        except asyncio.CancelledError:
            pass
        except BaseExceptionGroup as e:
            logging.error("Unhandled errors in TaskGroup:")
            for exc in e.exceptions:
                logging.error(f"Caught exception: {exc.__class__.__name__}: {exc}")
        finally:
            self.writer.close()
            DEAD_CONNECTIONS[self.conn_id] = CONNECTIONS.pop(self.conn_id)
            del NEW_CONNECTIONS[self.conn_id]
        await self.writer.wait_closed()


    async def run_reader(self):
        in_buffer = bytearray()
        try:
            while True:
                data = await self.reader.read(1024)  # Adjust the read size as appropriate
                if not data:
                    # EOF reached or connection closed
                    self.closed_by_client = True
                    self.task.cancel()
                    break
                
                in_buffer.extend(data)
                
                while b'\r\n' in in_buffer:
                    # Find the position of the first occurrence of \r\n
                    line_end_pos = in_buffer.find(b'\r\n')
                    
                    # Extract the line without stripping leading whitespace
                    line = in_buffer[:line_end_pos].decode('utf-8', errors='ignore')
                    
                    # Remove the processed line and \r\n from the buffer. We can use the del operator.
                    del in_buffer[:line_end_pos + 2]
                    
                    # Fire off the complete line
                    self.pending_commands.append(line)
                    PENDING_COMMANDS.add(self.conn_id)
                    
        except asyncio.CancelledError:
            pass
    
    async def run_writer(self):
        """
        Continuously writes messages from the out_queue to the StreamWriter.
        """
        while True:
            message: OutMessage = await self.out_queue.get()
            if message.flags == 0:
                self.writer.write(message.data)
            # Add any flag-based behavior here (e.g., special handling based on message.flags)
            await self.writer.drain()  # Ensure data is sent
            self.out_queue.task_done()  # Mark task as done
            if message.flags:
                self.task.cancel()

# dict[uuid, Connection]
CONNECTIONS: typing.Dict[uuid.UUID, Connection] = dict()
NEW_CONNECTIONS: typing.Set[uuid.UUID] = set()
DEAD_CONNECTIONS: typing.Dict[uuid.UUID, Connection] = set()
PENDING_COMMANDS: typing.Set[uuid.UUID] = set()

class Server:
    """
    Basic class for managing our game connection.
    """
    def __init__(self, host: str, port: int, db_pool: asyncpg.Pool):
        self.host = host
        self.port = port
        self.server = None
        self.db_pool = db_pool

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addr = self.server.sockets[0].getsockname()
        logging.info(f'Serving on {addr}')

    def handle_client(self, reader, writer):
        conn_id = uuid.uuid4()
        conn = Connection(self, conn_id, reader, writer)
        CONNECTIONS[conn_id] = conn
        logging.info(f'New connection: {conn_id}')
        conn.start()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()