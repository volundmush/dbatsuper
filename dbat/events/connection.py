import logging
from .base import EventHandler

class ClientSubmittedCommand(EventHandler):
    """
    Event handler for when a client sends a command to the server.
    """
    
    async def run(self, conn):
        logging.info(f"Command from conn ({self.parameters['conn_id']}): {self.parameters['command']}")


class ClientConnected(EventHandler):
    """
    Event handler for when a client sends a command to the server.
    """
    
    async def run(self, conn):
        logging.info(f"Client connected: {self.parameters['conn_id']}")


class ClientDisconnected(EventHandler):
    """
    Event handler for when a client has disconnected unexpectedly.
    """
    
    async def run(self, conn):
        logging.info(f"Client disconnected: {self.parameters['conn_id']}")


__all__ = ["ClientSubmittedCommand", "ClientConnected", "ClientDisconnected"]