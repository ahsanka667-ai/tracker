"""
service_a/websocket_manager.py
Manages WebSocket connections for real-time dashboard notifications.
Multiple dashboard tabs can connect simultaneously.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages all active WebSocket connections to the dashboard."""

    def __init__(self):
        # user_id -> list of active WebSocket connections
        self._connections: dict[int, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: int):
        await websocket.accept()
        if user_id not in self._connections:
            self._connections[user_id] = []
        self._connections[user_id].append(websocket)
        logger.info("WS connected: user=%d total_connections=%d", user_id, self.total)

    def disconnect(self, websocket: WebSocket, user_id: int):
        if user_id in self._connections:
            self._connections[user_id] = [
                ws for ws in self._connections[user_id] if ws != websocket
            ]
            if not self._connections[user_id]:
                del self._connections[user_id]
        logger.info("WS disconnected: user=%d total_connections=%d", user_id, self.total)

    @property
    def total(self) -> int:
        return sum(len(conns) for conns in self._connections.values())

    async def send_to_user(self, user_id: int, event: dict[str, Any]):
        """Send a notification to all tabs open by a specific user."""
        conns = self._connections.get(user_id, [])
        dead = []
        for ws in conns:
            try:
                await ws.send_text(json.dumps(event))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, user_id)

    async def broadcast(self, event: dict[str, Any]):
        """Send a notification to ALL connected dashboard users."""
        for user_id in list(self._connections.keys()):
            await self.send_to_user(user_id, event)

    def build_notification(
        self,
        type: str,
        title: str,
        body: str,
        data: dict | None = None,
    ) -> dict[str, Any]:
        return {
            "type": type,
            "title": title,
            "body": body,
            "data": data or {},
            "ts": datetime.now(timezone.utc).isoformat(),
        }


# Singleton — imported by both service_a and accessed via Redis pub/sub from service_b
ws_manager = ConnectionManager()
