"""WebSocket connection manager for real-time dashboard updates."""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: Dict[str, WebSocket] = {}
        self._client_channels: Dict[str, Set[str]] = {}
        self._channel_clients: Dict[str, Set[str]] = {}
        self._lock = threading.Lock()
        self._message_count = 0

    def connect(self, client_id: str, websocket: WebSocket, channels: Optional[List[str]] = None) -> None:
        with self._lock:
            self._connections[client_id] = websocket
            if channels is None:
                channels = ["all"]
            self._client_channels[client_id] = set(channels)
            for ch in channels:
                if ch not in self._channel_clients:
                    self._channel_clients[ch] = set()
                self._channel_clients[ch].add(client_id)
        logger.info(f"WS connected: {client_id} channels={channels}")

    def disconnect(self, client_id: str) -> None:
        with self._lock:
            self._connections.pop(client_id, None)
            client_channels = self._client_channels.pop(client_id, set())
            for ch in client_channels:
                if ch in self._channel_clients:
                    self._channel_clients[ch].discard(client_id)
        logger.info(f"WS disconnected: {client_id}")

    def subscribe(self, client_id: str, channels: List[str]) -> None:
        with self._lock:
            old_channels = self._client_channels.get(client_id, set())
            for ch in old_channels:
                if ch in self._channel_clients:
                    self._channel_clients[ch].discard(client_id)
            self._client_channels[client_id] = set(channels)
            for ch in channels:
                if ch not in self._channel_clients:
                    self._channel_clients[ch] = set()
                self._channel_clients[ch].add(client_id)

    async def broadcast(self, event_type: str, data: Any) -> None:
        message = json.dumps({"type": event_type, "data": data, "ts": time.time()}, default=str)
        with self._lock:
            target_ids = list(self._connections.keys())
            conns = dict(self._connections)
        for client_id in target_ids:
            ws = conns.get(client_id)
            if ws:
                try:
                    await ws.send_text(message)
                    self._message_count += 1
                except Exception:
                    self.disconnect(client_id)

    async def broadcast_to_channel(self, channel: str, event_type: str, data: Any) -> None:
        message = json.dumps({"type": event_type, "data": data, "ts": time.time()}, default=str)
        with self._lock:
            client_ids = list(self._channel_clients.get(channel, set()))
            conns = dict(self._connections)
        for client_id in client_ids:
            ws = conns.get(client_id)
            if ws:
                try:
                    await ws.send_text(message)
                except Exception:
                    self.disconnect(client_id)

    @property
    def active_connections(self) -> int:
        with self._lock:
            return len(self._connections)

    @property
    def total_messages(self) -> int:
        return self._message_count

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "active_connections": len(self._connections),
                "channels": {ch: len(ids) for ch, ids in self._channel_clients.items()},
                "total_messages": self._message_count,
            }
