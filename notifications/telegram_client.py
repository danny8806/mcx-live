"""Telegram Bot API client with queue-based async sending."""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from typing import Any, Optional

import urllib.request
import urllib.error

logger = logging.getLogger(__name__)


class TelegramClient:
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None,
                 on_delivery: Optional[object] = None):
        """``on_delivery`` is an optional ``(meta, ok, error) -> None`` callback
        invoked by the worker after each queued message settles, so the durable
        alert ledger can mark SENT/FAILED.  Never required for sending."""
        if bot_token:
            self.bot_token = bot_token
        else:
            try:
                from config import Config
                cfg = Config.get("telegram") or {}
                self.bot_token = cfg.get("bot_token", "") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
            except Exception:
                self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        # Support multiple chat_ids (comma-separated or list)
        if chat_id:
            raw = chat_id
        else:
            try:
                from config import Config
                cfg = Config.get("telegram") or {}
                raw = cfg.get("chat_id", "") or os.environ.get("TELEGRAM_CHAT_ID", "")
            except Exception:
                raw = os.environ.get("TELEGRAM_CHAT_ID", "")
        if isinstance(raw, list):
            self.chat_ids = [str(c).strip() for c in raw if str(c).strip()]
        else:
            self.chat_ids = [c.strip() for c in str(raw).split(",") if c.strip()]
        self._queue: queue.Queue = queue.Queue(maxsize=500)
        self._running = False
        self._worker: Optional[threading.Thread] = None
        self._sent_count = 0
        self._error_count = 0
        self._on_delivery = on_delivery
        self._last_error: Optional[str] = None

    def last_error(self) -> Optional[str]:
        """Most recent per-message failure detail (F5 — real send diagnostics
        for the alert ledger instead of an opaque empty error)."""
        return self._last_error

    def set_delivery_callback(self, callback: Optional[object]) -> None:
        """Attach/swap the post-delivery callback (used by the router's ledger)."""
        self._on_delivery = callback

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker = threading.Thread(target=self._run_worker, daemon=True, name="telegram-worker")
        self._worker.start()
        logger.info("Telegram worker started")

    def stop(self) -> None:
        self._running = False
        if self._worker:
            self._worker.join(timeout=5)

    def _run_worker(self) -> None:
        while self._running:
            try:
                text, parse_mode, silent, meta = self._queue.get(timeout=1.0)
                error = None
                try:
                    ok = self._send_message(text, parse_mode, silent)
                    if not ok:
                        error = self._last_error or "telegram send failed"
                        if not (self.bot_token or self.chat_ids):
                            error = "telegram not configured"
                except Exception as e:  # a per-message failure must not kill the worker
                    ok = False
                    error = str(e)
                    logger.error(f"Telegram worker error: {e}")
                finally:
                    self._queue.task_done()
                if self._on_delivery is not None:
                    try:
                        self._on_delivery(meta, bool(ok), error)
                    except Exception:
                        pass
            except queue.Empty:
                continue

    def _send_message(self, text: str, parse_mode: str = "HTML", silent: bool = False) -> bool:
        if not self.bot_token or not self.chat_ids:
            self._last_error = "telegram not configured (missing token/chat_id)"
            logger.warning("Telegram not configured - message dropped")
            return False
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        all_ok = True
        for chat_id in self.chat_ids:
            payload = json.dumps({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": silent,
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        self._sent_count += 1
                    else:
                        self._last_error = f"http_{resp.status}"
                        all_ok = False
            except urllib.error.URLError as e:
                self._error_count += 1
                self._last_error = str(e)
                logger.error(f"Telegram send to {chat_id} failed: {e}")
                all_ok = False
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                logger.error(f"Telegram error to {chat_id}: {e}")
                all_ok = False
        return all_ok

    def send(self, text: str, parse_mode: str = "HTML", silent: bool = False,
             meta: Optional[dict] = None) -> bool:
        try:
            self._queue.put_nowait((text, parse_mode, silent, meta or {}))
            return True
        except queue.Full:
            logger.warning("Telegram queue full - message dropped")
            return False

    def send_sync(self, text: str, parse_mode: str = "HTML", silent: bool = False) -> bool:
        return self._send_message(text, parse_mode, silent)

    def get_stats(self) -> dict[str, Any]:
        return {
            "configured": bool(self.bot_token and self.chat_ids),
            "chat_ids": self.chat_ids,
            "sent_count": self._sent_count,
            "error_count": self._error_count,
            "queue_size": self._queue.qsize(),
            "running": self._running,
        }
