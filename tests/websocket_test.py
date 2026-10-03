"""Вебсокет против локального сервера: подписки, отправка, реконнект, остановка."""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from hl_twap_watcher._websocket import Websocket


class Server:
    """Локальный сервер: запоминает входящие сообщения и умеет рвать соединения."""

    def __init__(self) -> None:
        self.received: list[dict[str, Any]] = []
        self.connections: list[ServerConnection] = []
        self.url = ""

    async def handler(self, conn: ServerConnection) -> None:
        self.connections.append(conn)
        await conn.send(json.dumps({"channel": "hello", "data": len(self.connections)}))
        async for raw in conn:
            self.received.append(json.loads(raw))


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    state = Server()
    async with serve(state.handler, "127.0.0.1", 0) as srv:
        port = next(iter(srv.sockets)).getsockname()[1]
        state.url = f"ws://127.0.0.1:{port}"
        yield state


async def wait_for(predicate: Any, timeout: float = 3.0) -> None:
    """Ждет, пока условие станет истинным."""
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_subscribe_send_reconnect_stop(server: Server) -> None:
    messages: list[dict[str, Any]] = []
    connects = 0

    async def on_message(msg: dict[str, Any]) -> None:
        messages.append(msg)

    async def on_connect() -> None:
        nonlocal connects
        connects += 1

    subscription = {"method": "subscribe", "subscription": {"type": "allMids"}}
    ws = Websocket(
        server.url,
        on_message,
        subscription_messages=[subscription],
        on_connect=on_connect,
        ping_message=None,
        reconnect_timeout=0.05,
    )
    task = asyncio.create_task(ws.start())

    # Подписка уходит сразу после подключения, сообщения доходят до callback.
    await wait_for(lambda: ws.connected and messages)
    assert server.received == [subscription]
    assert messages == [{"channel": "hello", "data": 1}]

    await ws.send({"method": "ping"})
    await wait_for(lambda: len(server.received) == 2)

    # Сервер рвет соединение — клиент переподключается и повторяет подписку.
    await server.connections[0].close()
    await wait_for(lambda: len(server.connections) == 2 and ws.connected)
    await wait_for(lambda: len(server.received) == 3)
    assert server.received[2] == subscription
    assert connects == 2

    await ws.stop()
    await asyncio.wait_for(task, timeout=3)
    assert not ws.running
    assert not ws.connected


async def test_send_without_connection_raises() -> None:
    async def on_message(msg: dict[str, Any]) -> None: ...

    ws = Websocket("ws://127.0.0.1:1", on_message)

    with pytest.raises(ConnectionError):
        await ws.send({"method": "ping"})


async def test_silence_watchdog_reconnects(server: Server) -> None:
    async def on_message(msg: dict[str, Any]) -> None: ...

    # Сервер шлет одно приветствие и молчит — сторож должен пересоздать соединение.
    ws = Websocket(
        server.url,
        on_message,
        ping_message=None,
        no_message_reconnect_timeout=0.2,
        reconnect_timeout=0.05,
    )
    task = asyncio.create_task(ws.start())

    await wait_for(lambda: len(server.connections) >= 2)

    await ws.stop()
    await asyncio.wait_for(task, timeout=3)


async def test_reconnect_on_request(server: Server) -> None:
    async def on_message(msg: dict[str, Any]) -> None: ...

    subscription = {"method": "subscribe", "subscription": {"type": "allMids"}}
    ws = Websocket(server.url, on_message, subscription_messages=[subscription], ping_message=None, reconnect_timeout=0.05)
    task = asyncio.create_task(ws.start())
    await wait_for(lambda: ws.connected)

    await ws.reconnect()

    # Новое соединение и повторная подписка, вебсокет продолжает работать.
    await wait_for(lambda: len(server.connections) == 2 and ws.connected)
    await wait_for(lambda: server.received.count(subscription) == 2)
    assert ws.running

    await ws.stop()
    await asyncio.wait_for(task, timeout=3)
