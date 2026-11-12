"""
本地日志接收端（模拟真实后台）。

职责：
  * 接收 POST /events 并完整落库“实际收到的请求”（原始头、正文、来自哪条 TCP 连接），
    用于演示代理是否真的把走私请求挡在外面、以及是否发生重复转发；
  * 提供查询 / 清空接口与 /docs 演示 API（FastAPI 自动文档）；
  * 支持 X-Demo-Delay 秒级延迟，用于演示代理 2 秒超时与连接池容量；
  * 所有响应一律使用 Content-Length 成帧（代理只信任该成帧方式）；
  * 内置原始 TCP 混沌端（端口 8001）：**先完整收下请求并落库**，再按
    X-Chaos-Mode 以三种典型故障使坏（坏回执 / 完整回执后关闭 / 收全后 RST），
    用于验证代理“收下日志后绝不重发”的语义。混沌端与 FastAPI 写入同一个
    STORE，GET /events 即可对照实收条数。
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

MAX_STORED = int(os.environ.get("UPSTREAM_MAX_STORED", "200"))
CHAOS_HOST = os.environ.get("CHAOS_HOST", "0.0.0.0")
CHAOS_PORT = int(os.environ.get("CHAOS_PORT", "8001"))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 与 uvicorn 同事件循环启动原始 TCP 混沌端（见文件下方 _chaos_connection）。
    chaos_server = await asyncio.start_server(
        _chaos_connection, CHAOS_HOST, CHAOS_PORT)
    _app.state.chaos_server = chaos_server
    try:
        yield
    finally:
        chaos_server.close()
        await chaos_server.wait_closed()


app = FastAPI(
    title="日志接收端（上游）",
    description="探针事件上报的模拟后台，记录代理转发上来的**实际请求**，用于验证反走私效果与“收下日志后不重发”语义。",
    version="1.1.0",
    lifespan=lifespan,
)


class Store:
    def __init__(self) -> None:
        self.records: deque[dict] = deque(maxlen=MAX_STORED)
        self.lock = asyncio.Lock()
        self.counter = 0

    async def add(self, record: dict) -> int:
        async with self.lock:
            self.counter += 1
            record["seq"] = self.counter
            self.records.append(record)
            return self.counter

    async def snapshot(self) -> list[dict]:
        async with self.lock:
            return list(self.records)

    async def clear(self) -> int:
        async with self.lock:
            n = len(self.records)
            self.records.clear()
            return n


STORE = Store()


# --------------------------------------------------------------------------- #
# 原始 TCP 混沌端：收下日志后使坏（验证代理“收下后绝不重发”）
# --------------------------------------------------------------------------- #

CHAOS_MODES = ("bad-receipt", "close-after-receipt", "reset-after-receipt")


def _parse_head(head: bytes) -> tuple[list[str], dict[str, str]]:
    """把请求头块拆成请求行字段与 {小写名: 值} 字典。"""
    lines = head.split(b"\r\n")
    request_parts = lines[0].decode("latin1").split(" ") if lines else ["?"]
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if b":" in line:
            k, v = line.split(b":", 1)
            headers[k.decode("latin1").strip().lower()] = \
                v.decode("latin1").strip()
    return request_parts, headers


async def _chaos_connection(reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
    """完整收下请求并落库，然后按 X-Chaos-Mode 以原始字节使坏。

    与 FastAPI 中间件写入同一个 STORE：代理若重发，这里会多出记录，
    GET /events 立即可见。
    """
    peer = writer.get_extra_info("peername")
    conn_key = f"{peer[0]}:{peer[1]}" if peer else "?"
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        request_parts, headers = _parse_head(head[:-4])
        try:
            cl = int(headers.get("content-length", "0"))
        except ValueError:
            cl = 0
        body = b""
        if cl > 0:
            body = await asyncio.wait_for(reader.readexactly(cl), timeout=5)
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError,
            asyncio.TimeoutError, ConnectionError, OSError):
        writer.close()
        return

    mode = headers.get("x-chaos-mode", "")
    # 关键：先落库再使坏 —— 模拟“接收端已收下日志”的真实故障时刻。
    seq = await STORE.add({
        "received_at": time.time(),
        "method": request_parts[0] if request_parts else "?",
        "path": request_parts[1] if len(request_parts) > 1 else "?",
        "http_version": (request_parts[2].removeprefix("HTTP/")
                         if len(request_parts) > 2 else "?"),
        "client_tcp": conn_key,
        "headers": headers,
        "body_bytes": len(body),
        "body_text": body.decode("utf-8", "replace"),
        "delay_seconds": 0.0,
        "via": "raw-chaos",
        "chaos_mode": mode or "none",
    })

    try:
        if mode == "bad-receipt":
            # 坏回执：声明 999 字节只给 5 字节就 FIN —— 回执解析必然失败。
            writer.write(b"HTTP/1.1 500 Internal Server Error\r\n"
                         b"Content-Type: text/plain\r\n"
                         b"Content-Length: 999\r\n\r\n"
                         b"short")
            await writer.drain()
            writer.close()
        elif mode == "close-after-receipt":
            # 完整合法回执 + Connection: close —— 代理须原样上交 202 与正文，
            # 仅这条连接不得回池。
            payload = json.dumps(
                {"accepted": True, "seq": seq,
                 "chaos": "close-after-receipt"}).encode()
            writer.write(b"HTTP/1.1 202 Accepted\r\n"
                         b"Content-Type: application/json\r\n"
                         b"Content-Length: " + str(len(payload)).encode()
                         + b"\r\nConnection: close\r\n\r\n" + payload)
            await writer.drain()
            writer.close()
        elif mode == "reset-after-receipt":
            # 日志已收全：直接 RST，一个字节都不回 —— 代理的发送收尾 /
            # 读取回执必然失败（旧实现会在此刻重发导致重复入库）。
            sock = writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                struct.pack("ii", 1, 0))
            writer.close()
        else:
            # 未指定模式：正常 202（单发后即关，声明 close 保持诚实）。
            payload = json.dumps(
                {"accepted": True, "seq": seq, "chaos": "none"}).encode()
            writer.write(b"HTTP/1.1 202 Accepted\r\n"
                         b"Content-Type: application/json\r\n"
                         b"Content-Length: " + str(len(payload)).encode()
                         + b"\r\nConnection: close\r\n\r\n" + payload)
            await writer.drain()
            writer.close()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class EventIn(BaseModel):
    # 演示用：结构故意宽松，重点是代理层的 HTTP 分帧安全而非业务校验。
    event: Optional[str] = Field(default=None, description="事件名")
    payload: Optional[dict | list | str | int | float | bool] = Field(
        default=None, description="任意事件负载")


@app.middleware("http")
async def record_request(request: Request, call_next):
    # 演示延迟头：让上游慢一点，触发代理 2 秒总限时 / 观察连接池排队。
    delay = 0.0
    raw_delay = request.headers.get("x-demo-delay")
    if raw_delay:
        try:
            delay = max(0.0, min(10.0, float(raw_delay)))
        except ValueError:
            delay = 0.0

    body = await request.body()
    received_at = time.time()
    if delay:
        await asyncio.sleep(delay)

    # 同一条 TCP 连接上的第 N 个请求（uvicorn 每次连接复用同一 socket 对端）。
    peername = request.scope.get("client") or ("?", 0)
    conn_key = f"{peername[0]}:{peername[1]}"

    record = {
        "received_at": received_at,
        "method": request.method,
        "path": request.url.path,
        "http_version": request.scope.get("http_version"),
        "client_tcp": conn_key,
        # 关键证据：上游逐字看到的头与正文。代理重建后这里应只有一个 Content-Length。
        "headers": {k.decode("latin1"): v.decode("latin1")
                    for k, v in request.headers.raw},
        "body_bytes": len(body),
        "body_text": body.decode("utf-8", "replace"),
        "delay_seconds": delay,
    }
    seq = await STORE.add(record)
    request.state.record_seq = seq

    response: Response = await call_next(request)
    response.headers["X-Upstream-Conn"] = conn_key
    response.headers["X-Upstream-Seq"] = str(seq)
    return response


@app.post("/events", status_code=202)
async def ingest_events(request: Request, event: EventIn | None = None) -> dict:
    """接收探针事件。代理只允许把 POST /events 转发到这里。

    `event` 仅用于在 /docs 中展示请求体 schema；实际落库以中间件读到的
    原始字节为准（证据需要逐字还原代理重建后的 Content-Length 正文）。
    """
    seq = request.state.record_seq
    return {"accepted": True, "seq": seq, "stored_total": STORE.counter}


@app.get("/events", summary="查看上游实际收到的请求")
async def list_events(limit: int = 50) -> dict:
    items = await STORE.snapshot()
    return {"count": len(items), "items": items[-limit:]}


@app.delete("/events", summary="清空已记录请求（演示前重置证据）")
async def clear_events() -> dict:
    removed = await STORE.clear()
    return {"cleared": removed}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/chaos", summary="混沌回执演示说明（经代理 8081 触发）")
async def chaos_info() -> dict:
    """说明如何用 X-Chaos-Mode 触发三类“收下日志后使坏”的故障。

    混沌端是原始 TCP 服务（上游容器内 8001 端口），由 compose 中的
    proxy-chaos 服务（宿主机 8081）代理。触发方式：

    ```bash
    curl -i -X POST http://localhost:8081/events \
         -H 'Content-Type: application/json' \
         -H 'X-Chaos-Mode: bad-receipt' \
         -d '{"event":"chaos-demo"}'
    ```

    之后用 `GET /events` 对照实收条数：每个模式只应新增 **1** 条
    `via=raw-chaos` 记录（代理若重发会变成 2 条）。
    """
    return {
        "chaos_proxy": "http://localhost:8081/events",
        "header": "X-Chaos-Mode",
        "available_modes": list(CHAOS_MODES),
        "modes": {
            "bad-receipt": "收下并落库后返回 Content-Length 不符的残缺回执 → 代理应 502 且不重发",
            "close-after-receipt": "收下并落库后返回完整 202 回执带 Connection: close → 代理应原样上交 202 与正文，仅连接不复用",
            "reset-after-receipt": "收下并落库后直接 RST 不回任何字节 → 代理应 502 且不重发",
        },
        "evidence": "GET /events 中 via=raw-chaos 的记录数即混沌端实收条数",
    }
