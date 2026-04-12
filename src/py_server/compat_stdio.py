"""Совместимый stdio transport для MCP.

Поддерживает оба варианта framing:
- newline-delimited JSON (как в текущем mcp Python SDK)
- Content-Length framed JSON-RPC (как у части клиентов)
"""

import sys
from contextlib import asynccontextmanager

import anyio
import anyio.lowlevel
import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp.shared.message import SessionMessage


async def _read_exactly(stream: anyio.AsyncFile[bytes], size: int) -> bytes:
	"""Считать ровно size байт или завершиться с EOFError."""
	buffer = bytearray()
	while len(buffer) < size:
		chunk = await stream.read(size - len(buffer))
		if not chunk:
			raise EOFError("unexpected EOF while reading framed MCP message")
		buffer.extend(chunk)
	return bytes(buffer)


async def _read_message(stream: anyio.AsyncFile[bytes]) -> tuple[str, bytes] | None:
	"""Считать одно входящее сообщение и определить framing."""
	while True:
		line = await stream.readline()
		if not line:
			return None

		stripped = line.strip()
		if not stripped:
			continue

		if stripped.lower().startswith(b"content-length:"):
			headers = {}
			current = stripped

			while True:
				header_line = current.decode("ascii", errors="replace")
				if ":" not in header_line:
					raise ValueError(f"invalid MCP header line: {header_line!r}")
				name, value = header_line.split(":", 1)
				headers[name.strip().lower()] = value.strip()

				current = await stream.readline()
				if not current:
					raise EOFError("unexpected EOF while reading MCP headers")
				if current in (b"\n", b"\r\n"):
					break

			content_length = headers.get("content-length")
			if content_length is None:
				raise ValueError("missing Content-Length header")

			try:
				body_size = int(content_length)
			except ValueError as exc:
				raise ValueError(f"invalid Content-Length value: {content_length!r}") from exc

			return "content-length", await _read_exactly(stream, body_size)

		return "line", stripped


@asynccontextmanager
async def compat_stdio_server():
	"""Транспорт stdio, совместимый с несколькими MCP framing-вариантами."""
	stdin = anyio.wrap_file(sys.stdin.buffer)
	stdout = anyio.wrap_file(sys.stdout.buffer)

	read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
	read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]

	write_stream: MemoryObjectSendStream[SessionMessage]
	write_stream_reader: MemoryObjectReceiveStream[SessionMessage]

	read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
	write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

	output_framing: str | None = None
	output_framing_ready = anyio.Event()

	async def stdin_reader():
		nonlocal output_framing

		try:
			async with read_stream_writer:
				while True:
					payload = await _read_message(stdin)
					if payload is None:
						break

					framing, body = payload
					if output_framing is None:
						output_framing = framing
						output_framing_ready.set()

					try:
						message = types.JSONRPCMessage.model_validate_json(body)
					except Exception as exc:
						await read_stream_writer.send(exc)
						continue

					await read_stream_writer.send(SessionMessage(message))
		except anyio.ClosedResourceError:  # pragma: no cover
			await anyio.lowlevel.checkpoint()

	async def stdout_writer():
		try:
			async with write_stream_reader:
				async for session_message in write_stream_reader:
					if output_framing is None:
						await output_framing_ready.wait()

					body = session_message.message.model_dump_json(
						by_alias=True,
						exclude_none=True
					).encode("utf-8")

					if output_framing == "content-length":
						header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
						await stdout.write(header + body)
					else:
						await stdout.write(body + b"\n")

					await stdout.flush()
		except anyio.ClosedResourceError:  # pragma: no cover
			await anyio.lowlevel.checkpoint()

	async with anyio.create_task_group() as tg:
		tg.start_soon(stdin_reader)
		tg.start_soon(stdout_writer)
		yield read_stream, write_stream
