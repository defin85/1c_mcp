"""Stdio сервер для MCP."""

import logging

from .compat_stdio import compat_stdio_server
from .mcp_server import MCPProxy
from .config import Config


logger = logging.getLogger(__name__)


async def run_stdio_server(config: Config):
	"""Запуск stdio сервера.

	Args:
		config: Конфигурация сервера
	"""
	logger.info("Запуск MCP сервера в режиме stdio")

	# Создаем прокси
	mcp_proxy = MCPProxy(config)

	try:
		# Запускаем сервер через stdio
		async with compat_stdio_server() as (read_stream, write_stream):
			await mcp_proxy.server.run(
				read_stream,
				write_stream,
				mcp_proxy.get_initialization_options()
			)
	except Exception as e:
		logger.error(f"Ошибка в stdio сервере: {e}")
		raise
