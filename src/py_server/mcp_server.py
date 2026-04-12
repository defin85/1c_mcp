"""Основной MCP-сервер, который проксирует запросы в 1С."""

import logging
import contextvars
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, AsyncIterator, Tuple

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.lowlevel import NotificationOptions
from mcp import types

from .onec_client import OneCClient, OneCUpstreamError
from .config import Config


logger = logging.getLogger(__name__)

# Context var для per-session креденшилов 1С (login, password)
current_onec_credentials: contextvars.ContextVar[Optional[Tuple[str, str]]] = contextvars.ContextVar(
	'current_onec_credentials',
	default=None
)


class MCPProxy:
	"""MCP-прокси сервер для взаимодействия с 1С."""

	def __init__(self, config: Config):
		"""Инициализация прокси.

		Args:
			config: Конфигурация сервера
		"""
		self.config = config
		self.onec_client: Optional[OneCClient] = None
		self.upstream_ready = False

		# Создаем MCP сервер
		self.server = Server(
			name=config.server_name,
			lifespan=self._lifespan
		)

		# Регистрируем обработчики
		self._register_handlers()

	@asynccontextmanager
	async def _lifespan(self, server: Server) -> AsyncIterator[Dict[str, Any]]:
		"""Управление жизненным циклом сервера."""
		logger.debug(f"Инициализация MCP сервера '{self.config.server_name}' v{self.config.server_version}")

		# Определяем креденшилы для текущей сессии
		# При auth_mode=oauth2 берём из context var (per-session), иначе из конфигурации
		session_creds = None
		if self.config.auth_mode == "oauth2":
			session_creds = current_onec_credentials.get()
			if not session_creds:
				logger.error("Отсутствуют сессионные креденшилы OAuth2. Требуется авторизация.")
				raise PermissionError("authorization required")
			username, password = session_creds
			logger.debug(f"Использую per-session креденшилы для пользователя: {username}")
		else:
			# Режим none - используем дефолтные креды
			username = self.config.onec_username
			password = self.config.onec_password
			logger.debug(f"Режим auth_mode=none, использую дефолтные креденшилы: {username}")

		# Инициализация при запуске
		self.onec_client = OneCClient(
			base_url=self.config.onec_url,
			username=username,
			password=password,
			service_root=self.config.onec_service_root
		)

		logger.debug(f"Подключение к 1С: {self.config.onec_url}")
		logger.debug(f"HTTP-сервис: {self.config.onec_service_root}")

		try:
			try:
				await self.onec_client.check_health(
					attempts=self.config.startup_healthcheck_attempts,
					retry_delay_sec=self.config.startup_retry_delay_sec
				)
				self.upstream_ready = True
				logger.info("Успешное подключение к 1С при старте MCP-прокси")
			except OneCUpstreamError as error:
				self.upstream_ready = False
				if not self.config.startup_allow_degraded:
					raise

				logger.warning(
					"1С недоступна при старте MCP-прокси. Запуск продолжается в деградированном режиме: %s",
					error
				)

			logger.debug("MCP сервер готов к работе")
			yield {
				"onec_client": self.onec_client,
				"upstream_ready": self.upstream_ready,
			}
		finally:
			# Очистка при завершении
			if self.onec_client:
				await self.onec_client.close()
				logger.debug("Соединение с 1С закрыто")

	async def _recover_upstream(self, onec_client: OneCClient, reason: Exception) -> bool:
		"""Попробовать восстановить доступ к upstream 1С после временной ошибки."""
		logger.warning("Временная ошибка upstream 1С: %s", reason)

		try:
			await onec_client.check_health(
				attempts=self.config.recovery_healthcheck_attempts,
				retry_delay_sec=self.config.recovery_retry_delay_sec
			)
			self.upstream_ready = True
			logger.info("Соединение с upstream 1С восстановлено")
			return True
		except OneCUpstreamError as error:
			self.upstream_ready = False
			logger.error("Не удалось восстановить доступ к upstream 1С: %s", error)
			return False

	async def _run_with_upstream_recovery(self, operation_name: str, operation, fallback):
		"""Выполнить операцию и попытаться восстановить upstream при транспортной ошибке."""
		ctx = self.server.request_context
		onec_client: OneCClient = ctx.lifespan_context["onec_client"]

		try:
			return await operation(onec_client)
		except OneCUpstreamError as error:
			recovered = await self._recover_upstream(onec_client, error)
			if recovered:
				try:
					return await operation(onec_client)
				except Exception as retry_error:
					logger.error("%s не выполнена после восстановления upstream: %s", operation_name, retry_error)
					return fallback(retry_error)

			logger.error("%s не выполнена: upstream 1С недоступен", operation_name)
			return fallback(error)
		except Exception as error:
			logger.error("%s не выполнена: %s", operation_name, error)
			return fallback(error)

	def _register_handlers(self):
		"""Регистрация обработчиков MCP."""

		@self.server.list_tools()
		async def handle_list_tools() -> List[types.Tool]:
			"""Получить список доступных инструментов."""
			async def operation(onec_client: OneCClient) -> List[types.Tool]:
				tools = await onec_client.list_tools()
				logger.debug(f"Получено инструментов: {len(tools)}")
				return tools

			return await self._run_with_upstream_recovery(
				"Получение списка инструментов",
				operation,
				lambda error: []
			)

		@self.server.call_tool()
		async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:
			"""Вызвать инструмент."""
			async def operation(onec_client: OneCClient) -> List[types.TextContent]:
				logger.debug(f"Вызов инструмента: {name} с аргументами: {arguments}")
				result = await onec_client.call_tool(name, arguments)

				if result.isError:
					logger.error(f"Ошибка выполнения инструмента {name}")

				return result.content

			return await self._run_with_upstream_recovery(
				f"Вызов инструмента {name}",
				operation,
				lambda error: [types.TextContent(
					type="text",
					text=f"Ошибка выполнения инструмента: {str(error)}"
				)]
			)

		@self.server.list_resources()
		async def handle_list_resources() -> List[types.Resource]:
			"""Получить список доступных ресурсов."""
			async def operation(onec_client: OneCClient) -> List[types.Resource]:
				resources = await onec_client.list_resources()
				logger.debug(f"Получено ресурсов: {len(resources)}")
				return resources

			return await self._run_with_upstream_recovery(
				"Получение списка ресурсов",
				operation,
				lambda error: []
			)

		@self.server.read_resource()
		async def handle_read_resource(uri: str) -> types.ReadResourceResult:
			"""Прочитать ресурс."""
			async def operation(onec_client: OneCClient) -> types.ReadResourceResult:
				logger.debug(f"Чтение ресурса: {uri}")
				result = await onec_client.read_resource(uri)
				return result

			return await self._run_with_upstream_recovery(
				f"Чтение ресурса {uri}",
				operation,
				lambda error: types.ReadResourceResult(
					contents=[
						types.TextResourceContents(
							uri=str(uri),
							mimeType="text/plain",
							text=f"Ошибка чтения ресурса: {str(error)}"
						)
					]
				)
			)

		@self.server.list_prompts()
		async def handle_list_prompts() -> List[types.Prompt]:
			"""Получить список доступных промптов."""
			async def operation(onec_client: OneCClient) -> List[types.Prompt]:
				prompts = await onec_client.list_prompts()
				logger.debug(f"Получено промптов: {len(prompts)}")
				return prompts

			return await self._run_with_upstream_recovery(
				"Получение списка промптов",
				operation,
				lambda error: []
			)

		@self.server.get_prompt()
		async def handle_get_prompt(name: str, arguments: Optional[Dict[str, str]] = None) -> types.GetPromptResult:
			"""Получить промпт."""
			async def operation(onec_client: OneCClient) -> types.GetPromptResult:
				logger.debug(f"Получение промпта: {name} с аргументами: {arguments}")
				result = await onec_client.get_prompt(name, arguments)
				return result

			return await self._run_with_upstream_recovery(
				f"Получение промпта {name}",
				operation,
				lambda error: types.GetPromptResult(
					description=f"Ошибка получения промпта: {str(error)}",
					messages=[]
				)
			)

	def get_capabilities(self) -> Dict[str, Any]:
		"""Получить capabilities сервера."""
		return {
			"tools": {
				"listChanged": True
			},
			"resources": {
				"subscribe": True,
				"listChanged": True
			},
			"prompts": {
				"listChanged": True
			},
			"logging": {}
		}

	def get_initialization_options(self) -> InitializationOptions:
		"""Получить опции инициализации."""
		return InitializationOptions(
			server_name=self.config.server_name,
			server_version=self.config.server_version,
			capabilities=self.server.get_capabilities(
				notification_options=NotificationOptions(
					tools_changed=True,
					resources_changed=True,
					prompts_changed=True
				),
				experimental_capabilities={}
			)
		)
