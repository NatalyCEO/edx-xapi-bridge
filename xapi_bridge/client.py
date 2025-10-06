"""
Клиент для отправки xAPI-высказываний в LRS.

"""

import importlib
import json
import logging
import socket
from typing import Any, Dict, Optional

from tincan import RemoteLRS, StatementList
from tincan.lrs_response import LRSResponse

import xapi_bridge.exceptions as exceptions
from xapi_bridge import settings
from xapi_bridge.lrs_backends.learninglocker import LRSBackend
from xapi_bridge.retry_queue import RetryQueue


logger = logging.getLogger(__name__)


class XAPIBridgeLRSPublisher:
    """Обертка для отправки xAPI-высказываний в LRS."""

    def __init__(self):
        self.lrs = self._configure_lrs()
        self.backend = LRSBackend()
        self.retry_queue = RetryQueue()

    def _configure_lrs(self) -> RemoteLRS:
        """Конфигурация подключения к LRS."""
        config = {
            'endpoint': settings.LRS_ENDPOINT,
        }

        if settings.LRS_BASICAUTH_HASH:
            config['auth'] = f"Basic {settings.LRS_BASICAUTH_HASH}"
        elif settings.LRS_USERNAME and settings.LRS_PASSWORD:
            config.update({
                'username': settings.LRS_USERNAME,
                'password': settings.LRS_PASSWORD
            })

        return RemoteLRS(**config)

    def publish_statements(self, statements: StatementList) -> LRSResponse:
        """
        Отправка пакета высказываний в LRS.

        Args:
            statements: Список xAPI-высказываний

        Returns:
            LRSResponse: Ответ от LRS

        Raises:
            XAPIBridgeLRSConnectionError: Ошибка подключения
            XAPIBridgeStatementError: Ошибка сохранения
        """
        try:
            logger.debug(f"Отправляем {len(statements)} высказываний в LRS")
            logger.debug(f"Тип statements: {type(statements)}")

            # Преобразуем StatementList в список словарей для корректной сериализации
            statement_dicts = []
            for i, stmt in enumerate(statements):
                try:
                    # Проверяем, что объект является Statement или имеет необходимые методы
                    if hasattr(stmt, 'as_version'):
                        statement_dicts.append(stmt.as_version('1.0.3'))
                    elif hasattr(stmt, 'to_dict'):
                        statement_dicts.append(stmt.to_dict())
                    elif hasattr(stmt, '__dict__'):
                        statement_dicts.append(stmt.__dict__)
                    elif isinstance(stmt, dict):
                        # Если это уже словарь, используем его напрямую
                        statement_dicts.append(stmt)
                    else:
                        logger.error(f"Не удалось сериализовать Statement[{i}]: {type(stmt)} - объект не имеет необходимых методов")
                        continue
                except Exception as e:
                    logger.warning(f"Ошибка при преобразовании Statement[{i}] в словарь: {e}")
                    # Попробуем альтернативный способ сериализации
                    try:
                        if hasattr(stmt, 'to_dict'):
                            statement_dicts.append(stmt.to_dict())
                        elif hasattr(stmt, '__dict__'):
                            statement_dicts.append(stmt.__dict__)
                        elif isinstance(stmt, dict):
                            statement_dicts.append(stmt)
                        else:
                            logger.error(f"Не удалось сериализовать Statement[{i}]: {type(stmt)}")
                            continue
                    except Exception as e2:
                        logger.error(f"Не удалось сериализовать Statement[{i}] альтернативным способом: {e2}")
                        continue

            if not statement_dicts:
                logger.warning("Нет валидных statements для отправки")
                return LRSResponse(success=True, data="No statements to send")

            logger.debug(f"Преобразовано в {len(statement_dicts)} словарей")

            # Сериализуем список словарей в JSON-строку
            # json_data = json.dumps(statement_dicts, ensure_ascii=False)
            # logger.debug(f"Сериализовано в JSON строку размером {len(json_data)} символов")

            response = self.lrs.save_statements(statement_dicts)
            self._handle_response(response, statements)
            return response
        except exceptions.XAPIBridgeStatementError:
            # Пробрасываем дальше, чтобы верхний уровень мог удалить проблемное высказывание
            raise
        except (socket.gaierror, ConnectionRefusedError) as e:
            error_msg = f"Ошибка подключения к LRS: {str(e)}"
            logger.error(error_msg)
            raise exceptions.XAPIBridgeLRSConnectionError(
                endpoint=settings.LRS_ENDPOINT,
                status_code=None
            ) from e
        except Exception as e:
            error_msg = f"Неожиданная ошибка при отправке в LRS: {str(e)}"
            logger.error(error_msg)
            logger.error(f"Тип исключения: {type(e).__name__}")
            raise exceptions.XAPIBridgeLRSConnectionError(
                endpoint=settings.LRS_ENDPOINT,
                status_code=None
            ) from e

    def _handle_response(self, response: LRSResponse, statements: StatementList) -> None:
        """Обработка ответа от LRS."""
        if response.success:
            logger.info(f"Успешно отправлено {len(statements)} высказываний")
            logger.debug(f"Успешно отправленные: {response.request.content}")
            return

        try:
            if isinstance(response.data, str):
                response_data = json.loads(response.data)
            else:
                response_data = response.data if response.data else {}
        except json.JSONDecodeError:
            response_data = {}

        logger.debug(f"Success {response.success}")

        logger.debug(response.request.headers)
        logger.debug(response.request.method)
        logger.debug(response.request.resource)
        logger.debug(response.request.ignore404)
        logger.debug(response.request.query_params)
        logger.debug(response.request.content)

        logger.debug(response.response.status)
        logger.debug(response.response.reason)
        logger.debug(response.response.read())
        logger.debug(response.content)

        # Преобразуем response_data в строку для методов бэкенда
        response_data_str = json.dumps(response_data) if isinstance(response_data, dict) else str(response_data)
        logger.debug(response_data_str)

        if self.backend.request_unauthorised(response_data_str):
            error_msg = "Ошибка авторизации в LRS"
            raise exceptions.XAPIBridgeLRSConnectionError(
                endpoint=settings.LRS_ENDPOINT,
                status_code=None
            )

        if self.backend.response_has_storage_errors(response_data_str):
            bad_index = self.backend.parse_error_response_for_bad_statement(response_data_str)
            bad_statement = statements[bad_index] if bad_index is not None else None
            error_msg = (f"Ошибка сохранения высказывания: {response_data.get('message', '')} ",
                        f"- {response_data_str} - {response.request.content}")
            # Создаем словарь с данными ошибки
            error_data = {
                'response_data': response_data,
                'bad_index': bad_index,
                'bad_statement': str(bad_statement) if bad_statement else None
            }
            raise exceptions.XAPIBridgeStatementError(
                raw_event=error_data,
                validation_errors={'message': error_msg},
                statement=bad_statement
            )

        # Если 404 — откладываем на повтор
        status_code = getattr(response.response, 'status', None)
        if self.backend.is_not_found(status_code, response_data_str):
            # сохраняем JSON контент запроса без повторной сериализации
            content = response.request.content
            if isinstance(content, (bytes, bytearray)):
                content = content.decode('utf-8', errors='ignore')
            try:
                self.retry_queue.enqueue(content)
            except Exception:
                logger.exception("Не удалось добавить высказывания в очередь отложенной отправки")
            # В любом случае сигнализируем об отложенной отправке, чтобы верхний уровень не считал как успех
            raise exceptions.XAPIBridgeDeferredRetry(
                reason=f"HTTP {status_code}",
                statements_count=len(statements)
            )

        error_msg = (f"Неопознанная ошибка отправки в LRS: {status_code} -"
                    + f" {response_data_str}. Высказывания: {response.request.content}")
        logger.error(error_msg)

# Инициализация клиента по умолчанию
lrs_publisher = XAPIBridgeLRSPublisher()

