"""
Реализация бэкенда Learning Locker для xAPI.

"""

import json
import re
from typing import Any, Dict, Optional

from .base import LRSBackendBase
from xapi_bridge import exceptions


class LRSBackend(LRSBackendBase):
    """Реализация взаимодействия с Learning Locker xAPI LRS."""

    def parse_error_response_for_bad_statement(self, response_data: str) -> Optional[int]:
        """
        Анализирует ответ LRS для определения индекса некорректного высказывания.

        Args:
            response_data: Ответ от LRS в виде строки JSON

        Returns:
            Индекс проблемного высказывания или None

        Raises:
            XAPIBridgeLRSBackendResponseParseError: Ошибка парсинга ответа
        """
        try:
            error = json.loads(response_data)
            warnings = error.get('warnings', [])

            if not warnings:
                return None

            # Пример сообщения: "Problem in 'statements.0.actor'..."
            problem_msg = warnings[0]
            match = re.search(r"'statements\.(\d+)", problem_msg)

            return int(match.group(1)) if match else None

        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise exceptions.XAPIBridgeLRSBackendResponseParseError(
                f"Ошибка парсинга ответа LRS: {str(exc)}"
            ) from exc

    def response_has_errors(self, response_data: str) -> bool:
        """Проверяет наличие ошибок в ответе LRS."""
        try:
            data = json.loads(response_data)
            return 'errorId' in data
        except json.JSONDecodeError:
            return False

    def request_unauthorised(self, response_data: str) -> bool:
        """Проверяет статус авторизации."""
        try:
            data = json.loads(response_data)
            return data.get('message') == 'Unauthorised'
        except json.JSONDecodeError:
            return False

    def response_has_storage_errors(self, response_data: str) -> bool:
        """Проверяет наличие ошибок хранения данных."""
        try:
            data = json.loads(response_data)
            return 'warnings' in data
        except json.JSONDecodeError:
            return False

    def is_not_found(self, status_code: int, response_data: Any) -> bool:
        # Для Learning Locker достаточно статуса 404
        return status_code == 404