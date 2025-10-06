"""
Базовый абстрактный класс для бэкендов xAPI хранилищ.

"""

from abc import ABC, abstractmethod
from typing import Any


class LRSBackendBase(ABC):
    """
    Абстрактный базовый класс для реализации бэкендов xAPI LRS.

    Определяет обязательные методы для обработки ответов хранилища.
    """

    @abstractmethod
    def request_unauthorised(self, response_data: Any) -> bool:
        """
        Проверяет наличие ошибки авторизации в ответе.

        Args:
            response_data: Данные ответа от LRS

        Returns:
            True если обнаружена ошибка авторизации
        """

    @abstractmethod
    def response_has_errors(self, response_data: Any) -> bool:
        """
        Проверяет общее наличие ошибок в ответе.

        Args:
            response_data: Данные ответа от LRS

        Returns:
            True если ответ содержит ошибки
        """

    @abstractmethod
    def response_has_storage_errors(self, response_data: Any) -> bool:
        """
        Проверяет ошибки хранения данных.

        Args:
            response_data: Данные ответа от LRS

        Returns:
            True если есть ошибки сохранения данных
        """

    @abstractmethod
    def parse_error_response_for_bad_statement(self, response_data: Any) -> int:
        """
        Идентифицирует индекс некорректного высказывания.

        Args:
            response_data: Данные ответа с ошибкой

        Returns:
            Индекс проблемного высказывания

        Raises:
            XAPIBridgeLRSBackendResponseParseError: Ошибка парсинга ответа
        """

    def is_not_found(self, status_code: int, response_data: Any) -> bool:
        """Возвращает True, если ответ указывает на 404 Not Found.

        По умолчанию проверяем только HTTP-статус.
        Реализации могут переопределить метод для тонкой логики.
        """
        return status_code == 404