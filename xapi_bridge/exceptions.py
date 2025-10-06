"""
Пользовательские исключения для xAPI-бриджа.

"""

import logging
import os
import time
from typing import Any, Dict, Optional
from sentry_sdk import capture_exception, capture_message, configure_scope



logger = logging.getLogger(__name__)


class XAPIBridgeBaseException(Exception):
    """Базовое исключение для всех ошибок xAPI-бриджа."""

    def __init__(self, message: str, context: Optional[Dict] = None):
        """
        Args:
            message: Человекочитаемое описание ошибки
            context: Дополнительный контекст для отладки
        """
        self.message = message
        self.context = context or {}
        super().__init__(self.message)

    def log_error(self) -> None:
        """Логирование ошибки с прикреплением контекста."""
        logger.error(
            f"{self.__class__.__name__}: {self.message}",
            extra=self.context
        )
        self._capture_sentry()

    def _capture_sentry(self) -> None:
        """Отправка информации в Sentry с дополнительным контекстом."""
        with configure_scope() as scope:
            for key, value in self.context.items():
                scope.set_extra(key, value)
            capture_exception(self)


class XAPIBridgeConfigError(XAPIBridgeBaseException):
    """Ошибка конфигурации приложения."""


class XAPIBridgeConnectionError(XAPIBridgeBaseException):
    """Ошибка подключения к внешним сервисам."""

    def __init__(self, service_name: str, context: Optional[Dict] = None):
        message = f"Ошибка подключения к {service_name}"
        super().__init__(message, context)


class XAPIBridgeLRSConnectionError(XAPIBridgeConnectionError):
    """Ошибка взаимодействия с LRS."""

    def __init__(self, endpoint: str, status_code: Optional[int] = None):
        context = {
            'endpoint': endpoint,
            'status_code': status_code
        }
        message = f"Ошибка связи с LRS ({endpoint}), код: {status_code}"
        super().__init__(service_name="LRS", context=context)
        self.message = message  # Переопределяем сообщение, если нужно
    
    def err_fail(self) -> None:
        """Обработка после исчерпания попыток повторной отправки."""
        self.log_error()
        # Эскалируем как критическую ошибку, чтобы корректно завершить поток/процесс
        raise XAPIBridgeCriticalError(self.message)


class XAPIBridgeDataError(XAPIBridgeBaseException):
    """Ошибка обработки данных."""


class XAPIBridgeDataError(XAPIBridgeBaseException):
    """Ошибка обработки данных."""


class XAPIBridgeCourseNotFoundError(XAPIBridgeBaseException):
    """Исключение при отсутствии курса в LMS."""

    def __init__(self, message: str, course_id: str = None):
        context = {'course_id': course_id} if course_id else {}
        super().__init__(message=message, context=context)



class XAPIBridgeUserNotFoundError(XAPIBridgeBaseException):
    """Exception class for no LMS user found."""

    def __init__(self, raw_event: Dict, username: str):
        super().__init__(
            message=f'Пользователь {username} не найден',
            context={'raw_event': raw_event, 'username': username}
        )

class XAPIBridgeStatementError(XAPIBridgeDataError):
    """Ошибка преобразования или валидации xAPI-высказывания."""

    def __init__(self, raw_event: Dict, validation_errors: Dict, statement: Optional[Any] = None):
        context = {
            'raw_event': raw_event,
            'validation_errors': validation_errors,
            'statement': statement
        }
        super().__init__(
            message=f"Некорректное xAPI-высказывание: {validation_errors}",
            context=context
        )
        # Храним проблемное высказывание для его удаления из батча на верхнем уровне
        self.statement = statement


class XAPIBridgeStatementConversionError(XAPIBridgeDataError):
    """Ошибка преобразования данных трекинга."""

    def __init__(self, event_type: str, event_data: Dict, reason: str):
        context = {
            'event_type': event_type,
            'event_data': event_data
        }
        super().__init__(
            message=f"Ошибка преобразования события: {event_type} по причине {reason}",
            context=context
        )


class XAPIBridgeSkippedConversion(XAPIBridgeBaseException):
    """Событие было пропущено согласно бизнес-логике."""

    def __init__(self, event_type: str, reason: str):
        super().__init__(
            message=f"Событие {event_type} пропущено: {reason}",
            context={'event_type': event_type, 'reason': reason}
        )


class XAPIBridgeCriticalError(XAPIBridgeBaseException):
    """Критическая ошибка, требующая остановки приложения."""

    def terminate(self) -> None:
        """Безопасное завершение работы приложения."""
        self.log_error()
        logger.critical("Экстренное завершение работы")
        time.sleep(1)  # Даем время для отправки логов
        os._exit(os.EX_UNAVAILABLE)


class XAPIBridgeDeferredRetry(XAPIBridgeBaseException):
    """Отложенная повторная отправка (например, при 404 от LRS)."""

    def __init__(self, reason: str, statements_count: int):
        super().__init__(
            message=f"Отложена повторная отправка {statements_count} высказываний: {reason}",
            context={'reason': reason, 'statements_count': statements_count}
        )