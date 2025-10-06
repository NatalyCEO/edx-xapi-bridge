"""
Инициализация пакета xapi_bridge.
"""

__version__ = "0.1"

# Экспорт настроек как атрибута пакета для удобного импорта
try:
    from . import settings  # type: ignore
except Exception:
    # В некоторых сценариях настройки могут отсутствовать до конфигурации
    settings = None  # type: ignore
