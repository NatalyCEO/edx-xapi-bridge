"""
Настройки для xAPI-бриджа между Open edX и LRS.

Создайте локальную копию этого файла как settings.py и настройте под ваше окружение.
"""

import os
from typing import Any, List, Optional

# =============================================
#  Настройки подключения к LRS
# =============================================

# URL конечной точки LRS
LRS_ENDPOINT: str = 'https://lrs.example.org/xapi/'

# Авторизация: Basic Auth или хеш
LRS_USERNAME: Optional[str] = 'your_username'
LRS_PASSWORD: Optional[str] = 'your_password'
LRS_BASICAUTH_HASH: Optional[str] = None  # Пример: 'base64_encoded_credentials'

# Authority sign for every LRS-data request
ORG_NAME = "test_org"
ORG_EMAIL = "test@test.com"

# Тип бэкенда LRS (например: 'learninglocker')
LRS_BACKEND_TYPE: str = 'learninglocker'

# =============================================
#  Настройки Open edX
# =============================================

# Базовый URL платформы Open edX
OPENEDX_PLATFORM_URI: str = 'https://openedx.example.org'

# OAuth2 клиент для доступа к API
OPENEDX_OAUTH2_CLIENT_ID: str = 'your_client_id'
OPENEDX_OAUTH2_CLIENT_SECRET: str = 'your_client_secret'

# API Endpoints
OPENEDX_USER_API_URI: str = f'{OPENEDX_PLATFORM_URI}/api/user/v1/'
OPENEDX_ENROLLMENT_API_URI: str = f'{OPENEDX_PLATFORM_URI}/api/enrollment/v1/'

# =============================================
#  Параметры публикации событий
# =============================================

# Максимальное время ожидания перед отправкой (сек)
PUBLISH_MAX_WAIT_TIME: int = 60

# Максимальный размер пакета для отправки
PUBLISH_MAX_PAYLOAD: int = 50

# Максимальное количество попыток отправки
PUBLISH_MAX_RETRIES: int = 3

# Задержка перед повторной отправкой (в секундах)
RETRY_DELAY_SECONDS: int = 0

# Путь к файлу очереди отложенных повторов
RETRY_QUEUE_FILE: Optional[str] = None  # Если None, используется ~/.xapi_bridge_retry.jsonl

# Период фоновой проверки очереди (в секундах)
RETRY_WORKER_INTERVAL_SECONDS: int = 300

# Если указан путь, повторно отправленные пакеты будут сохраняться в этот файл (JSONL)
RETRY_SINK_FILE: Optional[str] = None

# Ежедневный запуск ретраев в формате HH:MM (локальное время). Пример: '03:00'.
# Если задано, воркер будет просыпаться в это время и обрабатывать очередь
# (сначала 1 запись, если успех — затем остальные; при неуспехе — до следующего дня).
RETRY_DAILY_AT: Optional[str] = '03:00'

# =============================================
#  Настройки кэширования
# =============================================

# Использовать Memcached для кэширования
LMS_API_USE_MEMCACHED: bool = False
MEMCACHED_ADDRESS: str = '127.0.0.1:11211'

# =============================================
#  Мониторинг и логирование
# =============================================

# Путь к файлу логов
LOG_FILE: str = '/var/log/xapi-bridge/xapi-bridge.log'

# Уровень логирования (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL: str = 'INFO'

# Sentry DSN для мониторинга ошибок
SENTRY_DSN: Optional[str] = None  # Пример: 'https://key@sentry.example.org/1'

# HTTP-сервер для проверки статуса
HTTP_PUBLISH_STATUS: bool = True
HTTP_PUBLISH_IP: str = '0.0.0.0'
HTTP_PUBLISH_PORT: int = 9090

# =============================================
#  Интеграция с University 2035
# =============================================

UNTI_XAPI: bool = False
UNTI_XAPI_EXT_URL: str = 'https://my.2035.university/xapi-extensions'

# =============================================
#  Тестовые настройки
# =============================================

# Режим отладки (повышает уровень логирования)
DEBUG_MODE: bool = False

# Игнорируемые типы событий
IGNORED_EVENT_TYPES: List[str] = [
    'edx.ui.lms.link_clicked',
    'edx.ui.lms.outline.selected'
]

# Настройки нагрузочного тестирования
TEST_LOAD_SUCCESSFUL_STATEMENTS_BENCHMARK: int = 0

# =============================================
#  Продвинутые настройки
# =============================================

# Настройки inotify для отслеживания файлов
NOTIFIER_READ_FREQ: int = 2    # Частота проверки файла (сек)
NOTIFIER_POLL_TIMEOUT: int = 1000  # Таймаут опроса (мс)

# Принудительное завершение при ошибках (для отладки)
EXCEPTIONS_NO_CONTINUE: bool = False

# Переопределение настроек через переменные окружения
def get_env_setting(setting: str, default: Any = None) -> Any:
    return os.environ.get(f'XAPI_{setting}', globals().get(setting, default))

# Пример использования:
# LRS_ENDPOINT = get_env_setting('LRS_ENDPOINT', 'https://backup-lrs.example.org/xapi/')
