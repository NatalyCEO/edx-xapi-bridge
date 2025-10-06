"""
Основной модуль для обработки событий трекинга Open edX и отправки xAPI-высказываний.

"""
import argparse
import gzip
import json
import logging
import os
import re
import signal
import socketserver
import sys
import threading
import time

from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import FrameType
from typing import Any, Dict, Optional

from pyinotify import WatchManager, Notifier, NotifierError, EventsCodes, ProcessEvent
from tincan import StatementList

from xapi_bridge import client, converter, exceptions, settings
from xapi_bridge.retry_queue import RetryQueueWorker
from xapi_bridge.constants import OPENEDX_OAUTH2_TOKEN_URL
from xapi_bridge.historical_processor import process_historical_logs

if settings.HTTP_PUBLISH_STATUS:
    from xapi_bridge.server import httpd


logger = logging.getLogger('edX-xapi-bridge main')


class QueueManager:
    """Управление очередью и пакетной отправкой xAPI-высказываний."""

    def __init__(self):
        self.cache: list = []
        self.cache_lock = threading.Lock()
        self.publish_timer: Optional[threading.Timer] = None
        self.publish_retries = 0
        self.total_published = 0

    def __del__(self):
        self.destroy()

    def destroy(self) -> None:
        """Очистка ресурсов."""
        if self.publish_timer:
            self.publish_timer.cancel()

    def push(self, stmt) -> None:
        """Добавление высказывания в очередь."""
        with self.cache_lock:
            self.cache.append(stmt)

        if len(self.cache) == 1 and settings.PUBLISH_MAX_WAIT_TIME > 0:
            self.publish_timer = threading.Timer(settings.PUBLISH_MAX_WAIT_TIME, self.publish)
            self.publish_timer.start()

        if len(self.cache) >= settings.PUBLISH_MAX_PAYLOAD:
            self.publish()

    def publish(self) -> None:
        """Отправка высказываний в LRS."""
        with self.cache_lock:
            if not self.cache:
                return

            # Проверяем типы объектов в кэше перед созданием StatementList
            invalid_statements = []
            for i, stmt in enumerate(self.cache):
                if not hasattr(stmt, 'as_version') and not hasattr(stmt, 'to_dict') and not hasattr(stmt, '__dict__') and not isinstance(stmt, dict):
                    invalid_statements.append((i, type(stmt).__name__))
            
            if invalid_statements:
                logger.error(f"Обнаружены невалидные statements в кэше: {invalid_statements}")
                # Удаляем невалидные statements из кэша
                valid_cache = [stmt for stmt in self.cache if hasattr(stmt, 'as_version') or hasattr(stmt, 'to_dict') or hasattr(stmt, '__dict__') or isinstance(stmt, dict)]
                if not valid_cache:
                    logger.warning("Нет валидных statements в кэше, пропускаем отправку")
                    self.cache.clear()
                    return
                self.cache = valid_cache

            # Создаем StatementList из объектов Statement (преобразование в словари происходит в клиенте)
            logger.debug(f"Создаем StatementList из {len(self.cache)} высказываний")
            try:
                statements = StatementList(self.cache)
            except Exception as e:
                logger.error(f"Ошибка при создании StatementList: {e}")
                logger.error(f"Типы объектов в кэше: {[type(stmt).__name__ for stmt in self.cache]}")
                raise

            while statements:
                try:
                    client.lrs_publisher.publish_statements(statements)
                    self.total_published += len(statements)
                    logger.info(f"Отправлено {self.total_published} высказываний")
                    self._check_benchmark()
                    break
                except exceptions.XAPIBridgeLRSConnectionError as e:
                    self._handle_connection_error(e, statements)
                except exceptions.XAPIBridgeStatementError as e:
                    self._handle_storage_error(e, statements)
                except exceptions.XAPIBridgeDeferredRetry as e:
                    # Пакет отложен, выходим из цикла без инкремента счетчика
                    logger.warning(e.message)
                    break
                except Exception as e:
                    # Обработка всех остальных непредвиденных ошибок
                    logger.error(f"Произошла непредвиденная ошибка во время публикации высказываний: {e}")
                    for statement in statements:
                        logger.error(f"Неотправленное высказывание: {statement}")

            self.cache.clear()
            if self.publish_timer:
                self.publish_timer.cancel()

    def _check_benchmark(self) -> None:
        """Проверка достижения тестового показателя."""
        if settings.TEST_LOAD_SUCCESSFUL_STATEMENTS_BENCHMARK > 0:
            if self.total_published >= settings.TEST_LOAD_SUCCESSFUL_STATEMENTS_BENCHMARK:
                logger.info(f"Достигнут показатель: {self.total_published} высказываний")

    def _handle_connection_error(self, e: exceptions.XAPIBridgeLRSConnectionError, statements: StatementList) -> None:
        """Обработка ошибок соединения."""
        if self.publish_retries >= settings.PUBLISH_MAX_RETRIES:
            e.err_fail()
        self.publish_retries += 1
        time.sleep(1)

    def _handle_storage_error(self, e: exceptions.XAPIBridgeStatementError, statements: StatementList) -> None:
        """Обработка ошибок хранения."""
        logger.warning(f"Ошибка хранения: {e.message}")
        # Удаляем проблемное высказывание из списка
        if e.statement in statements:
            statements.remove(e.statement)
        # Если это отложенная отправка — просто выходим из цикла, не считаем как отправленные
        if isinstance(e, exceptions.XAPIBridgeDeferredRetry):
            raise


class NotifierLostINodeException(NotifierError):
    """Исключение при потере отслеживаемого файла."""


class TailHandler(ProcessEvent):
    """Обработчик изменений лог-файла."""

    MASK = EventsCodes.OP_FLAGS['IN_MODIFY'] | EventsCodes.OP_FLAGS['IN_MOVE_SELF'] | EventsCodes.OP_FLAGS['IN_DELETE_SELF']

    def __init__(self, filename: str, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename
        self.ifp = open(filename, 'r', 1)
        self.ifp.seek(0, 2)
        self.publish_queue = QueueManager()
        self.race_buffer = ''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.publish_queue.publish()
        self.publish_queue.destroy()
        self.ifp.close()

    def check_NOT_DAMAGED(self, event) -> Any:
        """ Проверка на целостность полученного события
        в логе события от Оценки просмотренного события
        (с watch_times) превышают лимит и ломают формат """
        if event[-1] != '}':
            # Ищем тег event и удаляем всю его инфу, т к
            # он слишком длинный
            pattern = r'(?<="event":).*'
            event = re.sub(pattern, '""', event)
            if event[-1] != '\"':
                event = event + '\"\"}'
            else:
                event = event + '}'
        return event

    def process_IN_MODIFY(self, event) -> None:
        """Обработка изменений файла."""
        buff = self.race_buffer + self.ifp.read()

        if buff and buff[-1] != '\n':
            self.race_buffer = buff
            return

        self.race_buffer = ''
        for line in buff.split('\n'):
            if not line:
                continue
            try:
                line = self.check_NOT_DAMAGED(line)
                event = json.loads(line)
                statements = converter.to_xapi(event)
                if statements:
                    for stmt in statements:
                        self.publish_queue.push(stmt)
            except json.JSONDecodeError as e:
                logger.warning(f"Ошибка json-конвертации события: {e}. \nСтрока {line} \nСобытие {event}")
            except Exception as e:
                raise exceptions.XAPIBridgeSkippedConversion(
                    event_type=event['event_type'],
                    reason=f"Ошибка обработки события: {e}"
                ) from e

    def process_IN_MOVE_SELF(self, event) -> None:
        logger.info("Файл перемещен")
        raise NotifierLostINodeException("IN_MOVE_SELF")

    def process_IN_DELETE_SELF(self, event) -> None:
        logger.info("Файл удален")
        raise NotifierLostINodeException("IN_DELETE_SELF")


def watch(file_path: str) -> None:
    """Запуск наблюдения за файлом."""
    logger.info(f"Начало наблюдения за {file_path}")
    wm = WatchManager()

    try:
        with TailHandler(file_path) as handler:
            notifier = Notifier(wm, handler,
                read_freq=settings.NOTIFIER_READ_FREQ,
                timeout=settings.NOTIFIER_POLL_TIMEOUT)
            wm.add_watch(file_path, TailHandler.MASK)
            notifier.loop()
    except NotifierLostINodeException:
        logger.info("Перезапуск наблюдения...")
        watch(file_path)
    finally:
        logger.info("Завершение наблюдения")


def signal_handler(signum: int, frame: Optional[FrameType]) -> None:
    """Обработчик сигналов завершения."""
    logger.info("Получен сигнал завершения")
    if settings.HTTP_PUBLISH_STATUS:
        logger.info("Остановка HTTP-сервера")
        httpd.shutdown()
        httpd.server_close()
    sys.exit(0)


# Регистрация обработчиков сигналов
for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM, signal.SIGABRT):
    signal.signal(sig, signal_handler)


def setup_logging() -> None:
    """Настройка логирования."""
    level = logging.DEBUG if settings.DEBUG_MODE else logging.INFO
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(name)s]: %(message)s',
        level=level,
        handlers=[
            logging.FileHandler(settings.LOG_FILE),
            logging.StreamHandler()
        ]
    )

    if settings.SENTRY_DSN:
        try:
            import sentry_sdk
            from sentry_sdk.integrations.logging import LoggingIntegration
            sentry_sdk.init(
                dsn=settings.SENTRY_DSN,
                integrations=[LoggingIntegration()]
            )
        except ImportError:
            logger.warning("Sentry SDK не установлен")


class StatusOKRequestHandler(BaseHTTPRequestHandler):
    """Обработчик HTTP-запросов для проверки статуса."""

    def do_GET(self):
        """Обработка GET-запроса."""
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def log_message(self, format, *args):
        """Переопределение логирования HTTP-сервера."""
        logger.info("%s - %s", self.client_address[0], format % args)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='xAPI Bridge for Open edX')
    parser.add_argument('watchfile', nargs='?', help='Path to the tracking log file to watch')
    parser.add_argument('--historical-logs-dir', help='Path to directory containing historical log files')
    parser.add_argument('--historical-logs-dates', help='Date range for historical logs in format YYYYMMDD-YYYYMMDD')
    return parser.parse_args()

def process_gzipped_logs(log_dir: str, date_range: Optional[str] = None) -> None:
    """
    Process gzipped log files in the specified directory.

    Args:
        log_dir: Path to directory containing log files
        date_range: Optional date range in format YYYYMMDD-YYYYMMDD
    """
    log_dir_path = Path(log_dir)
    if not log_dir_path.exists() or not log_dir_path.is_dir():
        logger.error(f"Directory not found: {log_dir}")
        return

    # Create temporary directory in user's home directory
    home_dir = Path.home()
    temp_dir = Path(getattr(settings, 'TEMP_DIR', None) or home_dir / '.xapi_bridge_temp')
    temp_dir.mkdir(exist_ok=True)
    logger.info(f"Using temporary directory: {temp_dir}")

    try:
        # Parse date range if provided
        start_date = None
        end_date = None
        if date_range:
            try:
                start_date_str, end_date_str = date_range.split('-')
                start_date = datetime.strptime(start_date_str, '%Y%m%d')
                end_date = datetime.strptime(end_date_str, '%Y%m%d')
            except ValueError:
                logger.error("Invalid date range format. Use YYYYMMDD-YYYYMMDD")
                return

        # Find all gzipped log files
        log_files = list(log_dir_path.glob('*.gz'))
        total_processed = 0
        total_statements = 0
        file_statistics = []  # Список для хранения статистики по каждому файлу

        for log_file in sorted(log_files):
            # Extract date from filename if possible
            date_match = re.search(r'(\d{8})', log_file.name)
            if date_match and start_date and end_date:
                file_date = datetime.strptime(date_match.group(1), '%Y%m%d')
                if not (start_date <= file_date <= end_date):
                    continue

            logger.info(f"Processing file: {log_file.name}")
            try:
                # Create temporary file in temp directory
                temp_file = temp_dir / log_file.stem
                with gzip.open(log_file, 'rt') as gz_file:
                    with open(temp_file, 'w') as out_file:
                        out_file.write(gz_file.read())

                # Process the decompressed file
                statements = process_historical_logs(str(temp_file))
                if statements:
                    statements_count = len(statements)
                    total_statements += statements_count
                    file_statistics.append({
                        'file': log_file.name,
                        'statements': statements_count,
                        'date': date_match.group(1) if date_match else 'unknown'
                    })
                    logger.info(f"Generated {statements_count} statements from {log_file.name}")
                total_processed += 1

                # Clean up temporary file
                temp_file.unlink()

            except Exception as e:
                logger.error(f"Error processing {log_file.name}: {e}")
                continue

        # Вывод итоговой статистики
        logger.info("\nProcessing Summary:")
        logger.info("=" * 50)
        logger.info(f"Total files processed: {total_processed}")
        logger.info(f"Total statements generated: {total_statements}")
        logger.info("\nDetailed Statistics:")
        logger.info("-" * 50)
        for stat in sorted(file_statistics, key=lambda x: x['date']):
            logger.info(f"File: {stat['file']}")
            logger.info(f"Date: {stat['date']}")
            logger.info(f"Statements: {stat['statements']}")
            logger.info("-" * 50)

    finally:
        # Clean up temporary directory if it's empty
        try:
            if not any(temp_dir.iterdir()):
                temp_dir.rmdir()
                logger.info(f"Removed empty temporary directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Could not remove temporary directory {temp_dir}: {e}")

def main():
    """Main entry point."""
    args = parse_args()
    setup_logging()

    # Фоновый воркер для повторной отправки из очереди
    retry_worker = RetryQueueWorker()
    retry_worker.start()

    if args.historical_logs_dir:
        process_gzipped_logs(args.historical_logs_dir, args.historical_logs_dates)
        return

    if settings.HTTP_PUBLISH_STATUS:
        server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

    watchfile = args.watchfile or settings.TRACKING_LOG
    watch(watchfile)


if __name__ == '__main__':
    main()
