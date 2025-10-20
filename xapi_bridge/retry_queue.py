"""
Файловая очередь для отложенной повторной отправки высказываний.
"""

import json
import logging
import threading
import time
from pathlib import Path
from typing import List, Optional

from tincan.lrs_response import LRSResponse

from xapi_bridge import settings


logger = logging.getLogger(__name__)


def _default_queue_path() -> Path:
    custom = getattr(settings, 'RETRY_QUEUE_FILE', None)
    if custom:
        return Path(custom)
    return Path.home() / '.xapi_bridge_retry.jsonl'


class RetryQueue:
    """Простая JSONL-очередь. Каждая строка — JSON с полями content, ready_at."""

    def __init__(self) -> None:
        self.path = _default_queue_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def enqueue(self, content_json: str, delay_seconds: Optional[int] = None) -> None:
        # В ежедневном режиме задержка не используется: запись должна быть готова к ближайшему окну
        if getattr(settings, 'RETRY_DAILY_AT', None):
            ready_at = int(time.time())
        else:
            # Периодический режим (на случай локальных тестов)
            retry_delay = int(getattr(settings, 'RETRY_DELAY_SECONDS', 0) or 0)
            ready_at = int(time.time()) + int(delay_seconds or retry_delay)
        record = {'content': content_json, 'ready_at': ready_at}
        line = json.dumps(record, ensure_ascii=False)
        with self.lock:
            with open(self.path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
        logger.info(f"Добавлено в очередь повторной отправки. ready_at={ready_at}")

    def read_ready(self) -> List[str]:
        now = int(time.time())
        ready: List[str] = []
        remaining_lines: List[str] = []
        if not self.path.exists():
            return ready
        with self.lock:
            with open(self.path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        # сломанные строки отбрасываем
                        continue
                    if int(rec.get('ready_at', 0)) <= now and rec.get('content'):
                        ready.append(rec['content'])
                    else:
                        remaining_lines.append(line)

            # перезаписываем файл оставшимися строками
            with open(self.path, 'w', encoding='utf-8') as f:
                if remaining_lines:
                    f.write('\n'.join(remaining_lines) + '\n')
        return ready
    
    def _save_to_sink(self, content_json: str) -> None:
        """Сохраняет содержимое в файл результатов (sink file)."""
        sink_file = getattr(settings, 'RETRY_SINK_FILE', None)
        if not sink_file:
            return
        
        sink_path = Path(sink_file)
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(sink_path, 'a', encoding='utf-8') as f:
            try:
                parsed = json.loads(content_json)
                if isinstance(parsed, list):
                    for stmt in parsed:
                        f.write(json.dumps(stmt, ensure_ascii=False) + '\n')
                else:
                    f.write(json.dumps(parsed, ensure_ascii=False) + '\n')
            except Exception:
                # если не удалось распарсить, пишем как есть одной строкой
                f.write(content_json + '\n')
        
        logger.info(f"Сохранены ретрай-записи в {sink_file}")


class RetryQueueWorker(threading.Thread):
    """Фоновая процесс обработки очереди ретраев.

    Ежедневный режим: раз в день в указанное время (settings.RETRY_DAILY_AT='HH:MM')
    пробует сначала одну запись; при успехе — отправляет остальные; при неуспехе — ждёт
    следующего дня, ничего не делая.
    """

    daemon = True

    def __init__(self) -> None:
        super().__init__(name='RetryQueueWorker')
        self.queue = RetryQueue()
        self.interval = int(getattr(settings, 'RETRY_WORKER_INTERVAL_SECONDS', 300))
        self._stop_event = threading.Event()
        self.sink_file = getattr(settings, 'RETRY_SINK_FILE', None)

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        #Если не задано — используем 03:00 по умолчанию
        daily_at = getattr(settings, 'RETRY_DAILY_AT', '03:00')
        try:
            hh, mm = map(int, str(daily_at).split(':'))
        except Exception:
            logger.error("Неверный формат RETRY_DAILY_AT, ожидается 'HH:MM'")
            hh = 3
            mm = 0

        while not self._stop_event.is_set():
            try:
                # Ждём до ближайшего HH:MM
                now = time.localtime()
                target_tuple = (now.tm_year, now.tm_mon, now.tm_mday, hh, mm, 0,
                                now.tm_wday, now.tm_yday, now.tm_isdst)
                target = time.mktime(time.struct_time(target_tuple))
                now_ts = time.mktime(now)
                if target <= now_ts:
                    target += 24 * 60 * 60
                sleep_for = max(0, int(target - now_ts))
                for _ in range(sleep_for):
                    if self._stop_event.is_set():
                        return
                    time.sleep(1)

                # Наступило окно: читаем текущие готовые
                ready_contents = self.queue.read_ready()
                if not ready_contents:
                    # ничего не готово — ждём следующего дня
                    continue

                first = ready_contents[:1]
                rest = ready_contents[1:]
                ok = self._process_batch(first)
                if ok and rest:
                    self._process_batch(rest)
                elif not ok and rest:
                    # вернём остальные в очередь ещё раз
                    for content_json in rest:
                        self.queue.enqueue(content_json)
                # затем снова ждём следующего дня
                continue
            except Exception:
                logger.exception("Сбой фонового воркера очереди, продолжаем")
                # Спим минуту и пробуем пересчитать окно
                for _ in range(60):
                    if self._stop_event.is_set():
                        return
                    time.sleep(1)

    def _process_batch(self, contents: List[str]) -> bool:
        all_ok = True
        for content_json in contents:
            try:
                if self.sink_file:
                    Path(self.sink_file).parent.mkdir(parents=True, exist_ok=True)
                    with open(self.sink_file, 'a', encoding='utf-8') as f:
                        try:
                            parsed = json.loads(content_json)
                            if isinstance(parsed, list):
                                for stmt in parsed:
                                    f.write(json.dumps(stmt, ensure_ascii=False) + '\n')
                            else:
                                f.write(json.dumps(parsed, ensure_ascii=False) + '\n')
                        except Exception:
                            # если не удалось распарсить, пишем как есть одной строкой
                            f.write(content_json + '\n')
                    logger.info(f"Сохранены ретрай-записи в {self.sink_file}")
                else:
                    from xapi_bridge import client  # type: ignore
                    payload = json.loads(content_json)
                    try:
                        response: LRSResponse = client.lrs_publisher.lrs.save_statements(payload)
                        if not response.success:
                            status = getattr(response.response, 'status', None)
                            all_ok = False
                            # 404 и повторяемые статусы — переочередяем
                            if (
                                client.lrs_publisher.backend.is_not_found(status, str(response.data))
                                or status in {408, 429, 500, 502, 503, 504}
                            ):
                                # Учитываем Retry-After если доступно
                                delay_seconds = None
                                try:
                                    getheader = getattr(response.response, 'getheader', None)
                                    if callable(getheader):
                                        ra = getheader('Retry-After')
                                        if ra:
                                            try:
                                                delay_seconds = int(ra)
                                            except Exception:
                                                delay_seconds = None
                                except Exception:
                                    pass
                                self.queue.enqueue(content_json, delay_seconds=delay_seconds)
                            else:
                                logger.error(f"Ошибка повторной отправки: {status} {response.data}")
                        else:
                            logger.info("Успешная повторная отправка из очереди")
                    except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError, TimeoutError) as net_err:
                        # transient сетевые ошибки — вернём обратно
                        all_ok = False
                        logger.warning(f"Сетевая ошибка при повторной отправке: {net_err}")
                        self.queue.enqueue(content_json)
                    except Exception as send_err:
                        # Прочие ошибки сохраним в лог, не теряем запись
                        all_ok = False
                        logger.error(f"Ошибка при отправке из очереди: {send_err}")
                        self.queue.enqueue(content_json)
            except Exception as e:
                all_ok = False
                logger.error(f"Ошибка при повторной отправке из очереди: {e}")
        return all_ok


