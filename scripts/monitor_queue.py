#!/usr/bin/env python3
"""
Скрипт для мониторинга очереди повторных попыток xAPI Bridge.

Использование:
    python scripts/monitor_queue.py
    python scripts/monitor_queue.py --watch
    python scripts/monitor_queue.py --json
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Добавляем корневую папку проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from xapi_bridge import settings


class QueueMonitor:
    """Мониторинг очереди повторных попыток."""
    
    def __init__(self):
        self.queue_path = Path(getattr(settings, 'RETRY_QUEUE_FILE', '.retry_queue.jsonl'))
        self.sink_path = Path(getattr(settings, 'RETRY_SINK_FILE', 'retried.jsonl'))
    
    def get_queue_stats(self):
        """Получает статистику очереди."""
        stats = {
            'queue_records': 0,
            'sink_records': 0,
            'old_records': 0,
            'ready_records': 0,
            'queue_size_bytes': 0,
            'sink_size_bytes': 0,
            'oldest_record_age': None,
            'newest_record_age': None,
        }
        
        # Проверяем очередь
        if self.queue_path.exists():
            with open(self.queue_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            stats['queue_records'] = len(lines)
            stats['queue_size_bytes'] = self.queue_path.stat().st_size
            
            current_time = time.time()
            record_ages = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    ready_at = record.get('ready_at', 0)
                    age = current_time - ready_at
                    record_ages.append(age)
                    
                    if age > (3600*24):  # Старше суток
                        stats['old_records'] += 1
                    
                    if ready_at <= current_time:
                        stats['ready_records'] += 1
                        
                except json.JSONDecodeError:
                    continue
            
            if record_ages:
                stats['oldest_record_age'] = max(record_ages)
                stats['newest_record_age'] = min(record_ages)
        
        # Проверяем файл результатов
        if self.sink_path.exists():
            with open(self.sink_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            stats['sink_records'] = len(lines)
            stats['sink_size_bytes'] = self.sink_path.stat().st_size
        
        return stats
    
    def print_stats(self, stats, json_output=False):
        """Выводит статистику."""
        if json_output:
            print(json.dumps(stats, indent=2))
            return
        
        print("=" * 60)
        print("Мониторинг очереди повторных попыток")
        print("=" * 60)
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Очередь
        print("Очередь:")
        print(f"  Записей в очереди: {stats['queue_records']}")
        print(f"  Готовых к обработке: {stats['ready_records']}")
        print(f"  Старых записей (>1ч): {stats['old_records']}")
        print(f"  Размер файла: {stats['queue_size_bytes']} байт")
        
        if stats['oldest_record_age'] is not None:
            oldest_hours = stats['oldest_record_age'] / 3600
            newest_hours = stats['newest_record_age'] / 3600
            print(f"  Возраст самой старой записи: {oldest_hours:.1f} часов")
            print(f"  Возраст самой новой записи: {newest_hours:.1f} часов")
        
        print()
        
        # Файл результатов
        print("Результаты:")
        print(f"  Успешно отправлено: {stats['sink_records']}")
        print(f"  Размер файла: {stats['sink_size_bytes']} байт")
        
        print()
        
        # Предупреждения
        if stats['old_records'] > 0:
            print("Предупреждения:")
            print(f"  {stats['old_records']} записей старше 1 суток!")
        
        if stats['queue_records'] > 100:
            print("Предупреждения:")
            print(f"  В очереди много записей: {stats['queue_records']}")
        
        if stats['ready_records'] > 0:
            print("Информация:")
            print(f"  {stats['ready_records']} записей готовы к обработке")
    
    def watch_mode(self, interval=30):
        """Режим непрерывного мониторинга."""
        print(f"Запуск мониторинга (обновление каждые {interval} секунд)")
        print("Нажмите Ctrl+C для остановки")
        print()
        
        try:
            while True:
                # Очищаем экран
                print("\033[2J\033[H", end="")
                
                stats = self.get_queue_stats()
                self.print_stats(stats)
                
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nМониторинг остановлен")


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description='Мониторинг очереди повторных попыток')
    parser.add_argument('--watch', '-w', action='store_true', 
                       help='Непрерывный мониторинг')
    parser.add_argument('--interval', '-i', type=int, default=30,
                       help='Интервал обновления в секундах (по умолчанию: 30)')
    parser.add_argument('--json', '-j', action='store_true',
                       help='Вывод в формате JSON')
    
    args = parser.parse_args()
    
    monitor = QueueMonitor()
    
    if args.watch:
        monitor.watch_mode(args.interval)
    else:
        stats = monitor.get_queue_stats()
        monitor.print_stats(stats, args.json)


if __name__ == '__main__':
    main()
