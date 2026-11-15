#!/usr/bin/env python3
"""
Ограниченный тест сайта с 1000 виртуальными пользователями.
ВАЖНО: 1000 VU — это логические пользователи, а не 1000 одновременных
HTTP-потоков. Реальная нагрузка ограничена: максимум 2 запроса/сек
и максимум 5 одновременных HTTP-запросов.
Использовать только для сайта, которым вы владеете или который вам
разрешено тестировать. Для production оставлены консервативные лимиты.
"""

import argparse
import asyncio
import statistics
import sys
import time
from collections import Counter

try:
    import aiohttp
except ImportError:
    print("Не установлена aiohttp. Выполните: pip install aiohttp")
    sys.exit(1)


class RateLimiter:
    def __init__(self, rate):
        self.interval = 1.0 / rate
        self.next_time = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_time - now)
            self.next_time = max(now, self.next_time) + self.interval
        if delay:
            await asyncio.sleep(delay)


async def main():
    parser = argparse.ArgumentParser(
        description="Безопасная проверка сайта с 1000 виртуальными пользователями"
    )
    parser.add_argument("url", help="URL своего сайта, например https://example.com/")
    parser.add_argument("--vus", type=int, default=1000, help="Число виртуальных пользователей (по умолчанию 1000)")
    parser.add_argument("--rps", type=float, default=2.0, help="Жёсткий лимит запросов/сек (максимум 2)")
    parser.add_argument("--concurrency", type=int, default=5, help="Одновременные HTTP-запросы (максимум 5)")
    parser.add_argument("--duration", type=int, default=120, help="Длительность в секундах (максимум 180)")
    args = parser.parse_args()

    if not (args.url.startswith("https://") or args.url.startswith("http://")):
        parser.error("URL должен начинаться с http:// или https://")
    if not 1 <= args.vus <= 1000:
        parser.error("--vus должен быть от 1 до 1000")
    if not 0.1 <= args.rps <= 2.0:
        parser.error("--rps ограничен диапазоном 0.1–2.0")
    if not 1 <= args.concurrency <= 5:
        parser.error("--concurrency ограничен диапазоном 1–5")
    if not 10 <= args.duration <= 180:
        parser.error("--duration ограничен диапазоном 10–180 секунд")

    print("\nОГРАНИЧЕННЫЙ ТЕСТ")
    print(f"URL: {args.url}")
    print(f"Виртуальные пользователи: {args.vus}")
    print(f"Предел нагрузки: {args.rps:.1f} запроса/сек")
    print(f"Максимум одновременных запросов: {args.concurrency}")
    print(f"Длительность: {args.duration} сек.")
    print("\nЭто НЕ 1000 реальных потоков. Запросы ограничены указанными пределами.")
    confirm = input("Введите TEST-MY-OWN-SITE для подтверждения разрешения: ").strip()
    if confirm != "TEST-MY-OWN-SITE":
        print("Отменено.")
        return

    limiter = RateLimiter(args.rps)
    semaphore = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=args.concurrency, ssl=True)
    stats = Counter()
    latencies = []
    stop_at = time.monotonic() + args.duration
    stop_event = asyncio.Event()

    async def request_once(session):
        if stop_event.is_set() or time.monotonic() >= stop_at:
            return
        await limiter.wait()
        if stop_event.is_set() or time.monotonic() >= stop_at:
            return
        async with semaphore:
            started = time.monotonic()
            try:
                async with session.get(args.url, allow_redirects=True) as response:
                    await response.content.read(1024)
                    elapsed = time.monotonic() - started
                    latencies.append(elapsed)
                    stats[f"http_{response.status}"] += 1
                    stats["total"] += 1
                    if response.status == 429:
                        print("\nПолучен HTTP 429 — тест остановлен.")
                        stop_event.set()
                    elif response.status >= 500:
                        stats["server_errors"] += 1
                        if stats["server_errors"] >= 3:
                            print("\nТри ответа 5xx — тест остановлен.")
                            stop_event.set()
            except asyncio.TimeoutError:
                stats["timeouts"] += 1
                stats["total"] += 1
                if stats["timeouts"] >= 3:
                    print("\nТри тайм-аута — тест остановлен.")
                    stop_event.set()
            except aiohttp.ClientError:
                stats["connection_errors"] += 1
                stats["total"] += 1
                if stats["connection_errors"] >= 3:
                    print("\nТри ошибки соединения — тест остановлен.")
                    stop_event.set()

    async def virtual_user(session, user_id):
        # Каждый VU имитирует редкое действие пользователя.
        # Общий rate limiter не позволяет превысить заданный RPS.
        while not stop_event.is_set() and time.monotonic() < stop_at:
            await request_once(session)
            await asyncio.sleep(1.0 + (user_id % 7) * 0.15)

    started_all = time.monotonic()
    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"User-Agent": "OwnerAuthorized-Limited-Health-Check/1.0"}
    ) as session:
        tasks = [asyncio.create_task(virtual_user(session, i)) for i in range(args.vus)]
        try:
            while time.monotonic() < stop_at and not stop_event.is_set():
                elapsed = max(0.1, time.monotonic() - started_all)
                rate = stats["total"] / elapsed
                print(
                    f"\rВремя: {int(elapsed):3d}/{args.duration}s | "
                    f"Запросов: {stats['total']:4d} | "
                    f"Факт. RPS: {rate:.2f} | "
                    f"Ошибки: {stats['timeouts'] + stats['connection_errors'] + stats['server_errors']}",
                    end="", flush=True
                )
                await asyncio.sleep(1)
        finally:
            stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = max(0.001, time.monotonic() - started_all)
    print("\n\n--- ИТОГ ---")
    print(f"Время: {elapsed:.1f} сек.")
    print(f"Виртуальные пользователи: {args.vus}")
    print(f"Всего запросов: {stats['total']}")
    print(f"Средний RPS: {stats['total'] / elapsed:.2f}")
    print(f"Тайм-ауты: {stats['timeouts']}")
    print(f"Ошибки соединения: {stats['connection_errors']}")
    print(f"Ответы 5xx: {stats['server_errors']}")
    if latencies:
        print(f"Средний ответ: {statistics.mean(latencies) * 1000:.0f} мс")
        print(f"Медиана ответа: {statistics.median(latencies) * 1000:.0f} мс")
        print(f"Максимальный ответ: {max(latencies) * 1000:.0f} мс")
    for key, value in sorted(stats.items()):
        if key.startswith("http_"):
            print(f"{key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())
