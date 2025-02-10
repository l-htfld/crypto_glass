import aiohttp
import asyncio
import nest_asyncio
import json
import logging
from datetime import datetime
from sortedcontainers import SortedDict
import backoff
from typing import Dict, List
import redis.asyncio as redis

logging.basicConfig(
    level=logging.INFO, # Устанавливаем уровень логирования на INFO
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[ # Обработчики, которые определяют куда отправлять логи
        logging.StreamHandler(),
        logging.FileHandler('orderbook.log')
    ]
)
logger = logging.getLogger(__name__)

class OrderBookManager:
    def __init__(self, use_redis: bool = False, redis_url: str = "redis://localhost"):
        self.use_redis = use_redis # Устанавливаем, нужно ли использовать Redis
        self.redis_url = redis_url # URL для подключения к Redis
        self.redis_client = None
        self.local_orderbooks: Dict[str, Dict] = {} # Локальные ордербуки, хранящиеся в памяти
        self.lock = asyncio.Lock() # Асинхронный замок для синхронизации доступа к данным

    async def connect(self):
        if self.use_redis:
            try:
                self.redis_client = await redis.from_url(self.redis_url) # Подключение к Redis
                await self.redis_client.ping() # Проверка соединения с Redis
                logger.info("Connected to Redis") # Логируем успешное подключение
            except redis.exceptions.ConnectionError:
                logger.error("Failed to connect to Redis. Ensure Redis is running.") # Логируем ошибку при подключении
                self.redis_client = None  # Убедимся, что клиент не используется, если подключение не удалось


    async def close(self):
        if self.redis_client:
            await self.redis_client.close() # Закрываем соединение
            logger.info("Redis connection closed")

    async def process_book_data(self, inst_id: str, data: dict): # Обрабатываем данные
        action = data.get('action', 'update')
        bids = data.get('bids', [])
        asks = data.get('asks', [])
        ts = data.get('ts', datetime.now().isoformat())

        if self.use_redis: # Если используется Redis, обрабатываем данные с Redis
            await self._process_redis(inst_id, action, bids, asks, ts)
        else:
            await self._process_local(inst_id, action, bids, asks, ts)

    async def _process_redis(self, inst_id: str, action: str, bids: list, asks: list, ts: str):
        pipe = self.redis_client.pipeline() # Создаем пайплайн для атомарных операций Redis
        key = f"orderbook:{inst_id}"
        
        if action == 'snapshot': # Если действие — снимок
            pipe.delete(key) # Удаляем существующий ордербук
            pipe.hset(key, "ts", ts) # Устанавливаем временную метку
            for price, qty, *_ in bids: # Добавляем бид в отсортированный список
                pipe.zadd(f"{key}:bids", {price: float(price)})
                pipe.hset(f"{key}:bids_data", price, qty)
            for price, qty, *_ in asks:
                pipe.zadd(f"{key}:asks", {price: float(price)}) # Добавляем аск в отсортированный список
                pipe.hset(f"{key}:asks_data", price, qty)
        else: # Если действие — обновление ордербука
            for price, qty, *_ in bids:
                if float(qty) == 0: # Если количество равно нулю, удаляем ордер, удаляем данные о покупках и продажах
                    pipe.zrem(f"{key}:bids", price)
                    pipe.hdel(f"{key}:bids_data", price)
                else:
                    pipe.zadd(f"{key}:bids", {price: float(price)})
                    pipe.hset(f"{key}:bids_data", price, qty)
            for price, qty, *_ in asks:
                if float(qty) == 0:
                    pipe.zrem(f"{key}:asks", price)
                    pipe.hdel(f"{key}:asks_data", price)
                else:
                    pipe.zadd(f"{key}:asks", {price: float(price)})
                    pipe.hset(f"{key}:asks_data", price, qty)
            pipe.hset(key, "ts", ts)
        
        await pipe.execute() # Выполняем все операции пайплайна

    async def _process_local(self, inst_id: str, action: str, bids: list, asks: list, ts: str):
        async with self.lock: # Блокируем доступ к локальным данным
            if inst_id not in self.local_orderbooks or action == 'snapshot':
                self.local_orderbooks[inst_id] = {
                    'bids': SortedDict(),
                    'asks': SortedDict(),
                    'ts': ts
                }
                for price, qty, *_ in bids:
                    self.local_orderbooks[inst_id]['bids'][float(price)] = float(qty)
                for price, qty, *_ in asks:
                    self.local_orderbooks[inst_id]['asks'][float(price)] = float(qty)
            else:
                for price, qty, *_ in bids:
                    price_f = float(price)
                    qty_f = float(qty)
                    if qty_f == 0:
                        self.local_orderbooks[inst_id]['bids'].pop(price_f, None)
                    else:
                        self.local_orderbooks[inst_id]['bids'][price_f] = qty_f
                
                for price, qty, *_ in asks:
                    price_f = float(price)
                    qty_f = float(qty)
                    if qty_f == 0:
                        self.local_orderbooks[inst_id]['asks'].pop(price_f, None)
                    else:
                        self.local_orderbooks[inst_id]['asks'][price_f] = qty_f
                self.local_orderbooks[inst_id]['ts'] = ts

    async def get_top(self, inst_id: str, depth: int = 5) -> dict: # Получаем топ данных из Redis
        if self.use_redis:
            return await self._get_redis_top(inst_id, depth)
        else:
            return self._get_local_top(inst_id, depth)

    async def _get_redis_top(self, inst_id: str, depth: int) -> dict: # Ключ для ордербука в Redis
        key = f"orderbook:{inst_id}"
        pipe = self.redis_client.pipeline()
        # Получаем топ-данные из Redis
        pipe.zrevrange(f"{key}:bids", 0, depth-1, withscores=True)
        pipe.zrange(f"{key}:asks", 0, depth-1, withscores=True)
        pipe.hget(key, "ts")
        
        bids, asks, ts = await pipe.execute()
        
        return {
            'bids': [(float(price.decode()), float((await self.redis_client.hget(f"{key}:bids_data", price)).decode())) 
                    for price, _ in bids],
            'asks': [(float(price.decode()), float((await self.redis_client.hget(f"{key}:asks_data", price)).decode())) 
                    for price, _ in asks],
            'ts': ts.decode() if ts else None
        }

    def _get_local_top(self, inst_id: str, depth: int) -> dict:
        book = self.local_orderbooks.get(inst_id) # Получаем ордербук из локального хранилища
        return {
            'bids': list(reversed(book['bids'].items()))[:depth] if book else [],
            'asks': list(book['asks'].items())[:depth] if book else [],
            'ts': book['ts'] if book else None
        }

class OKXOrderBookCollector:
    # URL для REST API и WebSocket-соединения
    def __init__(self, use_redis: bool = False, redis_url: str = "redis://localhost"):
        self.REST_URL = "https://www.okx.com/api/v5/public/instruments"
        self.WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
        self.book_manager = OrderBookManager(use_redis, redis_url) # Инициализация менеджера стаканов
        self.instruments: List[str] = [] # Список инструментов (пары торгов)
        self.ws: aiohttp.ClientWebSocketResponse = None # WebSocket-соединение и сессия HTTP
        self.session: aiohttp.ClientSession = None  # Единая сессия для всех запросов
        self.running = True
        
    async def __aenter__(self):
        """Support for async context manager."""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensure proper cleanup when used as context manager."""
        await self.close()
        
    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_time=300)
    async def get_instruments(self) -> List[str]:
        """Получает список спотовых инструментов через REST API OKX."""
        params = {"instType": "SPOT"}
        async with aiohttp.ClientSession() as session:
            async with session.get(self.REST_URL, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                return [inst["instId"] for inst in data.get("data", [])]
            
    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_time=300)
    async def connect_websocket(self):
        """Устанавливает WebSocket-соединение."""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect( # Подключение к WebSocket
            self.WS_URL,
            heartbeat=30, # Периодическое отправление пинга
            receive_timeout=60 # Время ожидания ответа от сервера
        )
        logger.info("WebSocket connected")
        
    async def subscribe_to_instruments(self):
        """Подписывается на обновления стаканов."""
        chunks = [self.instruments[i:i+20] for i in range(0, len(self.instruments), 20)]
        for chunk in chunks:
            sub_msg = {
                "op": "subscribe",
                "args": [{"channel": "books", "instId": inst} for inst in chunk]
            }
            await self.ws.send_json(sub_msg)
            logger.info(f"Subscribed to {len(chunk)} instruments") # Логирование успешного подключения

    async def handle_message(self, msg: str):
        """Обрабатывает входящие сообщения WebSocket."""
        try:
            data = json.loads(msg)
            if "event" in data:
                logger.info(f"Subscription update: {data}")
                return

            if "data" not in data or "arg" not in data:
                return

            inst_id = data["arg"]["instId"]
            book_data = data["data"][0]
            await self.book_manager.process_book_data(
                inst_id,
                {
                    "action": book_data.get("action", "update"),
                    "bids": book_data["bids"],
                    "asks": book_data["asks"],
                    "ts": book_data.get("ts")
                }
            )

            # Получаем топ-5 уровней bids и asks
            top = await self.book_manager.get_top(inst_id, depth=5)
            if top['ts']:
                logger.info(
                    f"Updated {inst_id} | "
                    f"Bids: {[f'{p:.2f}:{q:.4f}' for p, q in top['bids']]} | "
                    f"Asks: {[f'{p:.2f}:{q:.4f}' for p, q in top['asks']]}"
                )

        except Exception as e:
            logger.error(f"Error handling message: {str(e)}", exc_info=True)
        
    async def start(self):
        try:
            await self.book_manager.connect()
            self.instruments = await self.get_instruments()
            logger.info(f"Loaded {len(self.instruments)} instruments")

            while self.running:
                try:
                    await self.connect_websocket() # Устанавливаем WebSocket-соединение
                    await self.subscribe_to_instruments() # Подписываемся на стаканы

                    async for msg in self.ws: # Получаем и обрабатываем сообщения от WebSocket
                        if not self.running:
                            break
                        await self.handle_message(msg.data)

                except (aiohttp.ClientError, ConnectionError) as e:
                    logger.warning(f"Connection error: {e}, reconnecting...") # Ошибка подключения, пробуем переподключиться
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                finally:
                    await self.close_websocket() # Закрываем WebSocket после ошибки
                    await asyncio.sleep(5) # Пауза перед новой попыткой подключения

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
        finally:
            # Закрытие сессии в любом случае
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            await self.close()  # Закрытие других ресурсов


    async def close_websocket(self):
        """Закрывает WebSocket-соединение."""
        if self.ws and not self.ws.closed:
            await self.ws.close()
            self.ws = None

    async def close(self):
        """Корректное закрытие всех ресурсов."""
        self.running = False # Останавливаем работу коллектора
        await self.close_websocket() # Закрываем WebSocket
        if self.session and not self.session.closed: # Закрываем сессию
            await self.session.close() 
            self.session = None
        await self.book_manager.close() # Закрываем менеджер стаканов


    async def monitor_books(self, interval: int = 10):
        """Мониторит стаканы для отображения топ 5 заявок с заданным интервалом."""
        while self.running:
            for inst_id in (self.instruments[:3] if self.instruments else []):
                top = await self.book_manager.get_top(inst_id)
                if top['ts']:
                    logger.info(
                        f"{inst_id} | Bids: {[f'{p:.2f}:{q:.4f}' for p, q in top['bids'][:3]]} | "
                        f"Asks: {[f'{p:.2f}:{q:.4f}' for p, q in top['asks'][:3]]}"
                    )
            await asyncio.sleep(interval)

async def main():
    async with OKXOrderBookCollector(
        use_redis=True,
        redis_url="redis://localhost"
    ) as collector:
        await asyncio.gather(
            collector.start(),
            collector.monitor_books()
        )

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())