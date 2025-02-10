import aiohttp
import asyncio
import nest_asyncio
import json
import logging
from datetime import datetime
from sortedcontainers import SortedDict
import backoff
from typing import Dict, List
import os

# Настройка базовой конфигурации логирования
logging.basicConfig(
    level=logging.INFO, # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[ # Обработчики, которые определяют, куда будут отправляться сообщения
        logging.StreamHandler(),
        logging.FileHandler('orderbook.log')
    ]
)
logger = logging.getLogger(__name__)

class OrderBookManager:
    def __init__(self):
        self.orderbooks: Dict[str, Dict] = {} # Создаем пустой словарь для хранения стаканов по ключу 'inst_id'
        self.lock = asyncio.Lock() # Создаем объект блокировки для асинхронной работы с данными
        self.data_dir = "orderbook_data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    async def process_book_data(self, inst_id: str, data: dict): # Извлекаем необходимые данные из входящего сообщения
        action = data.get('action', 'update')
        bids = data.get('bids', [])
        asks = data.get('asks', [])
        ts = data.get('ts', datetime.now().isoformat()) # Время обновления стакана

        async with self.lock:
            if inst_id not in self.orderbooks or action == 'snapshot': # Если стакан для инструмента не существует или пришел полный снапшот
                self.orderbooks[inst_id] = { # Инициализируем новый стакан
                    'bids': SortedDict(),
                    'asks': SortedDict(),
                    'ts': ts
                }
                for price, qty, *_ in bids: # Добавляем новые заявки на покупку в стакан
                    self.orderbooks[inst_id]['bids'][float(price)] = float(qty)
                for price, qty, *_ in asks: # Добавляем новые заявки на продажу в стакан
                    self.orderbooks[inst_id]['asks'][float(price)] = float(qty)
            else: # Обрабатываем изменения (дельты) в стакане
                for price, qty, *_ in bids:
                    price_f = float(price)
                    qty_f = float(qty)
                    if qty_f == 0: # Если количество равно 0, убираем заявку
                        self.orderbooks[inst_id]['bids'].pop(price_f, None)
                    else: # Обновляем заявку
                        self.orderbooks[inst_id]['bids'][price_f] = qty_f
                
                for price, qty, *_ in asks: 
                    price_f = float(price)
                    qty_f = float(qty)
                    if qty_f == 0:
                        self.orderbooks[inst_id]['asks'].pop(price_f, None)
                    else:
                        self.orderbooks[inst_id]['asks'][price_f] = qty_f
                self.orderbooks[inst_id]['ts'] = ts # Обновляем время последнего изменения

            # Сохраняем данные в файл
            self.save_orderbook(inst_id)

    def save_orderbook(self, inst_id: str):
        # Сохраняем orderbook в формате JSON
        book = self.orderbooks.get(inst_id)
        if book:
            file_path = os.path.join(self.data_dir, f"{inst_id}_orderbook.json")
            with open(file_path, "w") as f:
                json.dump(book, f, indent=4)

    def get_top(self, inst_id: str, depth: int = 5) -> dict:
        # Метод для получения топовых заявок из стакана
        book = self.orderbooks.get(inst_id)
        return {
            'bids': list(reversed(book['bids'].items()))[:depth] if book else [],
            'asks': list(book['asks'].items())[:depth] if book else [],
            'ts': book['ts'] if book else None
        }
        
class OKXOrderBookCollector:
    def __init__(self):
        self.REST_URL = "https://www.okx.com/api/v5/public/instruments"
        self.WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
        self.book_manager = OrderBookManager() # Менеджер для обработки и хранения данных о стаканах
        self.instruments: List[str] = [] # Список инструментов, на которые будем подписываться
        self.ws: aiohttp.ClientWebSocketResponse = None # WebSocket соединение
        self.session: aiohttp.ClientSession = None # HTTP сессия для запросов
        self.running = True

    @backoff.on_exception(backoff.expo, Exception, max_time=300) # Получение списка инструментов через REST API
    async def get_instruments(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.REST_URL, params={'instType': 'SPOT'}) as resp:
                data = await resp.json()
                return [inst['instId'] for inst in data.get('data', [])]

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_time=300)
    async def connect_websocket(self): # Подключение к WebSocket серверу
        if self.session is None or self.session.closed: # Если сессия закрыта, создаем новую
            self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect(
            self.WS_URL,
            heartbeat=30, # Период отправки heartbeat сообщений
            receive_timeout=60 # Время ожидания сообщений
        )
        logger.info("WebSocket connection established")

    async def subscribe_to_instruments(self):
        # Подписка на инструменты через WebSocket
        chunks = [self.instruments[i:i+20] for i in range(0, len(self.instruments), 20)]
        for chunk in chunks:
            sub_msg = {
                "op": "subscribe",
                "args": [{"channel": "books", "instId": inst} for inst in chunk]
            }
            await self.ws.send_json(sub_msg)
            logger.info(f"Subscribed to {len(chunk)} instruments")

    async def handle_message(self, msg: str):
        # Обработка сообщений, получаемых через WebSocket
        try:
            data = json.loads(msg) # Десериализация сообщения из JSON
            if 'event' in data:
                logger.info(f"Subscription update: {data}") # Логируем обновление подписки
                return

            if 'data' not in data or 'arg' not in data:
                return

            inst_id = data['arg']['instId']
            book_data = data['data'][0]
            await self.book_manager.process_book_data(inst_id, {
                'action': book_data.get('action', 'update'),
                'bids': book_data['bids'],
                'asks': book_data['asks'],
                'ts': book_data.get('ts')
            })

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)

    async def start(self): # Основной метод для запуска процесса сбора данных
        try:
            self.instruments = await self.get_instruments() # Получаем список инструментов
            logger.info(f"Loaded {len(self.instruments)} instruments") # Логируем количество загруженных инструментов

            while self.running:
                try:
                    await self.connect_websocket() # Подключаемся к WebSocket
                    await self.subscribe_to_instruments() # Подписываемся на инструменты

                    async for msg in self.ws: # Получаем сообщения от WebSocket
                        if not self.running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT: # Если тип сообщения - текст, обрабатываем его
                            await self.handle_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PING: # Если получен ping, отвечаем pong
                            await self.ws.pong()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR): # Если соединение закрыто или ошибка
                            raise ConnectionError("WebSocket connection closed")

                except (aiohttp.ClientError, ConnectionError) as e:
                    logger.warning(f"Connection error: {str(e)}, reconnecting...") # Логируем ошибку соединения и повторное подключение
                except Exception as e:
                    logger.error(f"Unexpected error: {str(e)}", exc_info=True) # Логируем другие неожиданные ошибки
                finally:
                    await self.close_websocket() # Закрываем WebSocket соединение
                    await asyncio.sleep(5) # Ждем перед повторным подключением

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...") # Логируем завершение работы
        finally:
            await self.close() # Закрываем все ресурсы

    async def close_websocket(self):
        # Закрытие WebSocket соединения
        if self.ws and not self.ws.closed:
            await self.ws.close()
            self.ws = None

    async def close(self): # Завершаем работу и закрываем все соединения
        self.running = False
        await self.close_websocket()
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def monitor_books(self, interval: int = 10):
        # Мониторим топовые заявки
        while self.running:
            for inst_id in (self.instruments[:3] if self.instruments else []):
                top = self.book_manager.get_top(inst_id)
                if top['ts']: # Логируем топовые заявки
                    logger.info(
                        f"{inst_id} | Bids: {[f'{p:.2f}:{q:.4f}' for p, q in top['bids'][:3]]} | "
                        f"Asks: {[f'{p:.2f}:{q:.4f}' for p, q in top['asks'][:3]]}"
                    )
            await asyncio.sleep(interval)

async def main(): # Главная асинхронная функция для запуска процесса
    collector = OKXOrderBookCollector()
    await asyncio.gather(
        collector.start(),
        collector.monitor_books()
    )
    

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(main())