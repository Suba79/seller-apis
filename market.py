import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из магазина Яндекс.Маркета.
    
    Выполняет запрос к API Яндекс.Маркета для получения порции товаров.
    Используется пагинация — за один раз возвращается до 200 товаров.
    
    Args:
        page (str): Токен страницы для пагинации (пустая строка для первого запроса).
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Результат запроса, содержащий ключи:
            - offerMappingEntries: список товаров
            - paging: информация для пагинации с nextPageToken
    
    Examples:
        Корректный вызов:
            >>> get_product_list("", "12345", "token123")
            {'offerMappingEntries': [...], 'paging': {'nextPageToken': 'xyz789'}}
        
        Некорректный вызов (неверный токен):
            >>> get_product_list("", "12345", "wrong_token")
            requests.exceptions.HTTPError: 401 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров в Яндекс.Маркете.
    
    Отправляет информацию о количестве товаров на складе через API Маркета.
    
    Args:
        stocks (list): Список словарей с данными об остатках.
                      Каждый словарь содержит sku, warehouseId и items.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Ответ от API Яндекс.Маркета с результатами обновления.
    
    Examples:
        >>> stocks = [{"sku": "ABC123", "warehouseId": "1", "items": [{"count": 10}]}]
        >>> update_stocks(stocks, "12345", "token123")
        {'result': {...}}
        
        С пустым списком:
        >>> update_stocks([], "12345", "token123")
        {'result': {...}}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров в Яндекс.Маркете.
    
    Отправляет новые цены на указанные товары через API Маркета.
    
    Args:
        prices (list): Список словарей с данными о ценах.
                      Каждый словарь содержит id и price.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Ответ от API Яндекс.Маркета с результатами обновления.
    
    Examples:
        >>> prices = [{"id": "ABC123", "price": {"value": 5990, "currencyId": "RUR"}}]
        >>> update_price(prices, "12345", "token123")
        {'result': {...}}
        
        С некорректными данными:
        >>> update_price([], "12345", "token123")
        {'result': {...}}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы всех товаров из магазина Яндекс.Маркета.
    
    Проходит по всем страницам товаров и собирает их артикулы (shopSku).
    Использует функцию get_product_list для получения каждой страницы.
    
    Args:
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        list: Список строк с артикулами (shopSku) всех товаров.
    
    Examples:
        >>> get_offer_ids("12345", "token123")
        ['ABC123', 'DEF456', 'GHI789']
        
        Если товаров нет:
        >>> get_offer_ids("12345", "token123")
        []
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать структуру остатков для загрузки в Яндекс.Маркет.
    
    Фильтрует остатки из прайса поставщика, оставляя только те товары,
    которые уже есть в магазине. Добавляет товары с нулевым остатком
    для тех позиций, которые есть в Маркете, но отсутствуют в прайсе.
    
    Args:
        watch_remnants (list): Список товаров из файла поставщика.
                               Каждый элемент — словарь с 'Код' и 'Количество'.
        offer_ids (list): Список артикулов товаров, уже загруженных в Маркет.
        warehouse_id (str): Идентификатор склада в Яндекс.Маркете.
    
    Returns:
        list: Список словарей с остатками для отправки в API Маркета.
              Каждый словарь содержит sku, warehouseId и items.
    
    Examples:
        >>> remnants = [{'Код': 'ABC123', 'Количество': '>10'}]
        >>> ids = ['ABC123', 'DEF456']
        >>> create_stocks(remnants, ids, "1")
        [
            {'sku': 'ABC123', 'warehouseId': '1', 'items': [{'count': 100, 'type': 'FIT'}]},
            {'sku': 'DEF456', 'warehouseId': '1', 'items': [{'count': 0, 'type': 'FIT'}]}
        ]
        
        С особенностями обработки:
        >>> remnants = [{'Код': 'ABC123', 'Количество': '1'}]
        >>> create_stocks(remnants, ['ABC123'], "1")
        [{'sku': 'ABC123', 'warehouseId': '1', 'items': [{'count': 0, 'type': 'FIT'}]}]
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать структуру цен для загрузки в Яндекс.Маркет.
    
    Формирует список цен для товаров, которые есть в наличии у поставщика
    и уже загружены в Маркет. Цены преобразуются из формата поставщика
    в числовой формат через функцию price_conversion.
    
    Args:
        watch_remnants (list): Список товаров из файла поставщика.
                               Каждый элемент — словарь с 'Код' и 'Цена'.
        offer_ids (list): Список артикулов товаров, уже загруженных в Маркет.
    
    Returns:
        list: Список словарей с ценами для отправки в API Маркета.
              Каждый словарь содержит id и price.
    
    Examples:
        >>> remnants = [{'Код': 'ABC123', 'Цена': "5'990.00 руб."}]
        >>> ids = ['ABC123']
        >>> create_prices(remnants, ids)
        [
            {
                'id': 'ABC123',
                'price': {
                    'value': 5990,
                    'currencyId': 'RUR'
                }
            }
        ]
        
        Если товар есть в прайсе, но не в Маркете:
        >>> remnants = [{'Код': 'ABC123', 'Цена': "5'990.00 руб."}]
        >>> ids = ['DEF456']
        >>> create_prices(remnants, ids)
        []  # Товар не попадёт в список, так как его нет в offer_ids
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены в Яндекс.Маркет, автоматически разбивая на пачки.
    
    Получает список актуальных товаров в Маркете, формирует для них цены
    из прайса поставщика и отправляет пачками (до 500 товаров за раз).
    
    Args:
        watch_remnants (list): Список товаров из файла поставщика.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        list: Список отправленных цен (результат работы create_prices).
    
    Examples:
        >>> prices = await upload_prices(remnants, "12345", "token123")
        >>> len(prices)
        25  # Количество обновлённых цен
        
        Если нет товаров для обновления:
        >>> await upload_prices([], "12345", "token123")
        []  # Пустой список
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остатки в Яндекс.Маркет, автоматически разбивая на пачки.
    
    Получает список актуальных товаров в Маркете, формирует для них остатки
    из прайса поставщика и отправляет пачками (до 2000 товаров за раз).
    
    Args:
        watch_remnants (list): Список товаров из файла поставщика.
        campaign_id (str): Идентификатор кампании в Яндекс.Маркете.
        market_token (str): Токен доступа к API Яндекс.Маркета.
        warehouse_id (str): Идентификатор склада в Яндекс.Маркете.
    
    Returns:
        tuple: Кортеж из двух списков:
            - not_empty: Товары с ненулевыми остатками
            - stocks: Полный список отправленных остатков
    
    Examples:
        >>> not_empty, all_stocks = await upload_stocks(remnants, "12345", "token123", "1")
        >>> len(not_empty)
        15  # Количество товаров в наличии
        >>> len(all_stocks)
        25  # Всего обновлённых товаров (включая нулевые)
        
        Если товаров нет:
        >>> not_empty, all_stocks = await upload_stocks([], "12345", "token123", "1")
        >>> not_empty
        []
        >>> all_stocks
        []  # Пустые списки
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для запуска обновления цен и остатков в Яндекс.Маркете.
    
    Читает переменные окружения с токенами, скачивает актуальный прайс
    от поставщика и обновляет цены и остатки для двух моделей работы:
    FBS (склад продавца + доставка Маркета) и DBS (полное самовыполнение).
    
    Args:
        Нет аргументов. Все настройки берутся из переменных окружения:
        - MARKET_TOKEN: токен доступа к API Яндекс.Маркета
        - FBS_ID: идентификатор кампании для FBS
        - DBS_ID: идентификатор кампании для DBS
        - WAREHOUSE_FBS_ID: идентификатор склада для FBS
        - WAREHOUSE_DBS_ID: идентификатор склада для DBS
    
    Returns:
        None. Функция выполняет действия и выводит сообщения об ошибках
        в консоль при их возникновении.
    
    Examples:
        >>> main()  # Запуск обновления
        # В консоли может появиться:
        # "Превышено время ожидания..."
        # или "Ошибка соединения"
        # или код ошибки "ERROR_2"
        
    Note:
        Функция обрабатывает исключения:
        - ReadTimeout: превышено время ожидания ответа
        - ConnectionError: проблемы с соединением
        - Exception: прочие ошибки (выводятся с пометкой ERROR_2)
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
