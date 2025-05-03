import requests
from datetime import datetime, timedelta
from io import BytesIO
from zipfile import ZipFile
import argparse
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from functools import partial
import pandas as pd


def download_and_extract_data(pair, date):

    print(f"Обработка {pair} за {date}")
    try:
        url = f"https://data.binance.vision/data/spot/daily/trades/{pair}/{pair}-trades-{date}.zip"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Данные для {pair} не найдены (HTTP {response.status_code})")
            return None

        with ZipFile(BytesIO(response.content)) as zip_file:
            csv_filename = f"{pair}-trades-{date}.csv"
            with zip_file.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file, header=None, names=[
                    'trade_id', 'price', 'quantity', 'quote_qty', 'timestamp',
                    'is_buyer_maker', 'is_best_match'
                ])
        return df
    except Exception as e:
        print(f"Ошибка при обработке {pair} за {date}: {str(e)}")
        return None


def date_range():
    current_date = datetime(2025, 1, 1)
    end_date = (datetime.today() - timedelta(days=3))  # с задержкой 3 дня, чтобы данные точно появились
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description='Создание parquet файла из тиковых трейдов binance с 2025-01-01')
    parser.add_argument('pair', type=str, help='Торговая пара (например, BTCUSDT):')
    parser.add_argument('--num_threads', type=int, help='Количество потоков')
    args = parser.parse_args()

    pair = args.pair.strip().upper()

    num_threads = args.num_threads

    if not isinstance(num_threads, int) or num_threads > mp.cpu_count():
        num_threads = mp.cpu_count()

    part = partial(download_and_extract_data, pair)

    with ThreadPool(processes=num_threads) as pool:
        results = pool.map(part, list(date_range()))

    all_data = [df for df in results if df is not None]

    if not all_data:
        print("Нет данных для сохранения.")
        return

    combined_df = pd.concat(all_data)

    output_filename = f"{pair}_trades_to_{(datetime.today() - timedelta(days=3)).strftime('%Y-%m-%d')}.parquet"
    combined_df.to_parquet(output_filename)
    print(f"Данные сохранены в {output_filename}")


if __name__ == "__main__":
    main()
