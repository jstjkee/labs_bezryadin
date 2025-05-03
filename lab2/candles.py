import polars as pl
import argparse
import os
import re


def candlesticks(input_files, interval):

    return (
        pl
        .scan_parquet(input_files)  # чтобы обработать большие файлы
        .with_columns(pl.col("timestamp").cast(pl.Datetime))
        .sort("timestamp")
        .group_by_dynamic(
            "timestamp",
            every=interval,
            closed="left"  # данные [t, t+interval)
        )
        .agg(
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").first().alias("open"),  # добавил, ибо plotly без него не строит
            pl.col("price").last().alias("close"),
            pl.col("price").sample(1).alias("random"),
            pl.col("price").count().alias("num_trades"),
            (pl.col("price") * pl.col("quantity")).sum().alias("volume"),
        )
        .explode('random')
        .collect()
    )


def main():
    parser = argparse.ArgumentParser(description='Создание parquet файла из тиковых трейдов binance с 2025-01-01')
    parser.add_argument('input_files', type=str, help='Пути к Parquet-файлам (через пробел)')
    parser.add_argument('interval', type=str, help="интервал свечей (например, '500ms', '1s', '1m')")
    args = parser.parse_args()

    input_files = args.input_files.strip().split()

    for file in input_files:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Файл {file} не найден")

    interval = args.interval.strip()

    if not re.fullmatch(r"^(\d+)(ms|s|m|h|d)$", interval):
        raise ValueError(f"Некорректный формат интервала {interval}. Примеры: 500ms, 1s, 5m, 1h, 3d")

    candles = candlesticks(input_files, interval)
    print(f"Построено {len(candles)} свечей с интервалом {interval}")

    output_file = f"candles_{interval}.parquet"
    candles.write_parquet(output_file)
    print(f"Свечи сохранены в {output_file}")


if __name__ == "__main__":
    main()
