import polars as pl
import argparse
import os
import re


def agg_trades(input_files, interval):
    return (
        pl
        .scan_parquet(input_files)  # чтобы обработать большие файлы
        .with_columns(
            pl.col("timestamp").cast(pl.Datetime),
            pl.when(pl.col('is_buyer_maker'))
                .then(pl.lit('buyer'))
                .otherwise(pl.lit('seller'))
                .alias('trade_type'))
        .sort("timestamp")
        .group_by_dynamic(
            "timestamp",
            every=interval,
            closed="left",  # данные [t, t+interval)
            group_by='trade_type')
        .agg(
            (pl.col("price") * pl.col("quantity")).sum() / pl.col("quantity").sum().alias("price"),
            pl.col("quantity").sum(),
            (pl.col("price") * pl.col("quantity")).sum().alias("quote_qty"),
            pl.len().alias("num_trades")
        )
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

        if os.path.getsize(file) == 0:
            raise ValueError('Файл пуст')

    interval = args.interval.strip()

    if not re.fullmatch(r"^(\d+)(ms|s|m|h|d)$", interval):
        raise ValueError(f"Некорректный формат интервала {interval}. Примеры: 500ms, 1s, 5m, 1h, 3d")

    trades = agg_trades(input_files, interval)
    print(f"Построено {len(trades)} агрегаций трейдов с интервалом {interval}")

    output_file = f"trades_agg_{interval}.parquet"
    trades.write_parquet(output_file)
    print(f"Агрегация трейдов сохранены в {output_file}")


if __name__ == "__main__":
    main()
