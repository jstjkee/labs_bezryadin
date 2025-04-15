import argparse
import struct
import mmap
import os
from time import time
import multiprocessing as mp
from typing import Tuple
from multiprocessing.pool import ThreadPool


def sequential_read(filename):
    file_size = os.path.getsize(filename)

    print(f'Последовательно считает сумму, находит максимум и минимум из файла: {filename} размером {file_size / 1024 ** 3} Гб.')
    min_value = float('inf')
    max_value = float('-inf')
    total_sum = 0

    with open(filename, 'rb') as f:
        i = 0
        while chunk := f.read(4):
            i += 1
            if i % 100000000 == 0:
                read_gb = (i * 32) / (8 * 1024 ** 3)
                print(f"Прогресс: {read_gb:.2f} ГБ просчитано.\tТекущие: сумма: {total_sum}, минимум: {min_value}, максимум: {max_value}")
            number = struct.unpack('>I', chunk)[0]
            total_sum += number
            if number < min_value:
                min_value = number
            if number > max_value:
                max_value = number

    return total_sum, min_value, max_value


def process_chunk(filename: str, start: int, size: int) -> Tuple[int, int, int]:
    size = (size // 4) * 4

    with open(filename, 'rb') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
            mmapped_file.seek(start)

            chunk_data = mmapped_file.read(size)
            int_count = size // 4
            integers = struct.unpack(f'>{int_count}I', chunk_data)

            if not integers:
                return 0, float('inf'), float('-inf')

            print(f'Завершен поток с результатами\tСумма: {sum(integers)}, минимум: {min(integers)}, максимум: {max(integers)}')

            return sum(integers), min(integers), max(integers)


def analyze_binary_file(filename: str) -> Tuple[int, int, int]:
    file_size = os.path.getsize(filename)

    print(f'С использованием мультипроцессинга считает сумму, находит максимум и минимум из файла: {filename} размером {file_size / 1024 ** 3} Гб.')

    num_processes = mp.cpu_count()

    chunk_size = file_size // num_processes
    chunk_size = (chunk_size // 4) * 4

    chunks = []
    for i in range(num_processes):
        start = i * chunk_size
        if i == num_processes - 1:
            size = file_size - start
        else:
            size = chunk_size
        chunks.append((filename, start, size))

    with ThreadPool(processes=num_processes) as pool:
        results = pool.starmap(process_chunk, chunks)

    total_sum = sum(result[0] for result in results)
    total_min = min(result[1] for result in results)
    total_max = max(result[2] for result in results)

    return total_sum, total_min, total_max


def main():
    parser = argparse.ArgumentParser(description="Считает сумму 32-разрядных беззнаковых целых чисел (big endian) (с применением длинной арифметики), находит минимальное и максимальное число.")
    parser.add_argument('--filename', type=str, help="Название файла")
    parser.add_argument('--mode', type=int, choices={1, 2}, required=True, help="1 - последовательное чтение, 2 - параллельное")
    args = parser.parse_args()

    if not isinstance(args.filename, str):
        args.filename = 'randint.bin'

    if not os.path.exists(args.filename):
        raise FileNotFoundError(f"Файл {args.filename} не найден")

    file_size = os.path.getsize(args.filename)

    if file_size == 0:
        raise ValueError('Файл пуст')

    if file_size % 4 != 0:
        raise ValueError(
            f"Размер файла ({file_size} байт) не делится на 4, что требуется для 32-разрядных чисел!")

    start_time = time()

    if args.mode == 1:
        total_sum, min_value, max_value = sequential_read(args.filename)
    elif args.mode == 2:
        total_sum, min_value, max_value = analyze_binary_file(args.filename)
    end_time = time()

    print(f"Сумма: {total_sum}, минимум: {min_value}, максимум: {max_value}")
    print(f"Заняло времени: {end_time - start_time:.2f} seconds")

    return 0


if __name__ == "__main__":
    main()
