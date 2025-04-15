import os
import struct
import random
import argparse


def create_large_binary_file(filename, min_size_gb=2):
    file_size = min_size_gb * 8 * 1024 ** 3
    num_ints = file_size // 32

    print(f"Создание файла {filename} размером {min_size_gb} ГБ...")
    print(f"Будет записано {num_ints:,} чисел")

    # Открываем файл в бинарном режиме для записи
    with open(filename, 'wb') as f:
        for i in range(num_ints):
            num = random.getrandbits(32)
            f.write(struct.pack('>I', num))  # big-endian unsigned int

            # Выводим прогресс каждые 100 миллионов чисел
            if i > 0 and i % 100000000 == 0:
                written_gb = (i * 32) / (8 * 1024 ** 3)
                print(f"Прогресс: {written_gb:.2f} ГБ записано...")

    actual_size = os.path.getsize(filename)
    print(f"Файл {filename} создан. Размер: {actual_size / (1024 ** 3):.2f} ГБ")


def main():
    parser = argparse.ArgumentParser(description='Создание бинарного файла из случайных 32-разрядных беззнаковых целых чисел (big endian)')
    parser.add_argument('--filename', type=str, help='Название выходного файла (в конце .bin)')
    parser.add_argument('--filesize', type=float, help='Размер (мин 2 Гб)')
    args = parser.parse_args()

    if not isinstance(args.filename, str):
        args.filename = 'randint.bin'

    if not isinstance(args.filesize, float) or args.filesize < 2:
        args.filesize = 2

    create_large_binary_file(args.filename, args.filesize)


if __name__ == "__main__":
    main()

