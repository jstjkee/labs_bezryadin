from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, length, explode, avg, lower, when, count, sum, regexp_extract
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").appName('spark').getOrCreate()

df = spark.read.text("wiki.txt").\
    withColumn("split_array", split(col("value"), "\t"))\
    .select(
        # col("split_array")[0].alias("url"),  # Первый элемент — URL. Не нужен дальше
        # col("split_array")[1].alias("name"), # Второй элемент — name. Не нужен дальше
        col("split_array")[2].alias("text")  # Третий элемент — text
    )

# 0 находим все слова
words_df = df.select(
    explode(
        split(col('text'), '\\s+')
    ).alias('word'))

expr = '[\p{L}\p{N}\p{M}\.\-\_]+' # буквы, числа, доп символы вида ά

words_df = words_df.select(regexp_extract('word', expr, 0).alias('word'))
words_df = words_df.filter(words_df.word != '')

# 1. Находим самое длинное слово
words_df = words_df.withColumn('word_length', length('word'))
longest_word = words_df.orderBy(col('word_length').desc()).first()
print(f"Самое длинное слово: '{longest_word['word']}' (длина: {longest_word['word_length']})")

# 2. Находим среднюю длину слов
avg_word_length = words_df.select(avg('word_length')).first()[0]
print(f"Средняя длина слова: {avg_word_length:.2f} символов")

# 3. Находим самое частоупотребляемое слово из латинских букв
latin_words = words_df.filter(words_df.word.rlike('^[a-zA-Z]+$'))
latin_words = latin_words.select(lower('word').alias('word_lower'))
most_common_word = latin_words.groupBy('word_lower').count().orderBy(col('count').desc()).first()
print(f"Самое частое латинское слово: '{most_common_word['word_lower']}'. Оно встречается {most_common_word['count']} раз")

# 4. Слова, которые более чем в половине случаев начинаются с большой буквы и встречаются >10 раз
words_with_case = words_df.withColumn(
    'is_capitalized',
    when(words_df.word.rlike('^[A-ZА-Я]'), 1).otherwise(0)
)
word_stats = words_with_case.groupBy(lower(col('word')).alias('word_lower')).agg(
    count('*').alias('total_count'),
    sum('is_capitalized').alias('capitalized_count')
).filter(col('total_count') > 10)

result_words = word_stats.filter(
    (col('capitalized_count') / col('total_count')) > 0.5
)
print("Слова с >50% заглавных букв и встречающиеся >10 раз:")
result_words.show(20)

# 5. Устойчивые сокращения вида "пр.", "др."
abbr1_df = words_df.filter(words_df.word.rlike('^[\p{L}\p{M}]{1,2}\.$'))
abbr1_stats = abbr1_df.groupBy('word').count().orderBy(col('count').desc())
print("Устойчивые сокращения вида 'пр.', 'др.':")
abbr1_stats.show(20)

# 6. Устойчивые сокращения вида "т.п.", "н.э."
abbr2_df = words_df.filter(words_df.word.rlike('^[\p{L}]\.[\p{L}]\.$'))
abbr2_stats = abbr2_df.groupBy('word').count().orderBy(col('count').desc())
print("Устойчивые сокращения вида 'т.п.', 'н.э.':")
abbr2_stats.show(20)
