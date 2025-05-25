from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, length, posexplode, avg, lower, count, sum, regexp_extract, monotonically_increasing_id, udf, lag, lead
from pyspark.sql.window import Window
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
        col("split_array")[2].alias("text"),  # Третий элемент — text
    ).withColumn('id', monotonically_increasing_id())

# 0 находим все слова
words_df = df.select(
    posexplode(
        split(col('text'), '\\s+')
    ).alias('pos', 'word'),
    col('id'),
    col('text'))

expr = '[\p{L}\p{N}\p{M}\.\-\_]+' # буквы, числа, доп символы вида ά

words_df = words_df.select(regexp_extract('word', expr, 0).alias('word'), col('id'), col('pos'))
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

udf_check_cap = udf(lambda s: 1 if str(s)[0].isupper() else 0)

words_with_case = words_df.withColumn(
    'is_capitalized',
    udf_check_cap(words_df.word)
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
wind = Window.partitionBy('id').orderBy('pos')

wind_df = words_df.select(
    'id',
    lag('word').over(wind).alias('prev'),
    'word',
    lead('word').over(wind).alias('next')
)

abbr_5_df = wind_df.filter(words_df.word.rlike('^[\p{L}\p{M}]{1,2}\.$'))

@udf
def task_5(prev, word, next):
    if word.isupper(): #если большие буквы. Например, инициалы А. В.
        return 0
    elif isinstance(prev, str) and prev[0].isupper(): # если перед стоит заглавная буква, например, китайское имя Song Li. Больше не придумал(
        return 0
    else:
        return 1

words_with_case = abbr_5_df.withColumn(
    'is_suitable',
    task_5(wind_df.prev, wind_df.word, wind_df.next)
)

word_stats = words_with_case.groupBy(lower(col('word')).alias('word_lower')).agg(
    count('*').alias('total_count'),
    sum('is_suitable').alias('suitable_count')
)

result_words = word_stats.filter(
    (col('suitable_count') / col('total_count')) > 0.5
).orderBy(col('total_count').desc())
print("Устойчивые сокращения вида 'пр.', 'др.':")
result_words.show(20)

# 6. Устойчивые сокращения вида "т.п.", "н.э."
abbr_6_df = wind_df.filter(words_df.word.rlike('^[\p{L}]\.[\p{L}]\.$'))

@udf
def task_6(prev, word, next):
    if word.isupper(): #если с большой буквы. Например, инициалы кто-то слитно написал А.В. Или U.S. Больше не придумал(
        return 0
    else:
        return 1

words_with_case = abbr_6_df.withColumn(
    'is_suitable',
    task_6(wind_df.prev, wind_df.word, wind_df.next)
)

word_stats = words_with_case.groupBy(lower(col('word')).alias('word_lower')).agg(
    count('*').alias('total_count'),
    sum('is_suitable').alias('suitable_count')
)

result_words = word_stats.filter(
    (col('suitable_count') / col('total_count')) > 0.5
).orderBy(col('total_count').desc())
print("Устойчивые сокращения вида 'т.п.', 'н.э.':")
result_words.show(20)