#initialize the pyspark
import findspark
findspark.init()
import pyspark

#import SparkSession
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('FilterWords').getOrCreate()

#Path to file
file_path = "words.txt"

# Read the text file into an RDD
text_rdd = spark.sparkContext.textFile(file_path)

# Number 1: Filter words (ascending) starting with "b" and ending with "t". 
# USING FILTER
##filtered_words = text_rdd.filter(lambda word: word.startswith("b") and word.endswith("t"))

# Sort the filtered words in ascending order
#USING SORTBY
##sorted_words = filtered_words.sortBy(lambda word: word, ascending=True)

# Take the first 5 words
##result = sorted_words.take(5)

# The results
##for word in result:
##    print(word)

# Number 2: List the 10 longest words from the file
# Split each line into words and compute the length of each word
word_length_rdd = text_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, len(word)))

# Sort the words by their length in descending order
#USING SORTBY
sorted_words = word_length_rdd.sortBy(lambda x: x[1], ascending=False)

# Take the 10 longest words
#USING MAP
result = sorted_words.map(lambda x: x[0]).take(10)

# The result
for word in result:
    print(word)

# Stop the Spark session
spark.stop()
