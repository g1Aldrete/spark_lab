#initialize the pyspark
import findspark
findspark.init()
import pyspark

#import SparkSession
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('FilterWords').getOrCreate()

#Path to file
#file_path = "words.txt"

# Read the text file into an RDD
#text_rdd = spark.sparkContext.textFile(file_path)

# Number 3: Calculate the number of lines and the number of distinct words from file1.
# Path to file
file_path = "file1.txt"

# Read the text file into an RDD
text_rdd = spark.sparkContext.textFile(file_path)

# Count all lines
num_lines = text_rdd.count()

# Split each line into words and create a list of words
# USING FLATMAP
words_rdd = text_rdd.flatMap(lambda line: line.split())

# Count all distinct words
num_distinct_words = words_rdd.distinct().count()

# Results
print("Number of lines:", num_lines)
print("Number of distinct words:", num_distinct_words)

# Number 2: List the 10 longest words from the file
# Split each line into words and compute the length of each word
#word_length_rdd = text_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, len(word)))

# Sort the words by their length in descending order
#USING SORTBY
#sorted_words = word_length_rdd.sortBy(lambda x: x[1], ascending=False)

# Take the 10 longest words
#USING MAP
#result = sorted_words.map(lambda x: x[0]).take(10)

# The result
#for word in result:
#    print(word)

# Stop the Spark session
spark.stop()
