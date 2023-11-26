#initialize the pyspark
import findspark
findspark.init()
import pyspark

#import SparkSession
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('FilterWords').getOrCreate()

# Paths to files
file1_path = "file1.txt"
file2_path = "file2.txt"
file3_path = "file3.txt"

# Read the text files into RDDs
file1_rdd = spark.sparkContext.textFile(file1_path)
file2_rdd = spark.sparkContext.textFile(file2_path)
file3_rdd = spark.sparkContext.textFile(file3_path)

# Split lines into words and assign a count of 1 to each word in each file
file1_words = file1_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))
file2_words = file2_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))
file3_words = file3_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))

# Number 4: Calculate the word frequency by reducing by key
# USING REDUCEBYKEY
# Assume that X is a set of words which are shared between all files, 
#find common words like xi in X which maximize 
#F(xi)= f1(count xi in file 1) +f2(count xi in file 2) +f3(count xi in file 3)
word_freq = file1_words.union(file2_words).union(file3_words).reduceByKey(lambda a, b: a + b)

# Filter out words that appear in all three files
shared_words = word_freq.filter(lambda x: x[1] >= 3)

# Sort by frequency in descending order
sorted_shared_words = shared_words.sortBy(lambda x: x[1], ascending=False)

# Take the top 3 common words with the highest sum of frequencies
top_common_words = sorted_shared_words.take(3)

# The result
for word, frequency in top_common_words:
    print(f"Word: {word}, Frequency: {frequency}")






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

# Stop the Spark session
spark.stop()
