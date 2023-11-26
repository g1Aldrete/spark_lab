from pyspark.sql import SparkSession
import string

# Create a Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Load data from the file
lines = spark.read.text("file1.txt").rdd.map(lambda x: x[0])

# Define a function to remove punctuation and split words
def process_line(line):
    translator = str.maketrans("", "", string.punctuation)
    words = line.translate(translator).lower().split()
    return words

# Process the lines to remove punctuation and split words
words = lines.flatMap(process_line)

# Filter out stop words
stop_words = set(["and", "or", "that", "the", "a", "an", "is", "are", "have"])
filtered_words = words.filter(lambda word: word not in stop_words)

# Map words to (word, 1) pairs
word_count_pairs = filtered_words.map(lambda word: (word, 1))

# Reduce by key to get word frequencies
word_counts = word_count_pairs.reduceByKey(lambda x, y: x + y)

# Swap (word, count) pairs to (count, word) pairs and sort in descending order
sorted_word_counts = word_counts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

# Take the top 10 words
top_10_words = sorted_word_counts.take(10)

# Print the result
for count, word in top_10_words:
    print(f"Word: {word}, Count: {count}")

# Stop the Spark session
spark.stop()

