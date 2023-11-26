from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("word_groups").getOrCreate()

# Load data from the file
lines = spark.read.text("words.txt").rdd.map(lambda x: x[0])

#NUMBER 5:Group words for words.txt according to their first 4 characters
#and then output the number of members for the first 10 groups.

# Extract the first 4 characters from each word and create key-value pairs
word_groups = lines.map(lambda word: (word[:4], 1))

# Group by key and count the occurrences
grouped_counts = word_groups.reduceByKey(lambda x, y: x + y)

# Collect the first 10 groups and their counts
result = grouped_counts.take(10)

# Print the result
for group, count in result:
    print(f"Group: {group}, Count: {count}")

# Stop the Spark session
spark.stop()

