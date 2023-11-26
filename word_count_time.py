from pyspark import SparkContext, SparkConf
import re
from pyspark import StorageLevel

# Function to clean and tokenize the text
def clean_and_tokenize(text):
    words = re.findall(r'\b\w+\b', text.lower())
    return [word for word in words if word not in stop_words]

# Function to count the frequency of words
def count_words(words_list):
    return [(word, 1) for word in words_list]

# Function to filter out stop words
def is_not_stop_word(word):
    return word not in stop_words

# Path to your stop words file
stop_words_path = "stopwords.txt"

# Load stop words from file
with open(stop_words_path, 'r') as file:
    stop_words = set(file.read().split())

# Spark configuration
conf = SparkConf().setAppName("WordCountApp")
sc = SparkContext(conf=conf)

# Load the text file
file_path = "file1.txt"
text_rdd = sc.textFile(file_path)

# Clean and tokenize the text
words_rdd = text_rdd.flatMap(clean_and_tokenize).filter(is_not_stop_word)
#### Persist the RDD in MEMORY_ONLY or DISK_ONLY
# Change the persist level as needed
#words_rdd.persist(storageLevel=StorageLevel.MEMORY_ONLY)
words_rdd.persist(storageLevel=StorageLevel.DISK_ONLY)
#####

# Count the frequency of each word
word_count_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Take the top 10 words based on frequency
top_words = word_count_rdd.takeOrdered(10, key=lambda x: -x[1])

# Display the result
for word, count in top_words:
    print(f"{word}: {count}")

# Stop the Spark context
sc.stop()
