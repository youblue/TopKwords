# Top K Frequent Words

## Input files:
* Calculate top K frequent words from this file (Ulysses):
    http://www.gutenberg.org/files/4300/4300-0.txt
* Stop words are from this file:
    https://raw.githubusercontent.com/stanfordnlp/CoreNLP/master/data/edu/stanford/nlp/patterns/surface/stopwords.txt

## Usage:
```
TopK < inputFile> < outputFile> [-k K] [-skip skipPatternFile]
```
* Explanation of arguments:
  * < inputFile>: intput file for counting frequency, e.g., "4300-0.txt".
  * < outputFile>: folder name the program will write output to.
  * -k: Number of top results, with default 100.
  * -skip: stop words that want that word count program will skip.

## Command to run:
  ```
  cd $XXXX/topKwords
  ```

#### Local mode:
```
rm -rf output

hadoop jar target/mapreduce-0.0.1-SNAPSHOT.jar mapreduce.TopK
 -Dmapreduce.framework.name=local -Dfs.defaultFS=file:///
 4300-0.txt output -k 100 -skip stopwords.txt

# Check output result:
ls output/part-r-00000 # Use "cat" to open if it is a text file
```

#### Single node cluster mode

```
hadoop fs -put 4300-0.txt .
hadoop fs -put stopwords.txt .
hadoop fs -rm -r output

hadoop jar target/mapreduce-0.0.1-SNAPSHOT.jar mapreduce.TopK
 4300-0.txt output -k 100 -skip stopwords.txt

# Check output result:
hadoop fs -text output/part-r-00000
```

## References
https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
