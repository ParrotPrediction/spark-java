#Spark Starter

Application counting words in document.
Developed in Java 8 and Apache Spark 1.6 tech stack.

##Build it
  
    mvn clean package

Set the environment variable pointing to the $SPARK_HOME directory.

##Run it 
Via Apache Spark loader

    $SPARK_HOME/bin/spark-submit \
    --class com.parrotprediction.WordCount \
    ./target/spark-starter-0.1-SNAPSHOT.jar \
    ./story.txt
