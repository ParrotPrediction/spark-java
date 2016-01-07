package com.parrotprediction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: WordCount <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("word-count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load input file
        JavaRDD<String> lines = sc.textFile(args[0], 1);

        // Split up into words
        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));

        // Transform into pairs and count
        JavaPairRDD<String, Integer> count =
            words
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

        List<Tuple2<String, Integer>> output = count.collect();

        System.out.println("\n>>>RESULTS START\n");
        output.forEach( t -> System.out.println(t._1() + ":" + t._2()));
        System.out.println("\n>>>RESULTS END\n");

        sc.close();
    }
}
