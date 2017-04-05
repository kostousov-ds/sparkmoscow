package songs;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SongsMain {
    public static void main(String[] args) {
	SparkConf conf = new SparkConf().setAppName("taxi").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);

	final JavaRDD<String> song = sc.textFile("data/songs/califronia.txt");

	song.flatMap(line -> Arrays.asList(line.split("[,. ]+")))
			.filter(w->w.length()>0)
			.mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1))
			.reduceByKey(Integer::sum)
			.mapToPair(Tuple2::swap)
			.sortByKey(false)
			.mapToPair(Tuple2::swap)
			.take(10)
			.forEach(System.out::println);


    }
}
