package taxi;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

/**
 * Created by Evegeny on 05/04/2017.
 */
public class TaxiMain {
    public static void main(String[] args) {
	SparkConf conf = new SparkConf().setAppName("taxi").setMaster("local[*]");
	JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> stringJavaRDD = sc.textFile();


	final JavaRDD<String[]> splitted = sc.textFile("data/taxi_order.txt").map(row -> row.split(" ")).persist(StorageLevel.MEMORY_ONLY());
	final long count = splitted.count();

	final JavaPairRDD<String, Integer> bostonRdd = splitted
			.mapToPair(a -> new Tuple2<>(a[1].toLowerCase(), Integer.parseInt(a[2])))
			.filter(p -> p._1().equals("boston"));

	final Integer boston10 = bostonRdd
			.filter(p->p._2>10)
			.map(row -> 1)
			.reduce((i, j) -> i + j);

	final Integer bostonAll = bostonRdd.map(p -> p._2).reduce((i, j) -> i + j);

	final List<Tuple2<Integer, Integer>> top3 = splitted.mapToPair(row -> new Tuple2<>(Integer.valueOf(row[0]), Integer.valueOf(row[2]))).reduceByKey(Integer::sum)
			.mapToPair(Tuple2::swap)
			.sortByKey(false)
			.mapToPair(Tuple2::swap)
			.take(3);

	System.out.println(count);
	System.out.println(boston10);
	System.out.println(bostonAll);
	System.out.println(top3);

    }
}