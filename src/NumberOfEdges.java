import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;	
/**
 * Class to find the number of edges in Twitter social graph
 * @author Adithya Balasubramanian
 *
 */
public class NumberOfEdges
{
    public static void main(String[] args)
    {
	SparkConf sparkConf = new SparkConf().setAppName("NumberOfEdges");
	JavaSparkContext spark = new JavaSparkContext(sparkConf);
	//read file directly from the s3 location
	JavaRDD<String> file = spark.textFile("s3n://f15-p42/twitter-graph.txt");
	JavaRDD lines = file.flatMap(new FlatMapFunction<String, String>()
	{
	  public Iterable<String> call(String s) { return Arrays.asList(s.split("\n")); }
	});
	//write key as "Edge" with every edge encountered and 1 as corresponding value.
	JavaPairRDD<String, Integer> pairs = lines.distinct().mapToPair(new PairFunction <String, String, Integer>() 
	{
	    @Override
	    public Tuple2<String, Integer> call(String s) throws Exception
	    {
		return new Tuple2<String, Integer>("Edge", 1);
	    }
	});
	//sum all values corresponding to a particular key, "Edge" in this case, to find the number of edges.
	JavaPairRDD<?, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	  public Integer call(Integer a, Integer b) { return a + b; }
	});
	//location to write output
	counts.saveAsTextFile("hdfs:///outputTask1_1");
	spark.stop();
    }
}
