import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**
 * Class to find the number of followers for each user in the Twitter social graph.
 * @author Adithya Balasubramanian
 *
 */
public class NumberOfFollowers
{
    public static void main(String[] args)
    {
	SparkConf sparkConf = new SparkConf().setAppName("NumberOfFollowers");
	JavaSparkContext spark = new JavaSparkContext(sparkConf);
	//read file directly from the s3 location.
	JavaRDD<String> file = spark.textFile("s3n://f15-p42/twitter-graph.txt");
	//read 2nd column (user who is being followed) of every line
	JavaRDD lines = file.flatMap(new FlatMapFunction<String, String>()
	{
	  public Iterable<String> call(String s) 
	  { 
	      String[] list = s.split("\n");
	      ArrayList<String> followerList = new ArrayList<String>();
	      for(int i=0;i<list.length; i++)
	      {
		  String[] user = list[i].split(" ");
		  followerList.add(user[1]);
		  
	      }
	      return followerList; 
	  }
	});
	//key as the user and value as 1.
	JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction <String, String, Integer>() 
	{
	    @Override
	    public Tuple2<String, Integer> call(String s) throws Exception
	    {
		return new Tuple2<String, Integer>(s, 1);
	    }
	});
	//count number of followers of user based on the user key.
	JavaPairRDD<?, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	  public Integer call(Integer a, Integer b) { return a + b; }
	});
	//location to write output
	counts.saveAsTextFile("hdfs:///outputTask2");	
	
	spark.stop();
    }
}
