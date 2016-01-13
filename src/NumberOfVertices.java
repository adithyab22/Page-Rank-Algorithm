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
 * Class to find the number of vertices in Twitter social graph
 * @author Adithya Balasubramanian
 *
 */
public class NumberOfVertices
{
    public static void main(String[] args)
    {
	SparkConf sparkConf = new SparkConf().setAppName("NumberOfVertices");
	JavaSparkContext spark = new JavaSparkContext(sparkConf);
	//read file directly from the s3 location
	JavaRDD<String> file = spark.textFile("s3n://f15-p42/twitter-graph.txt");	
	
	JavaRDD lines = file.flatMap(new FlatMapFunction<String, String>()
	{
	  public Iterable<String> call(String s) 
	  { 
	      String[] list = s.split("\n");
	      ArrayList<String> vertices = new ArrayList<String>();
	      for(int i=0;i<list.length; i++)
	      {
	      //take out each node from the list
		  String[] vertex = list[i].split(" ");
		  for(int j= 0; j < vertex.length; j++)
		  {
		      vertices.add(vertex[j]);
		  }
	      }
	      return vertices; 
	  
	  }
	});

	JavaPairRDD<String, Integer> pairs = lines.distinct().mapToPair(new PairFunction <String, String, Integer>() 
	{
		//write key as "Vertex" with every vertex encountered and 1 as corresponding value.
	    @Override
	    public Tuple2<String, Integer> call(String s) throws Exception
	    {
		return new Tuple2<String, Integer>("Vertex", 1);
	    }
	});
	//sum all values corresponding to a particular key, "Edge" in this case, to find the number of edges.
	JavaPairRDD<?, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
	  public Integer call(Integer a, Integer b) { return a + b; }
	});
	//location to write output
	counts.saveAsTextFile("hdfs:///outputTask1_2");
	spark.stop();
    }
}
