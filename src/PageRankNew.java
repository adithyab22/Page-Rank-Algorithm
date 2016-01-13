import scala.Tuple2;

import com.google.common.collect.Iterables;
import java.util.TreeSet;
import org.apache.spark.storage.StorageLevel;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

/**
 * Program to rank each user by influence in the twitter social graph.
 * @author Adithya Balasubramanian
 *
 */

public class PageRankNew {
	final static long totalNodes = 2546953;

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile("s3n://f15-p42/twitter-graph.txt"); 
		// Loads in input file. It should be in format of:
        //     u1    v1
        //     u2    v2
        //     u3    v3
        //     ...
        //where user u(follower) follows user v(followee)
		
		//get all vertices in the graph
		JavaRDD vertices = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				String[] pairs = s.split("\n");
				TreeSet<String> vertices = new TreeSet<String>();
				for (String pair : pairs) {
					String[] vertexArray = pair.split(" ");
					for (String vert : vertexArray) {
						vertices.add(vert);
					}
				}
				return vertices;
			}
		});
		
		//get vertices of follower
		JavaRDD follower = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				String[] pairs = s.split("\n");
				ArrayList<String> vertices = new ArrayList<String>();
				for (String pair : pairs) {
					String[] vertice = pair.split(" ");
					if (vertice.length > 1) {
						vertices.add(vertice[0]);
					}
				}
				return vertices;
			}
		});
		
		//get vertices of followee
		JavaRDD followee = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				String[] pairs = s.split("\n");
				ArrayList<String> vertices = new ArrayList<String>();
				for (String pair : pairs) {
					String[] vertexArray = pair.split(" ");
					if (vertexArray.length > 1) {
						vertices.add(vertexArray[1]);
					}
				}
				return vertices;
			}
		});
		
		//Determine dangling users - dangling users are users who do not follow anyone
		JavaRDD<String> danglingusers = vertices.distinct().subtract(follower.distinct());
		
		follower.persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		//map each user to empty list
		JavaPairRDD<String, Iterable<String>> danglingUsersMap = danglingusers
				.mapToPair(new PairFunction<String, String, String>() {
					@Override
					public Tuple2<String, String> call(String s) {
						return new Tuple2<String, String>(s, "");
					}
				}).distinct().groupByKey();
		danglingusers.persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		
		//map follower to users that are following
		JavaPairRDD<String, Iterable<String>> followerMap = lines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] parts = s.split(" ");
				return new Tuple2<String, String>(parts[0], parts[1]);
			}
		}).distinct().groupByKey().union(danglingUsersMap);
		
		//set default rank
		JavaPairRDD<String, Double> rank = followerMap
				.mapValues(new Function<Iterable<String>, Double>() {
					@Override
					public Double call(Iterable<String> followedby) {
						return 1.0;
					}
				});
		
		//perform 10 iterations
		for (int i = 0; i < 10; i++) {
			JavaPairRDD<String, Double> followerContribution = followerMap.join(rank).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
							List<Tuple2<String, Double>> contributionFactor = null;
							//find contribution factor of non-dangling users
							if (s._1 != null || s._1.iterator().hasNext()) {
								int countContributingTo = 0;
								countContributingTo = Iterables.size(s._1);
								//count number of followees
								contributionFactor = new ArrayList<Tuple2<String, Double>>();
								//calculate contribution factor
								for (String neighbour : s._1) {
									if (!neighbour.equals(""))
										contributionFactor.add(
												new Tuple2<String, Double>(neighbour, s._2() / countContributingTo));
								}
							}
							return contributionFactor;
						}
					});
			
		//redistribute dangling user weight across all users
		final double dangling = danglingUsersMap.join(rank)
				.mapToDouble(new DoubleFunction<Tuple2<String, Tuple2<Iterable<String>, Double>>>() {
				@Override
				public double call(Tuple2<String, Tuple2<Iterable<String>, Double>> s) throws Exception {
						return s._2._2;
					}
				}).reduce(new Function2<Double, Double, Double>() {
					@Override
					public Double call(Double x1, Double x2) throws Exception {
						return x1 + x2;
					}
				});
			 
			//sum up the contributions received
		 	rank = followerContribution
				.reduceByKey(new Function2<Double, Double, Double>() {
				//sum of contributions from every follower
				@Override
				public Double call(Double x1, Double x2) throws Exception {
					return x1 + x2;
				}
			}).mapValues(new Function<Double, Double>() {
						
				//calculate rank
				@Override
				public Double call(Double sum) throws Exception {							
					 return 0.15 + (sum + dangling / totalNodes) * 0.85;
				}
			});
		}
		rank.saveAsTextFile("hdfs:///output_Task3_new");
		ctx.stop();
	}
}