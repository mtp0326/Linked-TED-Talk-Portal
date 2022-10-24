package edu.upenn.cis.nets2120.hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

public class LoadNetwork {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(LoadNetwork.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	Table talks;
	
	CSVParser parser;
	
	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	/**
	 * Helper function: swap key and value in a JavaPairRDD
	 * 
	 * @author zives
	 *
	 */
	static class SwapKeyValue<T1,T2> implements PairFunction<Tuple2<T1,T2>, T2,T1> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T2, T1> call(Tuple2<T1, T2> t) throws Exception {
			return new Tuple2<>(t._2, t._1);
		}
		
	}
	
	
	public LoadNetwork() {
		System.setProperty("file.encoding", "UTF-8");
		parser = new CSVParser();
	}
	
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			talks = db.createTable("ted_talks", Arrays.asList(new KeySchemaElement("talk_id", KeyType.HASH)), // Partition
																												// key
					Arrays.asList(new AttributeDefinition("talk_id", ScalarAttributeType.N)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			talks.waitForActive();
		} catch (final ResourceInUseException exists) {
			talks = db.getTable("ted_talks");
		}
	}
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		initializeTables();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	
	/*
		CODE: the filePath is split into String[] which has the nodeID in the first column and the follower nodeID in the
	 	second column. We use mapToPair to extract the arrays into separate tuple spots.
	*/
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		// Read into RDD with lines as strings
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split(" "));
		
		JavaPairRDD<Integer,Integer> tweetPairID = file.mapToPair(line -> {
			int nodeID = Integer.parseInt(line[0]);
			int followerID = Integer.parseInt(line[1]);
			
			return new Tuple2<Integer,Integer>(nodeID, followerID);
		});
		
    return tweetPairID;
    
	}

	/**
	 * Returns an RDD of parsed talk data
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	
	/*
		CODE: created a new schema with all the columns. Because we receive column names we make sure to identify
		the real index of the columns and rightly order them by storing them to tedType[] array.
		Then when the next reader is called with the data, we are able to store the right types
		in order using the index from tedType[].
		We then parallelStream and map the data into an RDD.
	 */
	JavaRDD<Row> getTalks(String filePath) throws IOException {
		CSVReader reader = null;
		Reader fil = null;
		
		int[] tedType = new int[9];
		
		StructType tedSchema = new StructType()
				.add("talk_id", "int")
				.add("description", "string")
				.add("title", "string")
				.add("speaker_1", "string")
				.add("views", "int")
				.add("duration", "int")
				.add("topics", "string")
				.add("related_talks", "string")
				.add("url", "string");
		List<String[]> tedTalks = new ArrayList<>();

		try {
			fil = new BufferedReader(new FileReader(new File(filePath)));
			reader = new CSVReader(fil);
			// Read + ignore header
			try {
				String[] arr;
				arr = reader.readNext();
				int index = 0;
				
				for(String tmp: arr) {
					if(tmp.equals("talk_id")) {
						tedType[0] = index;
					}
					else if(tmp.equals("description")) {
						tedType[1] = index;
					}
					else if(tmp.equals("title")) {
						tedType[2] = index;
					}
					else if(tmp.equals("speaker_1")) {
						tedType[3] = index;
					}
					else if(tmp.equals("views")) {
						tedType[4] = index;
					}
					else if(tmp.equals("duration")) {
						tedType[5] = index;
					}
					else if(tmp.equals("topics")) {
						tedType[6] = index;
					}
					else if(tmp.equals("related_talks")) {
						tedType[7] = index;
					}
					else if(tmp.equals("url")) {
						tedType[8] = index;
					}
					index++;
				}
				
			} catch (CsvValidationException e) {
				// This should never happen but Java thinks it could
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	
			String[] nextLine = null;
			String[] tedTalkArr = new String[9];
			try {
				do {
					try {
						
						nextLine = reader.readNext();
						if(nextLine != null) {
							tedTalkArr = new String[9];
							for(int i = 0; i < 9; i++) {
								tedTalkArr[i] = nextLine[tedType[i]];
							}
							tedTalks.add(tedTalkArr);
						}
					} catch (CsvValidationException e) {
						e.printStackTrace();
					}					
				} while (nextLine != null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			if (reader != null)
				reader.close();
			
			if (fil != null)
				fil.close();
		}
		
		List<Row> listTedTalks = tedTalks.parallelStream()
				.map(talk -> {
					return new GenericRowWithSchema(talk, tedSchema);
				}).collect(Collectors.toList());
		JavaRDD<Row> tedTalksRDD = context.parallelize(listTedTalks);
		
		return tedTalksRDD;
	}
	
	// CODE: the function has two RDDs and cogroups the RDD with the smaller size then returns the simplified tuple
	public static JavaPairRDD<Row, Integer>
	smallerRDDCogroup(JavaPairRDD<Long,Row> rdd1, JavaPairRDD<Long, Integer> rdd2) {
		if((int) rdd1.count() <= (int) rdd2.count()) {
			return (rdd1.cogroup(rdd2)).filter(iter -> {
				return iter._2()._1().iterator().hasNext() && iter._2()._2().iterator().hasNext();
			}).mapToPair(row -> {
				return new Tuple2<Row, Integer>(row._2()._1().iterator().next(), row._2()._2().iterator().next());
			});
		}
		return rdd2.cogroup(rdd1).filter(iter -> {
			return iter._2()._1().iterator().hasNext() && iter._2()._2().iterator().hasNext();
		}).mapToPair(row -> {
			return new Tuple2<Row, Integer>(row._2()._2().iterator().next(), row._2()._1().iterator().next());
		});
	}
	
	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Running");

		// Load + store the TED talks
		JavaRDD<Row> tedTalks = this.getTalks(Config.TED_TALK_PATH);

		// Load the social network
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH);
		
		/*
			CODE: We look at the number of times the key has been called by making the value into 1 and reduceByKey to count
			the frequency of keys. We then swap the key and value to sortedTweetPairID from countTweetFollowers
			to sort the lists in descending array based on the frequency in key.
		*/
		JavaPairRDD<Integer,Integer> countTweetFollowers = network.mapToPair(iter -> {
			return new Tuple2<>(iter._1(), 1);
		}).reduceByKey((a,b) -> a+b);
		
		JavaPairRDD<Integer,Integer> sortedTweetPairID = countTweetFollowers.mapToPair(iter -> {
			return new Tuple2<>(iter._2(), iter._1());
		}).sortByKey(false, Config.PARTITIONS);
		
		// CODE: the function takes in the tedTalks and sorts in descending order with 5 partitions
		JavaRDD<Row> sortedTedTalks = tedTalks.sortBy(row -> row.getAs("views"), false, Config.PARTITIONS);
		
		/*
			CODE: if sortedTedTalks is not empty, we partition sortedTedTalks in each iterator
			where each row is assigned to an item with the columns and gets added into queue
			similar to assignment1, we store the queue into dynamoDB when the queue exceeds size 25
			and takes in unprocessed items.
		*/
		if(!sortedTedTalks.isEmpty()) {
			HashSet<Item> queue = new HashSet<>();
			sortedTedTalks.foreachPartition(iter -> {
				DynamoDB db2 = DynamoConnector.getConnection(Config.DYNAMODB_URL);
				while(iter.hasNext()) {
					//iter is a pointer to the datapoint
					//iter.next() actually brings the data from the datapoint
					
					// CODE: assigning each machine their db connection in partition
					Row rowTalk = iter.next();
					Item item = new Item()
							.withPrimaryKey("talk_id", Integer.parseInt(rowTalk.getAs("talk_id")))
							.withString("description", rowTalk.getAs("description").toString())
							.withString("title", rowTalk.getAs("title").toString())
							.withString("speaker_1", rowTalk.getAs("speaker_1").toString())
							.withInt("views", Integer.parseInt(rowTalk.getAs("views"))) //toInt()???
							.withInt("duration", Integer.parseInt(rowTalk.getAs("duration")))
							.withString("topics", rowTalk.getAs("topics").toString())
							.withString("related_talks", rowTalk.getAs("related_talks").toString())
							.withString("url", rowTalk.getAs("url").toString());
					queue.add(item);
					
					if(queue.size() == 24) {
						TableWriteItems tableWriteItems = new TableWriteItems
								("ted_talks").withItemsToPut(queue);
						BatchWriteItemOutcome outcome = db2.batchWriteItem(tableWriteItems);
						
						while(outcome.getUnprocessedItems().size() != 0) {
							outcome = db2.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
						}
						queue.clear();
					}
					
					if(queue.size() != 0 && !iter.hasNext()) {
						TableWriteItems tableWriteItems = new TableWriteItems
								("ted_talks").withItemsToPut(queue);
						BatchWriteItemOutcome outcome = db2.batchWriteItem(tableWriteItems);
						
						while(outcome.getUnprocessedItems().size() != 0) {
							outcome = db2.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
						}
						queue.clear();
					}
				}
			});
			
		}
		
		/*
			CODE: We index the tweet and tedtalk RDDs then swap the value to the key so that we can combine the keys in cogroup.
			We then simplify the type of the RDD to <Iterable<Row>, Iterable<Integer>> because we do not need the Long index. 
			We also limit the size to the RDD with the smallest row size while combining RDDs.
			Combining the RDDs, we use foreachPartition to iterate through each row and update the table 
			creating a new column called "social_id" through updateItem.

		*/
		JavaRDD<Integer> sortedTweetKey = sortedTweetPairID.map(list -> (list._2()));
		
		JavaPairRDD<Long,Row> sortedTedZip = sortedTedTalks.zipWithIndex().mapToPair(row -> {
			return new Tuple2<Long,Row>(row._2(), row._1());
		});
		
		JavaPairRDD<Long,Integer> sortedTweetZip = sortedTweetKey.zipWithIndex().mapToPair(row -> {
			return new Tuple2<Long,Integer>(row._2(), row._1());
		});
		
		
		JavaPairRDD<Row, Integer> combineRDD = smallerRDDCogroup(sortedTedZip, sortedTweetZip);
		
		/*	
		 	CODE: for each iteration of combineRDD the db and table is called and the rows are updated with
		 	a new column social_id from the node ID in the twitter. The item is then updated to the table.
		 */
		
		combineRDD.foreachPartition(iter -> {
			DynamoDB db1 = DynamoConnector.getConnection(Config.DYNAMODB_URL);
			Table talks1;
			
			try {
				talks1 = db1.createTable("ted_talks", Arrays.asList(new KeySchemaElement("talk_id", KeyType.HASH)), // Partition
																													// key
						Arrays.asList(new AttributeDefinition("talk_id", ScalarAttributeType.N)),
						new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

				talks1.waitForActive();
			} catch (final ResourceInUseException exists) {
				talks1 = db1.getTable("ted_talks");
			}
			
			while(iter.hasNext()) {
				Tuple2<Row, Integer> iteration = iter.next();
				Row r1 = iteration._1();
				Integer r2 = iteration._2();
				AttributeUpdate newCo = new AttributeUpdate("social_id").addNumeric(r2);
				
				UpdateItemSpec uis = new UpdateItemSpec()
						.withPrimaryKey("talk_id", Integer.parseInt(r1.getAs("talk_id")))
						.withAttributeUpdate(newCo);
				talks1.updateItem(uis);
			}
		});
	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");
		
		DynamoConnector.shutdown();
		
		if (spark != null)
			spark.close();
	}
	
	public static void main(String[] args) {
		final LoadNetwork ln = new LoadNetwork();

		try {
			ln.initialize();

			ln.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			ln.shutdown();
		}
	}

}
