package id2221.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	// map receives the input throught the value and read it line by line (default) 
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// read the line and transform the xml line in a map with the helper function	
		Map <String, String> mapParsed = TopTen.transformXmlToMap(value.toString());

	    try {
	    	// get the id and reputation of the user (processed record)
			String id = mapParsed.get("Id");
			String reputation = mapParsed.get("Reputation");

			// only check the rows with user, the other discarded
			if (id != null) {
				// put the record in the map having reputation as key, converted to integer (TreeMap is a map that sorts on key)
				repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

				// remove the key with lowest value if the size of the map is more than 10 since we want the topten 
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	// Output our ten records to the reducers with a null key
	protected void cleanup(Context context) throws IOException, InterruptedException {

	    // iterate over the values of the map (the Text information) and write to the reducers using a key null
	    try {
	    	for (Text info : repToRecordMap.values()) {
	    		context.write(NullWritable.get(), info);
	    	}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	// 
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    try {

	    	for (Text value : values) {
	    		Map <String, String> mapParsed = TopTen.transformXmlToMap(value.toString());
	    		String reputation = mapParsed.get("Reputation");

	    		// put the record in the map having reputation as key, converted to integer (TreeMap is a map that sorts on key)
	    		repToRecordMap.put(Integer.parseInt(reputation), new Text(value));

				// remove the key with lowest value if the size of the map is more than 10 since we want the topten 
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
	    	}

	    	// reverse order to have everything in order of reputation since TreeMap order by key
	    	for (Text value : repToRecordMap.descendingMap().values()) {
	    		Map <String, String> reverseMapParsed = TopTen.transformXmlToMap(value.toString());

	    		String rep = reverseMapParsed.get("Reputation");
	    		String id = reverseMapParsed.get("Id");
	    	
	    		// create and insert value in HBase table 
	    		Put insHBase = new Put(rep.getBytes());
	    		insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));
	    		insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id)); 

	    		// write the value to Hbase table previously defined
	    		context.write(null, insHBase);
			}
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }   	
	} 	
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "TopTen");
		
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);

		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// force a single reducer
		job.setNumReduceTasks(1);

		// set input file
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// define the output table in HBase
		job.waitForCompletion(true);
    }
}



