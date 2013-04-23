package ch.epfl.advdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
	
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class contains the mapReduce job that is in chage of handeling the preprocessing
 * The preprocessing consists of formating the input data M such that 
 * for each user, the score is normalised accross the movies he rated.
 * The reducer outputs for the set S of movies a user rated : 
 * the sum of all normalised ratings for a user, as well as the
 * cardinality of S
 * @author Bernard Gutermann
 *
 */
public class Preprocessing {

	/**
	 * input : the line of the input file
	 * output : key : the userID, value : the tuple containing the movieID 
	 * score and date
	 * @author Bernard Gütermann
	 *
	 */
	public static class PreprocessingMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable userID = new IntWritable();
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(",");
			userID = new IntWritable(Integer.parseInt(words[0]));
			Text out = new Text(words[1]+","+words[2]+","+words[3]);
			context.write(userID, out);
		}
	}
 	
	/**
	 * input : 	for a userID : every rating this user made (a line of M) : <r, {<m,s,d>}
	 * output : for a userID : {<"M",r,m,v>} with v being the input rating s, normalised accross all input 
	 * 			ratings this used madem. 
	 * output: 	for a user id : <r, sum, card> with sum : sum of all normalised ratings here avove
	 * 							card : the number of said ratings
	 * note : the date is dropped at this level : we take only one rating for one user for one movie (the first 
	 * 			read)
	 * @author Bernard Gütermann
	 *
	 */
	public static class PreprocessingReduce extends Reducer<IntWritable, Text, Text, Text> {
		
		private MultipleOutputs<Text, Text> m_multipleOutputs;

		@Override
		public void setup(Context context) {
			m_multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		@Override
		public void cleanup(Context context) throws IOException,
		InterruptedException {
			if (m_multipleOutputs != null) {
				m_multipleOutputs.close();
			}
		}
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			double count = 0;
			String[] line;
			HashMap<Integer, String> postings = new HashMap<Integer, String>(); 
			Iterator<Text> it=values.iterator();
			//here we store in a treemap of int as to have a sorted output posting file
			while (it.hasNext()) {
				line = it.next().toString().split(",");
				//avoid multiple dates for same film/user
				if(postings.put(Integer.parseInt(line[0]), line[1]) == null) {
					count++;
					sum+= Integer.parseInt(line[1]); //score sum
				}
			}
			double mean = sum/count;
			for (Entry<Integer, String> e : postings.entrySet()){
				m_multipleOutputs.write("M", new Text("M"), new Text(key.toString()+","+e.getKey().toString()+","
						+String.valueOf(Double.parseDouble(e.getValue())-mean)), "M-");
			}
			m_multipleOutputs.write("sum", key, new Text(String.valueOf(sum) +","+String.valueOf(count)), "sum-");
		}
	}
	
	/**
	 * runs the pre-processing job. Outputs the utility matrix M
	 * @param args input/output folders
	 * @param REDUCERS number of reducers for this job
	 * @return the mean value of M
	 * @throws Exception
	 */
	public Double run(String[] args, final int REDUCERS) throws Exception{
		Configuration conf = new Configuration();
		//Save params
		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = new Job(conf, "normalisation");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(Preprocessing.class);
		job.setMapperClass(PreprocessingMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(PreprocessingReduce.class);
		
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		MultipleOutputs.addNamedOutput(job,"M", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sum", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/M"));

		if (!job.waitForCompletion(true)) return null;
		//crawl results and return rmse
		return getMeanFromDFS(conf, new Path(args[1]+"/M"));
	}
	
	/**
	 * Parses the file containing the the sums for the values of each rows of 
	 * M, as well as the cardinality of these rows in order to output mean(M)
	 * @param conf context config object that contains the paths to files
	 * @param path path of the output of the normalisation mapreduce job
	 * @return The mean value of (normalized) M
	 * @throws IOException
	 */
	private double getMeanFromDFS(Configuration conf, Path path) throws IOException {
		System.out.println("ENDJOB: Aggregating .:"+path.toString());
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(path);
		//For all files
		double sum=0;
		double cardinality=0;
		String line;
		String[] v;
		String fileName[];
		for (int i=0;i<status.length;i++){
			fileName = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("sum-")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//get rmse and cardinality, discard keys
					v = line.split(",");
					sum+= Double.valueOf(v[1]); 
					cardinality+= Double.valueOf(v[2]); 
					line = br.readLine();
				}
				br.close();
			}
		}
		fs.close();
		return Math.sqrt(sum/cardinality);
	}
}