package ch.epfl.advdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * This class implements the first mapReduce job of Solution 2 as described in the paper.
 * @author Bernard Gütermann
 *
 */
public class IterationUSol2 {

	/**
	 * The map task read lines of U and M files and emits rows of U, along with their corresponding rows
	 * of M to the reduces tasks. 
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorMapU extends Mapper<Text, Text, IntWritable, Text> {
		/*
		 *
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void map(Text key, Text value,  Context context) throws IOException, InterruptedException{
			//process the various inputs
			String[] words =  value.toString().split(",");
			String type = key.toString();
			int userID = Integer.parseInt(words[0]);
			//collect the inputs for the computation of both U and V
			if(type.equals("M")){
				Text out = new Text("M,"+words[1]+","+words[2]);
				context.write( new IntWritable(userID), out);
			}
			else if(type.equals("U")){
				Text out = new Text("U,"+words[1]+","+words[2]);
				context.write( new IntWritable(userID), out);
			}
			else throw new InterruptedException("Iteration U : Invalid mapper input: K:"+type+" V: "+words[0]);
		}
	}

	/**
	 * The reducer task takes as input all tuples of a row of of U along with the tuples
	 * of the corresponding row in M. 
	 * It constructs the data structures and read V from local memory
	 * It performs the algorithm as described in the paper
	 * it outputs the corresponding new row of U
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorReduceU extends Reducer<IntWritable, Text, Text, Text> {

		private double[][] V;
		
		/**
		 * here we load V in the local memory of the Reduce Worker 
		 * 
		 */
		@Override
		public void setup(Context context) throws IOException {
			V = getVMatrix(context.getConfiguration());
		}

		private double[][] getVMatrix(Configuration c) throws IOException{
			double[][] matrix = new double[Integer.valueOf(c.get("DIMENSIONS"))][Integer.valueOf(c.get("MOVIES"))];
			FileSystem fs = FileSystem.get(c);
			FileStatus[] status = fs.listStatus(new Path(c.get("VPATH")));  
			//For all files
			for (int i=0;i<status.length;i++){
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("part-")) {
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String line;
					line=br.readLine();
					while (line != null){
						String values[] = line.split(","); //get values
						//populate V
						matrix[Integer.valueOf(values[1])-1][Integer.valueOf(values[2])-1] = Double.parseDouble(values[3]);
						line = br.readLine();
					}
					br.close();
				}
			}
			//fs.close();
			return matrix;
		}
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//get iteration
			Configuration conf = context.getConfiguration();
			//int iteration = Integer.valueOf(conf.get("iteration"));
			//check if line of U &M :
			//get line of M and U and detect/put in order
			Iterator<Text> it = values.iterator();
			String[] line;
			Double[] U_r= new Double[Integer.valueOf(conf.get("DIMENSIONS"))];
			Double[] M_r= new Double[Integer.valueOf(conf.get("MOVIES"))];
			int lineNo = key.get();
			//construct the data containers for calculation
			while(it.hasNext()){
				line = it.next().toString().split(",");
				if(line[0].compareTo("M")==0){
					M_r[Integer.valueOf(line[1])-1] = Double.valueOf(line[2]);
				}
				else if (line[0].compareTo("U")==0){
					U_r[Integer.valueOf(line[1])-1] = Double.valueOf(line[2]);
				}
				else throw new InterruptedException("Iterator Reducer : Invalid value read from KV-pair: "+line[0]);
			}
			//for every line of u received :
			//case U line computation	
			double sum_s=0;	//squared sum
			//compute u_r*V for whole V
			double lineCol=0;
			double sum_j=0;
			for(int s = 0; s<U_r.length;s++){
				sum_j=0;
				sum_s=0;
				for (int j=0;j<V[0].length;j++){
					if(M_r[j] != null){			//only for non-blanks m_rm
						lineCol=0;
						for(int k = 0; k<V.length;k++){		//dimensions
							if (k!=s){
								lineCol+=U_r[k]*V[k][j];
							}
						}
						
						sum_j+=V[s][j]*(M_r[j]-lineCol);
						sum_s+=V[s][j]*V[s][j];
					}
				}
				//if this user has ratings
				if (sum_s!=0) U_r[s] = sum_j/sum_s;
				else U_r[s] = 0.0;
			}
			for(int s = 0; s<U_r.length;++s){
				//m_multipleOutputs.write("out",new Text("U"),new Text(String.valueOf(lineNo)+","
				//		+ String.valueOf(s+1)+","+String.valueOf(U_r[s])),"U-"+(iteration+1));
				context.write(new Text("U"),new Text(String.valueOf(lineNo)+","
						+ String.valueOf(s+1)+","+String.valueOf(U_r[s])));
			}
		}

	}

	/**
	 * @param args root inputs and outputs folders
	 * @param iteration
	 * @param DIMENSIONS
	 * @param MOVIES
	 * @param USERS
	 * @param REDUCERS
	 * @return 0 if success 1 if failure
	 * @throws Exception
	 */
	public int run(String[] args, int iteration, final int DIMENSIONS, final int MOVIES, 
			final int USERS, final int REDUCERS) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("DIMENSIONS", String.valueOf(DIMENSIONS));
		conf.set("MOVIES", String.valueOf(MOVIES));
		conf.set("USERS", String.valueOf(USERS));
		conf.set("iteration", String.valueOf(iteration));
		conf.set("VPATH", args[1]+"/V_"+iteration);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("key.value.separator.in.input.line", ",");

		Job job = new Job(conf, "iteration-"+iteration+"-U");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(IterationUSol2.class);
		job.setMapperClass(IteratorMapU.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(IteratorReduceU.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job,"out", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[1]+"/U_"+iteration+","+args[1]+"/M/M*");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/U_"+(iteration+1)));

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
