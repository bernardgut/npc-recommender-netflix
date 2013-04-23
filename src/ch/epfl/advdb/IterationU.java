package ch.epfl.advdb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * This class implements the first mapReduce job of Solution 1 as described in the paper.
 * @author Bernard Gütermann
 *
 */
public class IterationU {
	/**
	 * The map task read lines of U V and M files and emits chunks of rows of U along with their corresponding rows
	 * of M to the reduces tasks. The Mapper also emits a duplicate of V to each reduce task.
	 * The number of reduce tasks is 88*r_f, where r_f is the number of reduce task that a reducer will handle
	 *  We use round robin to distribute accross the key space
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorMapU extends Mapper<Text, Text, IntWritable, Text> {
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void map(Text key, Text value,  Context context) throws IOException, InterruptedException{
			int nreducers = Integer.valueOf(context.getConfiguration().get("REDUCERS"));
			int repFactor=  Integer.valueOf(context.getConfiguration().get("RF"));
			//process the various inputs
			String type = key.toString();
			String[] words =  value.toString().split(",");
			int userID = Integer.parseInt(words[0]);
			//collect the inputs for the computation of both U and V
			if(type.equals("M")){
				Text out = new Text("M,"+words[0]+","+words[1]+","+words[2]);
				context.write( new IntWritable(userID%(nreducers*repFactor)), out);
			}
			else if(type.equals("U")){
				Text out = new Text("U,"+words[0]+","+words[1]+","+words[2]);
				context.write( new IntWritable(userID%(nreducers*repFactor)), out);
			}
			//IF V : send V to everyone
			else if(type.equals("V")){
				Text out = new Text("V,"+words[0]+","+words[1]+","+words[2]);
				for (int i=0;i<(nreducers*repFactor);++i)
					context.write( new IntWritable(i), out);
			}
			else throw new InterruptedException("Iteration U : Invalid mapper input: K:"+type+" V: "+words[0]);
		}
	}

	/**
	 * The reducer task takes as input a chunk of rows of of U along with their corresponding rows in M. 
	 * It also takes as input V. The reducer first parses the tuples and construct the data structures,
	 * then performs the algorithm as described in the paper
	 * and for each row of the chunk, it outputs the corresponding new row of U
	  * @author Bernard Gütermann
	 *
	 */
	public static class IteratorReduceU extends Reducer<IntWritable, Text, Text, Text> {

		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//get iteration
			Configuration conf = context.getConfiguration();
			//check if line of U &M :
			//get line of M and U and detect/put in order
			Iterator<Text> it = values.iterator();
			Double [][] V = new Double[Integer.valueOf(conf.get("DIMENSIONS"))][Integer.valueOf(conf.get("MOVIES"))];
			HashMap<Integer, Double[]> M = new HashMap<Integer, Double[]>();
			HashMap<Integer, Double[]> U = new HashMap<Integer, Double[]>();
			String[] line;
			int lineNo;
			//construct the data containers for calculation
			while(it.hasNext()){
				line = it.next().toString().split(",");
				lineNo=Integer.valueOf(line[1]);
				if(line[0].compareTo("M")==0){
					if(M.get(lineNo-1) == null)
						M.put(lineNo-1, new Double[Integer.valueOf(conf.get("MOVIES"))]);
					M.get(lineNo-1)[Integer.valueOf(line[2])-1] = Double.valueOf(line[3]);
				}
				else if (line[0].compareTo("U")==0){
					if(U.get(lineNo-1) == null)
						U.put(lineNo-1, new Double[Integer.valueOf(conf.get("DIMENSIONS"))]);
					U.get(lineNo-1)[Integer.valueOf(line[2])-1] = Double.valueOf(line[3]);
				}
				else if (line[0].compareTo("V")==0){
					V[Integer.valueOf(line[1])-1][Integer.valueOf(line[2])-1]=Double.parseDouble(line[3]);
				}
				else throw new InterruptedException("Iterator Reducer : Invalid value read from KV-pair: "+line[0]);
			}
			Double[] vectU;
			Double[] vectM;
			//for every line of u received :
			for (Entry<Integer, Double[] > e : U.entrySet()){
				//case U line computation
				lineNo = e.getKey();
				vectU = e.getValue();
				vectM = M.get(lineNo);
				//squared sum
				double sum_s=0;
				//compute u_r*V for whole V
				double lineCol=0;
				double sum_j=0;

				for(int s = 0; s<vectU.length;s++){
					sum_j=0;
					sum_s=0;
					//if this user has ratings
					if (vectM!=null){						//only if there is a rating in M
						for (int j=0;j<V[0].length;j++){
							if(vectM[j] != null){			//only for non-blanks m_rm
								lineCol=0;
								for(int k = 0; k<V.length;k++){		//dimensions
									if (k!=s){
										lineCol+=vectU[k]*V[k][j];
									}
										
								}
								sum_j+=V[s][j]*(vectM[j]-lineCol);
								sum_s+=V[s][j]*V[s][j];
							}
						}
					}
					if (sum_s!=0) vectU[s] = sum_j/sum_s;
					else vectU[s] = 0.0;
				}
				for(int s = 0; s<vectU.length;++s){
					context.write(new Text("U"),new Text(String.valueOf(lineNo+1)+","
							+ String.valueOf(s+1)+","+String.valueOf(vectU[s])));
				}
			}
		}
	}

	/**
	 * Runs the first mapReduce job for the iteration.
	 * @param args root inputs and outputs folders
	 * @param iteration 
	 * @param DIMENSIONS
	 * @param MOVIES
	 * @param USERS
	 * @param REDUCERS
	 * @param REPLICATION_FACTOR
	 * @return 0 if success 1 if fail
	 * @throws Exception
	 */
	public int run(String[] args, int iteration, final int DIMENSIONS, final int MOVIES, 
			final int USERS, final int REDUCERS, final int REPLICATION_FACTOR) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("REDUCERS", String.valueOf(REDUCERS));
		conf.set("DIMENSIONS", String.valueOf(DIMENSIONS));
		conf.set("MOVIES", String.valueOf(MOVIES));
		conf.set("USERS", String.valueOf(USERS));
		conf.set("iteration", String.valueOf(iteration));
		conf.set("RF", String.valueOf(REPLICATION_FACTOR));
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("key.value.separator.in.input.line", ",");
		
		Job job = new Job(conf, "iteration-"+iteration+"-U");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(IterationU.class);
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

		FileInputFormat.addInputPaths(job, args[1]+"/V_"+iteration+","+args[1]+"/U_"+iteration+","+args[1]+"/M/M*");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/U_"+(iteration+1)));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
}