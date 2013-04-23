package ch.epfl.advdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class implements the second mapReduce job of Solution 1 as described in the paper.
 * @author Bernard Gütermann
 *
 */
public class IterationV {
	
	/**
	 * The map task read lines of U V and M files, and emits chunks of columns of V along with their corresponding 
	 * columns of M to the reduces tasks. The Mapper also emits a duplicate of U to each reduce task.
	 * The number of reduce tasks is 88*r_f, where r_f is the number of reduce task that a reducer will handle
	 * We use round robin to distribute accross the key space
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorMapV extends Mapper<Text, Text, IntWritable, Text> {
		/*
		 *
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
			int movieID =Integer.parseInt(words[1]);
			//collect the inputs for the computation of both U and V
			if(type.equals("M")){
				Text out = new Text("M,"+words[0]+","+words[1]+","+words[2]);
				context.write( new IntWritable(movieID%(nreducers*repFactor)), out);
			}
			else if(type.equals("V")){
				Text out = new Text("V,"+words[0]+","+words[1]+","+words[2]);
				context.write( new IntWritable(movieID%(nreducers*repFactor)), out);
			}
			//if u send to everyone
			else if(type.equals("U")){
				Text out = new Text("U,"+words[0]+","+words[1]+","+words[2]);
				for (int i=0;i<(nreducers*repFactor);++i)
					context.write( new IntWritable(i), out);
			}
			else throw new InterruptedException("Iteration V : Invalid mapper input: K:"+type+" V: "+words[0]);
		}
	}

	/**
	 * The reducer task takes as input a chunk of cols of of V along with thier corresponding cols of M. 
	 * It also takes as input U. The reducer recieve a set of tuples so it first needs to construct the
	 * data structures that will be used for computation. It then perform the algorithm as described in 
	 * the paper.
	 * and for each column of the chunk, it outputs the corresponding new column of V
	 * It also outputs the rmse for each of the cols along with the number of non blanks in M for each of the
	 * cols.
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorReduceV extends Reducer<IntWritable, Text, Text, Text> {

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
			Double [][] U = new Double[Integer.valueOf(conf.get("USERS"))][Integer.valueOf(conf.get("DIMENSIONS"))];
			HashMap<Integer, Double[] > M = new HashMap<Integer, Double[]>();
			HashMap<Integer, Double[] > V = new HashMap<Integer, Double[]>();
			String[] line;
			int colNo;
			//construct the data containers for calculation
			while(it.hasNext()){
				line = it.next().toString().split(",");
				colNo=Integer.valueOf(line[2]);
				if (line[0].compareTo("M")==0){
					if(M.get(colNo-1) == null)
						M.put(colNo-1, new Double[Integer.valueOf(conf.get("USERS"))]);
					M.get(colNo-1)[Integer.valueOf(line[1])-1] = Double.valueOf(line[3]);
				}
				else if (line[0].compareTo("V")==0){
					if(V.get(colNo-1)==null)
						V.put(colNo-1, new Double[Integer.valueOf(conf.get("DIMENSIONS"))]);
					V.get(colNo-1)[Integer.valueOf(line[1])-1] = Double.valueOf(line[3]);
				}
				else if (line[0].compareTo("U")==0){
					U[Integer.valueOf(line[1])-1][Integer.valueOf(line[2])-1]=Double.parseDouble(line[3]);
				}
				else throw new InterruptedException("Iterator Reducer : Invalid value read from KV-pair: "+line[0]);
			}
			Double[] vectV; //a column of V
			Double[] vectM; //a column of M
			//for every column of v received :
			for (Entry<Integer, Double[]> e : V.entrySet()){
				//case V col computation
				colNo = e.getKey();
				vectV = e.getValue();
				vectM = M.get(colNo);
				
				//squared sum
				double sum_s = 0;
				//compute u_r*V for whole V
				double lineCol=0;
				double sum_i=0;				
				for(int r = 0; r<vectV.length;r++){
					sum_i=0;
					sum_s=0;
					//if there is a rating for this film from the user
					if(vectM!=null){
						for (int i=0;i<U.length;i++){
							if(vectM[i] != null){			//only for non-blanks m_rm
								lineCol=0;
								for(int k = 0; k<U[0].length;k++){		//dimensions
									if (k!=r){
										lineCol+=vectV[k]*U[i][k];
									}	
								}
								sum_i+=U[i][r]*(vectM[i]-lineCol);
								sum_s+=U[i][r]*U[i][r];
							}
						}
					}
					//write to disk the new Value of V. 
					if (sum_s!=0) vectV[r] = sum_i/sum_s; 
					else vectV[r] = 0.0;
//					m_multipleOutputs.write("out",new Text("V"), new Text(String.valueOf(r+1)+","
//							+String.valueOf(colNo+1)+","+String.valueOf(vectV[r])),"V-"+(iteration+1));
					context.write(new Text("V"), new Text(String.valueOf(r+1)+","
							+String.valueOf(colNo+1)+","+String.valueOf(vectV[r])));

				}
//				//compute Squared Error
//				double se=0;
//				double n=0;
//				if(vectM!=null){
//					for (int r=0;r<U.length;r++){
//						if(vectM[r] != null){			//only for non-blanks m_rm
//							lineCol=0;
//							//count number of non-blancs un this line
//							n++;
//							//compute the value of UxV for line r col m
//							for(int d = 0; d<U[0].length;d++){		//dimensions
//								lineCol+=vectV[d]*U[r][d];
//							}
//							//add the square error for this value to SE of the line
//							se+=Math.pow((vectM[r]-lineCol),2); 
//						}
//					}
//				}
//				//and rmse of this col along with number of non-blanks in this col
//				m_multipleOutputs.write("rmse", new Text(String.valueOf(se)), new Text(String.valueOf(n)),"RMSE-"+(iteration+1));
			}
		}
	}

	
	/**
	 * Runs the second mapReduce job for the given iteration
	 * @param args the root inputs and outputs paths
	 * @param iteration
	 * @param DIMENSIONS
	 * @param MOVIES
	 * @param USERS
	 * @param REDUCERS
	 * @param REPLICATION_FACTOR
	 * @return the RMSE value between new UV and M; at the end this iteration
	 * @throws Exception
	 */
	public double run(String[] args, int iteration, final int DIMENSIONS, final int MOVIES, 
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
		
		Job job = new Job(conf, "iteration-"+iteration+"-V");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(IterationV.class);
		job.setMapperClass(IteratorMapV.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(IteratorReduceV.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);
		//MultipleOutputs.addNamedOutput(job,"out", TextOutputFormat.class, Text.class, Text.class);
		//MultipleOutputs.addNamedOutput(job, "rmse", TextOutputFormat.class, Text.class, Text.class);

		FileInputFormat.addInputPaths(job, args[1]+"/U_"+(iteration+1)+","+args[1]+"/V_"+iteration+","+args[1]+"/M/M*");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/V_"+(iteration+1)));

		//if (!job.waitForCompletion(true)) return -1;
		//crawl results and return rmse
		//return getRmsFromDFS(conf, new Path(args[1]+"/V_"+(iteration+1)));
		return (job.waitForCompletion(true) ? 0 : -1);
	}

	/**
	 * Parses the file containing the the rmse values for each columns of 
	 * M, as well as the cardinality of these columns in order to output RMSE(UV, M)
	 * @param c config object that contains the paths to files
	 * @param rmse path to the output of the IterationV mapreduce job
	 * @return the RMSE after the current iteration
	 * @throws IOException
	 */
	private double getRmsFromDFS(Configuration c, Path rmse) throws IOException{
		System.out.println("ENDJOB: Aggregating .:"+rmse.toString());
		FileSystem fs1 = FileSystem.get(c);
		FileStatus[] status = fs1.listStatus(rmse);
		//For all files
		double sum=0;
		double cardinality=0;
		String line;
		String[] v;
		String fileName[];
		for (int i=0;i<status.length;i++){
			fileName = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("RMSE-")) {
				FileSystem fs2 = FileSystem.get(c);
				BufferedReader br = new BufferedReader(new InputStreamReader(fs2.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//get rmse and cardinality, discard keys
					v = line.split(",");
					sum+= Double.valueOf(v[0]); 
					cardinality+= Double.valueOf(v[1]); 
					line = br.readLine();
				}
				br.close();
				fs2.close();
			}
		}
		fs1.close();
		return Math.sqrt(sum/cardinality);
	}
}