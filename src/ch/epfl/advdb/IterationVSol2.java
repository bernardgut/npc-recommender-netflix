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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class implements the second mapReduce job of Solution 2 as described in the paper.
 * @author Bernard Gütermann
 *
 */
public class IterationVSol2 {
	
	/**
	 * The map task read lines of V and M files and emits columns of U, along with their corresponding columns
	 * of M to the reduces tasks. 
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
			//process the various inputs
			String type = key.toString();
			String[] words =  value.toString().split(",");
			int movieID =Integer.parseInt(words[1]);
			//collect the inputs for the computation of both U and V
			if(type.equals("M")){
				Text out = new Text("M,"+words[0]+","+words[2]);
				context.write( new IntWritable(movieID), out);
			}
			else if(type.equals("V")){
				Text out = new Text("V,"+words[0]+","+words[2]);
				context.write( new IntWritable(movieID), out);
			}
			else throw new InterruptedException("Iteration V : Invalid mapper input: K:"+type+" V: "+words[1]);
		}
	}

	/**
	 * The reducer task takes as input all tuples of a column of of V along with the tuples
	 * of the corresponding column in M. 
	 * It constructs the data structures and read U from local memory
	 * It performs the algorithm as described in the paper
	 * it outputs the corresponding new column of V
	 * @author Bernard Gütermann
	 *
	 */
	public static class IteratorReduceV extends Reducer<IntWritable, Text, Text, Text> {

		private double[][] U;
		
		/**
		 * here we load V in the local memory of the Reduce Worker 
		 * 
		 */
		@Override
		public void setup(Context context) throws IOException {
			U=getUMatrix(context.getConfiguration());
		}

		private double[][] getUMatrix(Configuration c) throws IOException{
			double[][] matrix = new double[Integer.valueOf(c.get("USERS"))][Integer.valueOf(c.get("DIMENSIONS"))];
			FileSystem fs = FileSystem.get(c);
			FileStatus[] status = fs.listStatus(new Path(c.get("UPATH")));  
			//For all files
			for (int i=0;i<status.length;i++){
				String fileName[] = status[i].getPath().toString().split("/");
				if (fileName[fileName.length-1].contains("part")) {
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
			//check if line of U &M :
			//get line of M and U and detect/put in order
			Iterator<Text> it = values.iterator();
			Double[] M_m = new Double[Integer.valueOf(conf.get("USERS"))];
			Double[] V_m = new Double[Integer.valueOf(conf.get("DIMENSIONS"))];

			String[] line;
			int colNo = key.get();
			//construct the data containers for calculation
			while(it.hasNext()){
				line = it.next().toString().split(",");
				if (line[0].compareTo("M")==0){
					M_m[Integer.valueOf(line[1])-1] = Double.valueOf(line[2]);
				}
				else if (line[0].compareTo("V")==0){
					V_m[Integer.valueOf(line[1])-1] = Double.valueOf(line[2]);
				}
				else throw new InterruptedException("Iterator Reducer : Invalid value read from KV-pair: "+line[0]);
			}
			//for every column of v received :
			//case V col computation	
			double sum_s = 0; //squared sum
			//compute u_r*V for whole V
			double lineCol=0;
			double sum_i=0;				
			for(int r = 0; r<V_m.length;r++){
				sum_i=0;
				sum_s=0;
				for (int i=0;i<U.length;i++){
					if(M_m[i] != null){			//only for non-blanks m_rm
						lineCol=0;
						for(int k = 0; k<U[0].length;k++){		//dimensions
							if (k!=r){
								lineCol+=V_m[k]*U[i][k];
							}	
						}
						sum_i+=U[i][r]*(M_m[i]-lineCol);
						sum_s+=U[i][r]*U[i][r];
					}
				}

				//write to disk the new Value of V. 
				//if there is a rating for this film from the user
				if (sum_s!=0) V_m[r] = sum_i/sum_s; 
				else V_m[r] = 0.0;
//				m_multipleOutputs.write("out",new Text("V"), new Text(String.valueOf(r+1)+","
//						+String.valueOf(colNo)+","+String.valueOf(V_m[r])),"V-"+(iteration+1));
				context.write(new Text("V"), new Text(String.valueOf(r+1)+","
						+String.valueOf(colNo)+","+String.valueOf(V_m[r])));

			}
//			//compute Squared Error fo this col
//			double se=0;
//			double n=0;
//			for (int r=0;r<U.length;r++){
//				if(M_m[r] != null){			//only for non-blanks m_rm
//					lineCol=0;
//					//count number of non-blancs un this line
//					n++;
//					//compute the value of UxV for line r col m
//					for(int d = 0; d<U[0].length;d++){		//dimensions
//						lineCol+=V_m[d]*U[r][d];
//					}
//					//add the square error for this value to SE of the line
//					se+=Math.pow((M_m[r]-lineCol),2); 
//				}
//			}
//
//			//and rmse of this col along with number of non-blanks in this col
//			m_multipleOutputs.write("rmse", new Text(String.valueOf(se)), new Text(String.valueOf(n)),"RMSE-"+(iteration+1));
		}
	}

	/**
	 * Runs the second mapReduce job for the iteration.
	 * @param args root inputs and outputs folders
	 * @param iteration
	 * @param DIMENSIONS
	 * @param MOVIES
	 * @param USERS
	 * @param REDUCERS
	 * @return
	 * @throws Exception
	 */
	public double run(String[] args, int iteration, final int DIMENSIONS, final int MOVIES, 
			final int USERS, final int REDUCERS) throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("DIMENSIONS", String.valueOf(DIMENSIONS));
		conf.set("MOVIES", String.valueOf(MOVIES));
		conf.set("USERS", String.valueOf(USERS));
		conf.set("UPATH", args[1]+"/U_"+(iteration+1)+"/");
		conf.set("iteration", String.valueOf(iteration));
		conf.set("mapred.textoutputformat.separator", ",");
		conf.set("key.value.separator.in.input.line", ",");

		Job job = new Job(conf, "iteration-"+iteration+"-V");
		//metrics
		job.setNumReduceTasks(REDUCERS);
		//Classes
		job.setJarByClass(IterationVSol2.class);
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

		FileInputFormat.addInputPaths(job, args[1]+"/V_"+iteration+","+args[1]+"/M/M*");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/V_"+(iteration+1)));

		if (!job.waitForCompletion(true)) return -1;
		//crawl results and return rmse
//		return getRmsFromDFS(conf, new Path(args[1]+"/V_"+(iteration+1)));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Parses the file containing the the rmse values for each columns of 
	 * M, as well as the cardinality of these columns in order to output RMSE(UV, M)
	 * @param c config object that contains the paths to files
	 * @param rmse path to the output of the second mapreduce job
	 * @return the RMSE after the current iteration
	 * @throws IOException
	 */
	
	private double getRmsFromDFS(Configuration c, Path rmse) throws IOException{
		System.out.println("ENDJOB: Aggregating .:"+rmse.toString());
		FileSystem fs = FileSystem.get(c);
		FileStatus[] status = fs.listStatus(rmse);
		//For all files
		double sum=0;
		double cardinality=0;
		String line;
		String[] v;
		String fileName[];
		for (int i=0;i<status.length;i++){
			fileName = status[i].getPath().toString().split("/");
			if (fileName[fileName.length-1].contains("RMSE-")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//get rmse and cardinality, 
					v = line.split(",");
					sum+= Double.valueOf(v[0]); 
					cardinality+= Double.valueOf(v[1]); 
					line = br.readLine();
				}
				br.close();
			}
		}
		fs.close();
		return Math.sqrt(sum/cardinality);
	}
}