package ch.epfl.advdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This mapReduce function was implemented to compute the RMSE between initial values of UV and M
 * It is NOT used in the final execution process. 
 * (We computed this once)
 * @author Bernard Gütermann
 *
 */
public class Rmse {
	public static class RmseMap extends Mapper<Text, Text, IntWritable, Text> {
		@Override
		public void map(Text key, Text value,  Context context) throws IOException, InterruptedException{
			String[] words =  value.toString().split(",");
			int userID = Integer.parseInt(words[0]);
			//construct lines of M and U for the reducer
			String type = key.toString();
			Text out = null;
			if(type.equals("M")){
				out = new Text("M,"+words[1]+","+words[2]);
			}
			else if(type.equals("U")){
				out = new Text("U,"+words[1]+","+words[2]);
			}else throw new InterruptedException("RMSE : Invalid key-value input at Mapper, K:"+type+" V:"+words.toString());
			context.write( new IntWritable(userID), out);
		}
	}

	public static class RmseReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

		private double[][] getVMatrix(Configuration c) throws IOException{
			double[][] matrix = new double[Integer.valueOf(c.get("DIMENSIONS"))][Integer.valueOf(c.get("MOVIES"))];
			FileSystem fs = FileSystem.get(c);
			FileStatus[] status = fs.listStatus(new Path(c.get("VPATH"))); 
			String fileName[];
			String line;
			String values[];
			//For all files
			for (int i=0;i<status.length;i++){
				fileName = status[i].getPath().toString().split("/");
				if (!fileName[fileName.length-1].equals("_logs")) {
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					line=br.readLine();
					while (line != null){
						values = line.split("\t")[1].split(","); //get values, discard keys
						//populate V
						matrix[Integer.valueOf(values[0])-1][Integer.valueOf(values[1])-1] = Double.parseDouble(values[2]);
						//nextline
						line = br.readLine();
					}
					br.close();
				}
			}
			return matrix;
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			//construct vectors u_r,m_r and matrice V
			HashMap<Integer,Double> vectM = new HashMap<Integer,Double>();
			HashMap<Integer,Double> vectU = new HashMap<Integer,Double>();
			String[] line;
			//get line of M and U and detect order
			Iterator<Text> it = values.iterator();
			while(it.hasNext()){
				line = it.next().toString().split(",");
				if(line[0].compareTo("M")==0){
					vectM.put(Integer.valueOf(line[1])-1, Double.valueOf(line[2]));
				}else if (line[0].compareTo("U")==0){
					vectU.put(Integer.valueOf(line[1])-1, Double.valueOf(line[2]));
				}else throw new InterruptedException("RMSE : Invalid key-value input at Reducer, K:"+key.toString()+" V:"+line[0]);
			}
			//DEBUG
			//assert vectM.size()==Integer.valueOf(conf.get("MOVIES")): "RMSE-Reducer: Invalid vector size M :"+vectM.size();
			//assert vectU.size()==Integer.valueOf(conf.get("DIMENSIONS")): "RMSE-Reducer: Invalid vector size U :"+vectU.size();
			
			//load V
			double[][] V = getVMatrix(conf);
			//Compute a whole line of P_rm
			double P_rm=0;
			double se_r=0;	//square error for line r of P
			int n =0;		//number of non-blanks entries in M for this line
			for (int m=0;m<V[0].length;++m){
				if(vectM.get(m) != null){
					//count number of non-blancs un this line
					n++;
					//compute the value of UxV for line r col m
					P_rm=0;
					for(int s = 0; s<V.length;s++){		
						P_rm+=vectU.get(s)*V[s][m];		
					}
					//add the square error for this value to SE of the line
					se_r+=Math.pow((vectM.get(m)-P_rm),2); 
				}
			}
			//emit the sum squared error for this line as well as the number of values
			context.write(key,new Text(String.valueOf(se_r)+","+String.valueOf(n)));
		}		
	}
	
	
	public double run(String[] args, int iteration, final int DIMENSIONS, final int MOVIES, final int USERS) throws Exception{
		Configuration conf = new Configuration();
		conf.set("DIMENSIONS", String.valueOf(DIMENSIONS));
		conf.set("MOVIES", String.valueOf(MOVIES));
		conf.set("USERS", String.valueOf(USERS));
		conf.set("VPATH", args[1]+"/iter"+(iteration+1)+"/V");
		
		Job job = new Job(conf, "rmse");
		//metrics
		job.setNumReduceTasks(2);
		//Classes
		job.setJarByClass(Rmse.class);
		job.setMapperClass(RmseMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(RmseReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, args[1]+"/iter"+(iteration+1)+"/U"+","+args[1]+"/M");
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/iter"+(iteration+1)+"/RMSE"));
		if (!job.waitForCompletion(true)) return -1;
		//crawl results and return rmse
		return getRmsFromDFS(conf, new Path(args[1]+"/iter"+(iteration+1)+"/RMSE"));
	}


	private double getRmsFromDFS(Configuration c, Path rmse) throws IOException{
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
			if (!fileName[fileName.length-1].equals("_logs")) {
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				line=br.readLine();
				while (line != null){
					//get rmse and cardinality, discard keys
					v = line.split("\t")[1].split(",");
					//DEBUG
					assert v[0]!=null&&v[0]!="NaN":"RMSE-aggregator : Invalid sum number at "+line;
					assert v[1]!=null&&v[0]!="NaN":"RMSE-aggregator : Invalid cardinality number at "+line;
					sum+= Double.valueOf(v[0]); 
					cardinality+= Double.valueOf(v[1]); 
					line = br.readLine();
				}
				br.close();
			}
		}
		return Math.sqrt(sum/cardinality);
	}
}