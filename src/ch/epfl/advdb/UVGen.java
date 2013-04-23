package ch.epfl.advdb;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Compute initial values for U_0 and V_0
 * @author Bernard Gütermann
 *
 */
public class UVGen {
	Path dest;
	double means;
	int ndims;
	int nmovies;
	int nusers;

	public UVGen(Path dest, double meanSquare, int dim, int movies, int users){
		this.dest=dest;
		means=meanSquare;
		ndims=dim;
		nmovies=movies;
		nusers=users;
	}

	/*
	 * {{ Generate U and V matrices with v_i=sqrt(a/d)+salt, where
	 * a: mean value of all non-blanks elements of M
	 * d: number of dimensions
	 * salt: gaussian salt with mean= 0 and SD = sqrt(a/d) }}
	 * 
	 * This scheme was previously used, but now we replaced it with
	 * Gaussian values with mean 0 and sd 0.25 as this yields better
	 * results
	 * 
	 */
	public void generate(){
		try{
			JobConf conf = new JobConf(UVGen.class);
			Path pt=new Path(dest+"/U_0/part-0");
			FileSystem fs = FileSystem.get(conf);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			Random r = new Random();
			
			String line;
			for(int i=1;i<=nusers;i++){
				for (int j = 1; j <=ndims; j++) {
					double value = r.nextGaussian()*0.25;
					line="U,"+i+","+j+","+value+"\n";
					br.write(line);
				}
			}

			br.close();

			pt=new Path(dest+"/V_0/part-0");
			br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			for(int i=1;i<=ndims;i++){
				for (int j =1; j <=nmovies; j++) {
					double value = r.nextGaussian()*0.25;
					line="V,"+i+","+j+","+value+"\n";
					br.write(line);
				}
			}

			br.close();
			fs.close();
		}catch(Exception e){
			System.out.println("UVGen: Error"); 
			e.printStackTrace(); 
		}
	}
}
