/*
 * BERNARD GUTERMANN (c) 2013
 */
package ch.epfl.advdb;

import org.apache.hadoop.fs.Path;

/**
 * 
 * @author Bernard Gütermann
 *
 */
public class Main {
	//PICK ONE SOLUTION. (see description paper) : 1 or 2 
	static int SOLUTION=2;
	//COMPUTATION PARAMETERS
	final static int DIMENSIONS=10;
	final static int MOVIES=17770;
	final static int USERS=480189;
	final static int REDUCERS=88;
	//used only for SOLUTION 1
	final static int REPLICATION_FACTOR=3; //ideally 1. However with 1 we have heap size errors (not enough ram)
	
	/**
	 * Main
	 * @param args <input folder> <output folder> 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
//		double rmse_current=10;
//		double rmse_previous;
		System.out.println("###Preprocessing###");
		Preprocessing p = new Preprocessing();
		Double mean = p.run(args, REDUCERS);
		if(mean==null) printAndQuit("Preprocessing");
		System.out.println("MEAN : "+mean);
		System.out.println("###################");
		
		System.out.println("###Generate Initial U,V###");
		generateUV(new Path(args[1]), mean);
		System.out.println("###################");
		
		
//		System.out.println("###Compute Initial RMSE###");
//		Rmse r = new Rmse();
//		double rmse_current = r.run(args, -1, DIMENSIONS, MOVIES, USERS);
//		System.out.println("Iteration :\t-1");
//		System.out.println("initial RMSE:\t"+rmse_current);
//		System.out.println("###################");

		System.out.println("Starting Iterations...");
		int it=0;
		do
		{
//			rmse_previous = rmse_current;
			System.out.println("#####Iteration "+(it+1)+"#####");
			if(SOLUTION==1){
				IterationU u = new IterationU();
				if(u.run(args,it, DIMENSIONS, MOVIES, USERS, REDUCERS, REPLICATION_FACTOR)==1) printAndQuit("Iteration U");
				IterationV v = new IterationV();
				if(v.run(args,it, DIMENSIONS, MOVIES, USERS, REDUCERS, REPLICATION_FACTOR)==1) printAndQuit("Iteration V");
			}
			else if(SOLUTION==2){
				IterationUSol2 u = new IterationUSol2();
				if(u.run(args, it , DIMENSIONS, MOVIES, USERS, REDUCERS)==1) printAndQuit("Iteration U");
				IterationVSol2 v = new IterationVSol2();
				if(v.run(args, it, DIMENSIONS, MOVIES, USERS, REDUCERS)==1)  printAndQuit("Iteration V");
			}
//			if (rmse_current==-1) printAndQuit("Iteration V");
//			System.out.println("######RMSE######");
//			System.out.println("Iteration :\t"+(it+1));
//			System.out.println("Current RMSE:\t"+rmse_current);
//			System.out.println("Previous RMSE:\t"+rmse_previous);
//			System.out.println("Variation:\t"+(rmse_previous-rmse_current));
			++it;
		}while(true); //Add stoping conditions here
		
		//System.out.println("########END########");		
	}

	private static void printAndQuit(String string) {
		System.out.println("ERROR : "+string);
		System.exit(1);
		
	}
	
	/**
	 * Instantiates bufferedWriters to put U_0 and V_0 on dfs.  
	 * @param outFile output path : where the iterations will store/load results
	 * @param mean mean value of matrix M. Is used to compute the better initial values possible for U_0 and V_0
	 * @throws Exception in case of failure
	 */
	private static void generateUV(Path outFile, double mean) throws Exception {
//		UVbis handle = new UVbis(outFile,2,5,5);
//		handle.generate();
		UVGen handle = new UVGen(outFile,Math.sqrt(mean/DIMENSIONS),DIMENSIONS,MOVIES,USERS);
		handle.generate();
	}
	
}	
