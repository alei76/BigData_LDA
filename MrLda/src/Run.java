

	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;





public class Run {
	
	public void initializeAlpha(String alphaPath){
		/**
		 * TODO
		 * Set the initial value of alpha to either O or some random numbers.
		 */
	}
	
	public void initializeGamma(String gammaPath){
		/**
		 * TODO
		 * Same
		 */
	}
	
	public void initializeLambda(String lambdaPath){
		/**
		 * TODO
		 * Same
		 */
	}
	
	public static void main(String[] args) throws Exception {
		
		/**
		 * TODO
		 * We need to to the following : 
		 * -run the method multiple times : each time run the MR job
		 * -then run the driver
		 * -Make sure that all of them share the same infos (path, starting parameters)
		 * -Set a command format to retrieve parameters.
		 */
		
		
		//Important : for me the output of the reducer and the path to lambda should be the same.
		
		String inputFolder = args[0];
		String outputFolder = args[1];
		
		//These to be set and discussed beforehand.
		String pathAlpha = null;
		String pathGammas = null;
		String pathLambdas = null;
		
		
		//The Parameters class is set once and for all.
		//If I want to share it to all the classes, shouldn't I just have a setter method ?
		Parameters params = new Parameters(Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
		params.setPaths(pathAlpha, pathGammas, pathLambdas);
		
		//Create a simple run class to do our initialization jobs
		Run run = new Run();
	
		
		JobConf conf = new JobConf(LdaReducer.class);
		conf.setJobName("MrLDA");
 	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
 	
		conf.setMapperClass(LdaMapper.Map.class);
		conf.setCombinerClass(LdaReducer.Reduce.class);
		conf.setReducerClass(LdaReducer.Reduce.class);
 	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
 	
		FileInputFormat.setInputPaths(conf, new Path(inputFolder));
		FileOutputFormat.setOutputPath(conf, new Path(outputFolder));
		
		
		//We now do the algorithm : 
		
		int i = 0;
		
		//At i = 0, we need to initialise the files for alpha, gamma, lambda.
		run.initializeAlpha(Parameters.pathToAlphas);
		run.initializeGamma(Parameters.pathToGammas);
		run.initializeLambda(Parameters.pathToLambdas);
		
		
		
		while(i < Parameters.numberOfIterations){
			i++;
			JobClient.runJob(conf);
			Driver driver = new Driver();
			driver.setNewAlpha();
			driver.writeNewAlpha();
		}
		
		
		
	}
	
	

}