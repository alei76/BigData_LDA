

	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;





public class Run {
	
	private static void initializeAlpha(String alphaPath){
		/**
		 * TODO
		 * Set the initial value of alpha to either O or some random numbers.
		 */
		double[] alpha = new double[Parameters.numberOfTopics];
		
		//initialize to 0.1
		for (int i = 0; i < alpha.length; i++) {
			alpha[i] = 0.1;
		}
		
		FileSystemHandler.writeVector(Parameters.pathToAlphas, alpha);
	}
	
	
	private static void initializeGamma(String gammaPath){
		//initialization with equiprobable probabilities
		double[][] gamma = new double [Parameters.numberOfDocuments][Parameters.numberOfTopics];
		for (int i = 0; i < gamma.length; i++) {
			for (int j = 0; j < gamma[0].length; j++) {
				gamma[i][j] = (1.0/Parameters.numberOfTopics);
			}
		}
		
		FileSystemHandler.writeMatrix(Parameters.pathToGammas, gamma);
	}
	
	
	private static void initializeLambda(String lambdaPath){
		//initialization with equiprobable probabilities
		double[][] lambda = new double [Parameters.sizeOfVocabulary][Parameters.numberOfTopics];
		for (int i = 0; i < lambda.length; i++) {
			for (int j = 0; j < lambda[0].length; j++) {
				lambda[i][j] = 1.0/Parameters.sizeOfVocabulary;
			}
		}
		
		FileSystemHandler.writeMatrix(Parameters.pathToLambdas, lambda);
	}
	
	private static JobConf getJob(String inputPath, String outputPath) {
		JobConf conf = new JobConf(LdaReducer.class);
		
 	
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapperClass(LdaMapper.Map.class);
		conf.setCombinerClass(LdaReducer.Reduce.class);
		conf.setReducerClass(LdaReducer.Reduce.class);
 	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
 	
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		return conf;
		
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
		if (args.length != 7) {
			System.err.println("input format: <input folder> <outputfolder> <number of topics> <number of documents>" +
					"<size of vocabulary> <number of iterations> <number of mapper iterations>");
			System.exit(1);
			
		}
		
		//Important : for me the output of the reducer and the path to lambda should be the same.
		
		String inputFolder = args[0];
		String outputFolder = args[1];
		System.out.println("********************************* MR LDA CONFIGURATIONS ********************************************************");
		System.out.println("Input Folder : " + inputFolder);
		System.out.println("Output Folder :  " + outputFolder);
		
		//These to be set and discussed beforehand.
		String pathAlpha = outputFolder+"/final/alpha";
		
		System.out.println("Path to alpha : " + pathAlpha);
		String pathGammas = outputFolder+"/final/gamma";
		
		System.out.println("Path to gamma : " + pathGammas);
		String pathLambdas = outputFolder+"/final/lambda";
		System.out.println("Path to lambda : " + pathLambdas);
		String jobOutPath = outputFolder+"/temp";
		System.out.println("Job output path : " + jobOutPath);
		String pathToGradient = outputFolder+"/final/gradient";
		System.out.println("Path to gradient : " + pathToGradient);
		
		
		//The Parameters class is set once and for all.
		//If I want to share it to all the classes, shouldn't I just have a setter method ?
		Parameters.numberOfTopics = Integer.parseInt(args[2]);
		Parameters.numberOfDocuments = Integer.parseInt(args[3]);
		Parameters.sizeOfVocabulary = Integer.parseInt(args[4]);
		Parameters.numberOfIterations = Integer.parseInt(args[5]);
		Parameters.numberOfMapperIteration = Integer.parseInt(args[6]);
		Parameters.setPaths(pathAlpha, pathGammas, pathLambdas, jobOutPath, pathToGradient);
		


		System.out.println("Number of topics : " + Parameters.numberOfTopics);
		System.out.println("Number of documents : " + Parameters.numberOfDocuments);
		System.out.println("Size of vocabulary : " + Parameters.sizeOfVocabulary);
		System.out.println("Number of iterations : " + Parameters.numberOfIterations);
		System.out.println("Number of mapper iterations : " + Parameters.numberOfMapperIteration);
		
		System.out.println("***********************************************************************************************************************");
	
		
		
		
		
		//We now do the algorithm : 
		
		int i = 0;
		
		//At i = 0, we need to initialise the files for alpha, gamma, lambda.
		Run.initializeAlpha(Parameters.pathToAlphas);
		Run.initializeGamma(Parameters.pathToGammas);
		Run.initializeLambda(Parameters.pathToLambdas);
		
		
		
		
		while(i < Parameters.numberOfIterations){
			i++;
			System.out.println("iteration: "+ i);
			//FileSystemHandler.deleteReducerOutput(outputFolder+"/temp");
			JobConf conf = getJob(inputFolder, Parameters.pathJobOutput);
			conf.setJobName("LDA: v=54468, iteration="+i);
			conf.setNumMapTasks(75);
			conf.set("pathToAlphas", Parameters.pathToAlphas);
			conf.set("pathToGammas", Parameters.pathToGammas);
			conf.set("pathToLambdas", Parameters.pathToLambdas);
			conf.set("sizeOfVocabulary", ""+Parameters.sizeOfVocabulary);
			conf.set("numberOfDocuments","" + Parameters.numberOfDocuments);
			conf.set("numberOfTopics", "" + Parameters.numberOfTopics);
			conf.set("numberOfIterations","" +Parameters.numberOfMapperIteration);
			
			Job job = new Job(conf);
			job.setNumReduceTasks(75);
			job.waitForCompletion(true);
			
			//rewrite the files lambda and gamma to a good format
			//and write the delta
			
			FileSystemHandler.convertJobOutputToLambdaGammaGradient(Parameters.pathJobOutput, Parameters.pathToLambdas, Parameters.pathToGammas, Parameters.pathToGradient);
			

			//delete the output of the reducer
			
			FileSystemHandler.deletePath(Parameters.pathJobOutput);
			
		
			Driver driver = new Driver();
			driver.setNewAlpha();
			driver.writeNewAlpha();
			

		}
			
	}
	
	

}