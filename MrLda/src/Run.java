

	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;





public class Run {
	
	public static void initializeAlpha(String alphaPath){
		/**
		 * TODO
		 * Set the initial value of alpha to either O or some random numbers.
		 */
		double[] alpha = new double[Parameters.numberOfTopics];
		
		//initialize to 0
		for (int i = 0; i < alpha.length; i++) {
			alpha[i] = 0;
		}
		
		FileSystemHandler.writeVector(Parameters.pathToAlphas, alpha);
	}
	
	
	public static void initializeGamma(String gammaPath){
		//initialization with equiprobable probabilities
		double[][] gamma = new double [Parameters.numberOfDocuments][Parameters.numberOfTopics];
		for (int i = 0; i < gamma.length; i++) {
			for (int j = 0; j < gamma[0].length; j++) {
				gamma[i][j] = (1.0/Parameters.numberOfTopics);
				System.out.println(gamma[i][j]);
			}
		}
		
		FileSystemHandler.writeMatrix(Parameters.pathToGammas, gamma);
	}
	
	
	public static void initializeLambda(String lambdaPath){
		//initialization with equiprobable probabilities
		double[][] lambda = new double [Parameters.numberOfTopics][Parameters.sizeOfVocabulary];
		for (int i = 0; i < lambda.length; i++) {
			for (int j = 0; j < lambda[0].length; j++) {
				lambda[i][j] = 1.0/Parameters.sizeOfVocabulary;
			}
		}
		
		FileSystemHandler.writeMatrix(Parameters.pathToLambdas, lambda);
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
		
		//These to be set and discussed beforehand.
		String pathAlpha = outputFolder+"/final/alpha";
		String pathGammas = outputFolder+"/final/gamma";
		String pathLambdas = outputFolder+"/final/lambda";
		String reducerOutPath = outputFolder+"/temp/lambda";
		String mapperOutPath = outputFolder+"/temp/gamma";
		String pathToGradient = outputFolder+"/final/gradient";
		
		
		//The Parameters class is set once and for all.
		//If I want to share it to all the classes, shouldn't I just have a setter method ?
		Parameters.numberOfTopics = Integer.parseInt(args[2]);
		Parameters.numberOfDocuments = Integer.parseInt(args[3]);
		Parameters.sizeOfVocabulary = Integer.parseInt(args[4]);
		Parameters.numberOfIterations = Integer.parseInt(args[5]);
		Parameters.numberOfMapperIteration = Integer.parseInt(args[6]);
		Parameters.setPaths(pathAlpha, pathGammas, pathLambdas, reducerOutPath, mapperOutPath, pathToGradient);
		


		
		
	
		
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
		FileOutputFormat.setOutputPath(conf, new Path(Parameters.pathReducerOutput));
		
		
		//We now do the algorithm : 
		
		int i = 0;
		
		//At i = 0, we need to initialise the files for alpha, gamma, lambda.
		Run.initializeAlpha(Parameters.pathToAlphas);
		Run.initializeGamma(Parameters.pathToGammas);
		Run.initializeLambda(Parameters.pathToLambdas);
		
		
		
		while(i < Parameters.numberOfIterations){
			i++;
			JobClient.runJob(conf);
			//rewrite the files lambda and gamma to a good format
			//and write the delta
			FileSystemHandler.generateLambdaGammaGradient();
			
			Driver driver = new Driver();
			driver.setNewAlpha();
			driver.writeNewAlpha();
		}
			
	}
	
	

}