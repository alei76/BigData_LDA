import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FileSystem;


public class LdaMapper {

	//When these are completed, they need to be added inside the mapper class.


	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		//A few useful parameters retrieved from class Parameters.
		static int K = Parameters.numberOfTopics;
		static int V = Parameters.sizeOfVocabulary;
		static int D = Parameters.numberOfDocuments;
		static int numberOfMaxGammaIterations = Parameters.numberOfMapperIteration;
		static double[][] lambda;
		
		/**
		 * an double array of size nbDoc*nbTopics
		 */
		static double[][] gamma;
		static double[] alpha;
		
		
		
		private final static DoubleWritable outputValue = new DoubleWritable(1);
		private Text outputKey = new Text("");
		
		private static double[][] phi = new double[V][K];
		private static double[] sigma = new double[K];
		
		public static void setPhiSigma(){
			for(int i = 0; i<K; i++){
				for(int j = 0; j <V; j++){
					phi[j][i] = 0.0;
				}
				sigma[i]=0.0;
			}
		}
		
		public static boolean convergenceTest(int num){
			return num < numberOfMaxGammaIterations ;

		}
		//Sum to 1;
		//Not quite sure about this : lambda_v,* has to sum to 1 ?
		public static void normalizeLambda(){
			double sum = 0.0;
			for (int i = 0; i < lambda.length; i++) {
				for(int j = 0; j < lambda[0].length; j++){
					sum += lambda[i][j];
				}
				
				for(int j = 0; j < lambda[0].length; j++){
					lambda[i][j] = lambda[i][j]/sum;
				}

			}
		}

		public static double[] normalizePhiV(double[] phiV){
			double sum = 0.0;
			for(double e : phiV ){
				sum += e;
			}

			for (int i = 0; i < phiV.length; i++) {
				phiV[i] = phiV[i]/sum;
			}
			return phiV;
		}

		public static double[] addPlusVectorMultiplication(double[] sigma, int wordV, double[] phiV ){
			for (int i = 0; i < phiV.length; i++) {
				sigma[i] +=(double) wordV*phiV[i];
			}
			return sigma;
		}
		
		public static void retrieveLambda(){
			/**
			 * TODO
			 * Read from file the value of lambda
			 */
			
			lambda = FileSystemHandler.loadLambdas(Parameters.pathToLambdas);
			
			
		}
		
		public static void retrieveGamma(){
			/**
			 * TODO
			 * Read from file the value of Gamma
			 */
			gamma = FileSystemHandler.loadGammas(Parameters.pathToGammas);
			
		}
		
		public static void retrieveAlpha(){
			/**
			 * TODO
			 * Read from file the value of Alpha
			 */
			alpha = FileSystemHandler.loadAlpha(Parameters.pathToAlphas);
		}
		
		public static void writeNewGamma(){
			/**
			 * TODO
			 * We need to write the new value of gamma to the correct file.
			 */
			
		}
		//Ok... What we are going to do here !
		//We set the input type to be of the form "docId,countW1,countW2,...,countWV"
		//We need to figure out how to write on an intermediate file.
		//We need to figure out how to load all our intermediary values.
		
		//If Nd = number of docs
		// K = # of topics
		// V = Vocab size
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			//Three different keys value:
			//I- 1,k,v 
			//II- 2,-1,k
			//III- 3,k,d
			
			//We retrieve the values.
			retrieveLambda();
			retrieveGamma();
			retrieveAlpha();
			setPhiSigma();
			
			//We need to parse the line to fill in an array of the words in the doc :
			String[] formatLine = value.toString().split("\t");
			int documentId = Integer.parseInt(formatLine[0]); 
			String[] wordsInDocTemp = formatLine[1].split(" ");
			int[] wordsInDoc = new int[V];
			for (int i = 0; i < wordsInDocTemp.length; i++) {
				wordsInDoc[i] = Integer.parseInt(wordsInDocTemp[i]);
			}
			//We need to keep the sum over v of all lambdas.
			double[] sumLambda = new double[K];
			
			for (int v = 0; v < V; v++) {
				for (int k = 0; k < K; k++) {
					sumLambda[k] += lambda[v][k];
				}
				
			}
			
			//We need to keep track of convergence
			boolean notConverged = true;
			int numIterations = 0; 

			//We now go through the heavy computations : 
			while(notConverged){

				//We update the count of iterations :
				numIterations++;

				for (int v = 0; v < V; v++) {
					for (int k = 0; k < K; k++) {
						phi[v][k] = lambda[v][k]*Math.exp(MathFunctions.diGamma(gamma[documentId][k]))/sumLambda[k];
					}
					phi[v] = normalizePhiV(phi[v]);
					sigma = addPlusVectorMultiplication(sigma, wordsInDoc[v], phi[v]);
				}
				//We update row vector.
				gamma[documentId] = addPlusVectorMultiplication(sigma, 1, alpha);

				//Check to see if converged.
				notConverged = convergenceTest(numIterations);
			}

			//We want to compute the sum of the gammas : 
			double sumGamma = 0.0;
			for(int l = 0; l < K; l++){
				sumGamma += gamma[documentId][l];
			}

			//No we do the emission of our key value types.
			for(int k = 0; k < K; k++){
				for(int v = 0; v < V; v++){
					outputKey.set("1,"+k +"," + v);
					outputValue.set(wordsInDoc[v]*phi[v][k]);
					output.collect(outputKey, outputValue);
				}
				outputKey.set("2,"+ (-1) + "," + k);

				//Fuck here I need to comput diGamma(ydk) - diGamma(sum Gama))
				outputValue.set(MathFunctions.diGamma(gamma[documentId][k]) - MathFunctions.diGamma(sumGamma));
				output.collect(outputKey, outputValue);
				//Now here is the tricky part : emit gamma d,k to file.
				//We need to have a file, be able to clean it, or add new lines to the file.
				//This demands a little reflection.
				
				//write the gamma to the reducer
				outputKey.set("3,"+k+","+key.get());
				outputValue.set(gamma[documentId][k]);
				
				output.collect(outputKey, outputValue);

			}

		}
	}




}
