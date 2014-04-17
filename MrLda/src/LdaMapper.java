import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.*;


public class LdaMapper {
	public static int K;
	public static int V;
	public static int D;
	public static double[][] lambda = new double[V][K];
	public static double[][] gamma = new double[D][K];
	public static double[] alpha = new double[K];
	public static int numberOfMaxGammaIterations = 0;
	
	public static void retrieveLambda(){
		/**
		 * TODO
		 */
		
		normalizeLambda();
	}
	
	public static void retrieveGamma(){
		/**
		 * TODO
		 */
	}
	
	public static void retrieveAlpha(){
		/**
		 * TODO
		 */
	}
	
	public void setNumGammaIterations(int num){
		LdaMapper.numberOfMaxGammaIterations = num;
	}
	
	public static class Map extends MapReduceBase implements Mapper<IntWritable, Text, Text, DoubleWritable> {
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
		//Ok... What we are going to do here !
		//We set the input type to be of the form "docId,countW1,countW2,...,countWV"
		//We need to figure out how to write on an intermediate file.
		//We need to figure out how to load all our intermediary values.
		
		//If Nd = number of docs
		// K = # of topics
		// V = Vocab size
		
		
		public void map(IntWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			
			//We retrieve the values.
			retrieveLambda();
			retrieveGamma();
			retrieveAlpha();
			setPhiSigma();
			
			//We need to parse the line to fill in an array of the words in the doc : 
			String[] wordsInDocTemp = value.toString().split(",");
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
						phi[v][k] = lambda[v][k]*Math.exp(MathFunctions.diGamma(gamma[key.get()][k]))/sumLambda[k];
					}
					phi[v] = normalizePhiV(phi[v]);
					sigma = addPlusVectorMultiplication(sigma, wordsInDoc[v], phi[v]);
				}
				//We update row vector.
				gamma[key.get()] = addPlusVectorMultiplication(sigma, 1, alpha);

				//Check to see if converged.
				notConverged = convergenceTest(numIterations);
			}

			//We want to compute the sum of the gammas : 
			double sumGamma = 0.0;
			for(int l = 0; l < K; l++){
				sumGamma += gamma[key.get()][l];
			}

			//No we do the emission of our key value types.
			for(int k = 0; k < K; k++){
				for(int v = 0; v < V; v++){
					outputKey.set(""+k +"," + v);
					outputValue.set(wordsInDoc[v]*phi[v][k]);
					output.collect(outputKey, outputValue);
				}
				outputKey.set(""+ (-1) + "," + k);

				//Fuck here I need to comput diGamma(ydk) - diGamma(sum Gama))
				outputValue.set(MathFunctions.diGamma(gamma[key.get()][k]) - MathFunctions.diGamma(sumGamma));

				//Now here is the tricky part : emit gamma d,k to file.
				//We need to have a file, be able to clean it, or add new lines to the file.
				//This demands a little reflection.

			}

		}
	}

	public static boolean convergenceTest(int num){
		return num < numberOfMaxGammaIterations ;
		/**
		 * TODO
		 */
		//In their implementation of LDA : use a counter... This is bad but we will do the same for now.

		//Not quite sure on which value to test for evolution.
	}
	//Sum to 1;
	public static void normalizeLambda(){
		double max = 0.0;
		for (int i = 0; i < lambda.length; i++) {
			for(int j = 0; i < lambda[0].length; j++){
				if(lambda[i][j] > max){
					max = lambda[i][j];

				}
			}

		}
		for (int i = 0; i < lambda.length; i++) {
			for (int j = 0; j < lambda[0].length; j++) {
				lambda[i][j] = lambda[i][j]/max;
			}

		}
	}

	public static double[] normalizePhiV(double[] phiV){
		double max = 0.0;
		for(double e : phiV ){
			if(e > max){
				e = max;
			}
		}

		for (int i = 0; i < phiV.length; i++) {
			phiV[i] = phiV[i]/max;
		}
		return phiV;
	}

	public static double[] addPlusVectorMultiplication(double[] sigma, int wordV, double[] phiV ){
		for (int i = 0; i < phiV.length; i++) {
			sigma[i] +=(double) wordV*phiV[i];
		}
		return sigma;
	}


}
