import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class LdaMapper {

	//When these are completed, they need to be added inside the mapper class.


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		//A few useful parameters retrieved from class Parameters.
		private static int K;
		private static int V;
		private static int D;
		private static int numberOfMaxGammaIterations;
		private static String pathToAlphas;
		private static String pathToGammas;
		private static String pathToLambdas;
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("numberOfTopics"));
			V= Integer.parseInt(job.get("sizeOfVocabulary"));
			D = Integer.parseInt(job.get("numberOfDocuments"));
			numberOfMaxGammaIterations = Integer.parseInt(job.get("numberOfIterations"));
			pathToAlphas = job.get("pathToAlphas");
			pathToGammas = job.get("pathToGammas");
			pathToLambdas = job.get("pathToLambdas");

		}
		
		/**
		 * an array of V*K
		 */
		private double[][] lambda;

		/**
		 * an array of size D*K
		 */
		private double[][] gamma;
		
		/**
		 * an array of K
		 */
		private double[] alpha;
		
		/**
		 * an array of V*K
		 */
		private double[][] phi;
		
		/**
		 * an array of K
		 */
		private double[] sigma;




		private final static DoubleWritable outputValue = new DoubleWritable(1);
		private Text outputKey = new Text("");



		private void setPhiSigma(){

			phi = new double[V][K];
			sigma = new double[K];
			//NB:This is not needed because the arrays are instanciated to 0.
			/*for(int i = 0; i<K; i++){
				for(int j = 0; j <V; j++){
					phi[j][i] = 0.0;
				}
				sigma[i]=0.0;
			}*/
		}

		private boolean convergenceTest(int num){
			return num < numberOfMaxGammaIterations ;

		}
		//Sum to 1;
		//Not quite sure about this : lambda_v,* has to sum to 1 ?
		//IMPORTANT ** Khalil: For me, for each topic, the sum over the words of lambdas have to sum to 1
		//so it should be in the other way round. Remove my comment after u have red it.
		private void normalizeLambda(){
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

		private double[] normalizePhiV(double[] phiV, double sum){

			for (int i = 0; i < phiV.length; i++) {
				phiV[i] = phiV[i]/sum;
			}
			return phiV;
		}

		private double[] addPlusVectorMultiplication(double[] sigma, int wordV, double[] phiV ){
			if (wordV != 0) {
				for (int i = 0; i < phiV.length; i++) {
					sigma[i] +=(double) wordV*phiV[i];
				}

			}

			return sigma;
		}

		private void retrieveLambda(){
			/**
			 * TODO
			 * Read from file the value of lambda
			 */
			lambda = FileSystemHandler.loadLambdas(pathToLambdas, V, K);



		}

		private void retrieveGamma(){
			/**
			 * TODO
			 * Read from file the value of Gamma
			 */
			gamma = FileSystemHandler.loadGammas(pathToGammas,D, K);

		}

		private void retrieveAlpha(){
			/**
			 * TODO
			 * Read from file the value of Alpha
			 */
			alpha = FileSystemHandler.loadAlpha(pathToAlphas, K);
		}



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
					double sumPhi = 0;
					for (int k = 0; k < K; k++) {
						phi[v][k] = lambda[v][k]*Math.exp(MathFunctions.diGamma(gamma[documentId][k]))/sumLambda[k];
						sumPhi = sumPhi + phi[v][k];
					}
					phi[v] = normalizePhiV(phi[v], sumPhi);
					sigma = addPlusVectorMultiplication(sigma, wordsInDoc[v], phi[v]);
				}
				//We update row vector.
				gamma[documentId] = addPlusVectorMultiplication(sigma, 1, alpha);

				//Check to see if converged.
				notConverged = convergenceTest(numIterations);
			}//end while

			//We want to compute the sum of the gammas : 
			double sumGamma = 0.0;
			for(int l = 0; l < K; l++){
				sumGamma += gamma[documentId][l];
			}

			//No we do the emission of our key value types.

			//write the lambda to the reducer
			for (int v = 0; v < V; v++) {
				for (int k = 0; k < K; k++) {
					outputKey.set("1,"+k +"," + v);
					outputValue.set(wordsInDoc[v]*phi[v][k]);
					output.collect(outputKey, outputValue);

				}

			}

			//write the gradient and the lambda to the reducer
			for(int k = 0; k < K; k++){

				outputKey.set("2,"+ (-1) + "," + k);

				//write of the gradient to the reducer
				outputValue.set(MathFunctions.diGamma(gamma[documentId][k]) - MathFunctions.diGamma(sumGamma));
				output.collect(outputKey, outputValue);


				//write the gamma to the reducer
				outputKey.set("3,"+k+","+documentId);
				outputValue.set(gamma[documentId][k]);

				output.collect(outputKey, outputValue);

			}

		}
	}




}
