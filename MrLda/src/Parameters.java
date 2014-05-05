/**
 * 
 * This class represents the static final constants
 *
 */
public class Parameters {
	public static String pathToAlphas;
	public static String pathToGammas;
	public static String pathToLambdas;
	public static String pathJobOutput;
	public static String pathToGradient;


	public static int numberOfTopics;
	public static int numberOfDocuments;
	public static int sizeOfVocabulary;
	public static int numberOfIterations;
	public static int numberOfMapperIteration;
	
	
	public  Parameters(int numberTopics, int numberDocs, int vocabSize, int numbIter, int MapIter){
		Parameters.numberOfTopics = numberTopics;
		Parameters.numberOfDocuments = numberDocs;
		Parameters.sizeOfVocabulary = vocabSize;
		Parameters.numberOfIterations = numbIter;
		Parameters.numberOfMapperIteration = MapIter;
	}
	
	
	public static void setPaths(String pathAlpha, String pathGammas, String pathLambdas, String jobPathOutput, String pathToGradient){
		Parameters.pathToAlphas = pathAlpha;
		Parameters.pathToGammas = pathGammas;
		Parameters.pathToLambdas = pathLambdas;
		Parameters.pathJobOutput = jobPathOutput;
		Parameters.pathToGradient = pathToGradient;
		
	}

}
