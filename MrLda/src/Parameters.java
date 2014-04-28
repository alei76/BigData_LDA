/**
 * 
 * This class represents the static final constants
 *
 */
public class Parameters {
	public static String pathToAlphas;
	public static String pathToGammas;
	public static String pathToLambdas;
	public static String pathReducerOutput;
	public static String pathMapperOutput;
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
	
	
	public static void setPaths(String pathAlpha, String pathGammas, String pathLambdas, String reducerPathOutput, String mapperPathOutput, String pathToGradient){
		Parameters.pathToAlphas = pathAlpha;
		Parameters.pathToGammas = pathGammas;
		Parameters.pathToLambdas = pathLambdas;
		Parameters.pathReducerOutput = mapperPathOutput;
		Parameters.pathReducerOutput = reducerPathOutput;
		Parameters.pathToGradient = pathToGradient;
		
	}

}
