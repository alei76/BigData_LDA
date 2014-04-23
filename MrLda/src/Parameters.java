//We want to create a singeton class.
public class Parameters {
	public static String pathToAlphas;
	public static String pathToGammas;
	public static String pathToLambdas;
	public static int numberOfTopics;
	public static int numberOfDocuments;
	public static int sizeOfVocabulary;
	public static int numberOfIterations;
	public static int numberOfMapperIteration;
	
	
	public Parameters(int numberTopics, int numberDocs, int vocabSize, int numbIter, int MapIter){
		Parameters.numberOfTopics = numberTopics;
		Parameters.numberOfDocuments = numberDocs;
		Parameters.sizeOfVocabulary = vocabSize;
		Parameters.numberOfIterations = numbIter;
		Parameters.numberOfMapperIteration = MapIter;
	}
	
	
	public void setPaths(String pathAlpha, String pathGammas, String pathLambdas){
		Parameters.pathToAlphas = pathAlpha;
		Parameters.pathToGammas = pathGammas;
		Parameters.pathToLambdas = pathLambdas;
		
	}

}
