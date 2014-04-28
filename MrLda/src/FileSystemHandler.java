
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileSystemHandler {

	/**
	 * 
	 * @param fileName
	 * @param matrix in the form v1 v2 ... vn
	 * 							 v1 v2 ... vn
	 * 
	 * write it in the form: v1,v2,...,vn
	 * 						 v1,v2,...,vn
	 */
	public static void writeMatrix(String fileName, double[][] matrix){

		try {
			Path path = new Path(fileName);
			FileSystem fs = getFileSystem();
			fs.create(path, true);
			
			FSDataOutputStream outputStream = fs.create(path);
			

			
			String line ="";
			for (int i = 0; i < matrix.length; i++) {
				for (int j = 0; j < matrix[0].length; j++) {
					line = line + matrix[i][j] +" "; 

				}
				//remove the last space
				//line = line.substring(0, line.length()-1);
				//add the line jump
				
				
				line=line+"\n";
				outputStream.writeChars(line);
				line = "";
				
			}
			outputStream.close();


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}

	public static void writeVector(String fileName, double[]vector){
		try {
			Path path = new Path(fileName);
			FileSystem fs = getFileSystem();
			fs.create(path, true);
			
			FSDataOutputStream outputStream = fs.create(path);
			

			
			String line ="";
			for (int i = 0; i < vector.length; i++) {

				line = line + vector[i] +" "; 

			}
			//remove the last space
			line = line.substring(0, line.length()-1);
			
			outputStream.writeChars(line);
			outputStream.close();




		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}


	}
	
	/**
	 * takes the output of the reducers and generate the gamma file, the lambda file and the gradient file
	 * in the good format
	 */
	public static void generateLambdaGammaGradient() {
		//the lambda matrix that will be constituted
		double[][]lambda = new double[Parameters.numberOfTopics][Parameters.sizeOfVocabulary];
		
		
		//the gamma matrix that will be constituted
		double[][] gamma = new double [Parameters.numberOfDocuments][Parameters.numberOfTopics];
		
		//may be needed if we find some keys with value -1;
		double[] delta = new double [Parameters.numberOfTopics];

		
		//go throught all the files of the directory
		File folder = new File(Parameters.pathReducerOutput);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			//for each file read all the values and put them in the matrix
			String fileName = listOfFiles[i].getPath();


			try {
				FileSystem fs = getFileSystem();

				FSDataInputStream inputStream = fs.open(new Path(fileName));

				String line = inputStream.readLine();

				

				while (line != null) {
					String[] stringArray = line.split(",|\\s");
					int keyType = new Integer(stringArray[0]);
					if(keyType == 1) {
						//then it is the lambda
						lambda[new Integer(stringArray[1])][new Integer(stringArray[2])]= new Double(stringArray[3]);
					} else if(keyType == 2) {
						//then it is the delta
						delta[new Integer(stringArray[2])] = new Double(stringArray[2]);
					} else if(keyType == 3) {
						//then it is the gamma
						gamma[new Integer(stringArray[1])][new Integer(stringArray[2])]= new Double(stringArray[3]);
					}else {
						//should never be in that branch
						System.err.println("unexpected branch in FileSystemHandler.generateLambdaGammaGradient()");
					}

				}

				//do the write
				writeMatrix(Parameters.pathToLambdas, lambda);
				
				writeVector(Parameters.pathToGradient, delta);
				
				writeMatrix(Parameters.pathToGammas, gamma);
				

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();

			}


		}
	}

	public static double[][] loadGammas(String fileName){
		double[][] gammas = loadMatrix(fileName, Parameters.numberOfDocuments, Parameters.numberOfTopics);
		return gammas;
	}

	public static double[][] loadLambdas(String fileName){
		double[][] lambdas = loadMatrix(fileName, Parameters.numberOfTopics, Parameters.sizeOfVocabulary);
		return lambdas;
	}

	public static double[] loadAlpha(String fileName){
		double[]alpha = loadVector(fileName, Parameters.numberOfTopics);
		return alpha;
	}

	public static double[] loadGradient(String fileName){
		double[]gradient = loadVector(fileName, Parameters.numberOfTopics);
		return gradient;
	}

	private static double[] loadVector(String fileName, int size){
		try {
			FileSystem fs = getFileSystem();

			FSDataInputStream inputStream = fs.open(new Path(fileName));

			String line = inputStream.readLine();

			double[]vector = new double[size];

			String[] stringArray = line.split(" ");


			for (int i = 0; i < stringArray.length; i++) {
				vector[i] = new Double(stringArray[i]);
			}

			inputStream.close();
			return vector;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e){
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;

		} catch(IndexOutOfBoundsException e) {

			e.printStackTrace();
			return null;

		}

	}

	private static double[][] loadMatrix(String fileName, int sizeRows, int sizeColumns) {

		try {
			FileSystem fs = getFileSystem();

			FSDataInputStream inputStream = fs.open(new Path(fileName));

			String line = inputStream.readLine();

			double[][] matrix = new double[sizeRows][sizeColumns];
			int ind = 0;
			while (line != null) {
				String[] stringArray = line.split(" ");
				double[] row = new double[sizeColumns];

				for (int i = 0; i < stringArray.length; i++) {
					row[i] = new Double(stringArray[i]);
				}

				matrix[ind] = row;
				ind++;

				line = inputStream.readLine();
			}

			if (ind != matrix.length) {
				throw new IndexOutOfBoundsException();
			}
			inputStream.close();

			return matrix;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e){
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;

		} catch(IndexOutOfBoundsException e) {

			e.printStackTrace();
			return null;

		}

	}

	

	private static FileSystem getFileSystem() throws IOException {
		return FileSystem.get(new Configuration());
	}

}
