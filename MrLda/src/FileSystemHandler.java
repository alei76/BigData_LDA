
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * this class deals with the reads and writes from the hdfs
 * @author khalilhajji
 *
 */
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

			//	FSDataOutputStream outputStream = fs.create(path);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));


			String line ="";
			for (int i = 0; i < matrix.length; i++) {
				for (int j = 0; j < matrix[0].length; j++) {
					line = line + matrix[i][j] +" "; 

				}
				//remove the last space
				line = line.substring(0, line.length()-1);
				//add the line jump
				line=line+"\n";
				//	outputStream.writeChars(line);
				br.write(line);
				line = "";

			}
			//outputStream.close();
			br.close();


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

			//	FSDataOutputStream outputStream = fs.create(path);
			BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));


			String line ="";
			for (int i = 0; i < vector.length; i++) {

				line = line + vector[i] +" "; 

			}
			//remove the last space
			line = line.substring(0, line.length()-1);
			br.write(line);
			//outputStream.writeChars(line);
			//outputStream.close();
			br.close();



		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}


	}

	public static void deletePath(String fileName){
		try {
			FileSystem fs = getFileSystem();
			fs.delete(new Path(fileName),true);


		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	
	/**
	 * takes the output of the reducers and generate the gamma file, the lambda file and the gradient file
	 * in the good format
	 * @param pathJobOutput
	 * @param pathToLambda
	 * @param pathToGamma
	 * @param pathToGradient
	 */
	public static void convertJobOutputToLambdaGammaGradient(String pathJobOutput, String pathToLambda, String pathToGamma, String pathToGradient) {
		//the lambda matrix that will be constituted
		double[][]lambda = new double[Parameters.sizeOfVocabulary][Parameters.numberOfTopics];


		//the gamma matrix that will be constituted
		double[][] gamma = new double [Parameters.numberOfDocuments][Parameters.numberOfTopics];

		//may be needed if we find some keys with value -1;
		double[] delta = new double [Parameters.numberOfTopics];


		//go throught all the files of the directory
		File folder = new File(pathJobOutput);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			//for each file read all the values and put them in the matrix
			String fileName = listOfFiles[i].getPath();
			System.out.println("file names in our methods : " + fileName);
			if(!fileName.contains("_SUCESS") && !fileName.contains("crc")){
				System.out.println("We are reading the files.");


				try {
					System.out.println("We are trying.");
					FileSystem fs = getFileSystem();

					
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
					String line = br.readLine();


					while (line != null) {
						System.out.println(line);
						String[] stringArray = line.split(",|\\s");
						for(int i1 = 0; i1 < stringArray.length; i1 ++){
							System.out.println(stringArray[i1]);
						}
						int keyType = new Integer(stringArray[0]);
						if(keyType == 1) {
							//then it is the lambda
							lambda[new Integer(stringArray[2])][new Integer(stringArray[1])]= new Double(stringArray[3]);
						} else if(keyType == 2) {
							//then it is the delta
							delta[new Integer(stringArray[2])] = new Double(stringArray[2]);
						} else if(keyType == 3) {
							//then it is the gamma
							gamma[new Integer(stringArray[2])][new Integer(stringArray[1])]= new Double(stringArray[3]);
						}else {
							//should never be in that branch
							System.err.println("unexpected branch in FileSystemHandler.generateLambdaGammaGradient()");
						}
						line=br.readLine();

					}

					//do the write
					writeMatrix(pathToLambda, lambda);

					writeVector(pathToGradient, delta);

					writeMatrix(pathToGamma, gamma);


				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}
			}

		}
	}

	public static double[][] loadGammas(String fileName){
		double[][] gammas = loadMatrix(fileName, Parameters.numberOfDocuments, Parameters.numberOfTopics);
		return gammas;
	}

	public static double[][] loadLambdas(String fileName){
		double[][] lambdas = loadMatrix(fileName, Parameters.sizeOfVocabulary, Parameters.numberOfTopics);
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
		BufferedReader br;
		try {
			FileSystem fs = getFileSystem();

			br=new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));

			String line=br.readLine();

			double[]vector = new double[size];

			String[] stringArray = line.split(" ");


			for (int i = 0; i < stringArray.length; i++) {
				vector[i] = new Double(stringArray[i]);
			}

			br.close();
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
		BufferedReader br;
		try {
			FileSystem fs = getFileSystem();

			br=new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));

			String line=br.readLine();
			System.out.println("We have succesfully read the line." + line);

			double[][] matrix = new double[sizeRows][sizeColumns];
			int ind = 0;
			while (line != null) {
				System.out.println("Out line is not null");
				System.out.println("the line is : " + line );
				String[] stringArray = line.split(" ");
				double[] row = new double[sizeColumns];

				for (int i = 0; i < stringArray.length; i++) {
					System.out.println("*"+stringArray[i]+"*");
					Double val = Double.valueOf(stringArray[i]);
					row[i] = (double)val;
					System.out.println("row : " + i + " " + row[i]);
				}

				matrix[ind] = row;
				ind++;

				line = br.readLine();
				
			}

			if (ind != matrix.length) {
				System.out.println("Problem.");
				throw new IndexOutOfBoundsException();
			}

			br.close();
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
