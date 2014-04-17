

	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;





public class Run {
	public static void main(String[] args) throws Exception {
		
		String inputFolder = args[0];
		String outputFolder = args[1];
		String intermediateFolder = outputFolder + "-temp";
		String finalFolder = outputFolder + "final";
		
	
		
		JobConf conf2 = new JobConf(LdaReducer.class);
		conf2.setJobName("Counter");
 	
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(IntWritable.class);
		
 	
		//conf2.setMapperClass(SimpleCounter.Map.class);
		conf2.setCombinerClass(LdaReducer.Reduce.class);
		conf2.setReducerClass(LdaReducer.Reduce.class);
 	
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
 	
		FileInputFormat.setInputPaths(conf2, new Path(intermediateFolder));
		FileOutputFormat.setOutputPath(conf2, new Path(finalFolder));
 	
		JobClient.runJob(conf2);

		
		
		
	}
	
	
	public static double[] updateAlpha(double[] oldAlpha){
		return null;
		/**
		 * TODO
		 */
		
		//Follow part 3.4 of Mr. Lda paper.
	}
}