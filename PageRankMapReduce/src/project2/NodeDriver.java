package project2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Simple MapReduce Over Nodes
 *
 */

public class NodeDriver {
	
	/*
	 * There is a pre-processed file called Preprocess_76.txt made by a Python Script that Matthew did that rejects 
	 * 0.998754% of the entries in edges.txt.
	 * fromNetID = 0.76
	 * rejectMin = 0.99 * fromNetID;
	 * rejectLimit = rejectMin + 0.01;
	 * 
	 * Reject Min: 0.752400
	 * Reject Limit: 0.762400
	 */
	
	//TODO: Not Running: Getting java.lang.NoClassDefFoundError
	public static void main(String[] args) throws Exception {

		//Configuration conf = new Configuration();
		
        //String inputFile = "../../../Pre-Process Text Files/Preprocess_76.txt";
		//String inputFile = "../Pre-Process Text Files/Preprocess_76.txt"; REAL ONE
		String inputFile = "../Pre-Process Text Files/PreprocessFinalFile.txt";
		String outputPath = "OutputFolder/";
		
		//conf.set("inputPath", inputFile);
	    //conf.set("outputPath", outputPath);
		
        // Create a new job
        Job job = new Job();

        // Set job name to locate it in the distributed environment
        job.setJarByClass(project2.NodeDriver.class);
        job.setJobName("Simple Node - MapReduce  PageRank");
        
        // Set input and output Path, note that we use the default input format
        // which is TextInputFormat (each record is a line of input)
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Set Mapper and Reducer class
        job.setMapperClass(project2.LeMapper.class);
        job.setReducerClass(project2.LeReducer.class);
        
        //TODO: set input key/value?

        // Set Output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
	
}
