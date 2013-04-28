package project2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class LeReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> itr = values.iterator();
		Text input = new Text();
		String[] inputTokens = null;
		
		Float pageRank = (float) 0.0;
		Float oldPageRank = (float) 0.0;
		Float residual = (float) 0.0;
		Float threshold = (float) 0.01; //Tenth of a percent THIS IS THE CONVERGENCE VALUE
		
		ArrayList<String> edgeList = new ArrayList<String>();
		
		String output = "";

		while (itr.hasNext()) {
			input = itr.next();
			//System.out.println(input);
			String inputStr = input.toString();
			inputTokens = inputStr.split(" ");
			
			if (inputTokens.length == 1) {
				pageRank = pageRank + Float.parseFloat(inputTokens[0]);
			} else {
				oldPageRank = Float.parseFloat(inputTokens[0]);
				for (int i = 0; i < inputTokens.length; i++) {
					edgeList.add(i, new String(inputTokens[i]));
				}
			}
		}
		
		if (Float.compare(pageRank, threshold) > 1) {
			residual = (oldPageRank - pageRank) / pageRank;
		} else {
<<<<<<< HEAD
			//System.out.println("Error In The Reducer Class. Something went wrong in the PageRank versus Threshold test.");
			System.out.println(" ");
=======
			System.out.println("Error In The Reducer Class. Something went wrong in the PageRank versus Threshold test.");
			//System.out.println("");
>>>>>>> Updated PreprocessFinalFIle.txt
		}
		
		if (Float.compare(threshold, residual) > 1) {
			System.out.println("Convergence. Value is less than 0.01 terminal value! Yay!");
			pageRank = oldPageRank;
		}

		// TODO: Check if this condition is correct
		if (edgeList.size() > 0) {
			edgeList.remove(0);
		}
		edgeList.add(0, new String(String.valueOf(pageRank)));
		
		for (int i = 0; i < edgeList.size() - 1; i++) {
			output += edgeList.get(i) + " ";
		}
		
		output += edgeList.get(edgeList.size() - 1);

		Text outputText = new Text(output);

		context.write(key, outputText);
		cleanup(context);
	}

}
