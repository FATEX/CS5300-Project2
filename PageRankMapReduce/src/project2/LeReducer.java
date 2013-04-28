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
		
		// format of each incoming value is: <nodeID> <pageRank> <degrees>
		// this gives the pageRank & degrees for each node pointing to the key node
		Iterator<Text> itr = values.iterator();
		Text input = new Text();
		String[] inputTokens = null;
		
		Float pageRankIncomingSum = (float) 0.0;
		Float dampingFactor = (float) 0.85;
		Float randomJumpFactor = (1 - dampingFactor) / 685230;
		
		Float pageRankNew = (float) 0.0;
		//Float oldPageRank = (float) 0.0;
		//Float residual = (float) 0.0;
		//Float threshold = (float) 0.01; //Tenth of a percent THIS IS THE CONVERGENCE VALUE
		
		ArrayList<String> edgeList = new ArrayList<String>();
		
		String output = "";

		while (itr.hasNext()) {
			input = itr.next();
			//System.out.println(input);
			String inputStr = input.toString();
			inputTokens = inputStr.split(" ");
			
			edgeList.add(new String(inputTokens[0]));
			Float thisPageRank = new Float(Float.parseFloat(inputTokens[1]));
			Integer thisDegrees = new Integer(Integer.parseInt(inputTokens[2]));
			pageRankIncomingSum += (thisPageRank/thisDegrees);
			
			/*
			if (inputTokens.length == 1) {
				pageRank = pageRank + Float.parseFloat(inputTokens[0]);
			} else {
				oldPageRank = Float.parseFloat(inputTokens[0]);
				for (int i = 0; i < inputTokens.length; i++) {
					edgeList.add(i, new String(inputTokens[i]));
				}
			}
			*/
		}
		
		// to compute pageRankNew:
		// 	pageRankNew = dampingFactor * (sum(pageRank[v]/degrees[v])) + (1 - dampingFactor)/N
		pageRankNew = (dampingFactor * pageRankIncomingSum) + randomJumpFactor;
		
		
		/*if (Float.compare(pageRank, threshold) > 1) {
			residual = (oldPageRank - pageRank) / pageRank;
		} else {
			//System.out.println("Error In The Reducer Class. Something went wrong in the PageRank versus Threshold test.");
			System.out.println(" ");
		}
		
		if (Float.compare(threshold, residual) > 1) {
			System.out.println("Convergence. Value is less than 0.01 terminal value! Yay!");
			pageRank = oldPageRank;
		}
		*/

		
		// output should be 
		//	key:nodeID (for this node)
		//	value:<pageRankNew> <degrees> <outgoing nodeList>
		
		Integer degrees = new Integer(edgeList.size());
		output = pageRankNew + " " + degrees.toString() + " ";

		// TODO: Check if this condition is correct
		/*if (edgeList.size() > 0) {
			edgeList.remove(0);
		}
		edgeList.add(0, new String(String.valueOf(pageRank)));
		*/
		
		for (int i = 0; i < edgeList.size() - 1; i++) {
			output += edgeList.get(i) + ",";
		}
		output += edgeList.get(edgeList.size() - 1);

		Text outputText = new Text(output);

		context.write(key, outputText);
		cleanup(context);
	}

}
