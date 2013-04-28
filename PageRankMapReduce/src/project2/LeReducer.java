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
		Float randomJumpFactor = (1 - dampingFactor) / NodeDriver.totalNodes;
		
		Float pageRankNew = (float) 0.0;
		Float pageRankOld = (float) 0.0;
		Float residual = (float) 0.0;
		
		ArrayList<String> edgeList = new ArrayList<String>();
		
		String output = "";

		while (itr.hasNext()) {
			input = itr.next();
			String inputStr = input.toString();
			inputTokens = inputStr.split(" ");
			
			// the "PR" signifies this is the previous pagerank for this node
			if (inputTokens[0] == "PR") {
				pageRankOld = Float.parseFloat(inputTokens[1]);
			
			// otherwise it is the information for the incoming node
			} else {			
				edgeList.add(new String(inputTokens[0]));
				Float thisPageRank = new Float(Float.parseFloat(inputTokens[1]));
				Integer thisDegrees = new Integer(Integer.parseInt(inputTokens[2]));
				pageRankIncomingSum += (thisPageRank / thisDegrees);
			}
			
		}
		
		// compute pageRankNew:
		// 	pageRankNew = dampingFactor * (sum(pageRank[v]/degrees[v])) + (1 - dampingFactor)/N
		pageRankNew = (dampingFactor * pageRankIncomingSum) + randomJumpFactor;
		
		// compute the residual error for this node
		residual = Math.abs(pageRankOld - pageRankNew) / pageRankNew;
		
		// add the residual error to the counter that is tracking the overall sum (must be expressed as an integer)
		int residualAsInt = (int) Math.floor(residual * NodeDriver.precision);
		context.getCounter(NodeDriver.ProjectCounters.RESIDUAL_ERROR).increment(residualAsInt);
		
		// output should be 
		//	key:nodeID (for this node)
		//	value:<pageRankNew> <degrees> <outgoing nodeList>		
		Integer degrees = new Integer(edgeList.size());
		output = pageRankNew + " " + degrees.toString() + " ";
		
		for (int i = 0; i < edgeList.size() - 1; i++) {
			output += edgeList.get(i) + ",";
		}
		output += edgeList.get(edgeList.size() - 1);

		Text outputText = new Text(output);

		context.write(key, outputText);
		cleanup(context);
	}

}
