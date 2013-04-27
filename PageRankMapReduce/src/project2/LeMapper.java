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

public class LeMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected Text capture = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Line is in the format['node','PageRank Estimate','degrees(node)','other node'] delimiter = " "
		
		String line = value.toString();
		line = line.trim();
		//System.out.println(line);
		String[] temp = line.split(" ");
		/*System.out.println(temp[0]);
		System.out.println(temp[1]);
		System.out.println(temp[2]);
		System.out.println(temp[3]);*/
		
		Text Node = new Text(temp[0]);
		Float pageRank = new Float(temp[1]);
		Integer degree = new Integer(temp[2]);
		

		if (degree == 0) {
			pageRank = (float) 1.0;
		} //Already divided by the degree in the Preprocess
		
		//Concatenate The String For The Reducer To Process With The Outgoing Edges
		Text edgeList = new Text();
		
		//TODO: Pretty sure the file is already in a good format.
		/*String tempList = "";
		for (int i = 0; i < temp.length - 1; i++) {
			tempList += temp[i] + " ";
		}
		tempList += temp[temp.length - 1];*/
		
		//TODO: Verify How The File Should Look
		edgeList = new Text(line);
		context.write(Node, edgeList);
		cleanup(context);
		Text nodeOut = new Text();
		Text pageRankText = new Text(String.valueOf(pageRank));

		for (int i = 3; i < temp.length; i++) {
			nodeOut = new Text(temp[i]);
			context.write(nodeOut, pageRankText);
			cleanup(context);
		}
	}
}
