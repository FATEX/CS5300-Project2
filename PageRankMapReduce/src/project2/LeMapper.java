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
	//protected Text capture = new Text();
	private Float residualSum = new Float(0.0);
	private static int totalNodes = 685230;

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//value is in the format['node','PageRank Estimate','degrees(node)','outgoing nodelist'] delimiter = " "
		
		String line = value.toString();
		line = line.trim();
		String[] temp = line.split(" ");
		
		Text node = new Text(temp[0]);
		Float pageRank = new Float(temp[1]);
		Integer degree = new Integer(temp[2]);
		
		// to pass along previous pageRank, map key:node value:"PR" pageRank
		Text mapperKey = new Text(node);
		Text mapperPRValue = new Text("PR " + String.valueOf(pageRank));
		context.write(mapperKey, mapperPRValue);

		// now map key:nodeOut value:node pageRank degree
		Text mapperValue = new Text(node + " " + String.valueOf(pageRank) + " " + String.valueOf(degree));
		String[] edgeList = temp[3].split(",");

		for (int i = 0; i < edgeList.length; i++) {
			mapperKey = new Text(edgeList[i]);
			context.write(mapperKey, mapperValue);
		}
		cleanup(context);
	}
}
