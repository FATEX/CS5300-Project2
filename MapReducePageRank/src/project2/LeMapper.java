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
		
		String line = value.toString();
		String[] temp = line.split("\t");

		Integer degree = new Integer(temp[2]);
		Float pageRank = new Float(temp[1]);

		Text Node = new Text(temp[0]);

		if (degree == 0) {
			pageRank = (float) 1.0;
		} else {
			pageRank = (float) pageRank / degree;
		}

		Text edgeList = new Text();
		String tempList = "";

		for (int i = 1; i < temp.length - 1; i++) {
			tempList += temp[i] + " ";
		}

		tempList += temp[temp.length - 1];
		edgeList = new Text(tempList);

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
