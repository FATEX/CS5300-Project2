package project2;

/**
 * Compute filter parameters for netID mcg67 double fromNetID = 0.76; 
 * double rejectMin = 0.99 * fromNetID; (0.7524) 
 * double rejectLimit = rejectMin + 0.01; (0.7624)
 *
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterMechanics {
	public static final double rejectMin = 0.7524;
	public static final double rejectMax = 0.7624;

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\\n");
			String currentLine;
			String[] currentLinePieces;
			double w;

			while (itr.hasMoreTokens()) {
				currentLine = itr.nextToken();
				currentLinePieces = currentLine.split("\\s");
				w = new Double(currentLinePieces[2]);
				if ((w <= rejectMax) && (w >= rejectMin)) {
					word.set(currentLine);
					context.write(word, ONE);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable results = new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			context.getCounter(FinalProject.ProjectCounters.VERTICES)
					.increment(1);
			context.write(key, results);
		}
	}

	public static Counters run() throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "vertex");
		job.setJarByClass(FilterMechanics.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("../input"));
		FileOutputFormat.setOutputPath(job, new Path(
				"../output/FilterMechanics"));

		job.waitForCompletion(true);

		return job.getCounters();
	}
}
