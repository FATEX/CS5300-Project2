package project2;

/**
 * This class detects the implicit edges and performs spatial joins.
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DetectEdge {
	public static final double rejectMin = FilterMechanics.rejectMin;
	public static final double rejectMax = FilterMechanics.rejectMax;

	public static class Pass1Reducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			double rejectMax = context.getConfiguration().getFloat("rejectMax",
					0);

			Text pair = new Text();
			ArrayList<Point> points = new ArrayList<Point>();
			ArrayList<Point> boundary = new ArrayList<Point>();
			ArrayList<Edge> edges = new ArrayList<Edge>();

			for (Text val : values) {
				String[] pieces = val.toString().split(":");
				Point p = new Point(new Double(pieces[0]),
						new Double(pieces[1]));
				if (pieces.length == 3) {
					boundary.add(p);
				}
				points.add(p);
			}

			// Perform Spatial Join
			Collections.sort(points);

			int j = 0;
			TreeSet<Point> set = new TreeSet<Point>(new ComparePoints());

			for (int i = 0; i < points.size(); i++) {
				Point p1 = points.get(i);
				set.add(p1);
				System.out.println("Adding " + p1.toString());

				while (points.get(j).getX() < (points.get(i).getX() - rejectMax)) {
					set.remove(points.get(j));
					System.out.println("Removing " + points.get(j).toString());
					j++;
				}

				Point finish = set.ceiling(new Point(p1.getX(), p1.getY()
						+ rejectMax));
				Point start = set.ceiling(new Point(p1.getX(), p1.getY()
						- rejectMax));

				SortedSet<Point> subset;

				if (finish == null) {
					System.out.println("The Tailset:" + start.toString());
					subset = set.tailSet(start);
				} else {
					// System.out.println("subset:" + start.toString() + " " +
					// finish.toString());
					subset = set.subSet(start, finish);
				}

				for (Point p : subset) {
					double distance = (p1.x - p.x) * (p1.x - p.x)
							+ (p1.y - p.y) * (p1.y - p.y);
					System.out.println("Distance between points (DetectEdge): "
							+ distance);

					if (distance > 0 && distance <= (rejectMax * rejectMax)) {
						System.out.println("Adding Edge");
						edges.add(new Edge(p, p1));
					}
				}
			}

			for (Edge e : edges) {
				pair.set(e.toString());
				context.write(key, pair);
			}
		}
	}

	public static class Pass2Mapper extends	Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable result = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer iterator = new StringTokenizer(value.toString(), "\\n");
			while (iterator.hasMoreTokens()) {
				String[] pair = iterator.nextToken().split("\\s");
				word.set(pair[1]);
				context.write(word, result);
			}
		}
	}

	public static class Pass2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable(0);

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
			context.getCounter(FinalProject.ProjectCounters.EDGES).increment(1);
		}
	}
	
	public static int run(Configuration conf, String inputPath, String outputPath) throws Exception {

		    // Pass1 filters out unused vertices and finds implicit edges per group
		    Job job1 = new Job(conf, "Connected Components 1");
		    job1.setJarByClass(DetectEdge.class);
		    job1.setMapperClass(FinalProject.Pass1Mapper.class);
		    job1.setReducerClass(Pass1Reducer.class);
		    job1.setOutputKeyClass(IntWritable.class);
		    job1.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/EF1"));
		    job1.waitForCompletion(true);
		    
		    //TODO: Description
		    Job job2 = new Job(conf, "Connected Components 1");
		    job2.setJarByClass(DetectEdge.class);
		    job2.setMapperClass(Pass2Mapper.class);
		    job2.setReducerClass(Pass2Reducer.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job2, new Path(outputPath + "/EF1"));
		    FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/EF2"));
		    job2.waitForCompletion(true);

		    return ((int) job2.getCounters().findCounter(FinalProject.ProjectCounters.EDGES).getValue());
		  }
	
}
