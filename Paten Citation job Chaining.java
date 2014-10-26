// The program here generates patent citation integrating multiple jobs via job chaining
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CoC extends Configured implements Tool {
	public static final IntWritable ONE = new IntWritable(1);

	public static void main(String[] args) throws Exception {
		CoC driver = new CoC();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
			ToolRunner.printGenericCommandUsage(System.out);
			throw new IllegalArgumentException("Bad number of arguments: "
					+ arg0.length);
		}

		int count = arg0[0].lastIndexOf("/");

		String inputPath = arg0[0];
		String tempDirectory = arg0[0].substring(0, count) + "/tmp";
		String outputDir = arg0[1];
		System.out.println("inputPath : " + inputPath);
		System.out.println("tempDirectory " + tempDirectory);
		System.out.println("outputPath : " + outputDir);

		Configuration config = new Configuration();
		String jobName = "Patent Count";
		Job job = new Job(config, jobName);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tempDirectory));

		job.setJarByClass(CoC.class);
		job.setMapperClass(MapperPatentCitation.class);
		job.setReducerClass(ReducerPatentCitation.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		job.waitForCompletion(true);
		Job job2 = new Job(config, jobName);

		FileInputFormat.addInputPath(job2, new Path(tempDirectory));
		FileOutputFormat.setOutputPath(job2, new Path(outputDir));
		System.out.println("tempDirectory " + tempDirectory);
		System.out.println("outputPath : " + outputDir);

		job2.setJarByClass(CoC.class);
		job2.setMapperClass(MapperPatentCitationCount.class);
		job2.setReducerClass(ReducerPatentCitationCount.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.waitForCompletion(true);
		return 0;
	}


	public static class MapperPatentCitation extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private static IntWritable Pvalue;
		private static final IntWritable ONE = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)

		throws IOException, InterruptedException {
			String[] sValue = value.toString().split(",");
			try {
				Pvalue = new IntWritable(new Integer(sValue[1]));
				context.write(Pvalue, ONE);
			} catch (NumberFormatException E) {
				System.out.println(E.getMessage());
			}

		}
	}

	public static class ReducerPatentCitation extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		protected enum REDUCECOUNTER {
			PATENTS
		}

		protected void reduce(IntWritable key,
				Iterable<IntWritable> valuesList, Context context)
				throws IOException, InterruptedException {
			context.getCounter(REDUCECOUNTER.PATENTS).increment(1);
			int counter = 0;
			for (IntWritable value : valuesList) {
				counter += value.get();
			}
			context.write(key, new IntWritable(counter));

		}
	}

	public static class MapperPatentCitationCount extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private static final IntWritable uno = new IntWritable(1);
		private static IntWritable Pvalue = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] sValue = value.toString().split("\t");
			try {
				Pvalue = new IntWritable(new Integer(sValue[1]));
				context.write(Pvalue, uno);
			} catch (NumberFormatException E) {
				System.out.println(E.getMessage());
			} catch (java.lang.ArrayIndexOutOfBoundsException Aexc) {
				throw new IOException("Error@" + key.toString() + value);
			}

		}
	}

	public static class ReducerPatentCitationCount extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		// @Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int counter = 0;
			Iterator<IntWritable> iter = values.iterator();
			while (iter.hasNext()) {
				counter += iter.next().get();

			}
			context.write(key, new IntWritable(counter));
		}
	}

}
