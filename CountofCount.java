import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountofCount extends Configured implements Tool {
	public static final IntWritable ONE = new IntWritable(1);

	public static void main(String[] args) throws Exception {
		CountofCount driver = new CountofCount();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			throw new IllegalArgumentException("Bad number of arguments: "
					+ args.length);
		}
		String inputPath = args[0];
		String outputDir = args[1];

		System.out.println("inputPath : " + inputPath);
		System.out.println("outputPath : " + outputDir);

		Configuration config = new Configuration();
		String jobName = "Patent Count";
		Job job2 = new Job(config, jobName);
		FileInputFormat.addInputPath(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputDir));

		job2.setJarByClass(CitationCount.class);
		job2.setMapperClass(MapperPatentCitationCount.class);
		job2.setReducerClass(ReducerPatentCitationCount.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		// job.setNumReduceTasks(1);
		return (job2.waitForCompletion(true) ? 0 : 1);

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
			Iterator <IntWritable>iter = values.iterator();
			while (iter.hasNext()) {
				counter += iter.next().get();

			}
			context.write(key, new IntWritable(counter));
		}
	}
}
