import java.io.IOException;

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

class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static IntWritable ONE = new IntWritable(1);

	static enum Number {
		One, Two, Three, Four
	};

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String tokens[] = value.toString().split(" ");
		for (String word : tokens) {
			context.write(new Text(word), ONE);
			if (word.length() == 1)
				context.getCounter(Number.One).increment(1);
			else if (word.length() == 2)
				context.getCounter(Number.Two).increment(1);
			else if (word.length() == 3)
				context.getCounter(Number.Three).increment(1);
			else if (word.length() == 4)
				context.getCounter(Number.Four).increment(1);

		}

	}
}

class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> count, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable num : count) {
			sum += new Integer(num.toString());

		}
		context.write(key, new IntWritable(sum));
	}
}

public class WCClass extends Configured implements Tool {
	public static final IntWritable ONE = new IntWritable(1);

	public static void main(String[] args) throws Exception {
		WCClass driver = new WCClass();
		if (args.length != 2) {
			// System.out.println("",);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, WCClass.class.getName());
		job.setJarByClass(WCClass.class);

		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		// job.setCombinerClass(WCReducer.class);
		/*
		 * job.setMapOutputKeyClass(Text.class);
		 * job.setOutputKeyClass(IntWritable.class);
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// job.submit(); return 1;
		return job.waitForCompletion(true) ? 1 : 0;

	}
}
