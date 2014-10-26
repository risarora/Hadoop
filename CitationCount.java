import java.io.IOException;
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

public class CitationCount extends Configured implements Tool {
	public static final IntWritable ONE = new IntWritable(1);

	public static void main(String[] args) throws Exception {
		CitationCount driver = new CitationCount();
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
		Job job = new Job(config, jobName);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setJarByClass(CitationCount.class);
		job.setMapperClass(MapperPatentCitation.class);
		job.setReducerClass(ReducerPatentCitation.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// job.setNumReduceTasks(1);
		return (job.waitForCompletion(true) ? 0 : 1);
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
	protected	enum REDUCECOUNTER {
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
}
