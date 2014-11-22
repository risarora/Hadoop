package com.hadoop.mapjoin2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BabyNameAnalysis extends Configured implements Tool {
	public Configuration config = new Configuration();

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BabyNameAnalysis(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		Job job = new Job(config, BabyNameAnalysis.class.getName());
		job.setJarByClass(BabyNameAnalysis.class);
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		DistributedCache.addCacheFile(new URI(
				"/user/training/BN/ON/OldTestamentNames.txt"), job
				.getConfiguration());
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(PMapper1.class);
		job.setReducerClass(PReducer2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

}

class PMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private static List<String> nameList = new ArrayList<String>();
	private BufferedReader brReader;

	enum MYCOUNTER {
		FILE_EXISTS, SOME_OTHER_ERROR, FILE_NOT_FOUND
	};

	@Override
	public void setup(Context context) throws IOException {
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path eachPath : cacheFilesLocal) {
			if (eachPath.getName().toString().trim()
					.equals("OldTestamentNames.txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				fetchNamesList(eachPath, context);
			}

		}

	}

	private void fetchNamesList(Path filePath, Context context)
			throws IOException {

		String strLineRead = "";

		try {
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				nameList.add(strLineRead.toUpperCase());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		} catch (IOException e) {
			context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			e.printStackTrace();
		} finally {
			if (brReader != null) {
				brReader.close();
			}
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] token = value.toString().split(",");
		if (token.length == 4) {
			try {
				Integer Year = new Integer(token[0]);
				String Name = token[1].replaceAll("\"", "");
				double percentage = new Double(token[2]);
				String gender = token[3].replaceAll("\"", "");
				if (nameList.contains(Name.toUpperCase())) {
					context.write(new Text(Year + "\t" + gender), new Text(
					// context.write(new Text(""+Year ), new Text(
							new String(("FOUND," + percentage))));
				} else
					// context.write(new Text(""+Year ), new Text(
					context.write(new Text(Year + "\t" + gender), new Text(
							new String(("NOTFOUND," + percentage))));
			} catch (NumberFormatException numberFormatException) {
				System.out.println("NumberFormatException");
			}
		}
	}

}

class PReducer2 extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {

		double totalPercentage = 0d;
		double olDTestUsagePntg = 0d;
		int totalCount = 0;

		for (Text YNPG : value) {
			String[] token = YNPG.toString().split(",");

			String found = token[0];
			double percentage = new Double(token[1]);
			totalPercentage += percentage;
			if (found == "FOUND" || found.equalsIgnoreCase("FOUND")) {
				olDTestUsagePntg += percentage;
				totalCount++;

			}
		}
		context.write(key, new Text(new String("" + olDTestUsagePntg + "\t"
				+ totalCount + "\t" + totalPercentage)));

	}
}
