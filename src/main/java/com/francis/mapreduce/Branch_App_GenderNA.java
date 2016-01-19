package com.francis.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Branch_App_GenderNA extends Configured implements Tool {

	public static class LogMapper extends Mapper<Object, Text, Text, Text> {
		private Text outStr = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line_array = line.split("\t");

			if (line_array.length == 4) {
				//

				outStr.set(line_array[0]);
				word.set(line_array[1] + ":::" + line_array[2] + ":::"
						+ line_array[3]);

				// System.out.println(outStr);

				context.write(outStr, word);
			}

		}
	}

	public static class Partition extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			String line = key.toString().trim();
			return (line.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private static Text outKey = new Text();
		private static Text outValue = new Text();
		private static Text outSexValue = new Text();
		private double weight = 100.0;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			boolean sex = false;
			String keyStr = key.toString().trim();

			// System.out.println(keyStr);

			outKey.set(keyStr);

			for (Text item : values) {

				String itemStr = item.toString();
				// input format : category:::weight
				String[] item_array = itemStr.split(":::");
				outValue.set(item_array[0] + "\t"
						+ String.valueOf(item_array[1]) + "\t" + item_array[2]);
				context.write(outKey, outValue);

				if (item_array[0].equals("30007")
						|| item_array[0].equals("30008")) {
					sex = true;
				}

			}

			if (false == sex) {
				outSexValue.set("30009" + "\t" + String.valueOf(weight) + "\t"
						+ context.getConfiguration().get("todayStr"));
				context.write(outKey, outSexValue);
				outSexValue.set("30006" + "\t" + String.valueOf(weight) + "\t"
						+ context.getConfiguration().get("todayStr"));
				context.write(outKey, outSexValue);
				outSexValue.set("30005" + "\t" + String.valueOf(weight) + "\t"
						+ context.getConfiguration().get("todayStr"));
				context.write(outKey, outSexValue);
			}

			sex = false;

			// context.write(outKey, new Text("why"));

		}

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Branch_App_GenderNA.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LogMapper.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("todayStr", args[2]);

		// job.getConfiguration().set("filterWeight", args[2]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Branch_App_GenderNA(), args);

		System.exit(res);

	}
}