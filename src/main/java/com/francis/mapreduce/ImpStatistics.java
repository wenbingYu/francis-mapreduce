package com.francis.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class ImpStatistics extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(WordCount.class);

	public static class ImpStatisticsMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			String IdAdvertiserId = line[50];
			context.write(new Text(IdAdvertiserId), one);
		}

	}

	public static class ImpStatisticsReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> val, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> values = val.iterator();
			while (values.hasNext()) {
				sum += values.next().get();
			}
			context.write(key, new IntWritable(sum));

		}
	}

	@Override
	public int run(String[] arg0) throws Exception {

		logger.info("arg0======" + arg0[0]);
		logger.info("arg1======" + arg0[1]);
		Job job = Job.getInstance(getConf(), "ImpStatistics");
		job.setJarByClass(getClass());
		// set up the input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(arg0[0]));
		// Mapper
		job.setMapperClass(ImpStatisticsMap.class);
		// Reducer
		job.setReducerClass(ImpStatisticsReduce.class);

		// set up the output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextOutputFormat.setOutputPath(job, new Path(arg0[1]));

		boolean res = job.waitForCompletion(true);
		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {
		logger.info("args length=====" + args.length);
		for (String ar : args) {
			logger.info("args====" + ar);
		}

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new ImpStatistics(), args);
		System.exit(res);
	}
}
