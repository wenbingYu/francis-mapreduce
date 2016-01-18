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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenbing.yu
 * @version v1.1
 * @description:实现多路径和多文件输出
 *
 */
public class MutilOuput extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(MutilOuput.class);

	public static class MutilOuputMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreElements()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

	}

	public static class MutilOuputReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> mos;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		public void reduce(Text key, Iterable<IntWritable> val, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> values = val.iterator();
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if ("lisi".equals(key.toString())) {
				// mos.write("lisi", key, new IntWritable(sum), "lisi" + "/" +
				// "p");
				mos.write(key, new IntWritable(sum), "first/lisi/" + "p");
				// mos.write("lisi", key, new IntWritable(sum));
			} else if ("wangwu".equals(key.toString())) {
				// mos.write("wangwu", key, new IntWritable(sum), "wangwu" + "/"
				// + "p");
				mos.write(key, new IntWritable(sum), "second/wangwu/" + "p");

				// mos.write("wangwu", key, new IntWritable(sum));
			} else {
				// mos.write("others", key, new IntWritable(sum), "others" + "/"
				// + "p");
				mos.write(key, new IntWritable(sum), "third/others/" + "p");

				// mos.write("others", key, new IntWritable(sum));
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {

		Job job = Job.getInstance(getConf(), "MutilOuput");
		job.setJarByClass(getClass());
		// set up the input
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(arg0[0]));
		// Mapper
		job.setMapperClass(MutilOuputMap.class);
		// Reducer
		job.setReducerClass(MutilOuputReduce.class);

		// set up the output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		TextOutputFormat.setOutputPath(job, new Path(arg0[1]));
		// MultipleOutputs.addNamedOutput(job, "lisi", TextOutputFormat.class,
		// Text.class, IntWritable.class);
		// MultipleOutputs.addNamedOutput(job, "wangwu", TextOutputFormat.class,
		// Text.class, IntWritable.class);
		// MultipleOutputs.addNamedOutput(job, "others", TextOutputFormat.class,
		// Text.class, IntWritable.class);

		boolean res = job.waitForCompletion(true);
		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new MutilOuput(), args);
		System.exit(res);
	}
}
