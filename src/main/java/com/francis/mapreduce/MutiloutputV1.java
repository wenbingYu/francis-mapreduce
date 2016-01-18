package com.francis.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutiloutputV1 extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(MutiloutputV1.class);

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new MutiloutputV1(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {

		logger.info("args[0]========" + args[0]);
		logger.info("args[1]========" + args[1]);

		JobConf conf = new JobConf(getConf(), MutiloutputV1.class);

		conf.setJobName("MutiloutputV1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		conf.set("mapred.input.dir", args[0]);
		conf.set("mapred.output.dir", args[1]);
		conf.setOutputFormat(PinyouMultipleOutputs2.class);

		JobClient.runJob(conf);

		return 0;
	}

}

class PinyouMultipleOutputs2 extends
		MultipleTextOutputFormat<Text, IntWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value,
			String name) {

		if ("lisi".equals(key.toString())) {
			return "first";
		} else if ("wangwu".equals(key.toString())) {
			return "second";
		} else {
			return "third";
		}

	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws FileAlreadyExistsException, InvalidJobConfException,
			IOException {
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);

		if (outDir != null) {
			FileSystem fs = outDir.getFileSystem(job);
			// normalize the output directory
			outDir = fs.makeQualified(outDir);
			// setOutputPath(job, outDir);

		}
	}

}
