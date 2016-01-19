//package com.francis.mapreduce;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//import java.util.StringTokenizer;
//import java.util.Map.Entry;
//
//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.CommandLineParser;
//import org.apache.commons.cli.GnuParser;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.util.Options;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
//
//public class UserModelAdvMerge extends Configured implements Tool {
//
//	public static class LogMapper extends Mapper<Object, Text, Text, Text> {
//
//		private Text outStr = new Text();
//		private Text word = new Text();
//		String pyid = "";
//		String cate = "";
//		String weight = "";
//		String timestamp = "";
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//
//			String line = value.toString().trim();
//			String[] temp = line.split("\t");
//
//			if (temp.length > 3) {
//
//				pyid = temp[0].trim();
//				cate = temp[1].trim();
//				weight = temp[2].trim();
//				timestamp = temp[3].trim();
//
//				String[] cate_split = cate.split("#");
//
//				if (cate_split.length > 1) {
//					word.set(pyid + "@@@" + cate_split[0]);
//					outStr.set("1@@@" + timestamp + "@@@" + cate_split[1]
//							+ "@@@" + weight);
//					context.write(word, outStr);
//				}
//			}
//		}
//	}
//
//	public static class RuleMapper extends Mapper<Object, Text, Text, Text> {
//
//		// private final static IntWritable one = new IntWritable(2);
//		private Text outStr = new Text();
//		private Text word = new Text();
//		String pyid = "";
//		String cate = "";
//		String weight = "";
//		String timestamp = "";
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String line = value.toString().trim();
//			String[] temp = line.split("\t");
//
//			if (temp.length > 3 && temp[1].trim().charAt(0) == '8') {
//
//				pyid = temp[0].trim();
//				cate = temp[1].trim();
//				weight = temp[2].trim();
//				timestamp = temp[3].trim();
//
//				String[] cate_split = cate.split("#");
//
//				if (cate_split.length > 1) {
//					word.set(pyid + "@@@" + cate_split[0]);
//					outStr.set("2@@@" + timestamp + "@@@" + cate_split[1]
//							+ "@@@" + weight);
//					context.write(word, outStr);
//				}
//			}
//		}
//	}
//
//	public static class Partition extends Partitioner<Text, Text> {
//
//		@Override
//		public int getPartition(Text key, Text values, int numPartitions) {
//			// TODO Auto-generated method stub
//			String line = key.toString().trim();
//			String[] line_array = line.split("@@@");
//			String pyid = line_array[0];
//			return (pyid.hashCode() & Integer.MAX_VALUE) % numPartitions;
//		}
//	}
//
//	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//
//		private static Text outKey = new Text();
//		private static Text outValue = new Text();
//
//		// BehaviorLabelUtil blUtil = new BehaviorLabelUtil();
//
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//
//			// input format:
//			// EB59f17Qaf4K@@@8:955:p 2@@@20141121020005162@@@09009@@@2.0
//			// EB59f17Qaf4K@@@8:955:p 1@@@20141121020000086@@@09008@@@8.0
//			//
//			String keyStr = key.toString().trim();
//			String[] key_array = keyStr.split("@@@");
//
//			String predayCate = "";
//			String realtimeCate = "";
//			String predayTime = "";
//			String realtimeTime = "";
//			String predayWeight = "";
//			String realtimeWeight = "";
//
//			for (Text item : values) {
//				String[] value_list = item.toString().trim().split("@@@");
//
//				if (value_list.length > 3) {
//					System.out.println("*********" + value_list[0] + " "
//							+ value_list[1] + " " + value_list[2] + " "
//							+ value_list[3] + " " + predayTime + " "
//							+ realtimeTime);
//					if (value_list[0].equals("1")) {
//						System.out.println("-----------" + predayCate + " "
//								+ predayTime + " " + predayWeight);
//						if (value_list[1].compareTo(predayTime) > 0) {
//							predayCate = value_list[2];
//							predayTime = value_list[1];
//							predayWeight = value_list[3];
//							System.out.println("^^^^^^^^^^^^^" + predayCate
//									+ " " + predayTime + " " + predayWeight);
//						}
//					} else {
//						if (value_list[1].compareTo(realtimeTime) > 0) {
//							realtimeCate = value_list[2];
//							realtimeTime = value_list[1];
//							realtimeWeight = value_list[3];
//							System.out
//									.println("%%%%%%%%%%%%%%" + realtimeCate
//											+ " " + realtimeTime + " "
//											+ realtimeWeight);
//						}
//					}
//				}
//			}
//
//			System.out.println("#################" + predayCate + " "
//					+ predayTime + " " + predayWeight + " " + realtimeCate
//					+ " " + realtimeTime + " " + realtimeWeight);
//
//			String mergeCate = BehaviorLabelUtil.mergePureInfo(predayCate,
//					realtimeCate);
//
//			if (realtimeCate != "" && realtimeTime != ""
//					&& realtimeWeight != "") {
//				outKey.set(key_array[0]);
//				outValue.set(key_array[1] + "#" + mergeCate + "\t"
//						+ realtimeWeight + "\t" + realtimeTime);
//				context.write(outKey, outValue);
//			} else {
//				if (predayTime.compareTo(context.getConfiguration().get(
//						"30daysAgo")) > 0) {
//					outKey.set(key_array[0]);
//					outValue.set(key_array[1] + "#" + mergeCate + "\t"
//							+ predayWeight + "\t" + predayTime);
//					context.write(outKey, outValue);
//				}
//			}
//		}
//	}
//
//	public int run(String[] args) throws Exception {
//
//		Configuration conf = getConf();
//		Job job = Job.getInstance(conf);
//
//		job.setJarByClass(UserModelAdvMerge.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		job.setMapperClass(LogMapper.class);
//		job.setMapperClass(RuleMapper.class);
//		job.setPartitionerClass(Partition.class);
//		job.setReducerClass(Reduce.class);
//
//		MultipleInputs.addInputPath(job, new Path(args[0]),
//				TextInputFormat.class, LogMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[1]),
//				TextInputFormat.class, RuleMapper.class);
//		FileOutputFormat.setOutputPath(job, new Path(args[2]));
//
//		job.getConfiguration().set("30daysAgo", args[3]);
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		return 0;
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		Configuration conf = new Configuration();
//		int res = ToolRunner.run(conf, new UserModelAdvMerge(), args);
//
//		System.exit(res);
//
//	}
//}