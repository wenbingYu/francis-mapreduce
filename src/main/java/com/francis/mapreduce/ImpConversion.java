package com.francis.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenbing.yu
 * @time 2015-04-03
 * @version 1.0
 * @description:该MR程序为了计算人群报表，使用的hadoop2.X
 * @param
 * 
 **/

public class ImpConversion extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(ImpConversion.class);

	/**
	 * 
	 * @param implog
	 * @param noclickconv
	 * 
	 * */
	public static class ImpDetailMap extends
			Mapper<LongWritable, Text, Text, Text> {

		/**
		 * 处理category_Info输出的map,，作为BasicDataMap的输出
		 * */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] logType = line.split("\t");
			logger.info("impdetail data input record lenth====================================="
					+ logType.length);
			if (logType != null && logType.length == 99) {
				// 曝光的输入

				String pyid = logType[0].toString();
				String company_id = logType[1].toString();
				String imp_time = logType[2].toString();
				String advertiser_id = logType[3].toString();
				context.write(new Text(pyid + ":::" + company_id + ":::"
						+ imp_time), new Text("imp" + "\t" + advertiser_id));

			} else if (logType != null && logType.length == 50) {
				// 非点击转化的输出
				String pyid = logType[9].toString();
				String company_id = logType[48].toString();
				String conv_time = logType[5].toString();
				String strategy_id = logType[29].toString();
				context.write(new Text(pyid + ":::" + company_id + ":::"
						+ conv_time), new Text("conv" + "\t" + strategy_id));
			}

		}

	}

	/**
	 * reduce合并上面两个map的输出
	 * 
	 * */

	public static class ImpConvReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> val, Context context)
				throws IOException, InterruptedException {

			String last_pyid = "";
			String last_company_id = "";
			String last_type = "";

			String[] keyline = key.toString().split(":::");

			String cur_pyid = keyline[0];
			String cur_company_id = keyline[1];
			String cur_type = keyline[2];

			if (cur_pyid.equals(last_pyid)
					&& cur_company_id.equals(last_company_id)
					&& "conv".equals(cur_type) && "imp".equals(last_type)) {
				context.write(new Text(cur_company_id + "\t" + cur_pyid + "\t"
						+ cur_type), new Text(cur_company_id));
			}

			last_pyid = cur_pyid;
			last_company_id = cur_company_id;
			last_type = cur_type;

		}
	}

	public static class Partition extends Partitioner<Text, Text> {

		public int getPartition(Text key, Text value, int numPartitions) {
			String line = key.toString();
			String[] line_array = line.split(":::");
			String pyid = line_array[0];
			String company_id = line_array[1];
			String pkey = pyid + ":::" + company_id;

			return (pkey.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {

		for (int i = 0; i < arg0.length; i++) {
			logger.info("arg0" + i + "===============" + arg0[i]);
		}

		JobConf conf = new JobConf(getConf());
		conf.setJobName("impconvertion");
		Job job = Job.getInstance(getConf(), "impconvertion");
		job.setJarByClass(getClass());

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(arg0[0]));
		TextInputFormat.addInputPath(job, new Path(arg0[1]));
		// Mapper
		job.setMapperClass(ImpDetailMap.class);
		// Reducer
		job.setReducerClass(ImpConvReduce.class);
		// Partitioner
		job.setPartitionerClass(Partition.class);

		// set up the output
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextOutputFormat.setOutputPath(job, new Path(arg0[2]));

		boolean res = job.waitForCompletion(true);

		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new ImpConversion(), args);
		System.exit(res);
	}

}
