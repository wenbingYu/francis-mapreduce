package com.francis.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

public class AdunitMergeInfo extends Configured implements Tool {

	public static class UnbidMapper extends Mapper<Object, Text, Text, Text> {
		private Text outStr = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line_array = line.split("\t");

			// pyid, catelist, tid, rule_type
			if (line_array.length >= 99) {
				//

				String ActionId = line_array[0];
				String ActionType = line_array[1];
				String ActionPlatform = line_array[2];
				String maindomain = line_array[3];
				String IdAdUnitId = line_array[4];
				String AdUnitWidth = line_array[5];
				String AdUnitHeight = line_array[6];
				String DeviceType = line_array[7];
				String AdUnitType = line_array[8];
				String AgentType = line_array[9];
				String AdUnitLocation = line_array[10];
				String AdUnitViewType = line_array[11];
				String AdUnitFloorPrice = line_array[12];
				String PayBidPrice = line_array[13];
				String PayWinPrice = line_array[14];
				String AgentUrl = line_array[15];
				String IdAdvertiserId = line_array[16];
				String IdCreativeUnitId = line_array[17];
				String AgentAppid = line_array[18];
				String IdCompanyId = line_array[19];
				String domain = line_array[20];

				outStr.set(ActionId);
				word.set(ActionType + ":::" + ActionPlatform + ":::"
						+ maindomain + ":::" + IdAdUnitId + ":::" + AdUnitWidth
						+ ":::" + AdUnitHeight + ":::" + DeviceType + ":::"
						+ AdUnitType + ":::" + AgentType + ":::"
						+ AdUnitLocation + ":::" + AdUnitViewType + ":::"
						+ AdUnitFloorPrice + ":::" + PayBidPrice + ":::"
						+ PayWinPrice + ":::" + AgentUrl + ":::"
						+ IdAdvertiserId + ":::" + IdCreativeUnitId + ":::"
						+ AgentAppid + ":::" + IdCompanyId + ":::" + domain);
				context.write(outStr, word);
			} else {
				System.out.println("##### lack of data");
			}

		}
	}

	public static class ImpMapper extends Mapper<Object, Text, Text, Text> {
		private Text outStr = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line_array = line.split("\t");

			// pyid, catelist, tid, date
			if (line_array.length >= 10) {
				String actionId = line_array[0];
				String actionType = line_array[1];
				String actionClkid = line_array[2];
				String clk_pyid = line_array[3];
				String advertiser_company_id = line_array[4];
				String target_id = line_array[5];
				String money = line_array[6];
				String order_no = line_array[7];
				String product_list = line_array[8];
				String url = line_array[9];

				outStr.set(actionId);
				word.set(actionType + ":::" + actionClkid + ":::" + clk_pyid
						+ ":::" + advertiser_company_id + ":::" + target_id
						+ ":::" + money + ":::" + order_no + ":::"
						+ product_list + ":::" + url);
				context.write(outStr, word);
			} else {
				System.out.println("-------- lack of data");
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

		// public void reduce(Text key, Iterable<Text> values, Context context)
		// throws IOException, InterruptedException {
		//
		// Adunit adunit = new Adunit();
		//
		// String keyStr = key.toString().trim();
		//
		// adunit.setActionId(keyStr); //actionId
		//
		// for (Text item : values) {
		//
		// String itemStr = item.toString();
		// String[] item_list = itemStr.trim().split(":::");
		//
		// //System.out.println("##########" + keyStr + ":::" + itemStr);
		//
		// if (item_list.length < 1) continue;
		//
		//
		// //3:::Allyes:::pconline.com.cn:::335-01:::970:::90:::General:::Banner:::Browser:::OtherView:::Na:::150:::426:::150:::http://acc.pconline.com.cn/webcam/:::11320:::null
		// //2:::Allyes:::pconline.com.cn:::335-01:::970:::90:::General:::Banner:::Browser:::OtherView:::Na:::150:::426:::0:::http://acc.pconline.com.cn/webcam/:::11320:::null
		// if (Integer.parseInt(item_list[0]) <= 3){
		// if (item_list.length < 20) continue;
		// if(item_list[0].endsWith("1") || item_list[0].endsWith("2"))
		// {
		// adunit.setActionPlatform(item_list[1]);
		// adunit.setMainDomain(item_list[2]);
		// adunit.setIdAdUnitId(item_list[3]);
		// adunit.setAdUnitWidth(item_list[4]);
		// adunit.setAdUnitHeight(item_list[5]);
		// adunit.setDeviceType(item_list[6]);
		// adunit.setAdUnitType(item_list[7]);
		// adunit.setAgentType(item_list[8]);
		// adunit.setAdUnitLocation(item_list[9]);
		// adunit.setAdUnitViewType(item_list[10]);
		// adunit.setAdUnitFloorPrice(item_list[11]);
		// adunit.setAgentAppId(item_list[17]);
		// adunit.setIdCompanyId(item_list[18]);
		// adunit.setDomain(item_list[19]);
		// }
		// switch (Integer.parseInt(item_list[0]))
		// {
		// case 1:
		// adunit.setIsBid("0");
		// break;
		// case 2:
		// adunit.setIsBid("1");
		// adunit.setPayBidPrice(item_list[12]);
		// break;
		// case 3:
		// adunit.setIsImp("1");
		// adunit.setPayWinPrice(item_list[13]);
		// break;
		// }
		//
		//
		// }else if (Integer.parseInt(item_list[0]) > 3){
		// //4:::FB2AlhFFs67C:::FAMHECE8wmi:::4728:::4154:::null:::null:::金地·天地云墅:拨打电话:::http://www.udianfang.cn/dynamic/pc/Building/index?projectcode=jd_tdys_m3&qd_source=pyhd&creation=a&utm_source=pyhd&utm_campaign=jd_tdys
		// if (item_list.length < 9) continue;
		// adunit.setIsClk("1");
		// if (!item_list[3].equals("null") && !item_list[3].equals("-"))
		// adunit.setIsCvt("1");
		// else
		// adunit.setIsCvt("0");
		// adunit.setMoney(item_list[5]);
		//
		// }
		//
		//
		// }//for//
		//
		// String outStr = adunit.getActionPlatform() + "\t" +
		// adunit.getMainDomain() + "\t" + adunit.getIdAdUnitId() + "\t" +
		// adunit.getAdUnitWidth() + "\t" + adunit.getAdUnitHeight()
		// + "\t" + adunit.getAdUnitFloorPrice() + "\t" +
		// adunit.getPayBidPrice() + "\t" + adunit.getPayWinPrice() + "\t" +
		// adunit.getAgentAppId() + "\t" + adunit.getIdCompanyId()
		// + "\t" + adunit.getIsBid() + "\t" + adunit.getIsImp() + "\t" +
		// adunit.getIsClk() + "\t" + adunit.getIsCvt() + "\t" +
		// adunit.getDomain();
		//
		// outKey.set(adunit.getActionId());
		// outValue.set(outStr);
		// context.write(outKey, outValue);
		//
		//
		// }

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(AdunitMergeInfo.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setMapperClass(LogMapper.class);
		job.setPartitionerClass(Partition.class);
		job.setReducerClass(Reduce.class);

		FileSystem fs = FileSystem.get(conf);

		FileStatus[] input_status_0 = fs.globStatus(new Path(args[0]));
		Path[] input_Paths_0 = FileUtil.stat2Paths(input_status_0);

		FileStatus[] input_status_1 = fs.globStatus(new Path(args[1]));
		Path[] input_Paths_1 = FileUtil.stat2Paths(input_status_1);

		TextInputFormat.setMinInputSplitSize(job, 5368709122L);
		// 可以压缩下
		// conf.setCompressMapOutput(true);
		// conf.setMapOutputCompressorClass(GzipCodec.class);

		for (Path p : input_Paths_0) {
			MultipleInputs.addInputPath(job, p, TextInputFormat.class,
					UnbidMapper.class);
		}

		for (Path p : input_Paths_1) {
			MultipleInputs.addInputPath(job, p, TextInputFormat.class,
					ImpMapper.class);
		}

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// job.getConfiguration().set("60daysAgo", args[3]);
		// job.getConfiguration().set("todayStr", args[4]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new AdunitMergeInfo(), args);

		System.exit(res);

	}
}