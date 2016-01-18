package com.francis.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.francis.common.ConvertBlackList;

/**
 * @author wenbing.yu
 * @time 2015-04-03
 * @version 1.0
 * @description:该MR程序为了计算人群报表，使用的hadoop2.X
 * @param
 * 
 **/

public class PeopleUserCategoryV2 extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(PeopleUserCategoryV2.class);

	public static String CATEGORY_INFO = "category_Info";

	public static String AUDIENCES_INFO = "audiences_Info";

	/**
	 * userprofile和category_info处理的Map
	 * 
	 * @param userprofile
	 * @param category_info
	 *            （audience_category）
	 * @param imp
	 *            ,click,reach,impConv,clickConv
	 * 
	 * */
	public static class CategoryMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private Set<String> cateIdSet = new HashSet<String>();

		public void setup(Context context) throws IOException,
				InterruptedException {

			if (context.getCacheFiles() != null
					&& context.getCacheFiles().length > 0) {

				File categoryInfoFile = new File("./" + CATEGORY_INFO);
				if (categoryInfoFile.exists()) {
					if (categoryInfoFile.isDirectory()) {
						File[] subfile = categoryInfoFile.listFiles();
						if (subfile.length > 0) {
							for (File f : subfile) {
								if (f.isFile()) {
									String line;
									BufferedReader br = new BufferedReader(
											new FileReader(f));
									try {
										while ((line = br.readLine()) != null) {
											cateIdSet.add(line.trim());
										}
									} finally {
										br.close();
									}
								}
							}
						}
					} else {
						String line;
						BufferedReader br = new BufferedReader(new FileReader(
								categoryInfoFile));
						try {
							while ((line = br.readLine()) != null) {
								cateIdSet.add(line.trim());
							}
						} finally {
							br.close();
						}
					}

				}

			}

		}

		/**
		 * 处理category_Info输出的map,，作为BasicDataMap的输出
		 * */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] inputUserprofile = line.split("\t");
			logger.info("userprofile data input record lenth====================================="
					+ inputUserprofile.length);
			if (inputUserprofile != null && inputUserprofile.length > 2) {
				String pyid = inputUserprofile[0].toString();
				String cateId = inputUserprofile[1].toString();

				if (!cateIdSet.isEmpty() && cateIdSet.contains(cateId)) {

					context.write(new Text(pyid), new Text(cateId));

				}
			}
			if (inputUserprofile != null && inputUserprofile.length >= 8) {

				String userId = inputUserprofile[0].toString();
				String IdAdvertiserId = inputUserprofile[1].toString();
				String IdOrderId = inputUserprofile[2].toString();
				String IdCampaignId = inputUserprofile[3].toString();
				String IdCompanyId = inputUserprofile[4].toString();
				String IdStrategyId = inputUserprofile[5].toString();
				String type = inputUserprofile[6].toString();
				String targetId = inputUserprofile[7].toString();

				context.write(new Text(userId), new Text(IdAdvertiserId + "\t"
						+ IdOrderId + "\t" + IdCampaignId + "\t" + IdCompanyId
						+ "\t" + IdStrategyId + "\t" + type + "\t" + targetId));

			}

		}

	}

	/**
	 * reduce合并上面两个map的输出
	 * 
	 * */

	public static class CategoryReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> val, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> values = val.iterator();
			List<String> userLog = new LinkedList<String>();
			List<String> unionAll = new LinkedList<String>();
			while (values.hasNext()) {
				String line = values.next().toString();
				if ("".equals(line) && line == null) {
					continue;
				}

				if (line.split("\t").length > 1) {
					unionAll.add(line);
				} else {
					userLog.add(line);
				}
			}

			for (String ul : userLog) {

				String[] userAtt = ul.split("\t");
				if (userAtt.length != 8) {
					continue;
				}

				String IdAdvertiserId = userAtt[1].toString();
				String IdOrderId = userAtt[2].toString();
				String IdCampaignId = userAtt[3].toString();
				String IdCompanyId = userAtt[4].toString();
				String IdStrategyId = userAtt[5].toString();
				String type = userAtt[6].toString();
				// String targetId = userAtt[7].toString();

				for (String ua : unionAll) {
					context.write(key, new Text(IdAdvertiserId + "\t"
							+ IdOrderId + "\t" + IdCampaignId + "\t"
							+ IdCompanyId + "\t" + IdStrategyId + "\t" + type
							+ "\t" + ua));
				}

			}

		}
	}

	/**
	 * 
	 * 上面map的输出作为下一个map的输出处理，同时合并小文件audience
	 * 
	 * @param audience
	 * 
	 **/
	public static class AudiencesMap extends Mapper<Text, Text, Text, Text> {

		private Map<String, String> straAudience = new HashMap<String, String>();

		public void setup(Context context) throws IOException,
				InterruptedException {

			if (context.getCacheFiles() != null
					&& context.getCacheFiles().length > 0) {

				File audiencesInfo = new File("./" + AUDIENCES_INFO);
				if (audiencesInfo.exists()) {
					if (audiencesInfo.isDirectory()) {
						logger.info(audiencesInfo.getPath());
						File[] subfile = audiencesInfo.listFiles();
						if (subfile.length > 0) {
							for (File f : subfile) {
								if (f.isFile()) {
									getStrategyAuIdS(straAudience, f);
								}

							}
						}
					} else {
						getStrategyAuIdS(straAudience, audiencesInfo);

					}
				}

			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] categoryUsers = line.split("\t");
			if (categoryUsers != null && categoryUsers.length == 8) {
				String pyid = categoryUsers[0].toString();
				String IdAdvertiserId = categoryUsers[1].toString();
				String IdOrderId = categoryUsers[2].toString();
				String IdCampaignId = categoryUsers[3].toString();
				String IdCompanyId = categoryUsers[4].toString();
				String IdStrategyId = categoryUsers[5].toString();
				String type = categoryUsers[6].toString();
				String cateId = categoryUsers[7].toString();
				if (straAudience.containsKey(IdStrategyId)) {

					String blackList = straAudience.get(IdStrategyId);
					if (blackList == null || "".equals(blackList)
							|| blackList.indexOf(cateId) == -1) {

						// 1.计算IdAdvertiserId下的数据，按照IdCompanyId, IdAdvertiserId,
						// cateId分组

						context.write(new Text(IdCompanyId + "\t"
								+ IdAdvertiserId + "\t" + cateId), new Text(
								type + "\t" + pyid));

						// 2.计算IdOrderId下的数据，按照IdCompanyId, IdAdvertiserId,
						// IdOrderId , cateId

						context.write(new Text(IdCompanyId + "\t"
								+ IdAdvertiserId + "\t" + IdOrderId + "\t"
								+ cateId), new Text(type + "\t" + pyid));

						// 3.计算IdCampaignId下的数据，按照IdCompanyId, IdAdvertiserId,
						// IdOrderId , IdCampaignId,cateId
						context.write(new Text(IdCompanyId + "\t"
								+ IdAdvertiserId + "\t" + IdOrderId + "\t"
								+ IdCampaignId + "\t" + cateId), new Text(type
								+ "\t" + pyid));

						// 4.计算IdStrategyId下的数据，按照IdCompanyId, IdAdvertiserId,
						// IdOrderId , IdCampaignId,IdStrategyId,cateId

						context.write(new Text(IdCompanyId + "\t"
								+ IdAdvertiserId + "\t" + IdOrderId + "\t"
								+ IdCampaignId + "\t" + IdStrategyId + "\t"
								+ cateId), new Text(type + "\t" + pyid));

					}

				}

			}

		}

	}

	/**
	 * reduce合并上面两个map的输出,为group系列的数据，用于计算7种类别的28张表报的输出
	 * 
	 * */

	public static class PartiReduce extends
			Reducer<Text, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);// 初始化mos
		}

		public void reduce(Text key, Iterable<Text> val, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> values = val.iterator();
			// 获取前一天的时间
			String beforeDate = getBeforeDate();

			// 定义计算uv，pv的数据结构
			Set<String> impUv = new HashSet<String>();
			List<String> impPv = new LinkedList<String>();
			Set<String> clickUv = new HashSet<String>();
			List<String> clickPv = new LinkedList<String>();
			Set<String> reachUv = new HashSet<String>();
			List<String> reachPv = new LinkedList<String>();
			Set<String> impConvUv = new HashSet<String>();
			List<String> impConvPv = new LinkedList<String>();
			Set<String> clickConvUv = new HashSet<String>();
			List<String> clickConvPv = new LinkedList<String>();
			while (values.hasNext()) {
				String line = values.next().toString();
				if ("".equals(line) && line == null) {
					continue;
				}

				String[] str = line.split("\t");
				if (str.length == 2) {
					String type = str[0];
					String pyid = str[1];
					switch (type) {
					case "imp":
						impUv.add(pyid);
						impPv.add(pyid);
						break;
					case "click":
						clickUv.add(pyid);
						clickPv.add(pyid);
						break;
					case "reach":
						reachUv.add(pyid);
						reachPv.add(pyid);
						break;
					case "clickConv":
						clickConvUv.add(pyid);
						clickConvPv.add(pyid);
						break;
					case "impConv":
						impConvUv.add(pyid);
						impConvPv.add(pyid);
						break;
					default:
						break;
					}

				}

				// 对不同的key进行拆分求和

				String[] keystr = key.toString().split("\t");

				int flag = keystr.length;

				String outValue = getTypeValue(beforeDate, keystr, impUv,
						impPv, clickUv, clickPv, reachUv, reachPv, impConvUv,
						impConvPv, clickConvUv, clickConvPv);

				if (flag == 3) {
					// IdAdvertiserId的pv，uv,输出到/group_advertiser目录下
					mos.write("group_advertiser",
							new Text(outValue.toString()), NullWritable.get(),
							"group_advertiser" + "/" + "p");

				} else if (flag == 4) {
					// IdOrderId

					mos.write("group_order", new Text(outValue.toString()),
							NullWritable.get(), "group_order" + "/" + "p");

				} else if (flag == 5) {
					// IdCampaignId
					mos.write("group_campaign", new Text(outValue.toString()),
							NullWritable.get(), "group_campaign" + "/" + "p");

				} else if (flag == 6) {
					// IdStrategyId
					mos.write("group_strategy", new Text(outValue.toString()),
							NullWritable.get(), "group_strategy" + "/" + "p");

				}

			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}

	}

	/**
	 * 根据输入获取strategy对应的互斥属性
	 * 
	 * */
	public static void getStrategyAuIdS(Map<String, String> straAudience,
			File file) {

		// Map<String, String> straAuIds = new HashMap<String, String>();

		String line;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));

			while ((line = br.readLine()) != null) {

				logger.info("line=========" + line);

				String[] straAu = line.toString().split("\t");

				String strategyId = straAu[0].toString();

				String audiencesIds = straAu[1].toString();

				// straAudience.put(strategyId, audiencesIds);

				straAudience.put(strategyId,
						ConvertBlackList.getsplitCategory(audiencesIds));

			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	/**
	 * 根据不同的key构造不同的输出key/value，可以构造序列的对象输出
	 * 
	 * @param []key
	 * @param impUv
	 * @param impPv
	 * @param clickUv
	 * @param clickPv
	 * @param reachUv
	 * @param reachPv
	 * @param impConvUv
	 * @param impConvPv
	 * @param clickConvUv
	 * @param clickConvPv
	 * */

	public static String getTypeValue(String beforeDate, String[] key,
			Set<String> impUv, List<String> impPv, Set<String> clickUv,
			List<String> clickPv, Set<String> reachUv, List<String> reachPv,
			Set<String> impConvUv, List<String> impConvPv,
			Set<String> clickConvUv, List<String> clickConvPv) {

		StringBuilder sb1 = new StringBuilder();
		sb1.append(beforeDate).append(impPv.size()).append("\t")
				.append(impUv.size()).append("\t").append(clickPv.size())
				.append("\t").append(clickUv.size()).append("\t")
				.append(reachPv.size()).append("\t").append(reachUv.size())
				.append("\t").append(impConvPv.size()).append("\t")
				.append(impConvUv.size()).append("\t")
				.append(clickConvPv.size()).append("\t")
				.append(clickConvUv.size());

		int len = key.length;

		String IdCompanyId = "";
		String IdAdvertiserId = "";
		String IdOrderId = "";
		String IdCampaignId = "";
		String IdStrategyId = "";
		String cateId = "";

		StringBuilder sb = new StringBuilder();
		if (3 == len) {
			IdCompanyId = key[0].toString();
			IdAdvertiserId = key[1].toString();
			cateId = key[2].toString();
			sb.append("advertiser").append("\t").append(IdCompanyId)
					.append("\t").append(IdAdvertiserId).append("\t")
					.append(cateId).append("\t").append(sb1);

		} else if (4 == len) {
			IdCompanyId = key[0].toString();
			IdAdvertiserId = key[1].toString();
			IdOrderId = key[2].toString();
			cateId = key[3].toString();
			sb.append("order").append("\t").append(IdCompanyId).append("\t")
					.append(IdAdvertiserId).append("\t").append(cateId)
					.append("\t").append(sb1).append("\t").append(IdOrderId);

		} else if (5 == len) {
			IdCompanyId = key[0].toString();
			IdAdvertiserId = key[1].toString();
			IdOrderId = key[2].toString();
			IdCampaignId = key[3].toString();
			cateId = key[4].toString();
			sb.append("order").append("\t").append(IdCompanyId).append("\t")
					.append(IdAdvertiserId).append("\t").append(cateId)
					.append("\t").append(sb1).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId);
		} else if (6 == len) {
			IdCompanyId = key[0].toString();
			IdAdvertiserId = key[1].toString();
			IdOrderId = key[2].toString();
			IdCampaignId = key[3].toString();
			IdStrategyId = key[4].toString();
			cateId = key[5].toString();
			sb.append("order").append("\t").append(IdCompanyId).append("\t")
					.append(IdAdvertiserId).append("\t").append(cateId)
					.append("\t").append(sb1).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId).append("\t")
					.append(IdStrategyId);

		}

		return sb.toString();

	}

	// 获得当前日期的前一天日期
	public static String getBeforeDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance(); // 得到日历
		calendar.setTime(new Date());// 把当前时间赋给日历
		calendar.add(Calendar.DAY_OF_MONTH, -1); // 设置为前一天
		Date dBefore = calendar.getTime(); // 得到前一天的时间
		String defaultStartDate = sdf.format(dBefore);
		return defaultStartDate;
	}

	@Override
	public int run(String[] arg0) throws Exception {

		for (int i = 0; i < arg0.length; i++) {
			logger.info("arg0" + i + "===============" + arg0[i]);
		}

		JobConf conf = new JobConf(getConf());
		conf.setJobName("PeopleUserCategory");
		Job job = Job.getInstance(getConf(), "PeopleUserCategory");
		job.setJarByClass(getClass());

		// mapper and reducer input
		// 第一个input是category_info，放到cache

		Path category = new Path(arg0[0]);
		String category_info = category.toUri().toString() + "#"
				+ CATEGORY_INFO;
		job.addCacheFile(new URI(category_info));

		// 第二个input是userProfile
		MultipleInputs.addInputPath(job, new Path(arg0[1]),
				TextInputFormat.class, CategoryMap.class);

		// 第三个input是basicdata
		MultipleInputs.addInputPath(job, new Path(arg0[2]),
				TextInputFormat.class, CategoryMap.class);

		JobConf mapper1Conf = new JobConf(false);
		ChainMapper.addMapper(job, CategoryMap.class, LongWritable.class,
				Text.class, Text.class, Text.class, mapper1Conf);

		// 第四个input是audience小文件
		Path audience = new Path(arg0[3]);
		String audience_info = audience.toUri().toString() + "#"
				+ AUDIENCES_INFO;
		job.addCacheFile(new URI(audience_info));

		JobConf mapper2Conf = new JobConf(false);
		ChainMapper.addMapper(job, AudiencesMap.class, Text.class, Text.class,
				Text.class, Text.class, mapper2Conf);

		job.setReducerClass(CategoryReduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(arg0[4]));

		MultipleOutputs.addNamedOutput(job, "group_advertiser",
				TextOutputFormat.class, Text.class, NullWritable.class);

		MultipleOutputs.addNamedOutput(job, "group_order",
				TextOutputFormat.class, Text.class, NullWritable.class);

		MultipleOutputs.addNamedOutput(job, "group_campaign",
				TextOutputFormat.class, Text.class, NullWritable.class);

		MultipleOutputs.addNamedOutput(job, "group_strategy",
				TextOutputFormat.class, Text.class, NullWritable.class);

		boolean res = job.waitForCompletion(true);

		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new PeopleUserCategoryV2(), args);
		System.exit(res);
	}

}
