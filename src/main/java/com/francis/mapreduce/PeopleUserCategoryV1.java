package com.francis.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.francis.common.ConvertBlackList;
import com.francis.utils.DateUtils;


/**
 * @author wenbing.yu
 * @time 2015-04-10
 * @version 1.0
 * @description:该MR程序为了计算人群报表,使用的hadoop1.X
 * @param
 * 
 **/

public class PeopleUserCategoryV1 extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(PeopleUserCategoryV1.class);

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
	public static class CategoryMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Set<String> cateIdSet = new HashSet<String>();

		BufferedReader categoryReader = null;

		public void configure(JobConf job) {

			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(job);

				if (paths != null && paths.length > 0) {

					File file = new File(paths[0].toString());
					if (file.isDirectory()) {
						File[] subfile = file.listFiles();
						for (File sbfile : subfile) {
							String line;
							categoryReader = new BufferedReader(new FileReader(
									sbfile.toString()));
							while ((line = categoryReader.readLine()) != null) {
								String[] cateIds = line.split("\t");
								if (cateIds != null && cateIds.length > 0) {
									cateIdSet.add(cateIds[0]);
								}

							}
						}
					} else {
						String line;
						categoryReader = new BufferedReader(new FileReader(
								file.toString()));
						while ((line = categoryReader.readLine()) != null) {
							String[] cateIds = line.split("\t");
							if (cateIds != null && cateIds.length > 0) {
								cateIdSet.add(cateIds[0]);
							}

						}

					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (categoryReader != null) {
					try {
						categoryReader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString().trim();
			String[] inputUserprofile = line.split("\t");

			if (inputUserprofile != null && inputUserprofile.length == 3) {
				String pyid = inputUserprofile[0].toString();
				String cateId = inputUserprofile[1].toString();

				if (!cateIdSet.isEmpty() && cateIdSet.contains(cateId)) {

					output.collect(new Text(pyid + ":::A"), new Text(cateId));

				}
			}
			if (inputUserprofile != null && inputUserprofile.length == 8) {

				String userId = inputUserprofile[0].toString();
				String IdAdvertiserId = inputUserprofile[1].toString();
				String IdOrderId = inputUserprofile[2].toString();
				String IdCampaignId = inputUserprofile[3].toString();
				String IdCompanyId = inputUserprofile[4].toString();
				String IdStrategyId = inputUserprofile[5].toString();
				String type = inputUserprofile[6].toString();
				String targetId = inputUserprofile[7].toString();

				output.collect(new Text(userId + ":::B"), new Text(
						IdAdvertiserId + "\t" + IdOrderId + "\t" + IdCampaignId
								+ "\t" + IdCompanyId + "\t" + IdStrategyId
								+ "\t" + type + "\t" + targetId));

			}

		}

	}

	public static class CategoryPartitioner implements Partitioner<Text, Text> {

		@Override
		public void configure(JobConf job) {

		}

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {

			String[] line = key.toString().split(":::");

			String parkey = line[0];

			return (parkey.hashCode() & Integer.MAX_VALUE) % numPartitions;

		}

	}

	/**
	 * reduce合并上面两个map的输出,一个basic数据的输出，一个join的输出（pyid,cateId）
	 * 
	 * */
	public static class CategoryReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		String prekey = "";
		List<String> cateds = new LinkedList<String>();

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] line = key.toString().split(":::");

			String outkey = line[0];

			logger.info("key============" + outkey + "prekey============="
					+ prekey + "tag-==============" + line[1]
					+ "==================value=" + values);

			Iterator<String> it = cateds.iterator();
			while (it.hasNext()) {
				logger.info("cateds===============" + it.next());
			}

			if (!prekey.equals(outkey)) {
				cateds.clear();

			}

			if ("A".equals(line[1])) {

				while (values.hasNext()) {
					cateds.add(values.next().toString());
				}

				logger.info("A result===================" + cateds.size());

			}

			if (outkey.equals(prekey) && "B".equals(line[1])) {

				while (values.hasNext()) {

					String[] basicline = values.next().toString().split("\t");

					String IdAdvertiserId = basicline[0].toString();
					String IdOrderId = basicline[1].toString();
					String IdCampaignId = basicline[2].toString();
					String IdCompanyId = basicline[3].toString();
					String IdStrategyId = basicline[4].toString();
					String type = basicline[5].toString();

					for (int i = 0; i < cateds.size(); i++) {
						output.collect(new Text(outkey), new Text(
								IdAdvertiserId + "\t" + IdOrderId + "\t"
										+ IdCampaignId + "\t" + IdCompanyId
										+ "\t" + IdStrategyId + "\t" + type
										+ "\t" + cateds.get(i)));

					}

				}
				cateds.clear();

			}

			prekey = outkey;

		}

	}

	/**
	 * 
	 * 上面map的输出作为下一个map的输出处理，同时合并小文件audience
	 * 
	 * @param audience
	 * 
	 **/

	public static class AudiencesMap extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		private Map<String, String> straAudience = new HashMap<String, String>();

		BufferedReader audiencesReader = null;

		public void configure(JobConf job) {

			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(job);
				for (int i = 0; i < paths.length; i++) {
					logger.info("paths================" + paths[i]);
				}
				if (paths != null && paths.length > 0) {
					String line;
					audiencesReader = new BufferedReader(new FileReader(
							paths[0].toString()));
					while ((line = audiencesReader.readLine()) != null) {

						String[] straAu = line.toString().split("\t");

						String strategyId = straAu[0].toString();

						String audiencesIds = straAu[1].toString();

						straAudience
								.put(strategyId, ConvertBlackList
										.getsplitCategory(audiencesIds));

					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if (audiencesReader != null) {
					try {
						audiencesReader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
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

				if (straAudience.get(IdStrategyId) == null
						|| "".equals(straAudience.get(IdStrategyId))
						|| straAudience.get(IdStrategyId).indexOf(cateId) == -1) {

					// 1.计算IdAdvertiserId下的数据，按照IdCompanyId, IdAdvertiserId,
					// cateId分组

					output.collect(new Text(IdCompanyId + "\t" + IdAdvertiserId
							+ "\t" + cateId), new Text(type + "\t" + pyid));

					// 2.计算IdOrderId下的数据，按照IdCompanyId, IdAdvertiserId,
					// IdOrderId , cateId

					output.collect(new Text(IdCompanyId + "\t" + IdAdvertiserId
							+ "\t" + IdOrderId + "\t" + cateId), new Text(type
							+ "\t" + pyid));

					// 3.计算IdCampaignId下的数据，按照IdCompanyId, IdAdvertiserId,
					// IdOrderId , IdCampaignId,cateId
					output.collect(new Text(IdCompanyId + "\t" + IdAdvertiserId
							+ "\t" + IdOrderId + "\t" + IdCampaignId + "\t"
							+ cateId), new Text(type + "\t" + pyid));

					// 4.计算IdStrategyId下的数据，按照IdCompanyId, IdAdvertiserId,
					// IdOrderId , IdCampaignId,IdStrategyId,cateId

					output.collect(new Text(IdCompanyId + "\t" + IdAdvertiserId
							+ "\t" + IdOrderId + "\t" + IdCampaignId + "\t"
							+ IdStrategyId + "\t" + cateId), new Text(type
							+ "\t" + pyid));

				}
			}

		}

	}

	/**
	 * reduce合并上面两个map的输出,为group系列的数据，用于计算7种类别的28张表报的输出
	 * 
	 * */
	public static class PartiReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, NullWritable> {

		// 定义计算uv，pv的数据结构
		Set<String> impUv = new HashSet<String>();
		int impPv = 0;
		Set<String> clickUv = new HashSet<String>();
		int clickPv = 0;
		Set<String> reachUv = new HashSet<String>();
		int reachPv = 0;
		Set<String> impConvUv = new HashSet<String>();
		int impConvPv = 0;
		Set<String> clickConvUv = new HashSet<String>();
		int clickConvPv = 0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {

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
						impPv++;
						break;
					case "click":
						clickUv.add(pyid);
						clickPv++;
						break;
					case "reach":
						reachUv.add(pyid);
						reachPv++;
						break;
					case "clickConv":
						clickConvUv.add(pyid);
						clickConvPv++;
						break;
					case "impConv":
						impConvUv.add(pyid);
						impConvPv++;
						break;
					default:
						break;
					}

				}

				// 对不同的key进行拆分求和,输出

				String[] keystr = key.toString().split("\t");

				String[] dates = DateUtils.getBeforeDate();

				String outValue = getTypeValue(dates[0], keystr, impUv, impPv,
						clickUv, clickPv, reachUv, reachPv, impConvUv,
						impConvPv, clickConvUv, clickConvPv);

				output.collect(new Text(outValue), NullWritable.get());

			}

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
			Set<String> impUv, int impPv, Set<String> clickUv, int clickPv,
			Set<String> reachUv, int reachPv, Set<String> impConvUv,
			int impConvPv, Set<String> clickConvUv, int clickConvPv) {

		StringBuilder sb1 = new StringBuilder();
		sb1.append(beforeDate).append(impPv).append("\t").append(impUv.size())
				.append("\t").append(clickPv).append("\t")
				.append(clickUv.size()).append("\t").append(reachPv)
				.append("\t").append(reachUv.size()).append("\t")
				.append(impConvPv).append("\t").append(impConvUv.size())
				.append("\t").append(clickConvPv).append("\t")
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

	// @SuppressWarnings("deprecation")
	// @Override
	// public int run(String[] args) throws Exception {
	//
	// JobConf conf = new JobConf(getConf(), PeopleUserCategoryV1.class);
	//
	// for (int i = 0; i < args.length; i++) {
	//
	// logger.info("runargs========" + args[i]);
	// }
	//
	// conf.setJobName("PeopleUserCategoryV1");
	//
	// conf.setOutputKeyClass(Text.class);
	// conf.setOutputValueClass(NullWritable.class);
	//
	// // mapper and reducer input
	// // 第一个input是category_info，放到cache
	// conf.set("mapred.input.dir", args[0]);
	//
	// Path category = new Path(args[0]);
	// String category_info = category.toUri().toString();
	//
	// DistributedCache.addCacheFile(new Path(category_info).toUri(), conf);
	//
	// // 第二个input是userProfile
	// conf.set("mapred.input.dir", args[1]);
	// MultipleInputs.addInputPath(conf, new Path(args[1]),
	// TextInputFormat.class, CategoryMap.class);
	//
	// // 第三个input是basicdata
	// conf.set("mapred.input.dir", args[2]);
	// MultipleInputs.addInputPath(conf, new Path(args[2]),
	// TextInputFormat.class, CategoryMap.class);
	//
	// JobConf mapper1Conf = new JobConf(false);
	// ChainMapper.addMapper(conf, CategoryMap.class, LongWritable.class,
	// Text.class, Text.class, Text.class, true, mapper1Conf);
	//
	// // 第四个input是audience小文件
	// conf.set("mapred.input.dir", args[3]);
	// Path audience = new Path(args[3]);
	// String audience_info = audience.toUri().toString();
	// DistributedCache.addCacheFile(new Path(category_info).toUri(), conf);
	//
	// JobConf mapper2Conf = new JobConf(false);
	// ChainMapper.addMapper(conf, AudiencesMap.class, Text.class, Text.class,
	// Text.class, Text.class, true, mapper2Conf);
	//
	// conf.setOutputFormat(PinyouMultipleTextOutputFormat.class);
	//
	// JobClient.runJob(conf);
	//
	// return 0;
	//
	// }

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(getConf(), PeopleUserCategoryV1.class);

		for (int i = 0; i < args.length; i++) {

			logger.info("runargs========" + args[i]);
		}

		conf.setJobName("PeopleUserCategoryV1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		// mapper and reducer input
		// 第一个input是category_info，放到cache
		conf.set("mapred.input.dir", args[0]);

		Path category = new Path(args[0]);
		String category_info = category.toUri().toString();

		DistributedCache.addCacheFile(new Path(category_info).toUri(), conf);

		// 第二个input是userProfile
		conf.set("mapred.input.dir", args[1]);
		MultipleInputs.addInputPath(conf, new Path(args[1]),
				TextInputFormat.class, CategoryMap.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.set("mapred.output.dir", args[2]);

		// 第三个input是basicdata
		conf.set("mapred.input.dir", args[3]);
		MultipleInputs.addInputPath(conf, new Path(args[3]),
				TextInputFormat.class, CategoryMap.class);

		conf.setPartitionerClass(CategoryPartitioner.class);
		conf.setReducerClass(CategoryReduce.class);

		// JobConf mapper1Conf = new JobConf(false);
		// ChainMapper.addMapper(conf, CategoryMap.class, LongWritable.class,
		// Text.class, Text.class, Text.class, true, mapper1Conf);

		// 第四个input是audience小文件
		// conf.set("mapred.input.dir", args[4]);
		// Path audience = new Path(args[4]);
		// String audience_info = audience.toUri().toString();
		// DistributedCache.addCacheFile(new Path(category_info).toUri(), conf);
		//
		// JobConf mapper2Conf = new JobConf(false);
		// ChainMapper.addMapper(conf, AudiencesMap.class, Text.class,
		// Text.class,
		// Text.class, Text.class, true, mapper2Conf);
		//
		// conf.setOutputFormat(PinyouMultipleTextOutputFormat.class);

		JobClient.runJob(conf);

		return 0;

	}

	public static void main(String[] args) throws Exception {

		logger.info("args lenth==========" + args.length);

		for (String ar : args) {
			logger.info("args=====" + ar);
		}
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new PeopleUserCategoryV1(), args);
		System.exit(res);
	}

}

class PinyouMultipleTextOutputFormat extends
		MultipleTextOutputFormat<Text, NullWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, NullWritable value,
			String name) {

		String[] dates = DateUtils.getBeforeDate();
		String[] keystr = key.toString().split("\t");
		int flag = keystr.length;

		if (flag == 3) {
			// IdAdvertiserId的pv，uv,输出到/group_advertiser目录下
			return "group_advertiser/" + dates[1] + "p";
		} else if (flag == 4) {
			// IdOrderId
			return "group_order/" + dates[1] + "p";
		} else if (flag == 5) {
			return "group_campaign/" + dates[1] + "p";
		} else {
			return "group_strategy/" + dates[1] + "p";
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
