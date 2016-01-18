package com.francis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.francis.utils.DateUtils;

public class PeopleUserUVCategoryV1 extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(PeopleUserUVCategoryV1.class);

	/**
	 * usr_pool的cache处理和曝光，点击，到达，转化的数据join生成uv计算的数据
	 * 
	 * @param usr_pool
	 * @param imp
	 *            ,click,reach,impConv,clickConv
	 * */
	public static class ConvUvMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> adverPool = new HashMap<String, String>();

		BufferedReader userPoolReader = null;

		public void configure(JobConf job) {

			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(job);
				for (int i = 0; i < paths.length; i++) {
					logger.info("paths================" + paths[i]);
				}
				if (paths != null && paths.length > 0) {
					String line;
					userPoolReader = new BufferedReader(new FileReader(
							paths[0].toString()));
					while ((line = userPoolReader.readLine()) != null) {

						String[] str = line.split("\t");
						if (str.length == 2 && str[0] != null) {
							adverPool.put(str[0], str[1]);
						}

					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (userPoolReader != null) {
					try {
						userPoolReader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}

		/**
		 * 处理曝光，点击，到达，转化于usr_pool的输出，分组进行计算uv
		 * */
		@Override
		public void map(LongWritable key, Text value,
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
				String target_id = categoryUsers[7].toString();
				if (adverPool.containsKey(IdStrategyId)) {

					String pool_id = adverPool.get(IdAdvertiserId);

					// 为了在reduce端方便计算不同维度的数据，在key前面一次用A，B，C，D，E，F，G，H进行区分

					// 1.计算uv_advertiser的数据
					output.collect(new Text("A" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId), new Text(
							type + "\t" + pyid));

					// 2.计算uv_order的数据
					output.collect(new Text("B" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId), new Text(type + "\t" + pyid));

					// 3.计算uv_campaign的数据
					output.collect(new Text("C" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId + "\t" + IdCampaignId), new Text(type
							+ "\t" + pyid));

					// 4.计算uv_strategy的数据
					output.collect(new Text("D" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId + "\t" + IdCampaignId + "\t"
							+ IdStrategyId), new Text(type + "\t" + pyid));

					// 只需要type包含Conv的数据进行计算
					if (type.indexOf("Conv") != -1 && target_id != null
							&& target_id.indexOf("null") != -1
							&& IdCompanyId != null) {

						// 1.计算uv_advertiser_conv

						output.collect(new Text("E" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ target_id), new Text(type + "\t" + pyid));

						// 2.计算uv_order_conv的数据
						output.collect(new Text("F" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ IdOrderId + "\t" + target_id), new Text(type
								+ "\t" + pyid));
						// 3.计算uv_campaign_conv的数据

						output.collect(new Text("G" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ IdOrderId + "\t" + IdCampaignId + "\t"
								+ target_id), new Text(type + "\t" + pyid));

						// 4.计算uv_strategy_conv的数据
						output.collect(new Text("H" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ IdOrderId + "\t" + IdCampaignId + "\t"
								+ IdStrategyId + "\t" + target_id), new Text(
								type + "\t" + pyid));

					}

				}

			}

		}

	}

	/**
	 * reduce端分别计算map端输出的数据
	 * */
	public static class ConvUvReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, NullWritable> {

		// 定义计算uv,pv的数据结构
		public static Set<String> impUv = new HashSet<String>();
		static int impPv = 0;
		public static Set<String> clickUv = new HashSet<String>();
		static int clickPv = 0;
		public static Set<String> reachUv = new HashSet<String>();
		static int reachPv = 0;
		public static Set<String> impConvUv = new HashSet<String>();
		static int impConvPv = 0;
		public static Set<String> clickConvUv = new HashSet<String>();
		static int clickConvPv = 0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {

			String[] strDate = DateUtils.getBeforeDate();

			String[] keytypes = key.toString().split("\t");
			if (keytypes != null && keytypes.length > 0) {

				// 计算平v，uv,完成了set和list的初始化
				computePvUv(values);
				// 按照不同条件要求多目录输出
				exeTypeValueOutput(strDate, keytypes, impUv, impPv, clickUv,
						clickPv, reachUv, reachPv, impConvUv, impConvPv,
						clickConvUv, clickConvPv, output);

			}

		}

		// 计算pv，uv
		public static void computePvUv(Iterator<Text> values) {
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

			}
		}

	}

	/**
	 * 根据不同的key构造不同的输出key/value
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

	public static void exeTypeValueOutput(String[] strDate, String[] key,
			Set<String> impUv, int impPv, Set<String> clickUv, int clickPv,
			Set<String> reachUv, int reachPv, Set<String> impConvUv,
			int impConvPv, Set<String> clickConvUv, int clickConvPv,
			OutputCollector<Text, NullWritable> output) {

		// 公共输出
		StringBuilder sbuv_all = new StringBuilder();
		StringBuilder sbuv_conv = new StringBuilder();

		String timestamp = strDate[0];
		String creation_time = strDate[2];
		// 生成多目录时间
		String dirdate = strDate[1];
		String bid_pv = "0";
		String bid_uv = "0";
		String two_jump_pv = "0";
		String two_jump_uv = "0";
		String three_jump_pv = "0";
		String three_jump_uv = "0";
		String four_jump_pv = "0";
		String four_jump_uv = "0";
		String five_jump_pv = "0";
		String five_jump_uv = "0";
		sbuv_all.append(timestamp).append("\t").append(creation_time)
				.append("\t").append(bid_pv).append("\t").append(bid_uv)
				.append("\t").append(impPv).append("\t").append(impUv.size())
				.append("\t").append(clickPv).append("\t")
				.append(reachUv.size()).append("\t").append(reachPv)
				.append("\t").append(reachUv).append("\t").append(two_jump_pv)
				.append("\t").append(two_jump_uv).append("\t")
				.append(three_jump_pv).append("\t").append(three_jump_uv)
				.append("\t").append(four_jump_pv).append("\t")
				.append(four_jump_uv).append("\t").append(five_jump_pv)
				.append("\t").append(five_jump_uv).append("\t")
				.append(impConvPv).append("\t").append(impConvUv.size())
				.append("\t").append(clickConvPv).append("\t")
				.append(clickConvUv.size());

		sbuv_conv.append(timestamp).append("\t").append(creation_time)
				.append("\t").append(impConvPv).append("\t")
				.append(impConvUv.size()).append("\t").append(clickConvPv)
				.append("\t").append(clickConvUv.size());

		// 要输出的数据
		String IdAdvertiserId = "";
		String IdOrderId = "";
		String IdCampaignId = "";
		String IdStrategyId = "";
		String target_id = "";
		String pool_id = "";
		StringBuilder sb = new StringBuilder();
		String keytype = key[0].toString();

		if ("A".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(sbuv_all);
			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("B".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(sbuv_all);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("C".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();
			IdCampaignId = key[5].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId).append("\t")
					.append(sbuv_all);
			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("D".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();
			IdCampaignId = key[5].toString();
			IdStrategyId = key[6].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId).append("\t")
					.append(IdStrategyId).append("\t").append(sbuv_all);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("E".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			target_id = key[4].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(target_id)
					.append("\t").append(sbuv_conv);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("F".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();
			target_id = key[5].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(target_id).append("\t")
					.append(sbuv_conv);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("G".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();
			IdCampaignId = key[5].toString();
			target_id = key[6].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId).append("\t")
					.append(target_id).append("\t").append(sbuv_conv);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else if ("H".equals(keytype)) {

			pool_id = key[1].toString();
			IdAdvertiserId = key[3].toString();
			IdOrderId = key[4].toString();
			IdCampaignId = key[5].toString();
			IdStrategyId = key[6].toString();
			target_id = key[7].toString();

			sb.append("null").append("\t").append(pool_id).append("\t")
					.append(IdAdvertiserId).append("\t").append(IdOrderId)
					.append("\t").append(IdCampaignId).append("\t")
					.append(IdStrategyId).append("\t").append(target_id)
					.append("\t").append(sbuv_conv);

			try {
				output.collect(new Text(sb.toString()), NullWritable.get());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}

class PinyouMultipleTextOutputFormat2 extends
		MultipleTextOutputFormat<Text, IntWritable> {

	@Override
	protected String generateFileNameForKeyValue(Text key, IntWritable value,
			String name) {

		String[] dates = DateUtils.getBeforeDate();

		String[] keytypes = key.toString().split("\t");
		if (keytypes != null && keytypes.length > 0) {

			String keytype = keytypes[0];

			// 按照不同条件要求多目录输出
			if ("A".equals(keytype)) {
				return "uv/advertiser/" + dates[1] + "p";
			} else if ("B".equals(keytype)) {
				return "uv/order/" + dates[1] + "p";
			} else if ("C".equals(keytype)) {
				return "uv/campaign/" + dates[1] + "p";
			} else if ("D".equals(keytype)) {
				return "uv/strategy/" + dates[1] + "p";
			} else if ("E".equals(keytype)) {
				return "uv/advertiser_conversion" + dates[1] + "p";
			} else if ("F".equals(keytype)) {
				return "uv/order_conversion" + dates[1] + "p";
			} else if ("G".equals(keytype)) {
				return "uv/campaign_conversion" + dates[1] + "p";
			} else if ("H".equals(keytype)) {
				return "uv/strategy_conversion" + dates[1] + "p";

			}

		}

		return name;

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
