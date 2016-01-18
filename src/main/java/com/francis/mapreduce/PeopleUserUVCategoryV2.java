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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.francis.utils.DateUtils;

/**
 * @author wenbing.yu
 * @time 2015-04-03
 * @version 1.0
 * @description:该MR程序为了计算人群报表
 * 
 **/

public class PeopleUserUVCategoryV2 extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(PeopleUserUVCategoryV2.class);

	public static String USR_POOL = "usr_pool";

	/**
	 * usr_pool的cache处理和曝光，点击，到达，转化的数据join生成uv计算的数据
	 * 
	 * @param usr_pool
	 * @param imp
	 *            ,click,reach,impConv,clickConv
	 * */
	public static class ConvUvMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private Map<String, String> adverPool = new HashMap<String, String>();

		public void setup(Context context) throws IOException,
				InterruptedException {

			if (context.getCacheFiles() != null
					&& context.getCacheFiles().length > 0) {

				File usrpool = new File("./" + USR_POOL);
				if (usrpool.exists()) {
					if (usrpool.isDirectory()) {
						File[] subfile = usrpool.listFiles();
						if (subfile.length > 0) {
							for (File f : subfile) {
								if (f.isFile()) {
									String line;
									BufferedReader br = new BufferedReader(
											new FileReader(f));
									try {
										while ((line = br.readLine()) != null) {
											String[] str = line.split("\t");
											if (str.length == 2
													&& str[0] != null) {
												adverPool.put(str[0], str[1]);
											}
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
								usrpool));
						try {
							while ((line = br.readLine()) != null) {
								String[] str = line.split("\t");
								if (str.length == 2 && str[0] != null) {
									adverPool.put(str[0], str[1]);
								}
							}
						} finally {
							br.close();
						}
					}

				}

			}

		}

		/**
		 * 处理曝光，点击，到达，转化于usr_pool的输出，分组进行计算uv
		 * */
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
				String target_id = categoryUsers[7].toString();
				if (adverPool.containsKey(IdStrategyId)) {

					String pool_id = adverPool.get(IdAdvertiserId);

					// 为了在reduce端方便计算不同维度的数据，在key前面一次用A，B，C，D，E，F，G，H进行区分

					// 1.计算uv_advertiser的数据
					context.write(new Text("A" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId), new Text(
							type + "\t" + pyid));

					// 2.计算uv_order的数据
					context.write(new Text("B" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId), new Text(type + "\t" + pyid));

					// 3.计算uv_campaign的数据
					context.write(new Text("C" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId + "\t" + IdCampaignId), new Text(type
							+ "\t" + pyid));

					// 4.计算uv_strategy的数据
					context.write(new Text("D" + "\t" + pool_id + "\t"
							+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
							+ IdOrderId + "\t" + IdCampaignId + "\t"
							+ IdStrategyId), new Text(type + "\t" + pyid));

					// 只需要type包含Conv的数据进行计算
					if (type.indexOf("Conv") != -1 && target_id != null
							&& target_id.indexOf("null") != -1
							&& IdCompanyId != null) {

						// 1.计算uv_advertiser_conv

						context.write(new Text("E" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ target_id), new Text(type + "\t" + pyid));

						// 2.计算uv_order_conv的数据
						context.write(new Text("F" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ IdOrderId + "\t" + target_id), new Text(type
								+ "\t" + pyid));
						// 3.计算uv_campaign_conv的数据

						context.write(new Text("G" + "\t" + pool_id + "\t"
								+ IdCompanyId + "\t" + IdAdvertiserId + "\t"
								+ IdOrderId + "\t" + IdCampaignId + "\t"
								+ target_id), new Text(type + "\t" + pyid));

						// 4.计算uv_strategy_conv的数据
						context.write(new Text("H" + "\t" + pool_id + "\t"
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

	public static class ConvUvReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		// 定义计算uv,pv的数据结构
		public static Set<String> impUv = new HashSet<String>();
		public static List<String> impPv = new LinkedList<String>();
		public static Set<String> clickUv = new HashSet<String>();
		public static List<String> clickPv = new LinkedList<String>();
		public static Set<String> reachUv = new HashSet<String>();
		public static List<String> reachPv = new LinkedList<String>();
		public static Set<String> impConvUv = new HashSet<String>();
		public static List<String> impConvPv = new LinkedList<String>();
		public static Set<String> clickConvUv = new HashSet<String>();
		public static List<String> clickConvPv = new LinkedList<String>();

		private MultipleOutputs<Text, NullWritable> mos;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);// 初始化mos
		}

		public void reduce(Text key, Iterable<Text> val, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> values = val.iterator();
			// 获取前一天的时间
			String[] strDate = getBeforeDate();

			// 对于当前reducer的key进行判断
			String[] keytypes = key.toString().split("\t");
			if (keytypes != null && keytypes.length > 0) {

				// 计算平v，uv,完成了set和list的初始化
				computePvUv(values);
				// 按照不同条件要求多目录输出
				exeTypeValueOutput(strDate, keytypes, impUv, impPv, clickUv,
						clickPv, reachUv, reachPv, impConvUv, impConvPv,
						clickConvUv, clickConvPv, mos);

			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
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
			Set<String> impUv, List<String> impPv, Set<String> clickUv,
			List<String> clickPv, Set<String> reachUv, List<String> reachPv,
			Set<String> impConvUv, List<String> impConvPv,
			Set<String> clickConvUv, List<String> clickConvPv,
			MultipleOutputs<Text, NullWritable> mos) {

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
				.append("\t").append(impPv.size()).append("\t")
				.append(impUv.size()).append("\t").append(clickPv.size())
				.append("\t").append(reachUv.size()).append("\t")
				.append(reachPv.size()).append("\t").append(reachUv.size())
				.append("\t").append(two_jump_pv).append("\t")
				.append(two_jump_uv).append("\t").append(three_jump_pv)
				.append("\t").append(three_jump_uv).append("\t")
				.append(four_jump_pv).append("\t").append(four_jump_uv)
				.append("\t").append(five_jump_pv).append("\t")
				.append(five_jump_uv).append("\t").append(impConvPv.size())
				.append("\t").append(impConvUv.size()).append("\t")
				.append(clickConvPv.size()).append("\t")
				.append(clickConvUv.size());

		sbuv_conv.append(timestamp).append("\t").append(creation_time)
				.append("\t").append(impConvPv.size()).append("\t")
				.append(impConvUv.size()).append("\t")
				.append(clickConvPv.size()).append("\t")
				.append(clickConvUv.size());

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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/advertiser/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/order/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/campaign/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"strategy" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/advertiser_conversion/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/order_conversion/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/campaign_conversion/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
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
				mos.write(new Text(sb.toString()), NullWritable.get(),
						"uv/strategy_conversion/" + dirdate + "/" + "p");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	// 获得当前日期的前一天日期
	public static String[] getBeforeDate() {
		String[] strdate = new String[2];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance(); // 得到日历
		calendar.setTime(new Date());// 把当前时间赋给日历
		calendar.add(Calendar.DAY_OF_MONTH, -1); // 设置为前一天
		Date dBefore = calendar.getTime(); // 得到前一天的时间
		String defaultStartDate = sdf.format(dBefore);
		strdate[0] = defaultStartDate;

		SimpleDateFormat sdff = new SimpleDateFormat("yyyy/MM/dd");
		String d = sdff.format(dBefore);
		strdate[1] = d;

		// 当前时间戳
		SimpleDateFormat sdfcurrent = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String currentDate = sdfcurrent.format(new Date());
		strdate[2] = currentDate;
		return strdate;
	}

	@Override
	public int run(String[] arg0) throws Exception {

		for (int i = 0; i < arg0.length; i++) {
			logger.info("arg0" + i + "===============" + arg0[i]);
		}

		JobConf conf = new JobConf(getConf());
		conf.setJobName("PeopleUserUVCategory");
		Job job = Job.getInstance(getConf(), "PeopleUserUVCategory");
		job.setJarByClass(getClass());

		// mapper and reducer input
		// 第一个input是usr_pool，放到cache

		Path usrPool = new Path(arg0[0]);
		String usr_pool = usrPool.toUri().toString() + "#" + USR_POOL;
		job.addCacheFile(new URI(usr_pool));

		// 第二个input是imp,click,reach,impConv,clickConv
		MultipleInputs.addInputPath(job, new Path(arg0[1]),
				TextInputFormat.class, ConvUvMap.class);

		job.setReducerClass(ConvUvReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));

		// MultipleOutputs.addNamedOutput(job, "group_advertiser",
		// TextOutputFormat.class, Text.class, NullWritable.class);
		//
		// MultipleOutputs.addNamedOutput(job, "group_order",
		// TextOutputFormat.class, Text.class, NullWritable.class);
		//
		// MultipleOutputs.addNamedOutput(job, "group_campaign",
		// TextOutputFormat.class, Text.class, NullWritable.class);
		//
		// MultipleOutputs.addNamedOutput(job, "group_strategy",
		// TextOutputFormat.class, Text.class, NullWritable.class);

		boolean res = job.waitForCompletion(true);

		if (res)
			return 0;
		else
			return -1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new PeopleUserUVCategoryV2(), args);
		System.exit(res);
	}

	class PinyouMultipleTextOutputFormat2 extends
			MultipleTextOutputFormat<Text, NullWritable> {

		@Override
		protected String generateFileNameForKeyValue(Text key,
				NullWritable value, String name) {

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
			} else if (flag == 6) {
				return "group_strategy/" + dates[1] + "p";
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

}
