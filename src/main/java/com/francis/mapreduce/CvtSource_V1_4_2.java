//package com.francis.mapreduce;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Comparator;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//import java.util.StringTokenizer;
//import java.util.Map.Entry;
//import java.util.TreeMap;
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
//
//
//
//public class CvtSource_V1_4_2 extends Configured implements Tool {
//	// static Logger log = Logger.getLogger(CvrSource.class);
//
//	public static class RuleMapper extends Mapper<Object, Text, Text, Text> {
//		private Text outStr = new Text();
//		private Text word = new Text();
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String line = value.toString().trim();
//			String[] line_array = line.split("\t");
//
//			// pyid, atime, channel, ltype, compid, promotion, flowtype,
//			// orderno,money, target
//			// pyid, time, source, type, compid, money
//			if (line_array.length >= 12) {
//
//				String pyid = line_array[0];
//				String atime = line_array[1];
//				String source = line_array[2];
//				String type = line_array[3];
//				String compid = line_array[4];
//				String promotion = line_array[5];
//				String flowtype = line_array[6];
//				String orderno = line_array[7];
//				String money = line_array[8];
//				String target = line_array[9];
//				String sessionID = line_array[10];
//				String creative = line_array[11];
//
//				// orderno为空的情况，随机生成一个
//				// if(orderno.equals("") || orderno.equals("-1") )
//				// orderno = PyidUtils.generateUuid();
//
//				if (!pyid.equals("")) {
//					outStr.set(compid + ":::" + pyid + ":::" + atime);
//					word.set(type + ":::" + source + ":::" + money + ":::"
//							+ promotion + ":::" + flowtype + ":::" + orderno
//							+ ":::" + target + ":::" + sessionID + ":::"
//							+ creative);
//					context.write(outStr, word);
//				}
//			}
//
//		}
//
//	}
//
//	public static class Partition extends Partitioner<Text, Text> {
//
//		@Override
//		public int getPartition(Text key, Text value, int numPartitions) {
//			// TODO Auto-generated method stub
//			String[] line = key.toString().trim().split(":::");
//			String keyStr = line[0] + ":::" + line[1];
//
//			return (keyStr.hashCode() & Integer.MAX_VALUE) % numPartitions;
//		}
//	}
//
//	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//
//		private static Text outKey = new Text();
//		private static Text outValue = new Text();
//
//		private MultipleOutputs mos = null;
//
//		private String pre_key;
//		private String pre_session;
//		CvtPathGroup cvtGroup;
//		CvtPath cvtPath;
//
//		int isOldVisitor;
//
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			// super.setup(context);
//			mos = new MultipleOutputs(context);
//			cvtGroup = new CvtPathGroup();
//			cvtGroup.setStationTimes(0);
//			pre_key = "";
//			pre_session = "";
//			isOldVisitor = 0;
//		}
//
//		public static Long getDaysBetween(Date startDate, Date endDate) {
//			Calendar fromCalendar = Calendar.getInstance();
//			fromCalendar.setTime(startDate);
//			fromCalendar.set(Calendar.HOUR_OF_DAY, 0);
//			fromCalendar.set(Calendar.MINUTE, 0);
//			fromCalendar.set(Calendar.SECOND, 0);
//			fromCalendar.set(Calendar.MILLISECOND, 0);
//
//			Calendar toCalendar = Calendar.getInstance();
//			toCalendar.setTime(endDate);
//			toCalendar.set(Calendar.HOUR_OF_DAY, 0);
//			toCalendar.set(Calendar.MINUTE, 0);
//			toCalendar.set(Calendar.SECOND, 0);
//			toCalendar.set(Calendar.MILLISECOND, 0);
//
//			return (toCalendar.getTime().getTime() - fromCalendar.getTime()
//					.getTime()) / (1000 * 60 * 60 * 24);
//		}
//
//		/**
//		 * 计算转化渠道
//		 * 
//		 * @param sourceList
//		 * @param count
//		 * @param isOldVisitor
//		 */
//
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//
//			String keyStr = key.toString().trim();
//			// compid:::pyid:::time
//			String[] key_list = keyStr.trim().split(":::");
//			String compid = key_list[0];
//			String pyid = key_list[1];
//			String atime = key_list[2];
//			Long current_format_time = 0L;
//
//			String current_time = atime.substring(0, 4) + "-"
//					+ atime.substring(4, 6) + "-" + atime.substring(6, 8) + " "
//					+ atime.substring(8, 10) + ":" + atime.substring(10, 12)
//					+ ":" + atime.substring(12, 14);
//
//			SimpleDateFormat sdf_long = new SimpleDateFormat(
//					"yyyy-MM-dd HH:mm:ss");
//
//			try {
//
//				current_format_time = sdf_long.parse(current_time).getTime();
//
//			} catch (Exception e) {
//
//			}
//
//			// 考核日期
//			String predayStr = context.getConfiguration().get("predayStr");
//
//			for (Text item : values) {
//
//				String itemStr = item.toString();
//				String[] item_list = itemStr.trim().split(":::");
//
//				if (item_list.length < 9)
//					continue;
//
//				// promotion + ":::" + flowtype + ":::" + orderno + ":::" +
//				// target
//				String atype = item_list[0];
//				String source = item_list[1];
//				String money = item_list[2];
//				String promotion = item_list[3];
//				String flowtype = item_list[4];
//				String orderno = item_list[5];
//				String target = item_list[6];
//				String sessionID = item_list[7];
//				String creative = item_list[8];
//
//				// log.info("-----------------" + keyStr + "@@" + itemStr);
//
//				// 如果考核日期之前的回溯期内有转化，那么当天就属于新客，否则属于老客
//				if (atype.equals("cvt") && atime.compareTo(predayStr) < 0)
//					isOldVisitor = 1;
//
//				// 向CvtPath中添加渠道
//				cvtPath = new CvtPath();
//				cvtPath.setPathname(source);
//				cvtPath.setLandingTime(current_format_time);
//				cvtPath.setStayTime("0");
//				cvtPath.setPromotion(promotion);
//				cvtPath.setChannelType(flowtype);
//				cvtPath.setSession(sessionID);
//				cvtPath.setCreative(creative);
//				// cvtPath.setTarget(target);
//
//				// 一旦key发生变化，立刻清空转化渠道列表
//				if (!pre_key.equals(compid + ":::" + pyid)) {
//					// 清空cvtpath
//					cvtGroup.getPaths().clear();
//					cvtGroup.setStationTimes(0);
//				}
//
//				// 如果当前记录是访客日志的话，则考察是否要加入渠道列表
//				if (atype.equals("adv")) {
//					// 判断是否与上一个session相同
//					if (sessionID.equals(pre_session)) {
//						// 如果与前一个sessionId相同，则判断是否是推广渠道
//						if (!source.equals("-1")) {
//							// 如果统一session的前一个渠道是自然渠道，则删除
//							if (cvtGroup.getPaths().size() >= 1) {
//								if (cvtGroup.getPaths()
//										.get(cvtGroup.getPaths().size() - 1)
//										.getPathname().equals("-1")
//										&& cvtGroup.getPaths().size() >= 1) {
//									cvtGroup.removeLastItem();
//									cvtGroup.minusStationTimes();
//								}
//							}
//							cvtGroup.getPaths().add(cvtPath);
//							cvtGroup.addStationTimes();
//						}
//						// 如果是转化，则添加“已转化”节点
//						// if(atype.equals("cvt")) {
//						// cvtPath.setPathname("conversion");
//						// cvtGroup.getPaths().add(cvtPath);
//						// }
//
//					} else {
//						// 如果与前一个seesionId不同，则直接添加进cvtpath
//						cvtGroup.getPaths().add(cvtPath);
//						cvtGroup.addStationTimes();
//					}
//
//				} else if (atype.equals("cvt")
//						&& atime.compareTo(predayStr) >= 0) {
//					// 如果当前行日志类型是cvt，则输出路径
//
//					int length = cvtGroup.getPaths().size();
//
//					// 如果长度为0，则表示直接转化，需要添加一个自然渠道
//					if (length == 0) {
//
//						cvtPath = new CvtPath();
//						cvtPath.setPathname("-1");
//						cvtPath.setLandingTime(current_format_time);
//						cvtPath.setStayTime("0");
//						cvtPath.setPromotion(promotion);
//						cvtPath.setChannelType(flowtype);
//						cvtPath.setSession(sessionID);
//						cvtPath.setCreative(creative);
//						// cvtPath.setTarget(target);
//
//						cvtGroup.getPaths().add(cvtPath);
//						cvtGroup.addStationTimes();
//					}
//
//					length = cvtGroup.getPaths().size();
//
//					if (length == 1) {
//
//						outKey.set(key_list[0]);
//						// stb.append(String.valueOf(i)).append("\t").append(b);
//						// compid, pyid, channel,
//						// 0--订单数，1--anyclick，2--firstclick，3--lastclick,
//						// 4--newVistor, 5--oldVistor,6--订单金额,
//						outValue.set(key_list[1] + "\t"
//								+ cvtGroup.getPaths().get(0).getPathname()
//								+ "\t" + 1 + "\t" + 0 + "\t" + 1 + "\t" + 1
//								+ "\t" + (1 - isOldVisitor) + "\t"
//								+ isOldVisitor + "\t" + money + "\t"
//								+ cvtGroup.getPaths().get(0).getPromotion()
//								+ "\t"
//								+ cvtGroup.getPaths().get(0).getChannelType()
//								+ "\t" + orderno + "\t" + target + "\t"
//								+ cvtGroup.getPaths().get(0).getSession()
//								+ "\t" + current_format_time + "\t"
//								+ cvtGroup.getPaths().get(0).getCreative());
//						mos.write(
//								context.getConfiguration().get("splitPath_1"),
//								outKey, outValue);
//
//					} else if (length > 1) {
//						// 第一个行为为lastclick，最后一个行为为firstclick，其他行为为anyclick
//						// System.out.println("##############" +
//						// sourceList.get(0) + ":::" +
//						// count[(int)sourceList.get(0)][3]);
//						outKey.set(key_list[0]);
//						// stb.append(String.valueOf(i)).append("\t").append(b);
//						// compid, pyid, channel,
//						// 0--订单数，1--anyclick，2--firstclick，3--lastclick,
//						// 4--newVistor, 5--oldVistor,6--订单金额,
//						outValue.set(key_list[1] + "\t"
//								+ cvtGroup.getPaths().get(0).getPathname()
//								+ "\t" + 1 + "\t" + 0 + "\t" + 0 + "\t" + 1
//								+ "\t" + (1 - isOldVisitor) + "\t"
//								+ isOldVisitor + "\t" + money + "\t"
//								+ cvtGroup.getPaths().get(0).getPromotion()
//								+ "\t"
//								+ cvtGroup.getPaths().get(0).getChannelType()
//								+ "\t" + orderno + "\t" + target + "\t"
//								+ cvtGroup.getPaths().get(0).getSession()
//								+ "\t" + current_format_time + "\t"
//								+ cvtGroup.getPaths().get(0).getCreative());
//						mos.write(
//								context.getConfiguration().get("splitPath_1"),
//								outKey, outValue);
//
//						// anyclick
//						for (int i = 1; i < length - 1; i++) {
//							outKey.set(key_list[0]);
//							// stb.append(String.valueOf(i)).append("\t").append(b);
//							// compid, pyid, channel,
//							// 0--订单数，1--anyclick，2--firstclick，3--lastclick,
//							// 4--newVistor, 5--oldVistor,6--订单金额,
//							outValue.set(key_list[1]
//									+ "\t"
//									+ cvtGroup.getPaths().get(i).getPathname()
//									+ "\t"
//									+ 1
//									+ "\t"
//									+ 1
//									+ "\t"
//									+ 0
//									+ "\t"
//									+ 0
//									+ "\t"
//									+ (1 - isOldVisitor)
//									+ "\t"
//									+ isOldVisitor
//									+ "\t"
//									+ money
//									+ "\t"
//									+ cvtGroup.getPaths().get(i).getPromotion()
//									+ "\t"
//									+ cvtGroup.getPaths().get(i)
//											.getChannelType() + "\t" + orderno
//									+ "\t" + target + "\t"
//									+ cvtGroup.getPaths().get(i).getSession()
//									+ "\t" + current_format_time + "\t"
//									+ cvtGroup.getPaths().get(i).getCreative());
//							mos.write(
//									context.getConfiguration().get(
//											"splitPath_1"), outKey, outValue);
//						}
//
//						// lastclick
//						outKey.set(key_list[0]);
//						// stb.append(String.valueOf(i)).append("\t").append(b);
//						// compid, pyid, channel,
//						// 0--订单数，1--anyclick，2--firstclick，3--lastclick,
//						// 4--newVistor, 5--oldVistor,6--订单金额,
//						outValue.set(key_list[1]
//								+ "\t"
//								+ cvtGroup.getPaths().get(length - 1)
//										.getPathname()
//								+ "\t"
//								+ 1
//								+ "\t"
//								+ 0
//								+ "\t"
//								+ 1
//								+ "\t"
//								+ 0
//								+ "\t"
//								+ (1 - isOldVisitor)
//								+ "\t"
//								+ isOldVisitor
//								+ "\t"
//								+ money
//								+ "\t"
//								+ cvtGroup.getPaths().get(length - 1)
//										.getPromotion()
//								+ "\t"
//								+ cvtGroup.getPaths().get(length - 1)
//										.getChannelType()
//								+ "\t"
//								+ orderno
//								+ "\t"
//								+ target
//								+ "\t"
//								+ cvtGroup.getPaths().get(length - 1)
//										.getSession()
//								+ "\t"
//								+ current_format_time
//								+ "\t"
//								+ cvtGroup.getPaths().get(length - 1)
//										.getCreative());
//						mos.write(
//								context.getConfiguration().get("splitPath_1"),
//								outKey, outValue);
//
//					}
//
//					int interval = 0;
//					try {
//						// Date startDate=sdf.parse(start);
//						// Date endDate = sdf.parse(end);
//						// interval =
//						// (int)Math.floor((double)(current_format_time -
//						// cvtGroup.getPaths().get(cvtGroup.getPaths().size() -
//						// 1).getLandingTime())/1000/3600/24);
//						interval = (int) Math
//								.floor((double) (current_format_time - cvtGroup
//										.getPaths().get(0).getLandingTime()) / 1000 / 3600 / 24);
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//
//					if (interval < 0)
//						interval = 0;
//
//					String sourcePath = JSON.toJSONString(cvtGroup);
//
//					// lastclick
//					outKey.set(key_list[0]);
//					// stb.append(String.valueOf(i)).append("\t").append(b);
//					// compid, pyid, channel,
//					// 0--订单数，1--anyclick，2--firstclick，3--lastclick,
//					// 4--newVistor, 5--oldVistor,6--订单金额,
//					outValue.set(key_list[1] + "\t"
//							+ cvtGroup.getPaths().get(0).getPathname() + "\t"
//							+ orderno + "\t" + interval + "\t" + sourcePath
//							+ "\t" + String.valueOf(cvtGroup.getStationTimes())
//							+ "\t" + current_format_time + "\t" + target + "\t"
//							+ money);
//					mos.write(context.getConfiguration().get("splitPath_2"),
//							outKey, outValue);
//
//				} else {
//					// 该记录是转化记录，但不是当天转化，则不处理
//
//				}
//
//				// outKey.set(compid + ":::" + pyid + ":::" +atime );
//				// outValue.set(atype + ":::" + source + ":::" + money + ":::" +
//				// promotion + ":::" + flowtype + ":::" + orderno + ":::" +
//				// target + ":::" + sessionID + ":::" + creative);
//				// context.write(outKey, outValue);
//
//				// 赋值
//				pre_key = compid + ":::" + pyid;
//				pre_session = sessionID;
//
//			}// for Text item
//
//		}
//
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//
//			mos.close();
//		}
//	}
//
//	public int run(String[] args) throws Exception {
//
//		Configuration conf = getConf();
//		Job job = Job.getInstance(conf);
//
//		job.setJarByClass(CvtSource_V1_4_2.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		job.setMapperClass(RuleMapper.class);
//		job.setPartitionerClass(Partition.class);
//		job.setReducerClass(Reduce.class);
//
//		// MultipleInputs.addInputPath(job, new Path(args[0]),
//		// TextInputFormat.class, RuleMapper.class);
//
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//		job.getConfiguration().set("predayStr", args[2]);
//
//		job.getConfiguration().set("splitPath_1", args[3]);
//		job.getConfiguration().set("splitPath_2", args[4]);
//
//		MultipleOutputs.addNamedOutput(job,
//				job.getConfiguration().get("splitPath_1"),
//				TextOutputFormat.class, Text.class, Text.class);
//		MultipleOutputs.addNamedOutput(job,
//				job.getConfiguration().get("splitPath_2"),
//				TextOutputFormat.class, Text.class, Text.class);
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		return 0;
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		Configuration conf = new Configuration();
//		int res = ToolRunner.run(conf, new CvtSource_V1_4_2(), args);
//
//		System.exit(res);
//
//	}
//}