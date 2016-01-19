//package com.francis.mapreduce;
//
//import java.io.IOException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.GregorianCalendar;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.Partitioner;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
///*
// import org.apache.hadoop.conf.Configuration;  
// import org.apache.hadoop.conf.Configured;  
// import org.apache.hadoop.fs.FileSystem;  
// import org.apache.hadoop.fs.Path;  
// import org.apache.hadoop.io.IntWritable;  
// import org.apache.hadoop.io.LongWritable;  
// import org.apache.hadoop.io.NullWritable;  
// import org.apache.hadoop.io.Text;  
// import org.apache.hadoop.mapred.JobConf;  
// import org.apache.hadoop.mapred.RecordWriter;  
//
// import org.apache.hadoop.mapreduce.Job;  
// import org.apache.hadoop.mapreduce.Mapper;  
// import org.apache.hadoop.mapreduce.Reducer;  
//
// */
//import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
//import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.Progressable;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//
//
//public class CtrInputStream_v3 extends Configured implements Tool {
//
//	public static class LogMapper extends Mapper<Object, Text, Text, Text> {
//		private Text outStr = new Text();
//		private Text word = new Text();
//
//		TimeFormatConv tc = new TimeFormatConv();
//
//		public void outmulkey(Context context, String platform,
//				String adunitId, String domain, String winprice,
//				String vertical, String atype, String atime)
//				throws IOException, InterruptedException {
//
//			int INTERVAL = 5;
//
//			// CPM的离散化
//			int cpm = (int) Math.ceil(Integer.parseInt(winprice) * 1.0
//					/ INTERVAL)
//					* INTERVAL;
//
//			String outType = atype + "\t" + winprice + "\t"
//					+ tc.stringToTimestamp(atime);
//
//			// 多种维度组合的key
//			// key + time
//			// platform + adunit + cpm + time
//			outStr.set("p=" + platform + "|a=" + adunitId + "|"
//					+ tc.stringToTimestamp(atime));
//			word.set(outType);
//			context.write(outStr, word);
//
//			// platform + adunit + cpm + time
//			outStr.set("p=" + platform + "|a=" + adunitId + "|c=" + cpm + "|"
//					+ tc.stringToTimestamp(atime));
//			word.set(outType);
//			context.write(outStr, word);
//
//			if (!vertical.equals("null")) {
//				// platform + adunit + cpm + + time
//				outStr.set("p=" + platform + "|a=" + adunitId + "|v="
//						+ vertical + "|c=" + cpm + "|"
//						+ tc.stringToTimestamp(atime));
//				word.set(outType);
//				context.write(outStr, word);
//
//				// key + time
//				outStr.set("p=" + platform + "|a=" + adunitId + "|v="
//						+ vertical + "|" + tc.stringToTimestamp(atime));
//				word.set(outType);
//				context.write(outStr, word);
//			}
//
//			outStr.set("p=" + platform + "|d=" + domain + "|a=" + adunitId
//					+ "|c=" + cpm + "|" + tc.stringToTimestamp(atime));
//			word.set(outType);
//			context.write(outStr, word);
//		}
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String line = value.toString().trim();
//			String[] line_array = line.split("\t");
//			// filterWeight =
//			// Double.parseDouble(context.getConfiguration().get("filterWeight"));
//			if (line_array.length >= 11) {
//
//				String platform = line_array[0];
//				String domain = line_array[1];
//				String adunitId = line_array[2];
//				String width = line_array[3];
//				String height = line_array[4];
//
//				String winprice = line_array[5];
//				String appid = line_array[6];
//				String imptime = line_array[7];
//				String cvttime = line_array[8];
//				String isclk = line_array[9];
//				String advertiserId = line_array[10];
//
//				String atime = "";
//				String atype = "";
//				String aVertical = advertiserId;
//
//				// 小融8194、广众、贵金属
//				// if (advertiserId.equals("11949") ||
//				// advertiserId.equals("11891") || advertiserId.equals("5402"))
//				// aVertical = advertiserId;
//
//				// cvt log
//				if (isclk.equals("1")) {
//					atime = imptime;
//					atype = "4";
//					outmulkey(context, platform, adunitId, domain, winprice,
//							aVertical, atype, atime);
//
//				}
//
//				// 所有输入都是一次曝光
//				atime = imptime;
//				atype = "3";
//
//				outmulkey(context, platform, adunitId, domain, winprice,
//						aVertical, atype, atime);
//
//			}
//
//		}
//	}
//
//	public static class RuleMapper extends Mapper<Object, Text, Text, Text> {
//
//		// private final static IntWritable one = new IntWritable(2);
//		private Text outStr = new Text();
//		private Text word = new Text();
//		TimeFormatConv tc = new TimeFormatConv();
//
//		public void map(Object key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String line = value.toString().trim();
//			String[] line_array = line.split("\t");
//			String mydomain = "1";
//
//			String has_key = line_array[0];
//			long t1 = tc.stringToTimestamp(line_array[1]);
//			long t2 = tc.stringToTimestamp(line_array[2]);
//			String pv1 = line_array[3];
//			String pv2 = line_array[4];
//			String click1 = line_array[5];
//			String click2 = line_array[6];
//			String cost1 = line_array[7];
//			String cost2 = line_array[8];
//			String windowIndex = line_array[9];
//
//			outStr.set(has_key + "|" + "-1");
//			String strvalue = "-1" + "\t" + t1 + "\t" + t2 + "\t" + pv1 + "\t"
//					+ pv2 + "\t" + click1 + "\t" + click2 + "\t" + cost1 + "\t"
//					+ cost2 + "\t" + windowIndex;
//			word.set(strvalue);
//			context.write(outStr, word);
//
//		}
//	}
//
//	public static class Partition extends Partitioner<Text, Text> {
//
//		@Override
//		public int getPartition(Text key, Text value, int numPartitions) {
//			// TODO Auto-generated method stub
//			String[] line = key.toString().trim().split("\\|");
//			int length = line.length;
//			String hash_key = "";
//			for (int i = 0; i < length - 2; i++) {
//				hash_key = hash_key + line[i] + "|";
//			}
//			hash_key = hash_key + line[length - 2];
//
//			// hash_key=key.toString().substring(0,
//			// key.toString().lastIndexOf("\\|"));
//
//			return (hash_key.hashCode() & Integer.MAX_VALUE) % numPartitions;
//		}
//	}
//
//	public static class GroupComparator extends WritableComparator {
//		protected GroupComparator() {
//			super(Text.class, true);
//		}
//
//		@Override
//		public int compare(WritableComparable a, WritableComparable b) {
//			// TODO Auto-generated method stub
//			Text t1 = (Text) a;
//			Text t2 = (Text) b;
//
//			String[] line1 = t1.toString().trim().split("\\|");
//			int length1 = line1.length;
//			String hash_key1 = "";
//			for (int i = 0; i < length1 - 2; i++) {
//				hash_key1 = hash_key1 + line1[i] + "|";
//			}
//			hash_key1 = hash_key1 + line1[length1 - 2];
//
//			String[] line2 = t2.toString().trim().split("\\|");
//			int length2 = line2.length;
//			String hash_key2 = "";
//			for (int i = 0; i < length2 - 2; i++) {
//				hash_key2 = hash_key2 + line2[i] + "|";
//			}
//			hash_key2 = hash_key2 + line2[length2 - 2];
//
//			return hash_key1.compareTo(hash_key2);
//		}
//	}
//
//	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//
//		private static Text outKey = new Text();
//		private static Text outValue = new Text();
//		private static Text reTrainValue = new Text();
//
//		private MultipleOutputs mos = null;
//
//		protected void setup(Context context) throws IOException,
//				InterruptedException {
//			// super.setup(context);
//			mos = new MultipleOutputs(context);
//		}
//
//		TimeFormatConv tc = new TimeFormatConv();
//
//		CtrActorCaller ctrcon = new CtrActorCaller();
//
//		int timeIndex = 0;
//
//		String preKey = "";
//
//		public void reduce(Text key, Iterable<Text> values, Context context)
//				throws IOException, InterruptedException {
//
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//			GregorianCalendar cal = new GregorianCalendar();
//			cal.setTime(new Date());
//			cal.add(Calendar.DATE, -7);
//			String deldate = sdf.format(cal.getTime());
//
//			String[] keyStr = key.toString().trim().split("\\|");
//			// String platform = keyStr[0];
//			// String domain = keyStr[1];
//			// String adunitId =keyStr[2];
//			// String width = keyStr[3];
//			// String height = keyStr[4];
//			// String category = keyStr[5];
//			String atime = keyStr[keyStr.length - 1];
//
//			int length = keyStr.length;
//			String hash_key = "";
//			for (int i = 0; i < length - 2; i++) {
//				hash_key = hash_key + keyStr[i] + "|";
//			}
//			hash_key = hash_key + keyStr[length - 2];
//
//			/*
//			 * for (Text item : values){ outKey.set(key.toString());
//			 * outValue.set(item.toString());
//			 * mos.write(context.getConfiguration().get("splitPath_2"),
//			 * outKey,outValue
//			 * ,context.getConfiguration().get("splitPath_2")+"/"+"part");
//			 * mos.write(context.getConfiguration().get("splitPath_1"),
//			 * outKey,outValue
//			 * ,context.getConfiguration().get("splitPath_1")+"/"+"part"); }
//			 */
//
//			for (Text item : values) {
//
//				String itemStr = item.toString();
//				// input format : category:::weight
//				String[] item_array = itemStr.split("\t");
//
//				if (item_array.length < 3)
//					continue;
//				String atype = item_array[0];
//
//				if (!hash_key.equals(preKey)) {
//					ctrcon = new CtrActorCaller();
//				}
//
//				if ("-1".equals(atype)) {
//
//					TimeFormatConv tc = new TimeFormatConv();
//					int t1 = Integer.parseInt(item_array[1]);
//					if ("-1".equals(item_array[2]))
//						item_array[2] = "0";
//					int t2 = Integer.parseInt(item_array[2]);
//					int pv1 = Integer.parseInt(item_array[3]);
//					int pv2 = Integer.parseInt(item_array[4]);
//					int click1 = Integer.parseInt(item_array[5]);
//					int click2 = Integer.parseInt(item_array[6]);
//					long cost1 = Long.parseLong(item_array[7]);
//					long cost2 = Long.parseLong(item_array[8]);
//					int windowIndex = Integer.parseInt(item_array[9]);
//					if (windowIndex != 0)
//						windowIndex = 1;
//
//					// ctrcon.actor.CtrActorInit(t1, t2, pv1, pv2, click1,
//					// click2, cost1, cost2, windowIndex);
//
//				} else {
//					int winprice = Integer.valueOf(item_array[1]);
//
//					// if(atype.equals("3"))
//					// ctrcon.actor.updateActor(InputType.PV,
//					// Integer.parseInt(item_array[2]), winprice);
//					// else if (atype.equals("4"))
//					// ctrcon.actor.updateActor(InputType.CLICK,
//					// Integer.parseInt(item_array[2]), winprice);
//
//					if (ctrcon.actor.getWindowIndex() != timeIndex
//							&& ctrcon.actor.getACtr() >= 0)
//						timeIndex = ctrcon.actor.getWindowIndex();
//				}
//				preKey = hash_key;
//
//			}
//			outKey.set(hash_key);
//			if (Integer.parseInt(tc.timeStampToString(ctrcon.actor.getT1(),
//					"yyyyMMddHHmmss").substring(0, 8)) > Integer
//					.parseInt(deldate)) {
//				reTrainValue.set("");
//
//				// tc.timeStampToString(ctrcon.actor.getT1(), "yyyyMMddHHmmss")
//				// + "\t" + tc.timeStampToString(ctrcon.actor.getT2(),
//				// "yyyyMMddHHmmss")
//				// + "\t" + ctrcon.actor.getPv1() + "\t" + ctrcon.actor.getPv2()
//				// + "\t" + ctrcon.actor.getClick1() + "\t" +
//				// ctrcon.actor.getClick2()
//				// + "\t" + ctrcon.actor.getCost1() + "\t" +
//				// ctrcon.actor.getCost2() + "\t" +
//				// ctrcon.actor.getWindowIndex());
//
//				mos.write(context.getConfiguration().get("splitPath_2"),
//						outKey, reTrainValue,
//						context.getConfiguration().get("splitPath_2") + "/"
//								+ "part");
//
//				if (ctrcon.actor.getACtr() >= 0
//						|| ctrcon.actor.getWindowIndex() != 0) {
//
//					// outValue.set(
//					//
//					// tc.timeStampToString(ctrcon.actor.getT1(),
//					// "yyyyMMddHHmmss") + "\t" +
//					// tc.timeStampToString(ctrcon.actor.getT2(),
//					// "yyyyMMddHHmmss")
//					// + "\t" + ctrcon.actor.getPv1()
//					// + "\t" + ctrcon.actor.getClick1()
//					// + "\t" + ctrcon.actor.getCost1() );
//
//					// context.write(outKey, outValue);
//					mos.write(context.getConfiguration().get("splitPath_1"),
//							outKey, outValue,
//							context.getConfiguration().get("splitPath_1") + "/"
//									+ "part");
//				}
//			}
//
//		}
//
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//			mos.close();
//		}
//
//	}
//
//	public int run(String[] args) throws Exception {
//
//		Configuration conf = getConf();
//		Job job = Job.getInstance(conf);
//
//		job.setJarByClass(CtrInputStream_v3.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		job.setMapperClass(LogMapper.class);
//		job.setMapperClass(RuleMapper.class);
//		job.setPartitionerClass(Partition.class);
//		// job.setSortComparatorClass(KeyComparator.class);
//		job.setGroupingComparatorClass(GroupComparator.class);
//		job.setReducerClass(Reduce.class);
//
//		// FileInputFormat.setInputPaths(job, new Path(args[0]));
//
//		MultipleInputs.addInputPath(job, new Path(args[0]),
//				TextInputFormat.class, LogMapper.class);
//		MultipleInputs.addInputPath(job, new Path(args[1]),
//				TextInputFormat.class, RuleMapper.class);
//
//		FileOutputFormat.setOutputPath(job, new Path(args[2]));
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
//		// job.getConfiguration().set("filterWeight", args[2]);
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		return 0;
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		Configuration conf = new Configuration();
//		int res = ToolRunner.run(conf, new CtrInputStream_v3(), args);
//
//		System.exit(res);
//
//	}
//}