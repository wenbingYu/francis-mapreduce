package com.francis.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedCacheV1 extends Configured implements Tool {

	private static Logger logger = LoggerFactory
			.getLogger(DistributedCacheV1.class);

	public static class DistributedCacheV1Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, NullWritable> {

		private List<String> al1 = new ArrayList<String>();

		public void configure(JobConf job) {

			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(job);
				for (int i = 0; i < paths.length; i++) {
					logger.info("paths================" + paths[i]);
				}
				if (paths != null && paths.length > 0) {
					String line;
					BufferedReader joinReader = new BufferedReader(
							new FileReader(paths[0].toString()));
					while ((line = joinReader.readLine()) != null) {

						logger.info("reader line ======" + line);

						al1.add(line);

					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {

			logger.info("al1 size==============" + al1.size());

			for (int i = 0; i < al1.size(); i++) {
				output.collect(new Text(al1.get(i).toString()),
						NullWritable.get());
			}

		}
	}

	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(getConf(), DistributedCacheV1.class);

		conf.setJobName("DistributedCacheV1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(DistributedCacheV1Map.class);

		conf.set("mapred.input.dir", args[0]);
		conf.set("mapred.output.dir", args[1]);

		Path category = new Path(args[0]);
		String category_info = category.toUri().toString();

		DistributedCache.addCacheFile(new Path(category_info).toUri(), conf);

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new DistributedCacheV1(), args);
		System.exit(res);
	}

}
