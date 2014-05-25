/*
 * Copyright 2014 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
package tv.icntv.log.stb.login;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.commons.HadoopUtils;
import tv.icntv.log.stb.core.AbstractJob;
import tv.icntv.log.stb.filter.FilterMapper;

import java.util.Map;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/05/22
 * Time: 16:06
 */
public class ParserLoginJob extends AbstractJob {

	@Override
	public void run(Map<String, String> maps) throws Exception {
//		Configuration configuration=getConf();
//
//		Job stbParserLoginJob = Job.getInstance(configuration,"stb parser login.");
//		//setting job configuration .....
//		stbParserLoginJob.setMapperClass(ParserLoginMapper.class);
//		stbParserLoginJob.setOutputKeyClass(NullWritable.class);
//		stbParserLoginJob.setOutputValueClass(Text.class);
//		FileInputFormat.setInputPaths(stbParserLoginJob, null);
//		stbParserLoginJob.setJarByClass(getClass());
//
//		FileOutputFormat.setOutputPath(stbParserLoginJob, null);
//		LazyOutputFormat.setOutputFormatClass(stbParserLoginJob, TextOutputFormat.class);
//
//		stbParserLoginJob.setNumReduceTasks(0);
//
//		stbParserLoginJob.waitForCompletion(true);
	}

	public static void main(String[]args) throws Exception {
//		Configuration configuration = new Configuration();
//
//		int result = ToolRunner.run(configuration, new ParserLoginJob(), args);
//		System.exit(result);
	}
}
