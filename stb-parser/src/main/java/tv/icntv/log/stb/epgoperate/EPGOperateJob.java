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
package tv.icntv.log.stb.epgoperate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;

import java.util.Map;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/06/20
 * Time: 14:25
 */
public class EPGOperateJob extends AbstractJob {
	@Override
	public boolean run(Map<String, String> maps) throws Exception {
		return true;
	}

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration=getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        Job contentViewJob= Job.getInstance(configuration, "content view test");
        contentViewJob.setMapperClass(EPGOperateMapper.class);
        contentViewJob.setJarByClass(this.getClass());
        contentViewJob.setOutputKeyClass(NullWritable.class);
        contentViewJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(contentViewJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(contentViewJob, new Path(args[1]));
        contentViewJob.setNumReduceTasks(0);
        return contentViewJob.waitForCompletion(true)?0:1;
    }
    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();
        int result = ToolRunner.run(configuration,new EPGOperateJob(),args);
        System.exit(result);

    }
}
