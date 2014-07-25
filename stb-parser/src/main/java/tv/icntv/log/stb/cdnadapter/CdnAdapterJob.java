package tv.icntv.log.stb.cdnadapter;/*
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
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/07/21
 * Time: 16:00
 */
public class CdnAdapterJob extends AbstractJob {
    @Override
    public boolean run(Map<String, String> maps) throws Exception {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        Job cdnAdapter= Job.getInstance(configuration, "cdn adpater job");
        cdnAdapter.setJarByClass(getClass());
        cdnAdapter.setMapperClass(CdnAdapterMapper.class);
        cdnAdapter.setOutputKeyClass(NullWritable.class);
        cdnAdapter.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(cdnAdapter, new Path(args[0]));
        FileOutputFormat.setOutputPath(cdnAdapter, new Path(args[1]));
        cdnAdapter.setNumReduceTasks(0);
        return cdnAdapter.waitForCompletion(true)?0:1;
    }
    public static void main(String[]args) throws Exception {
        Configuration configuration=new Configuration();
        int result = ToolRunner.run(configuration, new CdnAdapterJob(), args);
        System.exit(result);
    }
}
