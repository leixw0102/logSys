package tv.icntv.log.stb.cdn;/*
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;

import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: wangliang
 * Date: 2014/06/04
 * Time: 10:54
 */
public class CdnModuleJob extends AbstractJob {



    public static void main(String[]args) throws Exception {
        Configuration configuration=new Configuration();
        int result = ToolRunner.run(configuration, new CdnModuleJob(), args);
        System.exit(result);
    }

    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        Job cdn= Job.getInstance(configuration, "cdn job");
        cdn.setJarByClass(getClass());
        cdn.setMapperClass(CdnModuleMapper.class);
        cdn.setOutputKeyClass(NullWritable.class);
        cdn.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(cdn, new Path("/icntv/parser/stb/filter/result/2014-07-15/cdn/1405440601"));
        FileOutputFormat.setOutputPath(cdn, new Path("/icntv/parser/stb/cdn/middle/2014-07-15"));
        cdn.setNumReduceTasks(0);
        return cdn.waitForCompletion(true)?0:1;
    }

    @Override
    public boolean run(Map<String, String> maps) throws Exception {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
