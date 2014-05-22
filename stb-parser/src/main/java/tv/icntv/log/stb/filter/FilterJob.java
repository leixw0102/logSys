package tv.icntv.log.stb.filter;/*
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


import com.google.common.base.Strings;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import tv.icntv.log.stb.commons.HadoopUtils;
import tv.icntv.log.stb.commons.LoadProperties;
import tv.icntv.log.stb.core.AbstractJob;


import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/16
 * Time: 13:54
 */
public class FilterJob extends AbstractJob{

    @Override
    public void setConf(Configuration conf){
        super.setConf(conf);
        conf.setBoolean("mapreduce.reduce.speculative",false);
        conf.setBoolean("mapreduce.map.speculative",false);
    }


    @Override
    public void run(Map<String, String> maps) throws Exception {
        Configuration configuration=getConf();
       //setting conf
        Properties properties= LoadProperties.loadProperitesByFileAbsolute(maps.get(RULE_FILE.toLowerCase()));
        String day=configuration.get(DAY_CONSTANT);
        Path input = new Path(MessageFormat.format(properties.getProperty(INPUT),day));
        Path back = new Path(MessageFormat.format(properties.getProperty(BACK),day));
        Path output = new Path(MessageFormat.format(properties.getProperty(OUTPUT_PREFIX),day));
        String paths=properties.getProperty(FILTER_JOB_PATHS);
        if(Strings.isNullOrEmpty(paths)){
            return;
        }
        configuration.set(OUTPUT_SUFFIX,properties.getProperty(OUTPUT_SUFFIX));
        configuration.set(OUTPUT_PREFIX,output.toString());
        configuration.set(OTHER_PATH,properties.getProperty(OTHER_PATH));

//        configuration
//        Path input=new Path("/icntv/log/stb/2014-05-19/stb-2014-05-18-23.lzo_deflate");
//        Path back=new Path("/icntv/parser/stb/filter/status/2014-05-18/");
//        Path output=new Path("/icntv/parser/stb/filter/result/2014-05-18/");
        Path[] in= HadoopUtils.createFile(input,back,new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith(file_success_suffix);  //To change body of implemented methods use File | Settings | File Templates.
            }
        },file_success_suffix,parseing_suffix,parsed_suffix);
        if(null == in || in.length==0){
            logger.info("input not exist;day={}",day);
            return;
        }
        Job stbFilterJob = Job.getInstance(configuration,"stb parser first:filter by rule file");
        //setting job configuration .....
        stbFilterJob.setMapperClass(FilterMapper.class);
        stbFilterJob.setOutputKeyClass(NullWritable.class);
        stbFilterJob.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(stbFilterJob, in);
        stbFilterJob.setJarByClass(getClass());

        FileOutputFormat.setOutputPath(stbFilterJob, output);
        LazyOutputFormat.setOutputFormatClass(stbFilterJob, TextOutputFormat.class);

        stbFilterJob.setNumReduceTasks(0);

       if(stbFilterJob.waitForCompletion(true)){;
            for(Path path:in){
                HadoopUtils.rename(new Path(path,parseing_suffix),new Path(path,parsed_suffix));
            }
       }
    }

    /**

     * @param args
     * @throws Exception
     */
    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();


        int result = ToolRunner.run(configuration,new FilterJob(),args);
        System.exit(result);
    }


}
