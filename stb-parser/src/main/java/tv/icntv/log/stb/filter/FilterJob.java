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
    public int run(String[] args) throws Exception {
        if(null == args || args.length<1){
            logger.error("usage :-ruleFile <rule_file>");
            return -1;
        }
        if (args.length % 2 != 0) {
            throw new RuntimeException("expected pairs of argName argValue");
        }
        Map<String,String> maps = Maps.newHashMap();
        for(int i=0;i<args.length;i++){
           String key = args[i];
            if(Strings.isNullOrEmpty(key)){
                continue;
            };
            key = key.trim().substring(1);
            try {
//                if(key.equals("-ruleFile")){
                FilterJobParameterEnum temp=FilterJobParameterEnum.valueOf(key.toUpperCase());
                maps.put(temp.toString().trim().toLowerCase(),args[i+1]);
//                }
            } catch (IllegalArgumentException e){
                i++;
                continue;
            }
            i++;
        }
        run(maps);
        return 0;
    }

    @Override
    public void run(Map<String, String> maps) throws Exception {
        Configuration configuration=getConf();
       //setting conf
        Properties properties= LoadProperties.loadProperitesByFileAbsolute(maps.get(RULE_FILE.toLowerCase()));
        DateTime dateTime=new DateTime(new Date());
        String day=  "2014-05-01";//dateTime.toString(DAY_YYYY_MM_DD);
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
        configuration.setLong(FILTER_TIME,dateTime.getMillis());
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

//        for(String str:paths.split(";")){
//            MultipleOutputs.addNamedOutput(stbFilterJob,str,TextOutputFormat.class, NullWritable.class,Text.class);
//        }

//        stbFilterJob.setOutputFormatClass(TextOutputFormat.class);
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
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzoCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzoCodec");
        configuration.setBoolean("mapreduce.reduce.speculative",false);
        configuration.setBoolean("mapreduce.map.speculative",false);
        System.out.println(configuration.get("mapreduce.map.output.compress")+"\t");
        int result = ToolRunner.run(configuration,new FilterJob(),args);
        System.exit(result);
    }

    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
