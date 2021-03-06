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


import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.HadoopUtils;
import tv.icntv.log.stb.core.AbstractJob;
import tv.icntv.log.stb.util.DateUtil;

import java.io.File;
import java.util.List;
import java.util.Map;

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

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration=getConf();
        Path input = new Path(args[0]);

        Path output = new Path(args[1]);
        Job stbFilterJob = Job.getInstance(configuration,"stb parser first:filter by rule file");
        //setting job configuration .....
        stbFilterJob.setMapperClass(FilterMapper.class);
        stbFilterJob.setOutputKeyClass(NullWritable.class);
        stbFilterJob.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(stbFilterJob, input);
        stbFilterJob.setJarByClass(getClass());

        FileOutputFormat.setOutputPath(stbFilterJob, output);
        LazyOutputFormat.setOutputFormatClass(stbFilterJob, TextOutputFormat.class);

        stbFilterJob.setNumReduceTasks(0);
        return stbFilterJob.waitForCompletion(true)?0:1;
    }

    @Override
    public boolean run(Map<String, String> maps) throws Exception {
        Configuration configuration=getConf();
        // 避免资源浪费，但是存在任务失败时，重新等待执行结果
        configuration.setBoolean("mapreduce.reduce.speculative",false);
        configuration.setBoolean("mapreduce.map.speculative",false);
       //setting conf
        Path input = new Path(maps.get(INPUT));
        Path back = new Path(maps.get(BACK));
        Path output = new Path(maps.get(OUTPUT_PREFIX));
        configuration.set(OUTPUT_SUFFIX,maps.get(OUTPUT_SUFFIX));
        configuration.set(OUTPUT_PREFIX,output.toString());
        configuration.set(OTHER_PATH,maps.get(OTHER_PATH));

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
            logger.info("input not exist;");
            return false;
        }
        List<Path> inTemp = Lists.newArrayList(in);
        String ye=DateUtils.addDay(input.getName(),"yyyy-MM-dd",-1);
        Path prefix=new Path(input.getParent()+File.separator+ye,"stb-"+ye+"-23.lzo");
        logger.info("prefix path ={}",prefix.toString());
        if(HadoopUtils.isExist(prefix)){
            logger.info("add today path= {}",prefix.toString());
            inTemp.add(prefix);
        }
        String day= DateUtils.addDay(input.getName(),"yyyy-MM-dd",1);
        Path nextPath = new Path(input.getParent()+File.separator+day ,"stb-"+day+"-00.lzo");
        logger.info("next path ={},writed path={}",nextPath.toString(),new Path(input.getParent()+File.separator+day,"stb-"+day+"-00.lzo.writed"));
        if(HadoopUtils.isExist(new Path(input.getParent()+File.separator+day,"stb-"+day+"-00.lzo.writed"))){
            logger.info("add today path= {}",nextPath.toString());
            inTemp.add(nextPath);
        }

        logger.info("input size = {}",inTemp.size());
//        inTemp.add(new Path(input.getParent()+ File.separator+ DateTime.now().toString("yyyy-MM-dd"),"")
        Job stbFilterJob = Job.getInstance(configuration,"stb parser first:filter by rule file");
        //setting job configuration .....
        stbFilterJob.setMapperClass(FilterMapper.class);
        stbFilterJob.setOutputKeyClass(NullWritable.class);
        stbFilterJob.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(stbFilterJob, inTemp.toArray(new Path[inTemp.size()]));
        stbFilterJob.setJarByClass(getClass());

        FileOutputFormat.setOutputPath(stbFilterJob, output);
        LazyOutputFormat.setOutputFormatClass(stbFilterJob, TextOutputFormat.class);

        stbFilterJob.setNumReduceTasks(0);

       if(stbFilterJob.waitForCompletion(true)){;
            for(Path path:in){
                HadoopUtils.rename(new Path(path+parseing_suffix),new Path(path+parsed_suffix));
            }
           return true;
       }
        return false;

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
