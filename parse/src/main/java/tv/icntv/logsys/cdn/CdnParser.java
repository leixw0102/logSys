/* Copyright 2013 Future TV, Inc.
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

package tv.icntv.logsys.cdn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.logsys.ParserJob;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wangl
 * Date: 13-10-22
 * Time: 下午5:12
 * To change this template use File | Settings | File Templates.
 */
public class CdnParser implements ParserJob{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        CdnParser     parser = new CdnParser();
        parser.start(HBaseConfiguration.create(),args);
//        Configuration  configuration= HBaseConfiguration.create();
//        String[] arrayArgs=new GenericOptionsParser(configuration,args).getRemainingArgs();
//        if(null == arrayArgs|| arrayArgs.length!=1){
//            System.out.println("please specify parameter < full_file_path>!");
//            return;
//        }
//        Job job=configureJob(configuration,arrayArgs);
//        System.exit(job.waitForCompletion(true)?0:1);
    }

    public boolean start(Configuration configuration, String[] arrayArgs){
        Job job= null;
        if(null == arrayArgs|| arrayArgs.length!=2){
            System.out.println("please specify parameter < full_file_path>!");
            return false;
        }
        try {
            logger.info("start mr parameter {},thread {}",arrayArgs,Thread.currentThread().getId());
            job = configureJob(configuration,arrayArgs);
            return job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return false;
    }



    private  Job configureJob(Configuration configuration,String[] arrayArgs) throws IOException {

       // XmlLog xmlLog = LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration","cdn_log_mapping.xml").getConf();
        Path inputPath=new Path(arrayArgs[0]);
        String tableName=arrayArgs[1];//xmlLog.getTable();
        Job job = new Job(configuration,"icntv_"+tableName);
        job.setJarByClass(tv.icntv.logsys.cdn.CdnParser.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(tv.icntv.logsys.cdn.CdnMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob(tableName,tv.icntv.logsys.cdn.CdnReducer.class,job);
        return job;
    }
}
