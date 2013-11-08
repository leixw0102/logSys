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

package tv.icntv.logsys.stb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.logsys.ParserJob;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-17
 * Time: 上午9:24
 * To change this template use File | Settings | File Templates.
 */
public class YstenParser extends Configured implements Tool {
    private Logger logger = LoggerFactory.getLogger(getClass());
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {

//        Configuration  configuration=HBaseConfiguration.create();
//        String[] arrayArgs=new GenericOptionsParser(configuration,args).getRemainingArgs();
//        if(null == arrayArgs|| arrayArgs.length!=1){
//            System.out.println("please specify parameter < full_file_path>!");
//            return;
//        }
//        Job job=configureJob(configuration,arrayArgs);
//        System.exit(job.waitForCompletion(true)?0:1);
    }


    public boolean start(Configuration configuration, String[] arrayArgs){
        if(null == arrayArgs|| arrayArgs.length!=2){
            System.out.println("please specify parameter < full_file_path>!");
            return false;
        }
        try {
            Job job=configureJob(configuration,arrayArgs);
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

        //XmlLog xmlLog = LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration","ysten_log_mapping.xml").getConf();

        Path inputPath=new Path(arrayArgs[0]);
        String tableName=arrayArgs[1];//xmlLog.getTable();
        Job job = new Job(configuration,"icntv_"+tableName);
        job.setJarByClass(YstenMapper.class);
        FileInputFormat.setInputPaths(job,inputPath);
        job.setMapperClass(YstenMapper.class);

        TableMapReduceUtil.initTableReducerJob(tableName,null,job);
        job.setNumReduceTasks(0);
        return job;
    }

    @Override
    public int run(String[] arrayArgs) throws Exception {
        Job job= null;
        if(null == arrayArgs|| arrayArgs.length!=2){
            System.out.println("please specify parameter < full_file_path>!");
            return 1;
        }
        try {
            logger.info("start mr parameter {},thread {}",arrayArgs,Thread.currentThread().getId());
            job = configureJob(super.getConf(),arrayArgs);
            return job.waitForCompletion(true)?0:1;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return 1;
    }
}
