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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wangl
 * Date: 13-10-22
 * Time: 下午5:12
 * To change this template use File | Settings | File Templates.
 */
public class CdnParser extends Configured implements Tool{
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private  Job configureJob(Configuration configuration,String[] arrayArgs) throws IOException {

        Path inputPath=new Path(arrayArgs[0]);
        String tableName=arrayArgs[1];//xmlLog.getTable();
        Job job = new Job(configuration,"icntv_"+tableName+"_"+inputPath);
        job.setJarByClass(tv.icntv.logsys.cdn.CdnParser.class);
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(tv.icntv.logsys.cdn.CdnMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob(tableName, tv.icntv.logsys.cdn.CdnReducer.class, job);
//        job.setReducerClass(CdnHdfsReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job,new Path(tableName));
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
            logger.info("start mr ,thread {}",Thread.currentThread().getId());
            Configuration configuration=super.getConf();
            job = configureJob(configuration,arrayArgs);
            return job.waitForCompletion(true)?0:1;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return 1;
    }


}
