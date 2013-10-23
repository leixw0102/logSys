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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.xmlObj.XmlLog;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-17
 * Time: 上午9:24
 * To change this template use File | Settings | File Templates.
 */
public class YstenParser {

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration  configuration=HBaseConfiguration.create();
        String[] arrayArgs=new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(null == arrayArgs|| arrayArgs.length!=1){
            System.out.println("please specify parameter < full_file_path>!");
            return;
        }
        Job job=configureJob(configuration,arrayArgs);
        System.exit(job.waitForCompletion(true)?0:1);
    }

    private static Job configureJob(Configuration configuration, String[] arrayArgs) throws IOException {

        XmlLog xmlLog = LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration").getConf();

        Path inputPath=new Path(arrayArgs[0]);
        String tableName=xmlLog.getTable();
        Job job = new Job(configuration,"icntv_"+tableName);
        job.setJarByClass(YstenMapper.class);
        FileInputFormat.setInputPaths(job,inputPath);
        job.setMapperClass(YstenMapper.class);

        TableMapReduceUtil.initTableReducerJob(tableName,null,job);
        job.setNumReduceTasks(0);
        return job;
    }
}
