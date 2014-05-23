package tv.icntv.log.stb;/*
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;
import org.apache.hadoop.io.Text;
import tv.icntv.log.stb.login.ParserLoginMapper;
import tv.icntv.log.stb.player.PlayerMapper;

import java.text.MessageFormat;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 14:36
 */
public class GenerateStbLogJob extends AbstractJob {
    @Override
    public void run(Map<String, String> maps) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.

        Configuration configuration=getConf();
        String day=configuration.get(DAY_CONSTANT);
        System.out.println();
        System.out.println(MessageFormat.format(maps.get(USER_LOGIN_JOB_INPUT), configuration.getLong(FILTER_TIME,0L))+"\t"+MessageFormat.format(maps.get(USER_LOGIN_JOB_OUTPUT),day));
        //user login
        Job userLogin= Job.getInstance(configuration, "user login job");
        userLogin.setMapperClass(ParserLoginMapper.class);
        userLogin.setJarByClass(this.getClass());
        userLogin.setOutputKeyClass(NullWritable.class);
        userLogin.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(userLogin, new Path(MessageFormat.format(maps.get(OUTPUT_PREFIX),day)+MessageFormat.format(maps.get(USER_LOGIN_JOB_INPUT), configuration.getLong(FILTER_TIME, 0L))));
        FileOutputFormat.setOutputPath(userLogin,new Path(MessageFormat.format(maps.get(USER_LOGIN_JOB_OUTPUT),day)));
        userLogin.setNumReduceTasks(0);
        ControlledJob userControlledJob=new ControlledJob(configuration);
        userControlledJob.setJob(userLogin);
        //
//
        Job player=Job.getInstance(configuration,"player job");
        player.setJarByClass(getClass());
        player.setMapperClass(PlayerMapper.class);
        player.setOutputValueClass(Text.class);
        player.setOutputKeyClass(NullWritable.class);
        FileInputFormat.addInputPath(player,new Path(MessageFormat.format(maps.get(OUTPUT_PREFIX),day)+MessageFormat.format(PLAYER_JOB_INPUT,configuration.getLong(FILTER_TIME,0L))));
        FileOutputFormat.setOutputPath(player,new Path(MessageFormat.format(maps.get(PLAYER_JOB_OUTPUT),day)));
        player.setNumReduceTasks(0);
        ControlledJob playControlledJob=new ControlledJob(configuration);
        playControlledJob.setJob(player);
        JobControl jobControl=new JobControl("stb log parser .eg: userLogin,devicePlayer,contentView");
        jobControl.addJob(userControlledJob);
        jobControl.addJob(playControlledJob);
        new Thread(jobControl).start();
        while (!jobControl.allFinished()) {
            Thread.sleep(5000);
        }
//        userLogin.waitForCompletion(true);
    }
    public static void main(String[]args) throws Exception {
        Configuration configuration=new Configuration();
        int result = ToolRunner.run(configuration, new GenerateStbLogJob(), args);
        System.exit(result);
    }
}
