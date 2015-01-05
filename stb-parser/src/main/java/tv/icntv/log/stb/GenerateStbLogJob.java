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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.cdnModule.CdnStbMapper;
import tv.icntv.log.stb.cdnadapter.CdnAdapterMapper;
import tv.icntv.log.stb.contentview.ContentViewMapperBack;
import tv.icntv.log.stb.core.AbstractJob;
import tv.icntv.log.stb.epgoperate.EPGOperateMapperVersion1;
import tv.icntv.log.stb.login.ParserLoginMapper;
import tv.icntv.log.stb.player.PlayerMapper;
import tv.icntv.log.stb.replay.ReplayMapper;

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
    public boolean run(Map<String, String> maps) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.

        Configuration configuration=getConf();
        //user login
        Job userLogin= Job.getInstance(configuration, "user login job");
        userLogin.setMapperClass(ParserLoginMapper.class);
        userLogin.setJarByClass(this.getClass());
        userLogin.setOutputKeyClass(NullWritable.class);
        userLogin.setOutputValueClass(Text.class);
        Path userLoginOutput=new Path(maps.get(USER_LOGIN_JOB_OUTPUT));
        FileInputFormat.addInputPath(userLogin, new Path(maps.get(USER_LOGIN_JOB_INPUT)));
        FileOutputFormat.setOutputPath(userLogin,userLoginOutput);
        userLogin.setNumReduceTasks(0);
        ControlledJob userControlledJob=new ControlledJob(configuration);
        userControlledJob.setJob(userLogin);
        //
        //player
        Job player=Job.getInstance(configuration,"player job");
        player.setJarByClass(getClass());
        player.setMapperClass(PlayerMapper.class);
        player.setOutputValueClass(Text.class);
        player.setOutputKeyClass(NullWritable.class);
        FileInputFormat.addInputPath(player, new Path(maps.get(PLAYER_JOB_INPUT)));
        FileOutputFormat.setOutputPath(player, new Path(maps.get(PLAYER_JOB_OUTPUT)));
        player.setNumReduceTasks(0);
        ControlledJob playControlledJob=new ControlledJob(configuration);
        playControlledJob.setJob(player);

        //contentView
        Job contentView = Job.getInstance(configuration,"content view job");
        contentView.setJarByClass(getClass());
        contentView.setMapperClass(ContentViewMapperBack.class);
        contentView.setOutputKeyClass(NullWritable.class);
        contentView.setOutputValueClass(Text.class);
        //System.out.println(MessageFormat.format(maps.get(OUTPUT_PREFIX),day)+MessageFormat.format(maps.get(CONTENT_VIEW_JOB_INPUT),configuration.getLong(FILTER_TIME,0L)));
        FileInputFormat.addInputPath(contentView, new Path(maps.get(CONTENT_VIEW_JOB_INPUT)));
        FileOutputFormat.setOutputPath(contentView,new Path(maps.get(CONTENT_VIEW_JOB_OUTPUT)));
        contentView.setNumReduceTasks(0);
        ControlledJob contentViewControlledJob = new ControlledJob(configuration);
        contentViewControlledJob.setJob(contentView);


        //reply
        Job replay = Job.getInstance(configuration,"reply job");
        replay.setJarByClass(getClass());
        replay.setMapperClass(ReplayMapper.class);
        replay.setOutputKeyClass(NullWritable.class);
        replay.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(replay,new Path(maps.get(LOOK_BACK_JOB_INPUT)));
        FileOutputFormat.setOutputPath(replay,new Path(maps.get(LOOK_BACK_JOB_OUTPUT)));
        replay.setNumReduceTasks(0);
        ControlledJob replayControlledJob = new ControlledJob(configuration);
        replayControlledJob.setJob(replay);

        //logEpg
        Job logEpg=Job.getInstance(configuration,"log epg job");
        logEpg.setJarByClass(getClass());
        logEpg.setMapperClass(EPGOperateMapperVersion1.class);
        logEpg.setOutputKeyClass(NullWritable.class);
        logEpg.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(logEpg, new Path(maps.get(LOG_EPG_JOB_INPUT)));
        FileOutputFormat.setOutputPath(logEpg, new Path(maps.get(LOG_EPG_JOB_OUTPUT)));
        logEpg.setNumReduceTasks(0);
        ControlledJob logEpgControlledJob=new ControlledJob(configuration);
        logEpgControlledJob.setJob(logEpg);

//        //cdn
//        Job cdn=Job.getInstance(configuration,"cdn job");
//        cdn.setJarByClass(getClass());
//        cdn.setMapperClass(CdnModuleMapper.class);
//        cdn.setOutputKeyClass(NullWritable.class);
//        cdn.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(cdn, new Path(maps.get(CDN_JOB_INPUT)));
//        FileOutputFormat.setOutputPath(cdn, new Path(maps.get(CDN_JOB_OUTPUT)));
//        cdn.setNumReduceTasks(0);
//        ControlledJob cdnControlledJob=new ControlledJob(configuration);
//        cdnControlledJob.setJob(cdn);
        //cdn stb
        Job cdn = Job.getInstance(configuration,"cdn stb job");
        cdn.setJarByClass(this.getClass());
        cdn.setMapperClass(CdnStbMapper.class);
        cdn.setOutputValueClass(Text.class);
        cdn.setOutputKeyClass(NullWritable.class);
        FileInputFormat.addInputPath(cdn,new Path(maps.get(CDN_JOB_INPUT)));
        FileOutputFormat.setOutputPath(cdn, new Path(maps.get(CDN_JOB_OUTPUT)));
        cdn.setNumReduceTasks(0);
        ControlledJob cdnControlledJob=new ControlledJob(configuration);
        cdnControlledJob.setJob(cdn);
        //cdn adapter

        Job cdnAdapterJob = Job.getInstance(configuration,"cdn adapter job ");
        cdnAdapterJob.setJarByClass(getClass());
        cdnAdapterJob.setMapperClass(CdnAdapterMapper.class);
        cdnAdapterJob.setOutputKeyClass(NullWritable.class);
        cdnAdapterJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(cdnAdapterJob, new Path(maps.get(CDN_ADAPTER_JOB_INPUT)));
        FileOutputFormat.setOutputPath(cdnAdapterJob, new Path(maps.get(CDN_ADAPTER_JOB_OUTPUT)));
        cdnAdapterJob.setNumReduceTasks(0);
        ControlledJob cdnAdapterControlleredJob = new ControlledJob(configuration);
        cdnAdapterControlleredJob.setJob(cdnAdapterJob);

        JobControl jobControl=new JobControl("stb log parser .eg: userLogin,devicePlayer,contentView,logEpg,cdn");
        jobControl.addJob(userControlledJob);
        jobControl.addJob(playControlledJob);
        jobControl.addJob(contentViewControlledJob);
        jobControl.addJob(replayControlledJob);
        jobControl.addJob(logEpgControlledJob);
        jobControl.addJob(cdnControlledJob);
        jobControl.addJob(cdnAdapterControlleredJob);
        new Thread(jobControl).start();
        while (!jobControl.allFinished()) {
            Thread.sleep(5000);
        }
        if(jobControl.getFailedJobList().size()>0){
            return false;
        }
        return true;
    }
    public static void main(String[]args) throws Exception {
        Configuration configuration=new Configuration();
        int result = ToolRunner.run(configuration, new GenerateStbLogJob(), args);
        System.exit(result);
    }
}
