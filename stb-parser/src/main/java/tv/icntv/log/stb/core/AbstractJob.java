package tv.icntv.log.stb.core;/*
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


import com.google.common.collect.Maps;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by leixw ,base hadoop mapreduce ,unit job to run.
 * <p/>
 * Author: leixw
 * Date: 2014/03/03
 * Time: 09:45
 */
public abstract class AbstractJob extends Configured implements Tool ,ParserConstant{
    protected Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }
    static Map<String,String> maps = Maps.newHashMap();
    static {
        maps.put("I",INPUT);
        maps.put("Back",BACK);
        maps.put("FOut",OUTPUT_PREFIX);
        maps.put("FKeys",OUTPUT_SUFFIX);
        maps.put("FOther",OTHER_PATH);
        maps.put("PMInput",PLAYER_JOB_INPUT);
        maps.put("PMOut",PLAYER_JOB_OUTPUT);
//        maps.put("-POut","stb.parser.device.player.dat.result");
        maps.put("UMInput",USER_LOGIN_JOB_INPUT);
        maps.put("UMOut",USER_LOGIN_JOB_OUTPUT);
//        maps.put("-UOut","stb.parser.user.login.dat.output");
        maps.put("CMInput",CONTENT_VIEW_JOB_INPUT);
        maps.put("CMOut",CONTENT_VIEW_JOB_OUTPUT);
        maps.put("LBMInput",LOOK_BACK_JOB_INPUT);
        maps.put("LBMOut",LOOK_BACK_JOB_OUTPUT);

        maps.put("LEMInput",LOG_EPG_JOB_INPUT);
        maps.put("LEMOut",LOG_EPG_JOB_OUTPUT);
        maps.put("CDNMInput",CDN_JOB_INPUT);
        maps.put("CDNMOut",CDN_JOB_OUTPUT);
        maps.put("CDN_ADAPTER_MInput",CDN_ADAPTER_JOB_INPUT);
        maps.put("CDN_ADAPTER_MOut",CDN_ADAPTER_JOB_OUTPUT);
//        maps.put("-COut","stb.parser.content.view.dat.output");
    }
    @Override
    public int run(String[] args) throws Exception {
        if(null == args || args.length<1){
            logger.error("usage :-ruleFile <rule_file>");
            return -1;
        }
        Map temp=Maps.newConcurrentMap();
        CommandLineParser parser = new PosixParser();
        CommandLine line =parser.parse(init(), args);

        for(Option option :line.getOptions()){
            System.out.println(maps.get(option.getOpt())+"\t"+option.getValue());
            temp.put(maps.get(option.getOpt()),option.getValue());
        };

        Configuration conf=super.getConf();

        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        return run(temp)?0:1;
    }

    private Options init() {
        Options options = new Options();
        options.addOption("h", false, "help me");
        options.addOption("I",  true, "input hdfs source file");
        options.addOption("Back", true, "back file.parsing ");
        options.addOption("FOut",  true, "filter result output");
        options.addOption("FKeys",true,"filter regular keys") ;
        options.addOption("FOther",true,"exclude FKeys");
        options.addOption("PMInput",true,"device player log ");
        options.addOption("PMOut",true,"mapreduce output");
        options.addOption("POut",true,"dat output file");
        options.addOption("UMInput",true,"user login mapreduce input");
        options.addOption("UMOut",true,"user login mapreduce output");
        options.addOption("UOut",true,"user login dat output file");
        options.addOption("CMInput",true,"contentView mapreduce input");
        options.addOption("CMOut",true,"contentView mapreduce output");
        options.addOption("COut",true,"dat output file");
        options.addOption("LBMInput",true,"look back input");
        options.addOption("LBMOut",true,"look back output");
        options.addOption("LEMInput",true,"log epg input");
        options.addOption("LEMOut",true,"log epg output");
        options.addOption("CDNMInput",true,"cdn input");
        options.addOption("CDNMOut",true,"cdn output");
        options.addOption("CDN_ADAPTER_MInput",true,"cdn adapter input");
        options.addOption("CDN_ADAPTER_MOut",true,"cdn adapter output");
        return options;
    }

    public abstract boolean run(Map<String,String> maps) throws Exception;

}
