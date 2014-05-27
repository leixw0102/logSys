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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.LoadProperties;

import java.io.IOException;
import java.util.*;

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
        maps.put("I","stb.input.path");
        maps.put("Back","stb.back.path");
        maps.put("FOut","stb.filter.result.path");
        maps.put("FKeys","stb.filter.file.keys");
        maps.put("FOther","stb.filter.file.other");
        maps.put("PMInput","stb.parser.device.player.relative.input");
        maps.put("PMOut","stb.parser.device.player.hadoop.output");
//        maps.put("-POut","stb.parser.device.player.dat.result");
        maps.put("UMInput","stb.parser.user.login.relative.input");
        maps.put("UMOut","stb.parser.user.login.hadoop.output");
//        maps.put("-UOut","stb.parser.user.login.dat.output");
        maps.put("CMInput","stb.parser.content.view.relative.input");
        maps.put("CMOut","stb.parser.content.view.hadoop.output");
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
//        if (args.length % 2 != 0) {
//            throw new RuntimeException("expected pairs of argName argValue");
//        }
//        Map<String,String> maps = Maps.newHashMap();
//        for(int i=0;i<args.length;i++){
//            String key = args[i];
//            if(Strings.isNullOrEmpty(key)){
//                continue;
//            };
//            key = key.trim().substring(1);
//            try {
////                if(key.equals("-ruleFile")){
////                FilterJobParameterEnum temp=FilterJobParameterEnum.valueOf(key.toUpperCase());
//                maps.put(key.toString().trim().toLowerCase(),args[i+1]);
////                }
//            } catch (IllegalArgumentException e){
//                i++;
//                continue;
//            }
//            i++;
//        }
        Configuration conf=super.getConf();

        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
//        Properties properties= LoadProperties.loadProperitesByFileAbsolute(maps.get(RULE_FILE.toLowerCase()));
//        Set<Object> keys =properties.keySet();
//        for(Object obj:keys){
//            maps.put(obj.toString(),properties.get(obj).toString());
//           // System.out.println(obj.toString()+"\t"+properties.get(obj.toString()));
//        }

//        DateTime dateTime=new DateTime(new Date());
//        conf.set(DAY_CONSTANT,"2014-05-01");
//        conf.setLong(FILTER_TIME, 1400823883967L);
        ;
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
        return options;
    }

    public abstract boolean run(Map<String,String> maps) throws Exception;

}
