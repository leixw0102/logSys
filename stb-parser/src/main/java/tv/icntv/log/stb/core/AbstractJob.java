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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
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
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        DateTime dateTime=new DateTime(new Date());
        conf.set(DAY_CONSTANT,"2014-05-01");
        conf.setLong(FILTER_TIME,dateTime.getMillis());
//        String day=  "2014-05-01";//dateTime.toString(DAY_YYYY_MM_DD);
        super.setConf(conf);
    }
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

    enum FilterJobParameterEnum{
        RULEFILE;
    }

    public abstract void run(Map<String,String> maps) throws Exception;
    /**
     * 获取hdfs paths
     * @param files
     * @return
     * @throws java.io.IOException
     */
    protected Path[] getPaths(String[] files) throws IOException {
        List<Path> paths = Lists.newArrayList();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(super.getConf());
            for (String file : files) {
                Path p = new Path(file);
                if (fileSystem.exists(p)) {
                    paths.add(p);
                }
            }
        } catch (Exception e) {

        } finally {
            if (null != fileSystem) {
                fileSystem.close();
            }
        }
        return paths.toArray(new Path[paths.size()]);
    }

}
