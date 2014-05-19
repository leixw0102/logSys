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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;

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
    public int run(String[] args) throws Exception {
        if(null == args || args.length<1){
            logger.error("usage : -input <input_dir> -back <back_dir> -output <output_dir> -ruleFile <rule_file>");
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
            key = key.trim();
            try {
                FilterJobParameterEnum temp=FilterJobParameterEnum.valueOf(key.toUpperCase());
                maps.put(temp.toString().trim().toLowerCase(),args[i+1]);
            } catch (IllegalArgumentException e){
                i++;
                continue;
            }
            i++;
        }
        run(maps);
        return 0;
    }

    @Override
    public void run(Map<String, String> maps) throws Exception {
        Configuration configuration=getConf();
        Path input = new Path(maps.get(INPUT));
        Path back = new Path(maps.get(BACK));
        Path output = new Path(maps.get(OUTPUT));

    }

    /**
     * --input
     * --back
     * --output
     * --rule
     * @param args
     * @throws Exception
     */
    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();
        int result = ToolRunner.run(configuration,new FilterJob(),args);
        System.exit(result);
    }

    enum FilterJobParameterEnum{
        INPUT,BACK,OUTPUT,RULEFILE;
    }
}
