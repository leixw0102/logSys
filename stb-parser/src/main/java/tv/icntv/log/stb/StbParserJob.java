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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;
import tv.icntv.log.stb.filter.FilterJob;

import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/16
 * Time: 13:55
 */
public class StbParserJob extends AbstractJob {


    private void runTool(Class<? extends AbstractJob> toolClass,Map<String,String> args) throws Exception {
        AbstractJob tool = ReflectionUtils.newInstance(toolClass,getConf());
        tool.run(args);
    }

    @Override
    public void run(Map<String, String> maps) throws Exception {
        if(null ==maps || maps.isEmpty()){
            logger.error("read file content,but null");
        }
        //execute filter job
        try{
            runTool(FilterJob.class,maps);
        }catch (Exception e){
            logger.error("error filter job ",e);
            return;
        }
        //
        try{
            runTool(GenerateStbLogJob.class,maps);
        }catch (Exception e){
            logger.error("error parser jobs ",e);
            return;
        }


    }
    public static void main(String[]args) throws Exception {
        Configuration conriguration=new Configuration();
        int result = ToolRunner.run(conriguration,new StbParserJob(),args);
        System.exit(result);
    }
}
