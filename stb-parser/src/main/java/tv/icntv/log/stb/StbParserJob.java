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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.core.AbstractJob;

import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/16
 * Time: 13:55
 */
public class StbParserJob extends AbstractJob {
    @Override
    public int run(String[] args) throws Exception {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private void runTool(Class<? extends AbstractJob> toolClass,Map<String,String> args) throws Exception {
        AbstractJob tool = ReflectionUtils.newInstance(toolClass,getConf());
        tool.run(args);
    }

    @Override
    public void run(Map<String, String> maps) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }
    public static void main(String[]args) throws Exception {
        Configuration conriguration=new Configuration();
        int result = ToolRunner.run(conriguration,new StbParserJob(),args);
        System.exit(result);
    }
}
