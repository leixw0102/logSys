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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
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
        super.setConf(conf);
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
