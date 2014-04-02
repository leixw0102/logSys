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

package tv.icntv.logsys;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.logsys.uncompress.HdfsStore;
import tv.icntv.logsys.utils.ReflectionUtils;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-7
 * Time: 下午3:51
 */
public class HadoopRun extends Configured implements Tool {
    //

    private static Logger logger = LoggerFactory.getLogger("parser-hadoop-log-main");
    final static String FILE_KEYS = "file.keys";
    final static String SPLIT = ",";
    final static String separator = File.separator;
    public Configuration parseConf=null;
    public static HdfsStore store = new HdfsStore();
    private static final String storeHbaseing = ".storing";
    private static final String storeHbaseSuffix = ".store";
    private static final String CLASS_NAME="%s.className";

    public HadoopRun(Configuration configuration) {
        this.parseConf=configuration;
    }

    public String getFileKeys() {
        return parseConf.get(FILE_KEYS);
    }
    protected List<Path> getFileStatus(String fromPath) {
        FileStatus[] fileStatuses = store.getFiles(fromPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith("writed");
            }
        });

        if (null == fileStatuses || fileStatuses.length == 0) {
            logger.info("fileStatuses is null");
            return null;
        }
        List<Path> list = Lists.newArrayList();
        for (FileStatus fileStatus : fileStatuses) {
            String name = fileStatus.getPath().getName();
            name = name.replace(".writed", "");
            if ((name.endsWith(".gz")||name.endsWith(".log")) && store.isExist(fromPath + separator + name)) {
                list.add(new Path(fromPath + separator + name));
            }
        }
        return list;
    }


    @Override
    public int run(String[] strings) throws Exception {
        String keys = null;
        if (null != strings && strings.length == 1) {
            keys = strings[0];
            logger.info("accept input file {}",keys);
        } else {
            keys = getFileKeys();
            logger.info("get xml config,name={},value={}",FILE_KEYS,keys);
        }
        if (null == keys || keys.equals("")) {
            logger.info("hadoop execute file directory null");
            return 1;
        }
        String fileFromPrefix = getFileFromPrefix();
        String fileToPrefix = getFileToPrefix();
        String parserToPrefix=getParserToPrefix();
        for (String file : keys.split(SPLIT)) {
            String fromPath = fileFromPrefix + separator + file;
            String toPath = fileToPrefix + separator + file;
            logger.info("from file path ={},to file path={}", fromPath, toPath);
//            String tableName = get(String.format(TABLE,file));  logger.info(file);
            String toHdfs=String.format(get("parser.to.hdfs"), file, new Date());
            String className=get(String.format(CLASS_NAME,file));
            List<Path> paths = getFileStatus(fromPath);

            if(null == paths||paths.isEmpty()){
                logger.info("paths is empty");
                continue;
            }
            for (Path path : paths) {

                String name = path.getName();

                String storeLogFile = fileToPrefix+ separator + file + separator + name.substring(0, name.length() - 3);
                String parserFile = fileFromPrefix + separator + file + separator + name;
                String hdfs=parserToPrefix+separator+toHdfs+separator+System.nanoTime();
                logger.info("from file name ={},to file name={}", parserFile, storeLogFile);
                if (!store.isExist(storeLogFile + storeHbaseSuffix)) {
                    store.createFile(storeLogFile + storeHbaseing);
                    Tool job = (Tool) ReflectionUtils.newInstance(className);
                    int res = ToolRunner.run(parseConf, job, new String[]{parserFile, hdfs});
                    if (res == 0) {
                        store.rename(storeLogFile + storeHbaseing, storeLogFile + storeHbaseSuffix);
                        logger.info("file {},persist 2 hbase table {} success..", parserFile, hdfs);
                    } else {
                        logger.error("file {},persist 2 hbase table {} failed....", parserFile, hdfs);
                    }
                } else {
                    logger.info("file {} 已经处理..", parserFile);
                }
            }
        }

        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }


    protected String getFileFromPrefix() {
        return parseConf.get("file.from.prefix", "/icntv/log");
    }

    protected String get(String key){
        return parseConf.get(key);
    }
    protected String getFileToPrefix() {
        return parseConf.get("file.to.prefix", "/icntv/store/log");
    }

    public static void main(String[] args) {
        Configuration configuration= new Configuration();
        configuration.addResource("log-parse.xml");
        HadoopRun main = new HadoopRun(configuration);
        try {
            int i = ToolRunner.run( HBaseConfiguration.create(),main, args);
            System.exit(i);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public String getParserToPrefix() {
        return  parseConf.get("hdfs.to.directory", "/icntv/parser");
    }
}
