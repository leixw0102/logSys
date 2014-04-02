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

import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.logsys.uncompress.HdfsStore;
import tv.icntv.logsys.utils.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;


/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-10-28
 * Time: 下午1:49
 * uncompress process:from {core-site.xml property file.from.prefix}  to {core-site.xml property file.to.prefix}
 * <p/>
 * 状态变化：处理gz文件前，创建file.storing,成功后将文件改名为file.store
 */
public class Main {
    private static Logger logger = LoggerFactory.getLogger("parser-log-main");
    final static String FILE_KEYS = "file.keys";
    final static String SPLIT = ",";
    final static String separator = File.separator;
    public static Configuration configuration = HBaseConfiguration.create();
    public static HdfsStore store = new HdfsStore();
    private static final String storeHbaseing = ".storing";
    private static final String storeHbaseSuffix = ".store";

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

        //create hadoop/hbase configuration
        Main main = new Main();
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        String keys = null;
        if (null != args && args.length == 1) {
            keys = args[0];
        } else {
            keys = main.getFileKeys();
        }
        if (null == keys || keys.equals("")) {
            return;
        }
        // get file prefix
        String fileFromPrefix = main.getFileFromPrefix();
        String fileToPrefix = main.getFileToPrefix();
        logger.info("file from prefix ={};to prefix={}", fileFromPrefix, fileToPrefix);
        String[] files = keys.split(SPLIT);
        for (String file : files) {
            String fromPath = fileFromPrefix + separator + file;
            String toPath = fileToPrefix + separator + file;
            logger.info("from file path ={},to file path={}", fromPath, toPath);
            List<Path> paths = main.getFileStatus(fromPath);
            for (Path path : paths) {
                executorService.submit(new ParserExecutor(fromPath, toPath, path, file));
            }
        }

    }

    public String getFileKeys() {
        return configuration.get(FILE_KEYS);
    }

    public String getFileFromPrefix() {
        return configuration.get("file.from.prefix", "/icntv/log");
    }

    public String getFileToPrefix() {
        return configuration.get("file.to.prefix", "/icntv/store/log");
    }


    static class ParserExecutor implements Callable<Boolean> {
        private String fromPath;
        private String toPath;
        private String className;
        private String tableName;
        private Path fileName;
        private String lastDirectory;

        ParserExecutor(String fromPath, String toPath, Path fileName, String lastDirectory) {
            this.fromPath = Preconditions.checkNotNull(fromPath);
            this.toPath = Preconditions.checkNotNull(toPath);
            this.fileName = Preconditions.checkNotNull(fileName);
            this.lastDirectory = lastDirectory;
            this.className = configuration.get(lastDirectory + ".className");
            this.tableName = configuration.get(lastDirectory + ".table");
        }

        @Override
        public Boolean call() throws Exception {
            String name = fileName.getName();
            String storeLogFile = this.toPath + separator + name.substring(0, name.length() - 3);
            String parserFile = this.fromPath + separator + name;
            //is compressed;
//            boolean unCompressSuccess = true;
//            String to = this.toPath + separator + unCompressedName;
//            if (!store.isExist(to + unCompressSuffix)) {
//                //un compress
//                store.createFile(to + unCompressing);
//                unCompressSuccess = store.uncompress(this.fromPath + separator + name, to);
//            }
//            if (!unCompressSuccess) {
//                //解压失败
//                logger.error("uncompress failed ,file ={}", this.fromPath + separator + name);
//                return false;
//            }
//            store.rename(to + unCompressing, to + unCompressSuffix);
            //to hbase
            logger.info("input file name {}", parserFile);
            try {
                if (!store.isExist(storeLogFile + storeHbaseSuffix)) {
                    store.createFile(storeLogFile + storeHbaseing);
                    logger.info("class name"+this.className +" \t" +this.lastDirectory);
                    ParserJob job = (ParserJob) ReflectionUtils.newInstance(this.className);
                    if (job.start(configuration, new String[]{parserFile, tableName})) {
                        store.rename(storeLogFile + storeHbaseing, storeLogFile + storeHbaseSuffix);
                        logger.info("file {},persist 2 hbase table {} success..", parserFile, tableName);
                        return true;
                    } else {
                        logger.error("file {},persist 2 hbase table {} failed....", parserFile, tableName);
                        return false;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("executor job error!", e);
            }
            logger.info("source file {},stored log file {},continue", parserFile, storeLogFile + storeHbaseSuffix);
            return true;  //To change body of implemented methods use File | Settings | File Templates.
        }


    }

    public List<Path> getFileStatus(String fromPath) {
        FileStatus[] fileStatuses = store.getFiles(fromPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().endsWith("writed");
            }
        });
        if (null == fileStatuses || fileStatuses.length == 0) {
            return null;
        }
        List<Path> list = Lists.newArrayList();
        for (FileStatus fileStatus : fileStatuses) {
            String name = fileStatus.getPath().getName();
            name = name.replace(".writed", "");
            if (name.endsWith(".gz") && store.isExist(fromPath + separator + name)) {
                list.add(new Path(fromPath + separator + name));
            }
        }
        return list;
    }

    public String get(String key){
        return configuration.get(key);
    }
}
