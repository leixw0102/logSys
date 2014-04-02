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

package tv.icntv.log.crawl.conf;

import com.google.common.collect.Lists;
import tv.icntv.log.crawl.store.FileStoreData;
import tv.icntv.log.crawl.store.StoreData;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-11
 * Time: 上午11:32
 * To change this template use File | Settings | File Templates.
 */
public class FtpConfig {
//    private static final String FTP_NAME_KEY="ftp.name";
//    private static final String FTP_PWD_KEY="ftp.pwd";
//    private static final String FTP_CONTENT_LIMIT="ftp.content.limit";
//    private static final String FTP_TIMEOUT="ftp.timeout";
//    private static final String FTP_SERVER_TIMEOUT="ftp.server.timeout";
//    private static final String FTP_KEEY_CONNECTION="ftp.keep.connection";
//    private static final String FTP_DATA_SORT_TYPE="ftp.data.sort.type";
//    private static final String FTP_ENCODING="ftp.encoding";
//    private static final String FTP_PROT="ftp.prot";
//    private static final String FTP_DIRECTORY_EXCLUDE="ftp.directory.exclude";
    private static Configuration configuration = null;
//    private static final String FTP_DST_DIRECTORY="ftp.dst.directory";
//    private static final String FILE_SUFFIX="file.suffix";

    public FtpConfig init(String config){
        configuration=Configuration.getInstanceConfig(config);
        return this;
    }

    public  String getFtpDirectoryExclude(){
        return configuration.get(FtpConstant.FTP_DIRECTORY_EXCLUDE,"");
    }

    public  List<String> getFileSuffixs(){
          String filesuffix=configuration.get(FtpConstant.FILE_SUFFIX);
        if(null == filesuffix|| filesuffix.equals("")){
            return null;
        }
        return Lists.newArrayList(filesuffix.split(","));
    }
    static class FtpConstant{
        private static final String FTP_NAME_KEY="ftp.name";
        private static final String FTP_PWD_KEY="ftp.pwd";
        private static final String FTP_CONTENT_LIMIT="ftp.content.limit";
        private static final String FTP_TIMEOUT="ftp.timeout";
        private static final String FTP_SERVER_TIMEOUT="ftp.server.timeout";
        private static final String FTP_KEEY_CONNECTION="ftp.keep.connection";
        private static final String FTP_DATA_SORT_TYPE="ftp.data.sort.type";
        private static final String FTP_ENCODING="ftp.encoding";
        private static final String FTP_PROT="ftp.prot";
        private static final String FTP_DIRECTORY_EXCLUDE="ftp.directory.exclude";
        private static final String FTP_DST_DIRECTORY="ftp.dst.directory";
        private static final String FILE_SUFFIX="file.suffix";
        private static final String FTP_DST_NAME_APPEND_LOCAL="ftp.dst.name.append.local"; //ftp.dst.name.append.local
        private static final String FTP_DIRECTORY_INCLUDE="ftp.directory.include";
    }
    public  String getFtpDstDirectory(){
        return configuration.get(FtpConstant.FTP_DST_DIRECTORY,"/");
    }

    public boolean getFtpDstNameAppendLocal(){
        return configuration.getBoolean(FtpConstant.FTP_DST_NAME_APPEND_LOCAL);
    }
    /**
     * get ftp.name
     * @return
     */
    public  String getFtpName(){
          return  configuration.get(FtpConstant.FTP_NAME_KEY,"anonymous");
    }

    /**
     * get ftp.pwd
     * @return
     */
    public  String getFtpPwd(){
        return configuration.get(FtpConstant.FTP_PWD_KEY,"");
    }

    public  String getFtpEncoding(){
        return configuration.get(FtpConstant.FTP_ENCODING,"utf-8");
    }
    /**
     * get ftp.content.limit
     * @return
     */
    public  long getFtpContentLimit(){
        return configuration.getLong(FtpConstant.FTP_CONTENT_LIMIT,-1L);
    }

    public  int getFtpPort(){
        return configuration.getInt(FtpConstant.FTP_PROT,21);
    }
    /**
     * get ftp.data.store.type
     * @return
     */
    public  String getFtpStoreType(){
        return configuration.get(FtpConstant.FTP_DATA_SORT_TYPE, "HDFS");
    }

    public String getDirectoryInclude(){
        return configuration.get(FtpConstant.FTP_DIRECTORY_INCLUDE);
    }
    /**
     * get ftp.timeout   ;default 6000s
     * @return
     */
    public  int getFtpTimeOut(){
        return  configuration.getInt(FtpConstant.FTP_TIMEOUT,6000);
    }

    /**
     * get ftp.server.timeout ;default 100000s
     * @return
     */
    public  int getFtpServerTimeOut(){
        return configuration.getInt(FtpConstant.FTP_SERVER_TIMEOUT,100000);
    }

    /**
     * get ftp.keep.connection ;default false;
     * @return
     */
    public  boolean getFtpKeepConnection(){
        return configuration.getBoolean(FtpConstant.FTP_KEEY_CONNECTION, false);
    }
    /**
     * store type
     */
    public static enum SortTypeClass{
        HDFS("tv.icntv.log.crawl.store.HdfsDefaultStore"),DB("");
        String className;

        private SortTypeClass(String className) {
            this.className = className;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }
        public StoreData getFileStoreTypeClass(){
            try {
                return (FileStoreData) Class.forName(this.className).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (IllegalAccessException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (ClassNotFoundException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            return null;
        }
    }
    public static void main(String[]args){
//        System.out.println(FtpConfig.getFtpDirectoryExclude());
//        System.out.println(FtpConfig.getFtpDirectoryExclude());
//        System.out.println(FtpConfig.getFtpDirectoryExclude());
    }
}
