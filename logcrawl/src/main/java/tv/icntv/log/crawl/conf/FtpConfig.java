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
    private static Configuration configuration = Configuration.getInstanceConfig();
    private static final String FTP_DST_DIRECTORY="ftp.dst.directory";
    private static final String FILE_SUFFIX="file.suffix";
    public static String getFtpDirectoryExclude(){
        return configuration.get(FTP_DIRECTORY_EXCLUDE,"");
    }

    public static List<String> getFileSuffixs(){
          String filesuffix=configuration.get(FILE_SUFFIX);
        if(null == filesuffix|| filesuffix.equals("")){
            return null;
        }
        return Lists.newArrayList(filesuffix.split(","));
    }

    public static String getFtpDstDirectory(){
        return configuration.get(FTP_DST_DIRECTORY,"/");
    }
    /**
     * get ftp.name
     * @return
     */
    public static String getFtpName(){
          return  configuration.get(FTP_NAME_KEY,"anonymous");
    }

    /**
     * get ftp.pwd
     * @return
     */
    public static String getFtpPwd(){
        return configuration.get(FTP_PWD_KEY,"");
    }

    public static String getFtpEncoding(){
        return configuration.get(FTP_ENCODING,"utf-8");
    }
    /**
     * get ftp.content.limit
     * @return
     */
    public static long getFtpContentLimit(){
        return configuration.getLong(FTP_CONTENT_LIMIT,-1L);
    }

    public static int getFtpPort(){
        return configuration.getInt(FTP_PROT,21);
    }
    /**
     * get ftp.data.store.type
     * @return
     */
    public static String getFtpStoreType(){
        return configuration.get(FTP_DATA_SORT_TYPE, "HDFS");
    }
    /**
     * get ftp.timeout   ;default 6000s
     * @return
     */
    public static int getFtpTimeOut(){
        return  configuration.getInt(FTP_TIMEOUT,6000);
    }

    /**
     * get ftp.server.timeout ;default 100000s
     * @return
     */
    public static int getFtpServerTimeOut(){
        return configuration.getInt(FTP_SERVER_TIMEOUT,100000);
    }

    /**
     * get ftp.keep.connection ;default false;
     * @return
     */
    public static boolean getFtpKeepConnection(){
        return configuration.getBoolean(FTP_KEEY_CONNECTION, false);
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
        System.out.println(FtpConfig.getFtpDirectoryExclude());
        System.out.println(FtpConfig.getFtpDirectoryExclude());
        System.out.println(FtpConfig.getFtpDirectoryExclude());
    }
}
