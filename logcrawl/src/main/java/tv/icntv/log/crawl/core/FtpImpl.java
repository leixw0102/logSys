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

package tv.icntv.log.crawl.core;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.conf.FtpConfig;
import tv.icntv.log.crawl.store.FileStoreData;

import java.io.*;


/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-11
 * Time: 上午11:17
 * To change this template use File | Settings | File Templates.
 */
public class FtpImpl implements FtpService{
    private Logger logger = LoggerFactory.getLogger(getClass());
    private FTPClient ftpClient;
    private String directorySplit=",";
    private void check(){
        //TODO
        ;
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }


    /**
     * ftp user login
     * @return
     */
    public boolean login(String strIp, int intPort){
       return login(strIp, intPort,FtpConfig.getFtpName(),FtpConfig.getFtpPwd());
    }

    @Override
    public boolean login(String strIp, int intPort, String user, String pwd) {
        boolean isLogin = false;
        this.ftpClient.setControlEncoding(FtpConfig.getFtpEncoding());
        try {
            if (intPort > 0) {
                this.ftpClient.connect(strIp, intPort);
            } else {
                this.ftpClient.connect(strIp);
            }

            // FTP服务器连接回答
            int reply = this.ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                this.ftpClient.disconnect();
                logger.error("登录FTP服务失败！");
                return isLogin;
            }
            this.ftpClient.login(user, pwd);
            // 设置传输协议
            this.ftpClient.enterLocalPassiveMode();
            this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            logger.info("恭喜" + FtpConfig.getFtpName() + "成功登陆FTP服务器");
            isLogin = true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(FtpConfig.getFtpName() + "登录FTP服务失败！" + e.getMessage());
        }
        this.ftpClient.setBufferSize(1024 * 2);
        this.ftpClient.setDataTimeout(FtpConfig.getFtpTimeOut());
        this.ftpClient.setConnectTimeout(FtpConfig.getFtpTimeOut());
        return isLogin;
    }


    /**
     * 判断name是不是不包含
     * @param name
     * @return
     */
     private boolean excludeDirectory(String name){
         String directory=FtpConfig.getFtpDirectoryExclude();
         if(null == directory||directory.equals("")){
             return true;
         }
         String [] dirs=directory.split(directorySplit);
         for(String dir:dirs){

             if(matcher(name,dir)){
                 return false;
             }
         }
         return true;
     }

    /**
     *
     * @param localDirectoryPath
     * @param remoteDirectory
     * @return
     */
    public boolean downLoadDirectory(String localDirectoryPath,String remoteDirectory) {
        try {
            FTPFile[] allFile = this.ftpClient.listFiles(remoteDirectory);
            for (int currentFile = 0; currentFile < allFile.length; currentFile++) {
                FTPFile file=allFile[currentFile];
                String name = file.getName();
                if (!file.isDirectory()) {
                    downloadFile(name, localDirectoryPath+remoteDirectory, remoteDirectory);
                } else{
                    //directory exclude logic
                    if(!excludeDirectory(name)){
                        logger.info("directory exclude name="+remoteDirectory+" \t regular="+FtpConfig.getFtpDirectoryExclude());
                        continue;
                    }
                    // create directory if necessary
                    System.out.println(localDirectoryPath);
                    if(!localDirectoryPath.equals(File.separator)){
                        FileStoreData store= (FileStoreData) FtpConfig.SortTypeClass.valueOf(FtpConfig.getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
                        if(!store.createDirectory(localDirectoryPath)){
                           continue;
                        }
                    }
                      downLoadDirectory(localDirectoryPath, (remoteDirectory.equals("/")?remoteDirectory:(remoteDirectory+File.separator))+name);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("下载文件夹失败");
            return false;
        }
        return true;
    }
    /***
     * 下载文件
     * @param remoteFileName   待下载文件名称
     * @param localDires 下载到当地那个路径下
     * @param remoteDownLoadPath remoteFileName所在的路径
     * */

    public boolean downloadFile(String remoteFileName, String localDires,
                                String remoteDownLoadPath) {
        String strFileSuffix = (localDires.equals(File.separator)?localDires:(localDires +File.separator));
        OutputStream outStream = null;
        boolean success = false;
        FileStoreData store= (FileStoreData) FtpConfig.SortTypeClass.valueOf(FtpConfig.getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
        try {
            if(store.isExist(strFileSuffix+remoteFileName)&&store.isExist(strFileSuffix+remoteFileName+".writed")){
                logger.info("file {} is down load",strFileSuffix+remoteFileName);
                return true;
            }
            store.createFile(strFileSuffix+remoteFileName+".writing");
            this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
            outStream = store.getOutputStream(strFileSuffix+remoteFileName);
            logger.info(remoteDownLoadPath+ File.separator+remoteFileName + "开始下载....");
            success = this.ftpClient.retrieveFile(remoteFileName, outStream);
            if (success == true) {
                logger.info(remoteDownLoadPath+ File.separator+remoteFileName  + "成功下载到" + strFileSuffix+remoteFileName);
                store.rename(strFileSuffix + remoteFileName + ".writing", strFileSuffix + remoteFileName + ".writed");
                return success;
            }
        } catch (Exception e) {
            store.delete(strFileSuffix+remoteDownLoadPath+".writing");
            store.delete(strFileSuffix+remoteFileName);
            logger.error(remoteFileName + "下载失败");
        } finally {
            if (null != outStream) {
                try {
                    outStream.flush();
                    outStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return success;
    }

    @Override
    public void logic(String strIp, int intPort,IFtpCallBack callBack) {
        try{
            if(!login(strIp,intPort)){
                return;
            }
            callBack.call();
        }catch(Exception e){
            e.printStackTrace();
        } finally {
            this.logOut();
        }
    }

    @Override
    public boolean matcher(String name, String regular) {
       if(null == regular || regular.equals("") || null ==name || name.equals("")){
           return true;
       }
       return name.contains(regular);
    }

    public FtpImpl() {
        ftpClient=new FTPClient();
    }

    /**
     *
     */
//    public void src2Dest(){
//        try{
//       if(!login(String strIp, int intPort) ){
//           return;
//       }
//        //TODO DIRECTORY
//            downLoadDirectory("d:\\test","/");
//        }catch(Exception e){
//            logger.error("ftp error!",e);
//        } finally {
//            logOut();
//        }
//    }

    /**
     * ftp logout
     */
    public void logOut() {
        if (null != this.ftpClient && this.ftpClient.isConnected()) {
            try {
                boolean reuslt = false;// 退出FTP服务器

                reuslt = this.ftpClient.logout();

                if (reuslt) {
                    logger.info("成功退出服务器");
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.warn("退出FTP服务器异常！" + e.getMessage());
            } finally {
                try {
                    this.ftpClient.disconnect();// 关闭FTP服务器的连接
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.warn("关闭FTP服务器的连接异常！");
                }
            }
        }
    }
}
