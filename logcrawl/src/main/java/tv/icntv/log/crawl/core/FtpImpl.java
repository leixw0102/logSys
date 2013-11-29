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

import com.google.common.base.Preconditions;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.conf.FtpConfig;
import tv.icntv.log.crawl.filter.*;
import tv.icntv.log.crawl.filter.FileFilter;
import tv.icntv.log.crawl.store.FileStoreData;
import tv.icntv.log.crawl.thread.FtpDownThreadPools;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;



/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-11
 * Time: 上午11:17
 * To change this template use File | Settings | File Templates.
 */
public class FtpImpl implements FtpService<String> {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private FTPClient ftpClient;
    private FtpConfig ftpConfig=null;
    private long BUFFER_SIZE = (2 << 20) * 10;
    private String ip;
    private int port;
    private String user;
    private String pwd;

    public FtpImpl(FtpConfig configuration) {
          this.ftpConfig=configuration;
          this.ftpClient=new FTPClient();
    }


    @Override
    public boolean login(String strIp, int intPort, String user, String pwd) {
        this.ip = strIp;
        this.port = intPort;
        this.user = user;
        this.pwd = pwd;
        boolean isLogin = false;
        this.ftpClient.setControlEncoding(ftpConfig.getFtpEncoding());
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
            logger.info("恭喜" + ftpConfig.getFtpName() + "成功登陆FTP服务器");
            isLogin = true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(ftpConfig.getFtpName() + "登录FTP服务失败！" + e.getMessage());
        }
        this.ftpClient.setBufferSize((int) BUFFER_SIZE);
        this.ftpClient.setDataTimeout(this.ftpConfig.getFtpTimeOut());
        this.ftpClient.setConnectTimeout(ftpConfig.getFtpTimeOut());
        return isLogin;
    }




//    /**
//     * 下载文件
//     *
//     * @param remoteFileName     待下载文件名称
//     * @param localDires         下载到当地那个路径下
//     * @param remoteDownLoadPath remoteFileName所在的路径
//     */
    public boolean downloadFile(FTPFile remoteFileName, String localDires,
                                String remoteDownLoadPath) {
        String strFileSuffix = (localDires.equals(File.separator) ? localDires : (localDires + File.separator));
        OutputStream outStream = null;
        boolean success = false;
        //System.out.println(remoteDownLoadPath+ File.separator+remoteFileName);
        FileStoreData store = (FileStoreData) FtpConfig.SortTypeClass.valueOf(this.ftpConfig.getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
        try {
            String file = strFileSuffix + remoteFileName.getName();
            if (store.isExist(file) && store.isExist(file + ".writed")) {
                if (remoteFileName.getSize() != store.getSize(file)) {
                    logger.info("file={} is problem,", file);
                    store.delete(file);
                    store.delete(file + ".writed");
                    logger.info("delete file {}, {}", file, file + ".writed");
                } else {
                    logger.info("file {} is down load", strFileSuffix + remoteFileName.getName());
                    return true;
                }
            }
            if (!store.isExist(strFileSuffix + remoteFileName.getName() + ".writing")) {
                logger.info("create file writing....");
                store.createFile(strFileSuffix + remoteFileName.getName() + ".writing");
            }

            this.ftpClient.changeWorkingDirectory(remoteDownLoadPath);
            outStream = store.getOutputStream(strFileSuffix + remoteFileName.getName());
            logger.info(remoteDownLoadPath + File.separator + remoteFileName.getName() + "开始下载....");
            success = this.ftpClient.retrieveFile(remoteFileName.getName(), outStream);
            if (success == true) {
                logger.info(remoteDownLoadPath + File.separator + remoteFileName.getName() + "成功下载到" + strFileSuffix + remoteFileName.getName());
                store.rename(strFileSuffix + remoteFileName.getName() + ".writing", strFileSuffix + remoteFileName.getName() + ".writed");
                return success;
            }
        } catch (Exception e) {
            success = false;
            store.delete(strFileSuffix + remoteDownLoadPath + ".writing");
//            store.delete(strFileSuffix+remoteFileName);
            logger.error(remoteFileName.getName() + "下载失败", e);
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
    public void logic(String strIp, int intPort,String user,String pwd, IFtpCallBack<String> callBack) {
        try {
            if (!login(strIp, intPort,user,pwd)) {
                return;
            }
           String downFile= callBack.call(this.ftpClient);
            //启动线程下载
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("ftp error! ",e);
        } finally {
            this.logOut();
        }
    }

    @Override
    public boolean downLoadDirectory(String localDirectoryPath, String remoteDirectory, DirectoryFilter directoryFilter, FileFilter fileFilter) {
        try {
            FTPFile[] allFile = this.ftpClient.listFiles(remoteDirectory);
            for (int currentFile = 0; currentFile < allFile.length; currentFile++) {

                FTPFile file = allFile[currentFile];
                String name = file.getName();
                if (!file.isDirectory() ) {
                    if(getFileFilter().accept(name) ){
                        String prefix=null;
                        if(ftpConfig.getFtpDstNameAppendLocal()){
                            //downloadFile(file, localDirectoryPath + remoteDirectory, remoteDirectory);
                            //FtpDownThreadPools.getExecutorService().submit(new DownLoad())
                            prefix=localDirectoryPath+remoteDirectory;
                        }else{
                            prefix=localDirectoryPath;
                        }
                        boolean success = false;
                        success = downloadFile(file,prefix,remoteDirectory);
                        int downloadCnt = 1;

                        for(int i=0;i<5;i++ ){
                            if(success == false){
                                logger.warn("下载重试!");
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                login(this.ip,this.port,this.user,this.pwd);
                                success = downloadFile(file,prefix,remoteDirectory);
                                downloadCnt+= 1;
                            }else{
                                break;
                            }
                        }
                        if(downloadCnt > 1 && success==true){
                            logger.warn("文件下载不稳定,下载了 {} 次",downloadCnt);
                        }else if(downloadCnt > 1 && success==false){
                            logger.error("文件下载失败,下载了 {} 次",downloadCnt);
                        }

//                        FtpDownThreadPools.getExecutorService().submit(new DownLoad(file,prefix,remoteDirectory));
                    }else{
                        logger.error("file suffix {},not include",ftpConfig.getFileSuffixs());
                    }
                } else {
                    //directory exclude logic
                    if (!getDirectoryFilter().accept(name)) {
                        logger.info("directory exclude name=" + remoteDirectory + " \t regular=" + ftpConfig.getFtpDirectoryExclude());
                        continue;
                    }
                    // create directory if necessary
//                    if (!localDirectoryPath.equals(File.separator)) {
//                        FileStoreData store = (FileStoreData) FtpConfig.SortTypeClass.valueOf(ftpConfig.getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
//                        if (!store.createDirectory(localDirectoryPath)) {
//                            continue;
//                        }
//                    }
                    downLoadDirectory(localDirectoryPath, (remoteDirectory.equals("/") ? remoteDirectory : (remoteDirectory + File.separator)) + name,directoryFilter,fileFilter);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("下载文件夹失败");
            return false;
        }
        return true;
    }

    protected DirectoryFilter getDirectoryFilter(){
        return new DefaultDirectoryFilter(this.ftpConfig);
    }
    protected FileFilter getFileFilter(){
        return new DefaultFileFilter(this.ftpConfig);
    }
    @Override
    public boolean downLoadDirectory(String localDirectoryPath, String remoteDirectory) {
        return downLoadDirectory(localDirectoryPath,remoteDirectory,new DefaultDirectoryFilter(this.ftpConfig),new DefaultFileFilter(this.ftpConfig));
    }

    public FtpImpl() {
        ftpClient = new FTPClient();
    }



    /**
     * ftp logout
     */
    public void logOut() {
        if (null != this.ftpClient && this.ftpClient.isConnected()) {
            try {
                boolean reuslt = this.ftpClient.logout();
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

    class DownLoad implements Runnable {
        private FTPFile name;
        private String srcPath;
        private String remoteDirectory;

        public DownLoad(FTPFile name, String srcPath, String remoteDirectory) {
            //To change body of created methods use File | Settings | File Templates.
            this.name = name;
            this.srcPath = srcPath;
            this.remoteDirectory = remoteDirectory;
        }

        @Override
        public void run() {
//            String strFileSuffix = (srcPath.equals(File.separator) ? srcPath : (srcPath + File.separator));
//        OutputStream outStream = null;
//        boolean success = false;
//        //System.out.println(remoteDownLoadPath+ File.separator+remoteFileName);
//        FileStoreData store = (FileStoreData) FtpConfig.SortTypeClass.valueOf(ftpConfig.getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
//        try {
//            String file = strFileSuffix + name.getName();
//            if (store.isExist(file) && store.isExist(file + ".writed")) {
//                if (name.getSize() != store.getSize(file)) {
//                    logger.info("file={} is problem,", file);
//                    store.delete(file);
//                    store.delete(file + ".writed");
//                    logger.info("delete file {}, {}", file, file + ".writed");
//                } else {
//                    logger.info("file {} is down load", strFileSuffix + name.getName());
//                    return ;
//                }
//            }
//            if (!store.isExist(strFileSuffix + name.getName() + ".writing")) {
//                logger.info("create file writing....");
//                store.createFile(strFileSuffix + name.getName() + ".writing");
//            }
//
//            ftpClient.changeWorkingDirectory(remoteDirectory);
//            outStream = store.getOutputStream(strFileSuffix + name.getName());
//            logger.info(remoteDirectory + File.separator + name.getName() + "开始下载....");
//            success = ftpClient.retrieveFile(name.getName(), outStream);
//            if (success == true) {
//                logger.info(remoteDirectory + File.separator + name.getName() + "成功下载到" + strFileSuffix + name.getName());
//                store.rename(strFileSuffix + name.getName() + ".writing", strFileSuffix + name.getName() + ".writed");
//            }
//        } catch (Exception e) {
//            store.delete(strFileSuffix + remoteDirectory + ".writing");
////            store.delete(strFileSuffix+remoteFileName);
//            logger.error(name.getName() + "下载失败", e);
//        } finally {
//            if (null != outStream) {
//                try {
//                    outStream.flush();
//                    outStream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return success;
        }
    }
//    public static void main(String[] args){
//        FtpConfig configuration = new FtpConfig().init("ftp-stb-crawl.xml");
//        FtpService ftpService = new FtpImpl(configuration);
//        try {
//            //ftp://172.16.2.60 / ftp-stb-crawl.xml
//            URL url = new URL("ftp://172.16.2.60");
//            boolean loginFlag = ftpService.login("192.168.30.35",url.getPort(),"cdnlog","CdNLoG");
//            int loginCnt=0;
//            for(int i=0;i<5;i++){
//
//                if(loginFlag==false){
//                    try {
//                        Thread.sleep(5000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    loginFlag = ftpService.login("172.16.2.60",url.getPort(),"icntv_log","icntvlog!@#");
//                    loginCnt = i+1;
//                }else{
//                    System.out.println("登陆成功!");
//                    break;
//                }
//            }
//            if(loginFlag==true&&loginCnt>1){
//                System.out.println("登陆尝试"+loginCnt+"次成功,网络可能不稳定!");
//            }else if(loginFlag==false){
//                System.out.println("登陆尝试"+loginCnt+"次失败,请检查网络连通性!");
//            }
//        } catch (MalformedURLException e) {
//            //e.printStackTrace();
//        }
//        ftpService.downLoadDirectory(configuration.getFtpDstDirectory(), "/");
//
//    }

}
