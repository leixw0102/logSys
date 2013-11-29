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
public class FtpImpl extends AbstractFtpService{

    public FtpImpl(String strIp, int intPort, FtpConfig configuration) {
        super(strIp,intPort,configuration);
    }

    /**
     * 下载文件
     *
     * @param remoteFileName     待下载文件名称
     * @param localDires         下载到当地那个路径下
     * @param remoteDownLoadPath remoteFileName所在的路径
     */
    public boolean downloadFile(FTPFile remoteFileName, String localDires,
                                String remoteDownLoadPath) {
        String strFileSuffix = (localDires.equals(File.separator) ? localDires : (localDires + File.separator));
        OutputStream outStream = null;
        boolean success = false;

        FileStoreData store = (FileStoreData) FtpConfig.SortTypeClass.valueOf(getFtpConfig().getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
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

            getFtpClient().changeWorkingDirectory(remoteDownLoadPath);
            outStream = store.getOutputStream(strFileSuffix + remoteFileName.getName());
            logger.info(remoteDownLoadPath + File.separator + remoteFileName.getName() + "开始下载....");
            success = getFtpClient().retrieveFile(remoteFileName.getName(), outStream);
            if (success == true) {
                logger.info(remoteDownLoadPath + File.separator + remoteFileName.getName() + "成功下载到" + strFileSuffix + remoteFileName.getName());
                store.rename(strFileSuffix + remoteFileName.getName() + ".writing", strFileSuffix + remoteFileName.getName() + ".writed");
                return success;
            }
        } catch (Exception e) {
            success = false;
            store.delete(strFileSuffix + remoteDownLoadPath + ".writing");
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
    public boolean downLoadDirectory(String localDirectoryPath, String remoteDirectory, DirectoryFilter directoryFilter, FileFilter fileFilter) {
        try {
            FTPFile[] allFile = getFtpClient().listFiles(remoteDirectory);
            for (int currentFile = 0; currentFile < allFile.length; currentFile++) {

                FTPFile file = allFile[currentFile];
                String name = file.getName();
                if (!file.isDirectory() ) {
                    if(getFileFilter().accept(name) ){
                        String prefix=null;
                        if(getFtpConfig().getFtpDstNameAppendLocal()){
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
                                login(getIp(),getPort(),getUser(),getPwd());
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

                    }else{
                        logger.error("file suffix {},not include",getFtpConfig().getFileSuffixs());
                    }
                } else {
                    //directory exclude logic
                    if (!getDirectoryFilter().accept(name)) {
                        logger.info("directory exclude name=" + remoteDirectory + " \t regular=" + getFtpConfig().getFtpDirectoryExclude());
                        continue;
                    }
                    // create directory if necessary
                    if (!localDirectoryPath.equals(File.separator)) {
                        FileStoreData store = (FileStoreData) FtpConfig.SortTypeClass.valueOf(getFtpConfig().getFtpStoreType().toUpperCase()).getFileStoreTypeClass();
                        if (!store.createDirectory(localDirectoryPath)) {
                            continue;
                        }
                    }
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
        return new DefaultDirectoryFilter(getFtpConfig());
    }
    protected FileFilter getFileFilter(){
        return new DefaultFileFilter(getFtpConfig());
    }



}
