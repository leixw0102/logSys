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

import org.apache.commons.net.ftp.FTPFile;
import tv.icntv.log.crawl.filter.DirectoryFilter;
import tv.icntv.log.crawl.filter.FileFilter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-21
 * Time: 上午10:42
 * To change this template use File | Settings | File Templates.
 */
public interface FtpService<T> {
    //public boolean login(String strIp, int intPort);
    public boolean login(String strIp, int intPort, String user, String pwd);
    public void logOut();

    public  boolean downLoadDirectory(String localDirectoryPath,String remoteDirectory) ;
    public boolean downloadFile(FTPFile remoteFileName, String localDires,
                               String remoteDownLoadPath);
    public void logic(String strIp, int intPort,String user,String pwd,IFtpCallBack<T> callBack);

    public boolean downLoadDirectory(String localDirectoryPath,String remoteDirectory,DirectoryFilter directoryFilter,FileFilter fileFilter) ;
}
