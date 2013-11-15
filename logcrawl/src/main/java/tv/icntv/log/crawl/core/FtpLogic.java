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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.conf.Configuration;
import tv.icntv.log.crawl.conf.FtpConfig;
import tv.icntv.log.crawl.store.FileStoreData;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-21
 * Time: 上午10:53
 * To change this template use File | Settings | File Templates.
 */
public class FtpLogic {


    private String strIp;
    private int intPort;
    private FtpService ftpService = null;
    private String user;
    private String pwd;
    private FtpConfig configuration = null;
    private String directory = null;
    private List<String> suffixes = null;
    private String directorySplit = ",";
    private Logger logger = LoggerFactory.getLogger(getClass());

    public FtpLogic(String strIp, int intPort, String user, String pwd, String config) {
        this.strIp = strIp;
        this.intPort = intPort;
        configuration = new FtpConfig().init(config);
        this.user = Preconditions.checkNotNull(configuration.getFtpName());
        this.pwd = Preconditions.checkNotNull(configuration.getFtpPwd());
        ftpService = new FtpImpl(configuration);
        this.directory = configuration.getFtpDirectoryExclude();
        this.suffixes = configuration.getFileSuffixs();
    }

    public FtpLogic(String authority, int port, String config) {
        this(authority,port,null,null,config);
    }

    public void src2Dest(final String path) {
        try {

            if (!ftpService.login(this.strIp, this.intPort, this.user, this.pwd)) {
                return;
            }

            ftpService.downLoadDirectory(configuration.getFtpDstDirectory(), path);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ftpService.logOut();
        }

    }



}
