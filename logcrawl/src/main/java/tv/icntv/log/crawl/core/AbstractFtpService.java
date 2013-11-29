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
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.conf.FtpConfig;
import tv.icntv.log.crawl.filter.DefaultDirectoryFilter;
import tv.icntv.log.crawl.filter.DefaultFileFilter;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-29
 * Time: 下午3:27
 */
public abstract class AbstractFtpService  implements FtpService<String>{
    protected Logger logger = LoggerFactory.getLogger(getClass());

    private FTPClient ftpClient;
    private String ip;
    private int port;
    private String user;
    private String pwd;
    private FtpConfig ftpConfig=null;

    private long BUFFER_SIZE = (2 << 20) * 10;

    protected AbstractFtpService(String ip, int port, FtpConfig ftpConfig) {
        this.ip = ip;
        this.port = port;
        this.ftpConfig = ftpConfig;
        this.user=ftpConfig.getFtpName();
        this.pwd=ftpConfig.getFtpPwd();
        this.ftpClient=new FTPClient();
    }

    public FTPClient getFtpClient() {
        return ftpClient;
    }

    public void setFtpClient(FTPClient ftpClient) {
        this.ftpClient = ftpClient;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

    @Override
    public boolean login(String strIp, int intPort, String user, String pwd) {

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
    @Override
    public boolean downLoadDirectory(String localDirectoryPath, String remoteDirectory) {
        return downLoadDirectory(localDirectoryPath,remoteDirectory,new DefaultDirectoryFilter(this.ftpConfig),new DefaultFileFilter(this.ftpConfig));
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
}
