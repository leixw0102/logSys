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

import tv.icntv.log.crawl.conf.FtpConfig;

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
    private static FtpService ftpService = new FtpImpl();
    private String user;
    private String pwd;

    public FtpLogic(String strIp, int intPort) {
        this.strIp = strIp;
        this.intPort = intPort;

    }

    public FtpLogic(String strIp, int intPort, String user, String pwd) {
        this.strIp = strIp;
        this.intPort = intPort;
        this.user = user;
        this.pwd = pwd;
    }

    public FtpLogic(String strIp) {
        this.strIp = strIp;
    }

    public void src2Dest() {
        try {
            if (null == user || user.equals("") || null == pwd || pwd.equals("")) {
                if (!ftpService.login(this.strIp, this.intPort)) {
                    return;
                }
            } else {
                if (!ftpService.login(this.strIp, this.intPort, this.user, this.pwd)) {
                    return;
                }
            }
            ftpService.downLoadDirectory(FtpConfig.getFtpDstDirectory(), "/");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ftpService.logOut();
        }

    }
}
