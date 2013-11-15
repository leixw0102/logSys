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

package tv.icntv.log.crawl.filter;

import com.google.common.base.Preconditions;
import tv.icntv.log.crawl.conf.FtpConfig;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-14
 * Time: 下午2:59
 */
public abstract class AbstractDirectoryFilter implements DirectoryFilter  {
    protected FtpConfig ftpConfig;
    public AbstractDirectoryFilter(FtpConfig ftpConfig) {
        this.ftpConfig=ftpConfig;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

    public abstract String[] getFilterRegular();
    @Override
    public boolean accept(String name){
       name= Preconditions.checkNotNull(name);

        String [] direcotrys= getFilterRegular();
        if(null == direcotrys|| direcotrys.length==0){
            return true;
        }
        for(String direcotry : direcotrys){
            if(null == direcotry || direcotry.equals("")){
                return true;
            }
            if(name.contains(direcotry)){
                return false;
            }
        }
        return true;
    }
}
