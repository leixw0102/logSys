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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.istack.Nullable;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.conf.FtpConfig;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-14
 * Time: 下午2:44
 */
public abstract class AbstractFileFilter implements FileFilter {

    protected FtpConfig ftpConfig;

    protected AbstractFileFilter(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }

    public abstract List<String> getFileRegular();
    private org.slf4j.Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    public boolean accept(String name) {
        name = Preconditions.checkNotNull(name);
        List<String> suffixs =getFileRegular();
        if (null == suffixs || suffixs.isEmpty()) {
            return false;
        }
        for (String suffix : suffixs) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    public FtpConfig getFtpConfig() {
        return ftpConfig;
    }

    public void setFtpConfig(FtpConfig ftpConfig) {
        this.ftpConfig = ftpConfig;
    }
}
