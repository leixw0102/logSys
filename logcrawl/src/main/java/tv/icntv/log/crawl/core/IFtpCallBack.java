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

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-21
 * Time: 上午10:48
 * To change this template use File | Settings | File Templates.
 */
public interface IFtpCallBack<T> {
    public T call(FTPClient client);
}
