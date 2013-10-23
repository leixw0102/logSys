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

package tv.icntv.log.crawl.store;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-21
 * Time: 下午1:59
 * To change this template use File | Settings | File Templates.
 */
public interface FileStoreData extends StoreData{
    /**
     *     create directory from store
     * @param directoryName
     * @return
     */
    public boolean createDirectory(String directoryName);

    public boolean isExist(String name);

    public OutputStream getOutputStream(String writeFile) throws IOException;

    public void uncompress(String from,String to);

    public void createFile(String name);

    public boolean rename(String srcName,String name);

    public void delete(String name);
}