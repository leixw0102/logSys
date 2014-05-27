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
package tv.icntv.log.tools;

import org.apache.hadoop.fs.Path;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/26
 * Time: 10:32
 */
public interface Api {
    public boolean writeDat(Path input,String regular,Path output);
    public boolean writeDat(Path intput ,Path output);
}
