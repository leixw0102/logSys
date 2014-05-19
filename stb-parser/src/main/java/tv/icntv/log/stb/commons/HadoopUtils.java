package tv.icntv.log.stb.commons;/*
 * Copyright 2014 Future TV, Inc.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/19
 * Time: 09:44
 */
public class HadoopUtils {

    private static Configuration configuration = new Configuration();

    public static void mv(Path from,Path to) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mo
    }
}
