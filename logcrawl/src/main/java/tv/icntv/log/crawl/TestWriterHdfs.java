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

package tv.icntv.log.crawl;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-15
 * Time: 下午1:05
 * To change this template use File | Settings | File Templates.
 */
public class TestWriterHdfs {

    public  static void main(String[] args) throws IOException, ClassNotFoundException {
        test1();
    }
    public static void test1() throws ClassNotFoundException, IOException {
        String codecClassName="org.apache.hadoop.io.compression.GzipCodec";
        Class<?> codecClass=Class.forName(codecClassName);
        Configuration config = new Configuration();
        CompressionCodec codec= (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
        CompressionOutputStream out=codec.createOutputStream(System.out);
        IOUtils.copyBytes(new FileInputStream(new File("d:\\11.txt")), System.out, 4096, false);
        out.close();
    }
    public static void test2() throws ClassNotFoundException, IOException {
        String codecClassName="org.apache.hadoop.io.compression.GzipCodec";
        Class<?> codecClass=Class.forName(codecClassName);
        Configuration config = new Configuration();
        CompressionCodec codec= (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
        CompressionOutputStream out=codec.createOutputStream(System.out);
        IOUtils.copyBytes(new FileInputStream(new File("d:\\11.txt")), System.out, 4096, false);
        out.close();
    }
}