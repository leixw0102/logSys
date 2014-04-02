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

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-18
 * Time: 上午11:07
 */
public class CompressTransfer {
    private static final int BUFFER=1*1024*1024*1024;
    /**
     * 数据解压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void decompress(InputStream is, OutputStream os)
            throws Exception {

        GZIPInputStream gis = new GZIPInputStream(is);
        int count;
        byte data[] = new byte[BUFFER];
        while ((count = gis.read(data, 0, BUFFER)) != -1) {
            os.write(data, 0, count);
        }

        gis.close();
    }

    public static void main(String[]args) throws Exception {
        String file="e:\\46294_20131121_w3c.gz";
        String to = "e:\\46294_20131121_w3c.bz2";
        //decompress(new FileInputStream(file),System.out);
        long start = System.nanoTime();
        transfer(new FileInputStream(file),new FileOutputStream(to));
        System.out.print((System.nanoTime()-start)/Math.pow(10,9));
    }
    /**
     * 数据压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void compress(InputStream is, OutputStream os)
            throws Exception {

        BZip2CompressorOutputStream gos = new BZip2CompressorOutputStream(os);

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = is.read(data, 0, BUFFER)) != -1) {
            gos.write(data, 0, count);
        }

        gos.finish();

        gos.flush();
        gos.close();
    }
    /**
     * 数据解压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void bzdecompress(InputStream is, OutputStream os)
            throws Exception {

        BZip2CompressorInputStream gis = new BZip2CompressorInputStream(is);

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = gis.read(data, 0, BUFFER)) != -1) {
            os.write(data, 0, count);
        }

        gis.close();
    }

    public static void transfer(FileInputStream srcIs,FileOutputStream destOut) throws IOException {
         ByteArrayOutputStream out = new ByteArrayOutputStream();
        BZip2CompressorOutputStream gos = new BZip2CompressorOutputStream(destOut);
        int count;
        byte data[] = new byte[BUFFER];
        while ((count = srcIs.read(data, 0, BUFFER)) != -1) {
            //out.write(data, 0, count);
             gos.write(data,0,count);
        }
        gos.flush();
        gos.close();
    }


}