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

package tv.icntv.log.crawl.thread;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-23
 * Time: 上午11:29
 * To change this template use File | Settings | File Templates.
 */
public class FtpDownThreadPools {

    private   static ExecutorService executorService=Executors.newCachedThreadPool();

    public static ExecutorService getExecutorService(){
        return executorService;
    }
    public static void main(String[]args) throws IOException {
        URL url = new URL("http://www.baidu.com");
        URLConnection urlConnection=url.openConnection();
        BufferedReader
        br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(),"gbk"));
        String line=null;
        while ((line=br.readLine())!=null){
//            System.out.println(line);
//            String str=new String(line.getBytes("gbk"),"iso-8859-1");
            System.out.println(new String(line.getBytes("gbk"),"utf-8"));
        }
        br.close();
    }
}
