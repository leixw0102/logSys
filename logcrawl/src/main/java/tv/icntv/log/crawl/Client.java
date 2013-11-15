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
import tv.icntv.log.crawl.core.FtpImpl;
import tv.icntv.log.crawl.core.FtpLogic;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.URL;
import java.util.zip.GZIPInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-11
 * Time: 下午1:35
 * To change this template use File | Settings | File Templates.
 */
public class Client {
    public static void main(String[] args) throws IOException {
        //
        if(args.length!=3 && args.length!=5){
//            System.out.println("ftp crawl to hdfs.example : ftp://192.168.30.35 user pwd or ftp://192.168.30.35");
            System.out.println("ftp crawl to hdfs example : ftp://192.168.30.35 user pwd / ftp-crawl.xml or ftp://192.168.30.35 / ftp-crawl.xml");
            System.out.println("");
            return;
        }
        URL url = new URL(args[0]);
        FtpLogic ftp=null;

        if(args.length==3){
             ftp=new FtpLogic(url.getAuthority(),url.getPort(),args[2]);
            ftp.src2Dest(args[1]);
        }  else if(args.length==5){
             ftp=new FtpLogic(url.getAuthority(),url.getPort(),args[1],args[2],args[4]);
             ftp.src2Dest(args[3]);
        }



    }

}
