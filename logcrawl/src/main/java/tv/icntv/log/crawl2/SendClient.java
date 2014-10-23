package tv.icntv.log.crawl2;/*
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

import com.google.common.base.Splitter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl2.compression.LzoCompressImpl;
import tv.icntv.log.crawl2.decompression.GzUnCompressImpl;
import tv.icntv.log.crawl2.push.HdfsDispatcher;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/04/04
 * Time: 14:09
 */
public class SendClient {
    private static String lzoDirectory = "/data/hadoop/lzoData";
    private static Logger logger= LoggerFactory.getLogger(SendClient.class);
    private static String suffix=".lzo_deflate";
    private static void help(){
        System.out.println("-s \t input source file");
        System.out.println("-r \t from gz to lzoPath");
        System.out.println("-t \t to hdfs url");
    }

    /**
     * 0--source  ;example /data/chinacache/2014-03-03/abc.gz
     * 1--lzoPath ;example /data/lzo/chinacache/2014-03-03/
     * 1--hdfs url ;example hdfs://uri/
     *
     * @param args
     */
    public static void main(String[] args) throws ParseException {

        if(args==null || args.length==0){
            help();
            return;
        }
        CommandLineParser parser = new PosixParser();
        CommandLine line =parser.parse(init(), args);
        if(line.hasOption("h")){
            help();
            return;
        }
        String source ="";
        String fileName="";
        if(line.hasOption("s")){
            source = line.getOptionValue("s");
            List<String> sources=Splitter.on(File.separator).splitToList(source);
            fileName=sources.get(sources.size()-1);
        }else{
            System.out.println("source null");
            return;
        }
        String lzoFile="";
        String lzoFileName="";
        if(line.hasOption("r")){
            String lzoPath=line.getOptionValue("r",lzoDirectory);
            lzoFileName=fileName.substring(0,fileName.lastIndexOf("."))+suffix;
            lzoFile=lzoPath+File.separator+ lzoFileName;
        }
        String hdfsUrl="";
        if(line.hasOption("t")){
            hdfsUrl=line.getOptionValue("t");
        } else{
            System.out.println("target url null");
            return;
        }

//        logger.info("source = "+source+"\r\n lzo="+lzoFile+"\r\n target = "+hdfsUrl);

        File file = new File(source);
        if (!file.exists()) {
            System.out.println("file =" + source + " not exist!");
            return;
        }

        //reCompress from gz to lzo
        long start=System.nanoTime();
         ReCompress reCompress = new Gz2LzoReCompress(source,lzoFile,new LzoCompressImpl(),new GzUnCompressImpl());
        try {
            reCompress.reCompress();
        } catch (IOException e) {
            System.out.println(e);
            return;

        }
        logger.info("use time = "+(System.nanoTime()-start)/Math.pow(10,9));
//        logger.info("source size = "+file.length()/1024/1024/1024+" G; lzo recompress size " + new File(lzoFile).length()/2014/1024/1024+" G");
        //get target
        HdfsDispatcher hdfsDispatcher = new HdfsDispatcher(lzoFile, hdfsUrl+File.separator+lzoFileName);
        hdfsDispatcher.send();
    }

    static Options init() {
        Options options = new Options();
        options.addOption("h", false, "help me");
        options.addOption("s", "sourceFile", true, "input source file");
        options.addOption("r", "recompressPath", true, "from gz to lzoPath");
        options.addOption("t", "targetHdfsUrl", true, "to hdfs url");
        return options;
    }
}
