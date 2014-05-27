package tv.icntv.log.tools;/*
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

import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/26
 * Time: 10:35
 */
public class FileApi implements Api {
    private static Configuration conf=new Configuration();
    static LzopCodec lzopInputStream=new LzopCodec();
    static {

        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        lzopInputStream.setConf(conf);
    }

    @Override
    public synchronized boolean writeDat(Path input, final String regular, Path output) {
        FileSystem fileSystem=null;
        BufferedReader reader=null;
        FSDataOutputStream outputStream=null;
        try{
            fileSystem=FileSystem.get(conf);
            ;
            FileStatus[] fileStatuses=fileSystem.listStatus(input,new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().matches(regular);  //To change body of implemented methods use File | Settings | File Templates.
                }
            });
            if(null == fileStatuses||fileStatuses.length==0){
                System.out.println("null...");
                return false;
            }
            System.out.println(fileStatuses.length);
            outputStream=fileSystem.create(output,true,40960);
            for(FileStatus status:fileStatuses){
                reader=new BufferedReader(new InputStreamReader(lzopInputStream.createInputStream(fileSystem.open(status.getPath())),"utf-8"));
                String line=null;
                while(null != (line=reader.readLine())){
                    byte[] lineByte=(line+"\r\n").getBytes("utf-8");
                    outputStream.write(lineByte,0,lineByte.length);
                }
            }
        }catch (IOException e){
            System.out.println(e);
            e.printStackTrace();
            return false;
        }finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(fileSystem);
        }

        return true;
    }

    @Override
    public synchronized boolean writeDat(Path intput, Path output) {
        //To change body of implemented methods use File | Settings | File Templates.
        return writeDat(intput,"part-m-\\d*.lzo",output);
    }
    public static void main(String args[]){
        String str="part-r-\\d*";
        System.out.println("part-r-00000".matches(str));
    }
}
