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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/19
 * Time: 09:44
 */
public class HadoopUtils {

    private static Configuration configuration = new Configuration();
    private static Logger logger = LoggerFactory.getLogger(HadoopUtils.class);

    public static Path[] createFile(Path from,Path to ,PathFilter filter,String fromSuffix,String toSuffix,String parsed) throws IOException {
        FileSystem fileSystem = null;
        try{
            fileSystem=FileSystem.get(configuration);
            Path[] paths=FileUtil.stat2Paths(fileSystem.listStatus(from, filter));
            List<Path> inputs=Lists.newArrayList();
            for(Path path:paths){
                //file name
                String name=path.getName().replace(fromSuffix,"");

                if(isExist(new Path(to,name.concat(parsed)))){
                    continue;
                }

                if(createFile(new Path(to,name.concat(toSuffix)))){
                    inputs.add(new Path(from,name));
                };
            }
            return inputs.toArray(new Path[inputs.size()]);
        }catch(IOException e){
            e.printStackTrace();
            return null;
        }finally{
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
    public static boolean isExist(Path path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            return fileSystem.exists(path);
        } catch ( Exception e){
            return false;
        }finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
//    public static boolean isExist(Path ...paths) throws IOException {
//        if(null == paths || paths.length==0){
//            return false;
//        }
//        for(Path path : paths){
//            if(!isExist(path)){
//                return false;
//            }
//        }
//        return true;
//    }

    public static   boolean createFile(Path path){
        FSDataOutputStream out = null;
        FileSystem fileSystem=null;
        try {

            fileSystem=FileSystem.get(configuration);
            if(fileSystem.exists(path)){
                logger.info("file {} existed",path.toString());
                return false;
            }
            out = fileSystem.create(path);
            out.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            logger.error("create File error!");
            return false;
        } finally {

            IOUtils.closeStream(out);
            if(null!=fileSystem){
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public void createFile(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return ;
        }
        createFile(new Path(name));
    }




    /**
     * mv
     * @param from
     * @param to
     * @throws IOException
     */
    public static void rename(Path from,Path to) throws IOException {
        FileSystem fileSystem = null;
        try{
            fileSystem=FileSystem.get(configuration);
            fileSystem.rename(from,to);
        }catch(IOException e){
             e.printStackTrace();
        }finally{
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
    public static void main(String[]args) throws IOException {
        System.out.print(args.length+"\t"+args[0]+"\t"+args[1]);
        // mv(new Path(args[0]),new Path(args[1]));
    }

//    public static void writeData(Path input,final String inputRegular,Path dat){
//        FileSystem fileSystem=null;
//        LzopCodec lzopInputStream=new LzopCodec();
//        BufferedReader reader=null;
//        FSDataOutputStream outputStream=null;
//        try{
//            fileSystem=FileSystem.get(configuration);
//            FileStatus[] fileStatuses=fileSystem.listStatus(input,new PathFilter() {
//                @Override
//                public boolean accept(Path path) {
//                    return path.toString().matches(inputRegular);  //To change body of implemented methods use File | Settings | File Templates.
//                }
//            });
//            if(null == fileStatuses||fileStatuses.length==0){
//                return;
//            }
//            outputStream=fileSystem.create(dat);
//            for(FileStatus status:fileStatuses){
//                reader=new BufferedReader(new InputStreamReader(lzopInputStream.createInputStream(fileSystem.open(status.getPath())),"utf-8"));
//                String line=null;
//                while(null != (line=reader.readLine())){
//                    outputStream.writeUTF(line);
//                }
//            }
//        }catch (IOException e){
//            e.printStackTrace();
//        }finally {
//            IOUtils.closeStream(reader);
//            IOUtils.closeStream(outputStream);
//            IOUtils.closeStream(fileSystem);
//        }
//    }
}
