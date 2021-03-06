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

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.crawl.thread.FtpDownThreadPools;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-11
 * Time: 下午1:28
 * To change this template use File | Settings | File Templates.
 */
public class HdfsDefaultStore implements FileStoreData {
    protected static Configuration configuration = new Configuration();
    private static Logger logger = LoggerFactory.getLogger(HdfsDefaultStore.class);

    @Override
    public boolean createDirectory(String directoryName) {
        if (isNull(directoryName)) {
            logger.info("directory path={} null", directoryName);
            return false;
        }
        FileSystem fileSystem=null;
        try {
            Path path= new Path(directoryName);
            fileSystem=FileSystem.get(configuration);
            if(!fileSystem.exists(path)) {
                logger.info("create directory {}", directoryName);
                return fileSystem.mkdirs(path);
            }
            logger.info("directory path={} exist",directoryName);
            return true;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } finally {
             if(null!=fileSystem){
                 try {
                     fileSystem.close();
                 } catch (IOException e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
        }
    }

    @Override
    public boolean isExist(String name) {
        if (isNull(name)) {
            return false;
        }
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            return fileSystem.exists(new Path(name));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } finally {
             if(null!=fileSystem){
                 try {
                     fileSystem.close();
                 } catch (IOException e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
        }
    }



    @Override
    public OutputStream getOutputStream(String writeFile) {
        if (isNull(writeFile)) {
            return null;
        }
        FileSystem fileSystem=null;

        try {
            fileSystem=FileSystem.get(configuration);
            return fileSystem.create(new Path(writeFile));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return null;
        }
    }


    @Override
    public void uncompress(String from, String to) {
       final HdfsDefaultStore store= new HdfsDefaultStore();
        for(int i=0;i<=4;i++){
            new Thread(new Runnable() {
                @Override
                public void run() {
                   System.out.println(store.isExist("test"));
                }
            }).start();
        }
//        InputStream in = null;
//        OutputStream out = null;
//        try {
//            FileSystem fileSystem = FileSystem.get(configuration);
//            Path fromPath = new Path(from);
//            Path toPath = new Path(to);
//            CompressionCodec codec = compressionCodecFactory.getCodec(fromPath);
//            if (codec == null) {
//
//                System.exit(1);
//            }
//            in = codec.createInputStream(fileSystem.open(fromPath));
//            out = fileSystem.create(toPath);
//            IOUtils.copyBytes(in, out, 1024 * 20);
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } finally {
//            IOUtils.closeStream(in);
//            IOUtils.closeStream(out);
//        }
    }

    @Override
    public void createFile(String name) {
        if (isNull(name)) {
            return ;
        }
        FSDataOutputStream out = null;
        FileSystem fileSystem=null;
        try {
            Path path =new Path(name);
            fileSystem=FileSystem.get(configuration);
            if(fileSystem.exists(path)){
                return;
            }
            out = fileSystem.create(path);
            out.flush();
            return ;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return ;
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

    @Override
    public boolean rename(String srcName, String name) {
        FSDataOutputStream out=null;
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            if (fileSystem.exists(new Path(srcName))) {
                return fileSystem.rename(new Path(srcName), new Path(name));
            }
            logger.info("try rename ,but name={} not exist,create file{} ", srcName,name);
             out =fileSystem.create(new Path(name));
            out.flush();
            return false;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            logger.error("rename error:", e);
            return false;
        } finally {
            if(null != out){
                IOUtils.closeStream(out);
            }
            if(null!=fileSystem){
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    @Override
    public void delete(String name) {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            if (fileSystem.exists(new Path(name))) {
                fileSystem.delete(new Path(name), true);
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            logger.error("rename error:", e);

        } finally {
             if(null!=fileSystem){
                 try {
                     fileSystem.close();
                 } catch (IOException e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
        }
    }

    @Override
    public long getSize(String file) {
        FileSystem fileSystem=null;
        if(null == file|| file.equals("")){
            return 0;
        }
        try {
             fileSystem=FileSystem.get(configuration);
            Path path=new Path(file);
            if(fileSystem.exists(path)){
                FileStatus fileStatus=fileSystem.getFileStatus(new Path(file));
                return fileStatus.getLen();
            }

        }   catch (Exception e){
            e.printStackTrace();
            return 0;
        }finally {
            if(null!=fileSystem){
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }

        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected boolean isNull(String name) {
        return null == name || name.equals("");
    }
    public static void main(String[]args){
        for(int i=0;i<3;i++){
        FtpDownThreadPools.getExecutorService().submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                HdfsDefaultStore store=new HdfsDefaultStore();
                System.out.println(store.isExist("test"));
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        }) ;
        }

    }
}
