package tv.icntv.logsys.uncompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: wangliang
 * Date: 13-10-21
 * Time: 下午5:40
 * To change this template use File | Settings | File Templates.
 */
public class HdfsStore {
    protected static Configuration configuration=new Configuration();
    protected static CompressionCodecFactory compressionCodecFactory=new CompressionCodecFactory(configuration);
//    protected static FileSystem fileSystem=null;
    private long BUFFER_SIZE=(2<<14);
//    static {
//        try {
//            fileSystem=FileSystem.get(configuration);
//            FilterFileSystem.
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }

    public void createFile(String file){
        if(null == file || file.equals("")){
            return;
        }
        FileSystem fileSystem=null;
        FSDataOutputStream out=null;
        try {
            Path path  = new Path(file);
            fileSystem=FileSystem.get(configuration);
            out=fileSystem.create(path);
            out.flush();
        }catch (Exception e){
            e.printStackTrace();
            return;
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

    public FileStatus[] getFiles(String file, PathFilter filter){
        FileSystem fileSystem=null;
        try{
            Path path = new Path(file);
            fileSystem=FileSystem.get(configuration);
            if(!fileSystem.exists(path)){
                return null;
            }
           return fileSystem.listStatus(path,filter);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }finally {
              if(null != fileSystem){
                  try {
                      fileSystem.close();
                  } catch (IOException e) {
                      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                  }
              }
        }
    }

    public void rename(String sourceName,String destName){
        FileSystem fileSystem=null;
        try {
           Path from =new Path(sourceName);
            Path to = new Path(destName);
            fileSystem=FileSystem.get(configuration);
           fileSystem.rename(from,to);
        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {
             if(null != fileSystem){
                 try {
                     fileSystem.close();
                 } catch (IOException e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
        }
    }

    public boolean isExist(String file){
        FileSystem fileSystem=null;
        try{
             Path path = new Path(file);
                fileSystem=FileSystem.get(configuration);
            return fileSystem.exists(path);
        }catch (Exception e){
            e.printStackTrace();
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

    public boolean uncompress(Path from,Path to){
        InputStream in=null;
        OutputStream out=null;
        FileSystem fileSystem=null;
        Decompressor compressor=null;
        try {
            fileSystem=FileSystem.get(configuration) ;
            CompressionCodec codec = compressionCodecFactory.getCodec(from);
            compressor=CodecPool.getDecompressor(codec);
            if(codec == null){
                return false;
            }
            in=codec.createInputStream(fileSystem.open(from),  compressor);
//            out= fileSystem.create(to);
            IOUtils.copyBytes(in,System.out, (int) BUFFER_SIZE);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
            CodecPool.returnDecompressor(compressor);
            try {
                if(null != fileSystem){
                fileSystem.close();
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        return true;
    }
    public boolean uncompress(String from, String to) {
        if(null == from || from.equals("") || null == to || to.equals("")){
            return false;
        }
          return uncompress(new Path(from),new Path(to));
    }

    public static void main(String[] args){
        HdfsStore hdfsStore = new HdfsStore();
        hdfsStore.uncompress("/icntv/log/chinacache/46294_20130713_w3c.gz", "/log/uncompress/46294_20130723_w3c");
//        FileStatus[] list= hdfsStore.getFiles("hdfs://slave102:8020/icntv/log/chinacache/", new PathFilter() {
//            @Override
//            public boolean accept(Path path) {
//                return path.getName().endsWith("writed");
//            }
//        });
//        for(FileStatus fileStatus:list){
//            System.out.println(fileStatus.getPath());
//        }
//        System.out.println(1024*1024);
//        System.out.println(2<<19);
    }
//     public static class ShutDownFileSystem extends Thread{
//        @Override
//        public void run() {
//            if(null == fileSystem ){
//                  return ;
//            }
//            try {
//                fileSystem.close();
//            } catch (IOException e) {
//                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//            }
//            //
//        }
//    }
}
