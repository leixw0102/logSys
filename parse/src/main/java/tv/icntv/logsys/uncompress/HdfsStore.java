package tv.icntv.logsys.uncompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    public void uncompress(String from, String to) {
        InputStream in=null;
        OutputStream out=null;
        try {
            FileSystem fileSystem=FileSystem.get(configuration);
            Path fromPath=new Path(from);
            Path toPath=new Path(to);
            CompressionCodec codec = compressionCodecFactory.getCodec(fromPath);
            if(codec == null){

                System.exit(1);
            }
            in=codec.createInputStream(fileSystem.open(fromPath));
            out= fileSystem.create(toPath);
            IOUtils.copyBytes(in,out,1024*20);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    public static void main(String[] args){
        HdfsStore hdfsStore = new HdfsStore();
        hdfsStore.uncompress("/log/46294_20130723_w3c.gz", "/log/uncompress/46294_20130723_w3c");
    }
}
