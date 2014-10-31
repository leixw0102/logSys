package tv.icntv.log.stb.cdnserver;/*
 * Copyright 2014 Future TV, Inc.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.icntv.tv/licenses/LICENSE-1.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.HadoopUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 15:06
 */
public class CdnServerJob extends Configured implements Tool {

    List<String> path = Lists.newArrayList("/icntv/log/cdn/backup.vod01/{0}/backup.vod01_{1}.lzo_deflate",
            "/icntv/log/cdn/hot.icntv/{0}/hot.icntv_{1}.lzo_deflate",
            "/icntv/log/cdn/hot.media/{0}/hot.media_{1}.lzo_deflate",
    "/icntv/log/cdn/mibox.vod01/{0}/mibox.vod01_{1}.lzo_deflate",
    "/icntv/log/cdn/payment.vod01/{0}/payment.vod01_{1}.lzo_deflate",
    "/icntv/log/cdn/vod01.icntvcdn/{0}/vod01.icntvcdn_{1}.lzo_deflate",
    "/icntv/log/cdn/vod01.media/{0}/vod01.media_{1}.lzo_deflate");
    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String WRITED=".writed";

    @Override
    public int run(final String[] args) throws Exception {
        Configuration configuration = super.getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
//        configuration.setLong("mapred.min.split.size",1024*1024*1024L);
        configuration.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
//        List<Path> inputs = Lists.newArrayList();
//        for(String str:path){
//            String fileName=MessageFormat.format(str,args[0],args[1]);
//            try {
//                    if(HadoopUtils.isExist(new Path(fileName+WRITED))){
//                        inputs.add(new Path(fileName));
//                    }
//                } catch (IOException e) {
//                    logger.error("null path={},exception "+e,fileName);
//                }
//        }

//        List<Path> inputs=Lists.transform(path,new Function<String, Path>() {
//            @Override
//            public Path apply( java.lang.String input) {
//                String fileName=MessageFormat.format(input,args[0],args[1]);
//                try {
//                    if(HadoopUtils.isExist(new Path(fileName+WRITED))){
//                       return new Path(fileName);
//                    }
//                    return null;
//                } catch (IOException e) {
//                    return null;
//                }
//            }
//        });
//        if(null == inputs || inputs.size()==0){
//            logger.error("input path null exception");
//            return 1;
//        }

        Job cdnServerJob=Job.getInstance(configuration,"统分二期CDN SERVER日志解析 ");
        cdnServerJob.setJarByClass(this.getClass());
        cdnServerJob.setMapperClass(CdnServerParserMaper.class);
        cdnServerJob.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        cdnServerJob.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
        cdnServerJob.setReducerClass(CdnServerReducer.class);
        cdnServerJob.setOutputKeyClass(NullWritable.class);
        cdnServerJob.setOutputValueClass(org.apache.hadoop.io.Text.class);
//        cdnServerJob.setNumReduceTasks(7);
//        FileInputFormat.setInputPaths(cdnServerJob, inputs.toArray(new Path[inputs.size()]));
        FileInputFormat.setInputPaths(cdnServerJob,new Path(args[0]));
        FileOutputFormat.setOutputPath(cdnServerJob,new Path(args[1]));
        return cdnServerJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();

        int result = ToolRunner.run(configuration, new CdnServerJob(), args);
        System.exit(result);
    }

}
