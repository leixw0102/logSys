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

import com.google.common.collect.Lists;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.Text;
import tv.icntv.log.stb.commons.HadoopUtils;

import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 15:06
 */
public class CdnServerJob extends Configured implements Tool {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final String WRITED=".writed";

    @Override
    public int run(final String[] args) throws Exception {
        Configuration configuration = super.getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        Path input = new Path(args[0]);

        Job cdnServerJob=Job.getInstance(configuration,"统分二期CDN SERVER日志解析 "+args[0]);
        cdnServerJob.setJarByClass(this.getClass());
        if(HadoopUtils.isLzo(input)){
            logger.info("lzo suffix .....................");
            cdnServerJob.setInputFormatClass(LzoTextInputFormat.class);
        }
        cdnServerJob.setMapperClass(CdnServerParserMaper.class);
        cdnServerJob.setMapOutputKeyClass(Text.class);
        cdnServerJob.setMapOutputValueClass(Text.class);
        cdnServerJob.setReducerClass(CdnServerReducer.class);
        cdnServerJob.setOutputKeyClass(NullWritable.class);
        cdnServerJob.setOutputValueClass(Text.class);
        cdnServerJob.setCombinerClass(CdnServerCombiner.class);
//        cdnServerJob.setNumReduceTasks(7);
//        FileInputFormat.setInputPaths(cdnServerJob, inputs.toArray(new Path[inputs.size()]));
        FileInputFormat.setInputPaths(cdnServerJob,input);
        FileOutputFormat.setOutputPath(cdnServerJob,new Path(args[1]));
        return cdnServerJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();

        int result = ToolRunner.run(configuration, new CdnServerJob(), args);
        System.exit(result);
    }

}
