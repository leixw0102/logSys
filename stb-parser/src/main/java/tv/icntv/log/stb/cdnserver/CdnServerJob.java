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

    List<String> path = Lists.newArrayList("/icntv/log/cdn/backup.vod01/backup.vod01_{0}.lzo_deflate",
            "/icntv/log/cdn/hot.icntv/hot.icntv_{0}.lzo_deflate",
            "/icntv/log/cdn/hot.media/hot.media_{0}.lzo_deflate",
    "/icntv/log/cdn/mibox.vod01/mibox.vod01_{0}.lzo_deflate",
    "/icntv/log/cdn/payment.vod01/payment.vod01_{0}.lzo_deflate",
    "/icntv/log/cdn/vod01.icntvcdn/vod01.icntvcdn_{0}.lzo_deflate",
    "/icntv/log/cdn/vod01.media/vod01.media_{0}.lzo_deflate");
    @Override
    public int run(final String[] args) throws Exception {
        Configuration configuration = super.getConf();
        List<Path> inputs=Lists.transform(path,new Function<String, Path>() {
            @Override
            public Path apply( java.lang.String input) {
                return new Path(MessageFormat.format(input,args[0]));
            }
        });
        Path output = new Path(args[1]);
        Job cdnServerJob=Job.getInstance(configuration,"统分二期CDN SERVER日志解析 ");
        cdnServerJob.setJarByClass(this.getClass());
        cdnServerJob.setMapperClass(CdnServerMapper.class);
        cdnServerJob.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        cdnServerJob.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
        cdnServerJob.setReducerClass(CdnServerReducer.class);
        cdnServerJob.setOutputKeyClass(NullWritable.class);
        cdnServerJob.setOutputValueClass(org.apache.hadoop.io.Text.class);
        FileInputFormat.setInputPaths(cdnServerJob, inputs.toArray(new Path[inputs.size()]));
        FileOutputFormat.setOutputPath(cdnServerJob,output);
        return cdnServerJob.waitForCompletion(true)?0:1;
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration = new Configuration();
        int result = ToolRunner.run(configuration, new CdnServerJob(), args);
        System.exit(result);
    }

}
