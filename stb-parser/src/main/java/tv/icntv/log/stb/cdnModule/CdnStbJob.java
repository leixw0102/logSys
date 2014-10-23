package tv.icntv.log.stb.cdnModule;/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.log.stb.cdnserver.CdnServerJob;
import tv.icntv.log.stb.core.AbstractJob;
import tv.icntv.log.stb.login.ParserLoginMapper;

import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/23
 * Time: 15:54
 */
public class CdnStbJob extends AbstractJob {
    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = super.getConf();
        configuration.set("mapreduce.output.fileoutputformat.compress","true");
        configuration.set("mapreduce.output.fileoutputformat.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec","com.hadoop.compression.lzo.LzopCodec");
        Job cdn= Job.getInstance(configuration, "cdn stb job");
        cdn.setJarByClass(getClass());
        cdn.setMapperClass(CdnStbMapper.class);
        cdn.setOutputKeyClass(NullWritable.class);
        cdn.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(cdn, new Path(args[0]));
        FileOutputFormat.setOutputPath(cdn, new Path(args[1]));
        cdn.setNumReduceTasks(0);
        return cdn.waitForCompletion(true)?0:1;
    }

    @Override
    public boolean run(Map<String, String> maps) throws Exception {

        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration= new Configuration();
        int i=ToolRunner.run(configuration,new CdnStbJob(),args);
        System.exit(i);
    }
}
