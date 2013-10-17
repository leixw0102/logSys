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

package tv.icntv.logsys.stb;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.config.LogConfigurable;
import tv.icntv.logsys.config.LogConfiguration;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.mapper.IcntvHdfsNoWithReducer;

import javax.xml.parsers.FactoryConfigurationError;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-17
 * Time: 上午10:24
 * To change this template use File | Settings | File Templates.
 */
public class YstenMapper extends IcntvHdfsNoWithReducer<LongWritable,Text,ImmutableBytesWritable,Put> {
    @Override
    protected String[] getSplit(Object value) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void parser(String[] values, LogConfigurable configurable, Context context) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LogConfigurable getConf() {
        return LogConfigurationFactory.getLogConfigurableInstance("");  //To change body of implemented methods use File | Settings | File Templates.
    }

//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        if(null == value || null==value.toString()||"".equals(value.toString())){
//            return;
//        }
//        String[] arrRow = value.toString().split("</>");
//
//        String rowkey = arrRow[0] ;
//        Put put = new Put(Bytes.toBytes(rowkey)) ;
//        put.add(Bytes.toBytes("base"),Bytes.toBytes("log_id"),Bytes.toBytes(arrRow[0])) ;
//        put.add("base".getBytes(), "seq_id".getBytes(), arrRow[1].getBytes()) ;
//        context.write(new ImmutableBytesWritable(rowkey.getBytes()), put) ;
//    }


}
