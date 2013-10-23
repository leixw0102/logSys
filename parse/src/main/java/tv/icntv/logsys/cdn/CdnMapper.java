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

package tv.icntv.logsys.cdn;


import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.mapper.IcntvHdfsNoWithReducer;
import tv.icntv.logsys.xmlObj.XmlLog;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wangl
 * Date: 13-10-22
 * Time: 下午5:12
 * To change this template use File | Settings | File Templates.
 */
public class CdnMapper extends IcntvHdfsNoWithReducer<LongWritable,Text,ImmutableBytesWritable,Put> {

    protected String[] getSplit(Object value) {
       Text test = (Text) value;
        return test.toString().split(getConf().getToken());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.

    }

    public void parser(String[] values, XmlLog xmlLog, Mapper.Context context) throws IOException, InterruptedException {

        String rowkey = values[0] ;
        Put put = new Put(Bytes.toBytes(rowkey)) ;
        String[][] rowConf = xmlLog.getLogToTableMaping();

        for(int i=0;i<rowConf.length;i++){
            int index = Integer.parseInt(rowConf[i][0]);
            if(index>values.length){
                break;
            }
            put.add(rowConf[i][1].getBytes(),rowConf[i][2].getBytes(),values[index-1].getBytes());
        }

        context.write(new ImmutableBytesWritable(rowkey.getBytes()), put) ;
    }

    public XmlLog getConf() {
       return  LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration","cdn_log_mapping.xml").getConf();
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
