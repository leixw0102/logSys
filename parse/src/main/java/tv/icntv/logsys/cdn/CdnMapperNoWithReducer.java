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

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.mapper.IcntvHdfsNoWithReducer;
import tv.icntv.logsys.utils.GenerateId;
import tv.icntv.logsys.xmlObj.XmlLog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-1
 * Time: 上午11:58
 */
public class CdnMapperNoWithReducer extends IcntvHdfsNoWithReducer<LongWritable,Text,ImmutableBytesWritable,Put> {
    private static final String STB_PARSER_CLASSNAME="parser.log.config.className";
    private static final String STB_PARSER_DEFAULT_CLASS="tv.icntv.logsys.config.LogConfiguration";
    private static final String CDN_PARSER_CONFIGXML="cdn.parser.configXml";
    private String className=null;
    private String configXml=null;
    private String regular = "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s\\-\\s\\-\\s\\[([^]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s\"-\"\\s\"([^]]+)\"";

    @Override
    protected String[] getSplit(Object value) {
        return new String[]{((Text)value).toString()};  //To change body of implemented methods use File | Settings | File Templates.
    }
    Pattern pattern =null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration=context.getConfiguration();
        className=configuration.get(STB_PARSER_CLASSNAME,STB_PARSER_DEFAULT_CLASS);
        configXml=configuration.get(CDN_PARSER_CONFIGXML,"cdn_log_mapping.xml");
        String reg=context.getConfiguration().get("cdn.log.regular",regular);
        pattern=Pattern.compile(reg);
    }

    @Override
    public void parser(String[] values, XmlLog configurable, Context context) throws IOException, InterruptedException {

        Matcher matcher = pattern.matcher(values[0].toString());
        String g3[],g6[];
        String keyTemp="";
        String valueTemp="";
        String[][] rowConf = configurable.getLogToTableMaping();
        String[] arrResultRow = new String[rowConf.length];
        if(matcher.find()&& matcher.groupCount()==6){

            g3 = matcher.group(3).split(" ");
            g6 = matcher.group(6).split("#");
            if(null==g6[0]||g6[0].length()!=15){
                return;
            }

            keyTemp = g6[0]+g3[1]+ GenerateId.generateIdSuffix();
            Put put = new Put(keyTemp.getBytes(Charsets.UTF_8));
            arrResultRow[0] = matcher.group(1);//IP地址
            arrResultRow[1]=matcher.group(2);
            arrResultRow[2]="";
            arrResultRow[3]=matcher.group(3);
            arrResultRow[4]=matcher.group(4);
            arrResultRow[5]=matcher.group(5);
            arrResultRow[6]=matcher.group(6);

            for(int i=0;i<rowConf.length;i++){
                int index = Integer.parseInt(rowConf[i][0]);
                if(index>arrResultRow.length){
                    break;
                }
                put.add(rowConf[i][1].getBytes(),rowConf[i][2].getBytes(),arrResultRow[index-1].getBytes());
            }
//            valueTemp = matcher.group(1)+"</>"+matcher.group(2)+"</>"+matcher.group(3)+"</>"+matcher.group(4)+"</>"+matcher.group(5)+"</>"+matcher.group(6);
            context.write(new ImmutableBytesWritable(keyTemp.getBytes(Charsets.UTF_8)),put);
        }

    }

    @Override
    public XmlLog getConf() {
       return LogConfigurationFactory.getLogConfigurableInstance(className, configXml).getConf();
    }

//    public static void main(String[]args){
//        String str="114.239.105.99 - - [28/Oct/2013:00:00:06 +0800] \"GET http://mibox.icntv.cctvcdn.net/media/new/2013/09/12/hd_dsj_xbbl27_20130912.ts HTTP/1.1\" 206 1049019 \"-\" \"010133501572528#00000032AmlogicMDZ-05-201302261821793###Sep  2 2013,14:15:19\"";
//        String regular = "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s\\-\\s\\-\\s\\[([^]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s\"-\"\\s\"([^]]+)\"";
//       Pattern p= Pattern.compile(regular) ;
//       Matcher m= p.matcher(str);
//        if(m.find()){
//            System.out.println(m.group(6));
//            for(int i=0;i<=m.groupCount();i++){
//                System.out.println(m.group(i) +" \t"+ i);
//            }
//        }
//    }
}
