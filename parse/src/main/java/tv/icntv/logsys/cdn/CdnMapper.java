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


import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.utils.GenerateId;
import tv.icntv.logsys.xmlObj.XmlLog;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: wangl
 * Date: 13-10-22
 * Time: 下午5:12
 * To change this template use File | Settings | File Templates.
 */
public class CdnMapper extends Mapper<LongWritable,Text,Text,Text> {
    private static final String STB_PARSER_CLASSNAME="parser.log.config.className";
    private static final String STB_PARSER_DEFAULT_CLASS="tv.icntv.logsys.config.LogConfiguration";
    private static final String CDN_PARSER_CONFIGXML="cdn.parser.configXml";
    private String className=null;
    private String configXml=null;
    private String regular = "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s\\-\\s\\-\\s\\[([^]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s\"-\"\\s\"([^]]+)\"";
    Pattern pattern=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration=context.getConfiguration();
        className=configuration.get(STB_PARSER_CLASSNAME,STB_PARSER_DEFAULT_CLASS);
        configXml=configuration.get(CDN_PARSER_CONFIGXML,"ysten_log_mapping.xml");
        String reg=context.getConfiguration().get("cdn.log.regular",regular);
         pattern = Pattern.compile(reg);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = pattern.matcher(value.toString());
        String g3[],g6[];
        String keyTemp="";
        String valueTemp="";
        if(matcher.find()&& matcher.groupCount()==6){
            g3 = matcher.group(3).split(" ");
            g6 = matcher.group(6).split("#");
            if(null==g6 || g6.length==0 || null == g3 || g3.length==0 || g3.length<2){
                return;
            }
            if(!g6[0].matches("\\d*")||Strings.isNullOrEmpty(g6[0])){
                return;
            }
            keyTemp = g6[0]+g3[1];

            valueTemp = matcher.group(1)+"</>"+matcher.group(2)+"</>"+matcher.group(3)+"</>"+matcher.group(4)+"</>"+matcher.group(5)+"</>"+matcher.group(6);
            context.write(new Text(keyTemp), new Text(valueTemp));
        }

    }

    public XmlLog getConf() {
       return  LogConfigurationFactory.getLogConfigurableInstance(className,configXml).getConf();
    }

    public static void main(String[] args){
        System.out.println(Strings.isNullOrEmpty(""));
//
//        String str = "61.171.246.250 - - [18/Jan/2014:00:00:00 +0800] \"GET /media/new/2013/10/23/hd_dsj_jczm04_20131023.ts HTTP/1.1\" 206 1049126 \"-\" \"010233501205867#00000032AmlogicMDZ-05-201302261821793###Dec  6 2013,1\n" +
//                "6:41:03\"";
//
//        String reg = "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s\\-\\s\\-\\s\\[([^]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s\"-\"\\s\"([^]]+)\"";
//        Pattern pattern = Pattern.compile(reg);
//        Matcher matcher = pattern.matcher(str);
//        while(matcher.find()){
//            System.out.println("Group 0:"+matcher.group(0));
//            System.out.println("Group 1:"+matcher.group(1));
//            System.out.println("Group 2:"+matcher.group(2));
//            System.out.println("Group 3:"+matcher.group(3));
//            System.out.println("Group 4:"+matcher.group(4));
//            System.out.println("Group 5:"+matcher.group(5));
//            System.out.println("Group 6:"+matcher.group(6));
//        }
//
//        Matcher matcher = pattern.matcher(str.toString());
//        String g3[],g6[];
//        String keyTemp="";
//        String valueTemp="";
//        if(matcher.find()&& matcher.groupCount()==6){
//            g3 = matcher.group(3).split(" ");
//            g6 = matcher.group(6).split("#");
//            if(null==g6 || g6.length==0 || null == g3 || g3.length==0 || g3.length<2){
//                return;
//            }
//            if(!g6[0].matches("\\d*")){
//                return;
//            }
//            keyTemp = g6[0]+g3[1];
//
//            valueTemp = matcher.group(1)+"</>"+matcher.group(2)+"</>"+matcher.group(3)+"</>"+matcher.group(4)+"</>"+matcher.group(5)+"</>"+matcher.group(6);
////            context.write(new Text(keyTemp), new Text(valueTemp));
//        }
//        System.out.println(keyTemp);
//        System.out.println(valueTemp);
//        String[] arrA = a.split("#");
//        for(int i=0;i<arrA.length;i++){
//            System.out.println(arrA[i]);
//        }
//
//
//        DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.UK);
//        DateFormat formatCN = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.CHINA);
//        String rawTime = "23/Jul/2013:00:00:29 +0800";
//        try {
//            Date date = format.parse(rawTime);
//            System.out.println("format date: " + date);
//            System.out.println("format: " + formatCN.format(date));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
    }
}
