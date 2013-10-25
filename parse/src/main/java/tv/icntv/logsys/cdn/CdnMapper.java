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


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.config.LogConfigurationFactory;
import tv.icntv.logsys.xmlObj.XmlLog;
import java.io.IOException;
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

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String reg = "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s\\-\\s\\-\\s\\[([^]]+)\\]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s\"-\"\\s\"([^]]+)\"";
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(value.toString());
        String g3[],g6[];
        String keyTemp="";
        String valueTemp="";
        if(matcher.find()){
            g3 = matcher.group(3).split(" ");
            g6 = matcher.group(6).split("#");
            if(null==g6[0]||g6[0].length()!=15){
                return;
            }
            keyTemp = g6[0]+g3[1];

            valueTemp = matcher.group(1)+"</>"+matcher.group(2)+"</>"+matcher.group(3)+"</>"+matcher.group(4)+"</>"+matcher.group(5)+"</>"+matcher.group(6);
            context.write(new Text(keyTemp), new Text(valueTemp));
        }

    }

    public XmlLog getConf() {
       return  LogConfigurationFactory.getLogConfigurableInstance("tv.icntv.logsys.config.LogConfiguration","cdn_log_mapping.xml").getConf();
    }

//    public static void main(String[] args){
//
//        String str = "122.143.12.29 - - [23/Jul/2013:00:00:00 +0800] \"GET /media/new/2012/02/02/hd_dy_mny2_20120202.ts HTTP/1.1\" 206 1049074 \"-\" \"010133501227729#00000032AmlogicMDZ-05-201302261821793###Mar  4 2013,11:11:54\"";
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
//        String a = "010133501227729#00000032AmlogicMDZ-05-201302261821793###Mar  4 2013,11:11:54";
//        String[] arrA = a.split("#");
//        for(int i=0;i<arrA.length;i++){
//            System.out.println(arrA[i]);
//        }
//
//
//        DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.UK);
//        DateFormat formatCN = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.CHINA);
//        String rawTime = "23/Jul/2013:00:00:29 +0800";
//        try {
//            Date date = format.parse(rawTime);
//            System.out.println("format date: " + date);
//            System.out.println("format: " + formatCN.format(date));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//    }
}
