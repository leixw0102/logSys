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

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.core.ParserConstant;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/30
 * Time: 12:52
 * * key = icntv+ip  + url
 * value=  ip sliceSize status  time  useragent
 */
public class CdnServerParserMaper extends Mapper<LongWritable, Text, Text, Text> {
    private static String split = "|";
    static DateFormat formatCN = new SimpleDateFormat("yyyyMMdd HHmmss", Locale.CHINA);
    static DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
    private static Logger logger = LoggerFactory.getLogger(CdnServerParserMaper.class);
    public static Text transformTextToUTF8(Text text, String encoding) {
        String value = null;
        try {
            value = new String(text.getBytes(), 0, text.getLength(), encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new Text(value);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String v = transformTextToUTF8(value,"gbk").toString();
        if (Strings.isNullOrEmpty(v)) {
            return;
        }
        List<String> results = Lists.newArrayList(Splitter.on(" ").limit(13).split(v));
        if (null == results || results.isEmpty() || results.size() != 13 || Strings.isNullOrEmpty(results.get(0))) {
            logger.error("cdn server log error size " + results.size());
            return;
        }
        String icntvId = results.get(12).split("#")[0];
        if (Strings.isNullOrEmpty(icntvId)) {

            return;
        }
        icntvId = icntvId.replace("\"", "");
//        if (!icntvId.matches("\\d{15}")) {
//            logger.error("icntvId ={} error", icntvId);
//            return;
//        }
        String url = results.get(7);
        if (url.endsWith("\"")) {
            url = url.substring(0, url.length() - 1);
        }
        String k = icntvId + split + results.get(0) + split + url;
        String time = results.get(4) + " " + results.get(5);
        if (Strings.isNullOrEmpty(time)) {
            return;
        }
        time = time.replace("[", "").replace("]", "");
        String ua = results.get(12);
        try {
            String mValue = results.get(1) + split + results.get(9) + split + results.get(8) + split + formatCN.format(format.parse(time)) + split + ua;//results.get(1)+split+
            context.write(new Text(k), new Text(mValue));
        } catch (ParseException e) {
            logger.error(" parser time error", e);
            return;
        }

    }
//    private static boolean check(List<String> lineSplit) {
//        if(lineSplit==null||lineSplit.isEmpty() || lineSplit.size()!=16){
//            return false;
//        }
//        // TODO Auto-generated method stub
//        return lineSplit.get(0).matches("\\d{36}") && lineSplit.get(2).matches("(\\d*)") && lineSplit.get(3).matches("\\d{15}")
//                &&lineSplit.get(7).matches("[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}")
//                &&lineSplit.get(8).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
//                &&lineSplit.get(9).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
//                &&lineSplit.get(10).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
//                &&lineSplit.get(11).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
//                &&lineSplit.get(12).matches("\\d{1}")
//                &&lineSplit.get(13).matches("\\d*")
//                &&lineSplit.get(14).matches("[A-Za-z]*")
//                ;
//    }
    public static void main(String[] args) throws IOException {
//        List<String> str = Files.readLines(new File("d:\\abc.txt"), Charsets.UTF_8);
//        for (String v : str) {
        String v="015168004002859201412032111331580000/20141203211133157/0/015168004002859////10.207.43.154, 117.136.29.137/2014-12-03 21:11:33 157/2014-12-03 21:11:33 157/2014-12-03 21:11:33 158/2014-12-03 21:11:33 158/1/900/ConsumAction/catgId=219109, startDate=, endReason=, deviceCode=015168004002859, endDate=2014-12-03 21:11:33 157, contentType=MOVIE, videoType=, id=, programId=9989870, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=983404, ipAddress=, programName=";
        System.out.println( new String(v.getBytes(), 0, v.length(), "gbk"));
//        List<String> lineSplit=Lists.newArrayList(Splitter.on(ParserConstant.STB_SPLITER).limit(16).split(v.toString()));
//        System.out.println(lineSplit.get(7).matches("([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3},?).*")+"\t"+lineSplit.get(7));
//        for(int i=0;i<lineSplit.size();i++){
//            System.out.println(lineSplit.get(i) + lineSplit);
//        }
//        if(!check(lineSplit)){
//            System.out.println(v);
////            logger.info("regular error line ...\t"+value.toString());
//            return;
//        }
//            List<String> results = Lists.newArrayList(Splitter.on(" ").limit(13).split(v));
//            if (null == results || results.isEmpty() || results.size() != 13 || Strings.isNullOrEmpty(results.get(0))) {
//                logger.error("cdn server log error size " + results.size());
//                for(int i=0;i<results.size();i++){
//                    logger.info(i+"\t"+results.get(i));
//                }
//                return;
//            }
//            String icntvId = results.get(12).split("#")[0];
//            if (Strings.isNullOrEmpty(icntvId)) {
//
//                return;
//            }
//            icntvId = icntvId.replace("\"", "");
//            if (!icntvId.matches("\\d{15}")) {
//                logger.error("icntvId ={} error", icntvId);
//                return;
//            }
//            String url = results.get(7);
//            if (url.endsWith("\"")) {
//                url = url.substring(0, url.length() - 1);
//            }
//            String k = icntvId + split + results.get(0) + split + url;
//            String time = results.get(4) + " " + results.get(5);
//            if (Strings.isNullOrEmpty(time)) {
//                return;
//            }
//            time = time.replace("[", "").replace("]", "");
//            String ua = results.get(12);
//            try {
//                String mValue = results.get(1) + split + results.get(9) + split + results.get(8) + split + formatCN.format(format.parse(time)) + split + ua;//results.get(1)+split+
//                logger.info(k + "\t" + mValue);
////                for(int i=0;i<results.size();i++){
////                    logger.info(i+"\t"+results.get(i));
////                }
//            } catch (ParseException e) {
//                logger.error(" parser time error", e);
//                return;
//            }
//        }
    }

}
