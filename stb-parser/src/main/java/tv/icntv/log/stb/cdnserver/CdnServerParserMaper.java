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

import java.io.File;
import java.io.IOException;
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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String v = value.toString();
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
        if (!icntvId.matches("\\d{15}")) {
            logger.error("icntvId ={} error", icntvId);
            return;
        }
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

    public static void main(String[] args) throws IOException {
        List<String> str = Files.readLines(new File("d:\\abc.txt"), Charsets.UTF_8);
        for (String v : str) {
            List<String> results = Lists.newArrayList(Splitter.on(" ").limit(13).split(v));
            if (null == results || results.isEmpty() || results.size() != 13 || Strings.isNullOrEmpty(results.get(0))) {
                logger.error("cdn server log error size " + results.size());
                for(int i=0;i<results.size();i++){
                    logger.info(i+"\t"+results.get(i));
                }
                return;
            }
            String icntvId = results.get(12).split("#")[0];
            if (Strings.isNullOrEmpty(icntvId)) {

                return;
            }
            icntvId = icntvId.replace("\"", "");
            if (!icntvId.matches("\\d{15}")) {
                logger.error("icntvId ={} error", icntvId);
                return;
            }
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
                logger.info(k + "\t" + mValue);
//                for(int i=0;i<results.size();i++){
//                    logger.info(i+"\t"+results.get(i));
//                }
            } catch (ParseException e) {
                logger.error(" parser time error", e);
                return;
            }
        }
    }

}
