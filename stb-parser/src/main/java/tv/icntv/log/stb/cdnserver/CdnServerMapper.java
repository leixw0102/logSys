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
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/22
 * Time: 10:03
 * key = icntv+url+ip
 * value=  sliceSize status  time  useragent
 */
public class CdnServerMapper extends Mapper<LongWritable,Text,Text,Text> {
    static Pattern pattern = Pattern.compile( "([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3})\\s-\\s-\\s\\[([^]]+)]\\s\"([^\"]+)\"\\s(\\d+)\\s(\\d+)\\s(.+)\\s\"-\"\\s\"([^]]+)\"");
    static DateFormat formatCN = new SimpleDateFormat("yyyyMMdd HHmmss", Locale.CHINA);
    static DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String v = value.toString();
        if(Strings.isNullOrEmpty(v)){
            return;
        }
        Matcher matcher = pattern.matcher(v);
        String[]g4,g8;
        String k,mValue;
        if(matcher.find() && matcher.groupCount()==8){
            g8=matcher.group(8).split("#");
            if(!g8[0].matches("\\d{15}")){
                return;
            }
            String icntvId=g8[0];
            g4 = matcher.group(4).split(" ");
            String url = g4[1];
            k=icntvId+"|"+matcher.group(1)+"|"+url;

            try {
                mValue = matcher.group(6)+"|"+matcher.group(5)+"|"+formatCN.format(format.parse(matcher.group(3)))+"|"+matcher.group(8);
            } catch (ParseException e) {
                return;
            }
            context.write(new Text(k),new Text(mValue));
        }
    }

    public static void main(String[]args) throws IOException, ParseException {

        List<String> list=Files.readLines(new File("d:\\cdn-server"), Charsets.UTF_8);
//        for(String line:list){

        Matcher matcher = pattern.matcher(list.get(0));
        int i=1;
        while (matcher.find()) {
             System.out.println(matcher.groupCount());
//            if(i<=matcher.groupCount()){
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println();//,"dd/MM/yyyy:hh:mm:ss Z"));
            System.out.println(matcher.group(4));
            System.out.println(matcher.group(5));
            System.out.println(matcher.group(6));
            System.out.println(matcher.group(7));
            System.out.println(matcher.group(8));
            System.out.println(matcher.group(4).split(" ")[1]);
//            }

        }}}
////        }

