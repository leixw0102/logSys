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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.ReflectUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/23
 * Time: 09:36
 */
public class CdnStbMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements CdnStb{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values= value.toString().split(SPLIT_T);
        if(null == values || values.length!=16){
            return;
        }
        String content=values[15];
        if(Strings.isNullOrEmpty(content)){
            return;
        }
        CdnStbDomain cdnStbDomain = new CdnStbDomain();
        cdnStbDomain.setCntvId(values[3].trim());
        cdnStbDomain.setUserIp(values[7]);
        cdnStbDomain.setEndTime(DateUtils.getFormatDate(values[8]));
        if("CloseSession".equalsIgnoreCase(values[14])){
            List<String> fieldValue=getKeys();
            Map<String,String> maps = null;
            try{
                maps = Splitter.on("&").omitEmptyStrings().withKeyValueSeparator("=").split(content);
            }catch (Exception e){
                logger.error("error input ",e);
                return ;
            }
            for(String filed:fieldValue){
                try {
                    ReflectUtils.setFieldValue(cdnStbDomain.getClass().getDeclaredField(filed), cdnStbDomain, new String[]{maps.get(filed)});
                } catch (NoSuchFieldException e) {
                    logger.error("reflect error",e);

                }
            }
            if(Strings.isNullOrEmpty(cdnStbDomain.getUrl())){
                return;
            }
            String v = cdnStbDomain.toString();
            if(Strings.isNullOrEmpty(v)){
                return;
            }
            context.write(NullWritable.get(), new Text(v));
            return;
        }

        return;


    }
    public static void main(String[]args) throws IOException {
        String test="id=493&url=http://hot.sp.media.ysten.com/media/new/2013/icntv2/media/2014/09/04/HD1M2d97d9a54\n" +
                "75f48cb8b368bfb6f6714a4.ts&block3=0&block5=0&block10=0&host=111.20.240.41&taskCnt=4&sucCnt=4&failCnt=0&conFailCnt=0&timeOutCnt=0&nooFileErrorCnt=0&srvCloseCnt=0&srvErrorCnt=0&socketErrorCnt=0&reqUn\n" +
                "acceptCnt=0&revByte=5914kB&revSpeed=552kB/s&dnsAvgTime=0ms&dnsMaxTime=0ms&dnsMinTime=0ms&conAvgTime=28ms&conMaxTime=73ms&conMinTime=0ms&dnsRedList=111.20.240.41(,120.192.247.55),120.192.247.55(),dnsRedList=mibox.vod01.icntvcdn.com(111.1.57.14),010121009660446201410232000184450000/20141023200018445/0/010121009660446////100.107.182.162/2014-10-23 20:00:18 445/2014-10-23 20:00:18 445/2014-10-23 20:00:18 445/2014-10-23 20:00:18 445/1/900/ConsumAction/catgId=, startDate=2014-10-23 20:00:18 445, endReason=, deviceCode=010121009660446, endDate=, contentType=MOVIE, videoType=, id=, programId=9381361, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=966269, ipAddress=, programName=";
        Map<String,String> maps = Splitter.on("&").withKeyValueSeparator("=").split(test);
//        Set<String> keys = maps.keySet();
//        for(String key:keys){
//            System.out.println(key+"\t"+maps.get(key));
//        }

        List<String> fieldValue=Lists.newArrayList("dnsRedList", "conMinTime", "conMaxTime", "conAvgTime", "dnsMinTime", "dnsMaxTime", "dnsAvgTime", "revSpeed", "revByte", "socketErrorCnt", "srvErrorCnt", "srvCloseCnt", "nooFileErrorCnt", "timeOutCnt", "conFailCnt", "failCnt", "sucCnt", "taskCnt", "host", "url");
//        Map<String,String> maps = Splitter.on("&").omitEmptyStrings().withKeyValueSeparator("=").split(content);
        CdnStbDomain cdnStbDomain = new CdnStbDomain();
        for(String filed:fieldValue){
            try {
                ReflectUtils.setFieldValue(cdnStbDomain.getClass().getDeclaredField(filed), cdnStbDomain, new String[]{maps.get(filed)});
            } catch (NoSuchFieldException e) {
                System.out.println("reflect error"+e);
            }
        }
        System.out.println(cdnStbDomain.toString());

    }

    @Override
    public List<String> getKeys() {
        return Lists.newArrayList("dnsRedList","conMinTime","conMaxTime","conAvgTime","dnsMinTime","dnsMaxTime","dnsAvgTime","revSpeed","revByte","socketErrorCnt","srvErrorCnt","srvCloseCnt","nooFileErrorCnt","timeOutCnt","conFailCnt","failCnt","sucCnt","taskCnt","host","url");  //To change body of implemented methods use File | Settings | File Templates.
    }
}
