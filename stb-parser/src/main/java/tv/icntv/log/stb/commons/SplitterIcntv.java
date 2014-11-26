package tv.icntv.log.stb.commons;/*
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.contentview.ContentViewDomain;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/11/21
 * Time: 16:02
 */
public class SplitterIcntv {

    private static Logger logger = LoggerFactory.getLogger(SplitterIcntv.class);
    public static Map<String,String> toMap(Iterable<String> iterable,String split){
        Iterator<String> it = iterable.iterator();
        Map<String,String> maps = Maps.newHashMap();
        while (it.hasNext()){
            String value = it.next();
            List<String> result = Lists.newArrayList(Splitter.on(split).trimResults().limit(2).split(value));
            if(result.size()!=2){
                logger.error("message = {}",value);
                continue;
            }
            maps.put(result.get(0),result.get(1));
        }
        return maps;
    }
    public static void main(String[]args){
        //List<String> list=Splitter.on(",").limit(17).trimResults().omitEmptyStrings().splitToList("catgId=217532, startDate=2014-11-13 11:38:10 324, endReason=, deviceCode=010143002008852, endDate=, contentType=MOVIE, videoType=, id=, programId=2569998, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=788109, ipAddress=, programName=010533501273793201411131138105870000/2/0/010533501273793/:/00000032AmlogicMDZ-05-201302261821793//117.151.171.132/2014-11-13 11:38:10 556/2014-11-13 11:37:47 926/2014-11-13 11:37:47 957/2014-11-13 11:38:10 587/1/401/Dispatch/result=0&strategy=0&hlist=centerdispatch\\r");
        List<String> abc =Lists.newArrayList("deviceCode","catgId","startDate","endReason","endDate","videoType","programId","chargeType","epgCode","outerCode");
        Map<String,String> maps = SplitterIcntv.toMap(Splitter.on(",").limit(17).split("catgId=, startDate=2014-11-13 11:38:10 324, endReason=, deviceCode=010143002008852, endDate=, contentType=MOVIE, videoType=, id=, programId=2569998, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=788109, ipAddress=, programName="),"=");//.withKeyValueSeparator("=").split(content);
        ContentViewDomain view = new ContentViewDomain();
        System.out.println(view.toString());
        for(String k : abc){
            try {
                ReflectUtils.setFieldValue(view.getClass().getDeclaredField(k), view, new String[]{maps.get(k)});
            } catch (NoSuchFieldException e) {
                logger.error("reflect error",e);
            }
        }
        System.out.println(view.getEpgCode());

    }
}
