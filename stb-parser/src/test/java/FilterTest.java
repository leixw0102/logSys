/*
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
import tv.icntv.log.stb.core.ParserConstant;

import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/12/12
 * Time: 11:14
 */
public class FilterTest {
    private static boolean check(List<String> lineSplit) {
        if(lineSplit==null||lineSplit.isEmpty() || lineSplit.size()!=16){
            return false;
        }
        // TODO Auto-generated method stub
        return lineSplit.get(0).matches("\\d*") && lineSplit.get(2).matches("(\\d*)") && lineSplit.get(3).matches("\\d{15,17}")
                //&&lineSplit.get(7).matches("[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}")
                //&&lineSplit.get(7).matches("([\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3},?).*")
                &&lineSplit.get(8).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(9).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(10).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(11).matches("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s\\d{0,3}")
                &&lineSplit.get(12).matches("\\d{1}")
                &&lineSplit.get(13).matches("\\d*")
                &&lineSplit.get(14).matches("[A-Za-z]*")
                ;
    }
    public static void main(String[]args){
        String line = "01012100900165802201412101511533740000/20141210151153373/0/01012100900165802////117.175.252.76/2014-12-10 15:11:53 373/2014-12-10 15:11:53 373/2014-12-10 15:11:53 374/2014-12-10 15:11:53 374/1/900/ConsumAction/catgId=217377, startDate=2014-12-10 15:11:53 373, endReason=, deviceCode=01012100900165802, endDate=, contentType=MOVIE, videoType=, id=, programId=10010098, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=985432, ipAddress=, programName=";
        List<String> lineSplit= Lists.newArrayList(Splitter.on(ParserConstant.STB_SPLITER).limit(16).split(line.toString()));
        if(!check(lineSplit)){
            System.out.println("regular error line ...\t"+line.toString());
            return;
        }
    }
}
