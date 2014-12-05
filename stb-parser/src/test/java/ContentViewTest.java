/*
 * Copyright 2014 Future TV, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.contentview.ContentView;
import tv.icntv.log.stb.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Author: wangliang
 * Date: 2014/05/26
 * Time: 10:16
 */
public class ContentViewTest implements ContentView {
    public static void main(String[]args) throws IOException {
         String lines = "015168004002859201412032111331580000/20141203211133157/0/015168004002859////10.207.43.154, 117.136.29.137/2014-12-03 21:11:33 157/2014-12-03 21:11:33 157/2014-12-03 21:11:33 158/2014-12-03 21:11:33 158/1/900/ConsumAction/catgId=219109, startDate=, endReason=, deviceCode=015168004002859, endDate=2014-12-03 21:11:33 157, contentType=MOVIE, videoType=, id=, programId=9989870, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=983404, ipAddress=, programName=";
                 //"015168004002859201412032137300750000/20141203213730074/0/015168004002859////10.210.86.177, 117.136.29.143/2014-12-03 21:37:30 074/2014-12-03 21:37:30 074/2014-12-03 21:37:30 075/2014-12-03 21:37:30 075/1/900/ConsumAction/catgId=219109, startDate=2014-12-03 21:37:30 074, endReason=, deviceCode=015168004002859, endDate=, contentType=MOVIE, videoType=, id=, programId=9989870, bufferingTotalTime=, programSeriesName=, bufferingCnt=, chargeType=0, epgCode=, outerCode=983404, ipAddress=, programName=";
//        int cnt = 0;
//        String strEndTime="";
//        String strStartTime="";
//        String operateTime = "";
//        List<String> lines=Files.readLines(new File("e:\\test\\sample5.txt"), Charsets.UTF_8);
//        System.out.println(lines.size());
//        for(String value1 : lines){
//            String[] values = value1.split(SPLIT_T);
//            //logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
//            if(values.length != 16){
//                continue;
//            }
//            String logContent = values[15];
//
//            if(logContent==null || logContent.trim().length()<=0){
//                System.out.println("logContent为空");
//                return ;
//            }
//            String[] contentArr = logContent.split(COMMA_SIGN);//得到一行content
//
//            if(contentArr==null || contentArr.length!=17){
//                System.out.println("logContentArr为空");
//                return ;
//            }
//
//            StringBuffer stringBuffer=new StringBuffer();
//
//            //1.CNTVID用户序列号
//            stringBuffer.append(StringsUtils.getEncodeingStr(values[3])).append(SPLIT);
//
//            //2.OperateTtype	操作类型 1:开始 2:结束
//
//            strEndTime = StringUtils.substringAfter(contentArr[4].trim(), EQUAL_SIGN);
//            if (strEndTime != null && !EMPTY.equals(strEndTime)) {
//                stringBuffer.append(StringsUtils.getEncodeingStr("2")).append(SPLIT);
//                operateTime = DateUtil.convertDateToString( "yyyyMMdd HHmmss",
//                        DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",strEndTime));
//
//            }
//            strStartTime = StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN);
//            if (strStartTime != null && !EMPTY.equals(strStartTime)) {
//                stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
//                operateTime = DateUtil.convertDateToString( "yyyyMMdd HHmmss",
//                        DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",strStartTime));
//            }
//
//            //3.OperateTime	操作时间,格式是：YYYYMMDDHH24MISS
//            if (operateTime == null || EMPTY.equals(operateTime)) {
//                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//            } else {
//                stringBuffer.append(StringsUtils.getEncodeingStr(operateTime)).append(SPLIT);
//            }
//
//            //4.ServiceType	业务类型：1:点播；
//            stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
//
//            //5.VideoType	视频类型1：普通视频 2.: 微视频
//            stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[6].trim(), EQUAL_SIGN))).append(SPLIT);
//
//            //6.EndReason 流媒体服务中断原因
//            if (null == contentArr[2].trim() || EMPTY.equals(contentArr[2].trim())) {
//                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//            } else {
//                stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN))).append(SPLIT);
//            }
//
//            //7.ChargeType 计费类型  1：免费；2：收费
//            if (null == contentArr[7].trim() || EMPTY.equals(contentArr[7].trim())) {
//                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//            } else {
//                stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[7].trim(), EQUAL_SIGN))).append(SPLIT);
//            }
//
//            //8.CategoryID	栏目ID,如果日志中值为空的情况，需要讨论解决方案
//            if (null == contentArr[0].trim() || EMPTY.equals(contentArr[0].trim())) {
//                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//            } else {
//                stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))).append(SPLIT);
//            }
//
//            //9.ProgatherID	节目集ID唯一
//            stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[14].trim(), EQUAL_SIGN)+EMPTY)).append(SPLIT);
//
//            //10.ProgramID	节目ID唯一
//            stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[8].trim(), EQUAL_SIGN)+EMPTY)).append(SPLIT);
//
//            //11.subjectID 专题id 目前为空
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//
//            //12.EPGCode EPG版本编号,见EPGCode版本编号表
//            stringBuffer.append(StringsUtils.getEncodeingStr("06")).append(SPLIT);
//
//            //13.std_path 目前没有
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//
//            //14.VODPathType 收视来源实际路径
//            stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
//
//            //15.DataSource系统来源1：易视腾2：云立方
//            stringBuffer.append(DATA_SOURCE).append(SPLIT);
//
//            //16.Fsource数据来源，见数据来源表
//            stringBuffer.append(F_SOURCE).append(SPLIT);
//
//            //17.resolution 视频码率
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//
//            //18.Remark1 保留字段
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
//
//            //19.Remark2 保留字段
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));
//                System.out.println(stringBuffer.toString());
//                cnt++;
//        }
//        System.out.println("总行数:"+cnt);
    }

    @Override
    public List<String> getKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
