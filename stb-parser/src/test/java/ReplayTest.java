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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: wangliang
 * Date: 2014/05/26
 * Time: 10:16
 */
public class ReplayTest implements ContentView {
    public static void main(String[]args) throws IOException {

        int cnt = 0;
        String operateTime = "";
        String operateType = "";
        String uuid = "";
        String programId = "";
        List<String> lines=Files.readLines(new File("e:\\test\\sample4.txt"), Charsets.UTF_8);
        System.out.println(lines.size());
        for(String value1 : lines){
            String[] logArr = value1.split(SPLIT_T);
            //logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
            if(logArr.length != 16){
                continue;
            }
            String logContent = logArr[15];

            if(logContent==null || logContent.trim().length()<=0){
                System.out.println("logContent为空");
                return ;
            }
            String[] contentArr = logContent.split(COMMA_SIGN);//得到一行content

            if(contentArr == null || contentArr.length != 3){
                System.out.println("logContentArr长度错误:"+contentArr.length);
                return ;
            }

            StringBuffer stringBuffer=new StringBuffer();
            System.out.println(logArr[11].trim());
            //1.日期
            operateTime = DateUtil.convertDateToString( "yyyyMMddHHmmss",DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",logArr[11].trim()));
            if (operateTime == null || EMPTY.equals(operateTime)) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(operateTime)).append(SPLIT);
            }
            //2.CNTVID用户序列号
            stringBuffer.append(StringsUtils.getEncodeingStr(logArr[3])).append(SPLIT);

            //3.ip地址
            if (logArr[7] == null || EMPTY.equals(logArr[7])) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(logArr[7].trim())).append(SPLIT);
            }

            //4.OperateTtype	操作类型 1:开始 2:结束
            operateType = StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN);
            if (operateType == null || EMPTY.equals(operateType)) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(operateType)).append(SPLIT);
            }

            //5.uuid频道
            uuid =  StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN);
            if (uuid == null || EMPTY.equals(uuid)) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(uuid)).append(SPLIT);
            }

            //6.programId 节目id
            programId =  StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN);
            if(!programId.matches("\\d+")){
                continue;
            }

            if (programId == null || EMPTY.equals(programId)) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(programId));
            }

            System.out.println(stringBuffer.toString());
            cnt++;
        }
        System.out.println("总行数:"+cnt);
    }

    @Override
    public List<String> getKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    enum FilterJobParameterEnum{
        RULEFILE;
    }

}
