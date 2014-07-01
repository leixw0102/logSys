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
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 14-7-1
 * Time: 下午5:59
 * To change this template use File | Settings | File Templates.
 */

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
public class CdnModuleTest implements ContentView {
    public static void main(String[]args) throws IOException {
        int cnt=0;
        List<String> lines= Files.readLines(new File("e:\\test\\sample6.txt"), Charsets.UTF_8);
        System.out.println(lines.size());
        for(String value1 : lines){
            String[] values = value1.split(SPLIT_T);
            //logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
            if(values.length != 16){
                continue;
            }
            String logContent = values[15];

            if(logContent==null || logContent.trim().length()<=0){
                System.out.println("logContent为空");
                return ;
            }

            //过滤异常日志
            String[] contentArr = logContent.split(COMMA_SIGN);

            if(contentArr == null || contentArr.length == 0){
                System.out.println("contentArr长度错误");
                return ;
            }

            String[] arrTemp = contentArr[0].split(":");
            String status = arrTemp[1];
            if("confail".equals(status) || "nofile".equals(status) || "srvclose".equals(status)
                    || "srverr".equals(status) || "timeout".equals(status)){
                return;
            }

            StringBuffer stringBuffer=new StringBuffer();
            String programId = "";


            //1.CNTVID用户序列号
            stringBuffer.append(StringsUtils.getEncodeingStr(values[3])).append(SPLIT);

            //2.用户IP
            if (null == values[7] || EMPTY.equals(values[7])) {
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            } else {
                stringBuffer.append(StringsUtils.getEncodeingStr(values[7].trim())).append(SPLIT);
            }

            //3.useragent	非必需
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //4.CdnFactory cdn厂家CDN厂家：1.蓝汛 2. 网宿  非必需
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //5.CarrierOperator	运营商
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //TODO 6.domain 访问域名
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //TODO 7.CdnNodeIP	CDN的节点IP
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //TODO 8.ConnectResult	节点连接情况 1.成功 2.超时 3.失败 4.302跳转
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //9.transDomain	302跳转域名
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //10.NodeSpeed	节点下载速度
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //11.VisitStartTime	访问开始时间: YYYYMMDD HH24MISS
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //12.VisitEndTime	访问结束时间:YYYYMMDD HH24MISS
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //13.programURL	节目URL
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //14.ProgramID	节目ID
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //15.SliceSize	分片大小
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //16.Reserved1	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //17.Reserved2	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //18.Reserved3	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));

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
