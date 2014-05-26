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
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.core.ParserConstant;
import tv.icntv.log.stb.login.LoginConstant;
import tv.icntv.log.stb.login.UserLoginDomain;
import tv.icntv.log.stb.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/20
 * Time: 11:26
 */
public class Test implements LoginConstant{
    public static void main(String[]args) throws IOException {


        List<String> lines=Files.readLines(new File("d:\\userLogin-m-00023"), Charsets.UTF_8);
        System.out.println(lines.size());
        for(String value1 : lines){
//            System.out.println(".");
            String commonLoggerStr=value1.toString();

            String[] logArr = commonLoggerStr.split(SPLIT_T);

        //logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
            String logContent = logArr[15];

            if(logContent==null || logContent.trim().length()<=0){
                System.out.println("logContent为空");
                return ;
            }
            String[] logContentArr = logContent.split(COMMA_SIGN);

            if(logContentArr==null || logContentArr.length<=0){
                System.out.println("logContentArr为空");

                return ;
            }
        UserLoginDomain userLogin=new UserLoginDomain();
        for(String str : logContentArr){

            String key = StringUtils.substringBefore(str, EQUAL_SIGN);
            String value = StringUtils.substringAfter(str, EQUAL_SIGN);
//            value = StringsUtils.getEncodeingStr(value);
            if(!Strings.isNullOrEmpty(key)){
                key=key.trim().replace("{","");
            }
            if(!Strings.isNullOrEmpty(value)){
                value=value.trim().replace("}","");
            }
            if(KEY_CONSUM_DEVICE_CODE.equalsIgnoreCase(key)){
                //icntv编号
                userLogin.setIcntvId(value);
            }else if(KEY_EPG_OPERTYPE.equalsIgnoreCase(key)){
                //操作类型
                userLogin.setOperateType(value);
            }else if(KEY_DEVICE_OPERATE_DATE.equalsIgnoreCase(key)){
                //操作时间
//				Date date = DateUtil.convertStringToDate(COMMON_DATE_FORMAT, value);
//				value = DateUtil.convertDateToString(COMMON_DATE_FORMAT, date);
                userLogin.setOperateTime(value);
            }else if(KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
                //IP地址
                if(!value.matches("[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}")){
//                    System.out.println(logContent);
                    break;
                }
                userLogin.setIpAddress(value);
            }

        }
            System.out.println(userLogin.toString());
        }

        }

    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
