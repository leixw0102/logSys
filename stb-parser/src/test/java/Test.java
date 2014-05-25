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
//        String str="800-userLogin/{0,number,#0000000000000}/userLogin;900-contentView/{0,number,#0000000000000}/contentView;100-devicePlayer/{0,number,#0000000000000}/devicePlayer;101,102,103,104,105,106,107-logEpg/{0,number,#0000000000000}/logEpg";
////        MessageFormat mf=MessageFormat
//        List<String> files= Lists.transform(Lists.newArrayList(Splitter.on(ParserConstant.FILTER_SPILTER).split(str)), new Function<String,String>() {
//            @Override
//            public String apply(java.lang.String input) {
//                return MessageFormat.format(input, System.currentTimeMillis());  //To change body of implemented methods use File | Settings | File Templates.
//            }
//        });
//        System.out.println(new Date(System.currentTimeMillis()).toLocaleString());
//        System.out.println(files);
//        Map<String,String> maps  = Maps.newHashMap();
//        for(String str1:files){
//            String[]kv=str1.split("-");
//            String[] keys=kv[0].split(",");
//            for(String key:keys){
//                maps.put(key,kv[1]);
//            }
//
//        }
////        System.out.println(maps);
////        System.out.println(MessageFormat.format("other/{0,number,#0000000000000}/other",System.currentTimeMillis()));
//        String st1=maps.get("800");
//         System.out.println(st1.substring(st1.lastIndexOf("/")+1));
//        String str="010127001061399201405010501070820000    2014-05-01 05:01:07 120 0       010127001061399         2720130807163420732     78:6A:89:6A:41:81       172.16.2.30     2014-05-01 05:01:07 120 2014-05-01 05\n" +
//                ":01:07 120      2014-05-01 05:01:07 082 2014-05-01 05:01:07 082 1       800     Device  operateDate=2014-05-01 05:01:07 120, operateType=STARTUP, deviceCode=010127001061399, versionId=, mac=78:6A:8\n" +
//                "9:6A:41:81, platformId=2720130807163420732, ipAddress=118.180.16.82";
//        System.out.println(str.split("\t").length);

        List<String> lines=Files.readLines(new File("d:\\userLogin-m-00023"), Charsets.UTF_8);

        for(String line : lines){
//            System.out.println(line.split("\t").length);
            StringBuffer out = new StringBuffer();
           // for(String str : logContentArr){
                 String str=line.split("\t")[15];
                String key = StringUtils.substringBefore(str, EQUAL_SIGN);
                String value = StringUtils.substringAfter(str, EQUAL_SIGN);
                value = StringsUtils.getEncodeingStr(value);
                if(Strings.isNullOrEmpty(key)){
                    key=key.trim();
                }
                if(Strings.isNullOrEmpty(value)){
                    value=value.trim();
                }
                if(KEY_CONSUM_DEVICE_CODE.equalsIgnoreCase(key)){
                    //icntv编号
                    out.append(value).append(SPLIT);
                }else if(KEY_EPG_OPERTYPE.equalsIgnoreCase(key)){
                    //操作类型
                    if("STARTUP".equalsIgnoreCase(value)){
                        out.append(OP_TYPE_STARTUP).append(SPLIT);
                    }else if("SHUTDOWN".equalsIgnoreCase(value)){
                        out.append(OP_TYPE_SHUTDOWN).append(SPLIT);
                    }else if("ACTIVATE".equalsIgnoreCase(value)){
                        out.append(OP_TYPE_ACTIVATE).append(SPLIT);
                    }else{
                        out.append(-1).append(SPLIT);
                    }
                }else if(KEY_DEVICE_OPERATE_DATE.equalsIgnoreCase(key)){
                    //操作时间
                    Date date = DateUtil.convertStringToDate(COMMON_DATE_FORMAT, value);
                    value = DateUtil.convertDateToString(COMMON_DATE_FORMAT, date);
                    out.append(value).append(SPLIT);
                }else if(KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
                    //IP地址
                    out.append(value.replace("}", "")).append(SPLIT);
                }
            //系统来源
            out.append(SYS_SOURCE).append(SPLIT);
            //Fsource
            out.append(F_SOURCE).append(SPLIT);
            //遥控设备类型
            out.append(EMPTY);
            System.out.println(str);
            }

        }

    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
