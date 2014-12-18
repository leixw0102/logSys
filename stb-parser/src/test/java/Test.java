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
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.core.ParserConstant;
import tv.icntv.log.stb.login.LoginConstant;
import tv.icntv.log.stb.login.UserLoginDomain;
import tv.icntv.log.stb.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/20
 * Time: 11:26
 */
public class Test implements LoginConstant{
//    public static void main(String[]args) throws IOException {
////        String str="800-userLogin/{0,number,#0000000000000}/userLogin;900-contentView/{0,number,#0000000000000}/contentView;100-devicePlayer/{0,number,#0000000000000}/devicePlayer;101,102,103,104,105,106,107-logEpg/{0,number,#0000000000000}/logEpg";
//////        MessageFormat mf=MessageFormat
////        List<String> files= Lists.transform(Lists.newArrayList(Splitter.on(ParserConstant.FILTER_SPILTER).split(str)), new Function<String,String>() {
////            @Override
////            public String apply(java.lang.String input) {
////                return MessageFormat.format(input, System.currentTimeMillis());  //To change body of implemented methods use File | Settings | File Templates.
////            }
////        });
////        System.out.println(new Date(System.currentTimeMillis()).toLocaleString());
////        System.out.println(files);
////        Map<String,String> maps  = Maps.newHashMap();
////        for(String str1:files){
////            String[]kv=str1.split("-");
////            String[] keys=kv[0].split(",");
////            for(String key:keys){
////                maps.put(key,kv[1]);
////            }
////
////        }
//////        System.out.println(maps);
//////        System.out.println(MessageFormat.format("other/{0,number,#0000000000000}/other",System.currentTimeMillis()));
////        String st1=maps.get("800");
////         System.out.println(st1.substring(st1.lastIndexOf("/")+1));
////        String str="010127001061399201405010501070820000    2014-05-01 05:01:07 120 0       010127001061399         2720130807163420732     78:6A:89:6A:41:81       172.16.2.30     2014-05-01 05:01:07 120 2014-05-01 05\n" +
////                ":01:07 120      2014-05-01 05:01:07 082 2014-05-01 05:01:07 082 1       800     Device  operateDate=2014-05-01 05:01:07 120, operateType=STARTUP, deviceCode=010127001061399, versionId=, mac=78:6A:8\n" +
////                "9:6A:41:81, platformId=2720130807163420732, ipAddress=118.180.16.82";
////        System.out.println(str.split("\t").length);
//
//        List<String> lines=Files.readLines(new File("d:\\userLogin-m-00023"), Charsets.UTF_8);
//
//        for(String line : lines){
////            System.out.println(line.split("\t").length);
//            StringBuffer out = new StringBuffer();
//           // for(String str : logContentArr){
//                 String str=line.split("\t")[15];
//                String key = StringUtils.substringBefore(str, EQUAL_SIGN);
//                String value = StringUtils.substringAfter(str, EQUAL_SIGN);
//                value = StringsUtils.getEncodeingStr(value);
//                if(Strings.isNullOrEmpty(key)){
//                    key=key.trim();
//                }
//                if(Strings.isNullOrEmpty(value)){
//                    value=value.trim();
//                }
//                if(KEY_CONSUM_DEVICE_CODE.equalsIgnoreCase(key)){
//                    //icntv编号
//                    out.append(value).append(SPLIT);
//                }else if(KEY_EPG_OPERTYPE.equalsIgnoreCase(key)){
//                    //操作类型
//                    if("STARTUP".equalsIgnoreCase(value)){
//                        out.append(OP_TYPE_STARTUP).append(SPLIT);
//                    }else if("SHUTDOWN".equalsIgnoreCase(value)){
//                        out.append(OP_TYPE_SHUTDOWN).append(SPLIT);
//                    }else if("ACTIVATE".equalsIgnoreCase(value)){
//                        out.append(OP_TYPE_ACTIVATE).append(SPLIT);
//                    }else{
//                        out.append(-1).append(SPLIT);
//                    }
//                }else if(KEY_DEVICE_OPERATE_DATE.equalsIgnoreCase(key)){
//                    //操作时间
//                    Date date = DateUtil.convertStringToDate(COMMON_DATE_FORMAT, value);
//                    value = DateUtil.convertDateToString(COMMON_DATE_FORMAT, date);
//                    out.append(value).append(SPLIT);
//                }else if(KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
//                    //IP地址
//                    out.append(value.replace("}", "")).append(SPLIT);
//                }
//            //系统来源
//            out.append(SYS_SOURCE).append(SPLIT);
//            //Fsource
//            out.append(F_SOURCE).append(SPLIT);
//            //遥控设备类型
//            out.append(EMPTY);
//            System.out.println(str);
//            }
//
//        }


    public static void main(String[]args) throws IOException {
        Path abc=new Path("/d/d/sd/df","dsfsd.lzo");
        System.out.println(abc.getName());
//		 List<String>	lines=Files.readLines(new File("d:\\useroperepginfo_20140625.dat"),Charsets.UTF_8);
//	    Set s= Sets.newHashSet();
//	    for(String line:lines){
//		    s.add(Splitter.on("|").splitToList(line).get(7));
//	    }
//        Pattern pattern = Pattern.compile("(.*)|(.*)");
//        String abc="sdf|dsf|dfdd|[s|b]";
//        Iterator<String> it=Splitter.on("|").limit(4).split(abc).iterator();
//        while(it.hasNext()){
//            System.out.println(it.next());
//        }
//	    System.out.print("aaa\rbbb\rccc".split("\r").length);
//        List<String> lines=Files.readLines(new File("d:\\userLogin-m-00023"), Charsets.UTF_8);
//        System.out.println(lines.size());
//        for(String value1 : lines){
////            System.out.println(".");
//            String commonLoggerStr=value1.toString();
//
//            String[] logArr = commonLoggerStr.split(SPLIT_T);
//
//        //logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
//            String logContent = logArr[15];
//
//            if(logContent==null || logContent.trim().length()<=0){
//                System.out.println("logContent为空");
//                return ;
//            }
//            String[] logContentArr = logContent.split(COMMA_SIGN);
//
//            if(logContentArr==null || logContentArr.length<=0){
//                System.out.println("logContentArr为空");
//
//                return ;
//            }
//        UserLoginDomain userLogin=new UserLoginDomain();
//        for(String str : logContentArr){
//
//            String key = StringUtils.substringBefore(str, EQUAL_SIGN);
//            String value = StringUtils.substringAfter(str, EQUAL_SIGN);
////            value = StringsUtils.getEncodeingStr(value);
//            if(!Strings.isNullOrEmpty(key)){
//                key=key.trim().replace("{","");
//            }
//            if(!Strings.isNullOrEmpty(value)){
//                value=value.trim().replace("}","");
//            }
//            if(KEY_CONSUM_DEVICE_CODE.equalsIgnoreCase(key)){
//                //icntv编号
//                userLogin.setIcntvId(value);
//            }else if(KEY_EPG_OPERTYPE.equalsIgnoreCase(key)){
//                //操作类型
//                userLogin.setOperateType(value);
//            }else if(KEY_DEVICE_OPERATE_DATE.equalsIgnoreCase(key)){
//                //操作时间
////				Date date = DateUtil.convertStringToDate(COMMON_DATE_FORMAT, value);
////				value = DateUtil.convertDateToString(COMMON_DATE_FORMAT, date);
////                Date d = DateUtil.convertStringToDate(COMMON_DATE_FORMAT2, value);
////                String opTime = DateUtil.convertDateToString(COMMON_DATE_FORMAT1, d);
////                System.out.println(d.toLocaleString()+"\t"+opTime);
//                userLogin.setOperateTime(value);
//            }else if(KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
//                //IP地址
//                if(!value.matches("[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}")){
////                    System.out.println(logContent);
//                    break;
//                }
//                userLogin.setIpAddress(value);
//            }
//
//        }
//            System.out.println(userLogin.toString());
//        }

        }

    enum FilterJobParameterEnum{
        RULEFILE;
    }
}
