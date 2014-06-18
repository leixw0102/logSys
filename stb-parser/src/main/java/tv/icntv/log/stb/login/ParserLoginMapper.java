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
package tv.icntv.log.stb.login;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.util.DateUtil;

import java.io.IOException;
import java.util.Date;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/05/20
 * Time: 17:18
 */
public class ParserLoginMapper extends Mapper<LongWritable, Text, NullWritable, Text> implements LoginConstant {
     private Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 *
	 * @param key1
	 * @param value1 字符串例子及对应的字段描述如下（括号中为字段名称及描述）：
	 *      010121009271802201406150827055880000（log_id	日志编号）
	 *		2014-06-15 08:27:05 970（seq_id	请求编号）
	 *		0（retry_seqid	重试请求编号）
	 *		010121009271802（device_id	设备编号）
	 *   	4（version_id	终端版本号）
	 *		2120131022154440312（platform_id	平台号）
	 *		0c:c6:55:17:c7:46（mac	终端mac）
	 *		112.25.7.28（ip	终端ip）
	 *		2014-06-15 08:27:05 970（req_time	终端发送日志请求的时间）
	 *		2014-06-15 08:27:05 970（cur_time	日志产生时的终端时间）
	 *		2014-06-15 08:27:05 588（client_time	日志产生时的服务器时间）
	 *		2014-06-15 08:27:05 588（server_time	接收到日志时的服务器时间）
	 *		1（level	日志等级）
	 *		800（module	模块）
	 *		Device（action	操作）
	 *   	operateDate=2014-06-15 08:27:05 970, operateType=STARTUP, deviceCode=010121009271802, versionId=4, mac=0c:c6:55:17:c7:46, platformId=2120131022154440312, ipAddress=112.25.7.5（content	日志内容）
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {

		String commonLoggerStr=value1.toString();
		if(commonLoggerStr==null || commonLoggerStr.trim().length()<=0){
            logger.error("commonLoggerStr为空");
			return ;
		}
		String[] logArr = commonLoggerStr.split(SPLIT_T);

		if(logArr==null || logArr[15]==null || logArr[15].trim().length()<=0){
            logger.error("logArr为空");
			return ;
		}
		//logArr的第16个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
		String logContent = logArr[15];

		if(logContent==null || logContent.trim().length()<=0){
            logger.error("logContent为空");
			return ;
		}
		String[] logContentArr = logContent.split(COMMA_SIGN);

		if(logContentArr==null || logContentArr.length<=0){
            logger.error("logContentArr为空");
			return ;
		}
        UserLoginDomain userLogin=new UserLoginDomain();
		for(String str : logContentArr){

			String key = StringUtils.substringBefore(str, EQUAL_SIGN);
			String value = StringUtils.substringAfter(str, EQUAL_SIGN);
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
//				Date d = DateUtil.convertStringToDate(COMMON_DATE_FORMAT2, value);
//				String opTime = DateUtil.convertDateToString(COMMON_DATE_FORMAT1, d);
                userLogin.setOperateTime(value);
			}else if(KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
				//IP地址
                if(!value.matches(ipRegular)){
                    return;
                }
                userLogin.setIpAddress(value);
			}

		}
		context.write(NullWritable.get(),new Text(userLogin.toString()));
	}
    private String ipRegular="[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}";

}
