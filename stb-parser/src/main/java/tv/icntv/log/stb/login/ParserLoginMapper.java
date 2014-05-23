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

	public void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {

		String commonLoggerStr=value1.toString();
		if(commonLoggerStr==null || commonLoggerStr.trim().length()<=0){
			System.out.println("commonLoggerStr为空");
			return ;
		}
		String[] logArr = commonLoggerStr.split(SPLIT_T);

		if(logArr==null || logArr[15]==null || logArr[15].trim().length()<=0){
			System.out.println("logArr为空");
			return ;
		}
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
		StringBuffer out = new StringBuffer();
		for(String str : logContentArr){

			String key = StringUtils.substringBefore(str, EQUAL_SIGN);
			String value = StringUtils.substringAfter(str, EQUAL_SIGN);
			value = StringsUtils.getEncodeingStr(value);

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
			}else{
				System.out.println(value1.toString());
				return;	//out.append(SPLIT);
			}
		}
		//系统来源
		out.append(SYS_SOURCE).append(SPLIT);
		//Fsource
		out.append(F_SOURCE).append(SPLIT);
		//遥控设备类型
		out.append(EMPTY);
		context.write(NullWritable.get(),new Text(out.toString()));
	}


}
