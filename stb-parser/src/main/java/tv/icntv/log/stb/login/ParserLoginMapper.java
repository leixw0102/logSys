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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.log.stb.util.ConstantDefine;
import tv.icntv.log.stb.util.DateUtil;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/05/20
 * Time: 17:18
 */
public class ParserLoginMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	public void map(LongWritable key1, Text value1, Context context)
			throws IOException, InterruptedException {

		String commonLoggerStr=value1.toString();
		if(commonLoggerStr==null || commonLoggerStr.trim().length()<=0){
			System.out.println("commonLoggerStr为空");
			return ;
		}
		String[] logArr = commonLoggerStr.split("\t");

		if(logArr==null || logArr[15]==null || logArr[15].trim().length()<=0){
			System.out.println("logArr为空");
			return ;
		}
		//logArr的第15个元素为日志内容，格式类似：operateDate=2014-04-25 17:59:59 621, operateType=STARTUP, deviceCode=010333501065233, versionId=, mac=10:48:b1:06:4d:23, platformId=00000032AmlogicMDZ-05-201302261821793, ipAddress=60.10.133.10
		String logContent = logArr[15];

		if(logContent==null || logContent.trim().length()<=0){
			System.out.println("logContent为空");
			return ;
		}
		String[] logContentArr = logContent.split(ConstantDefine.COMMA_SIGN);

		if(logContentArr==null || logContentArr.length<=0){
			System.out.println("logContentArr为空");
			return ;
		}
		StringBuffer out = new StringBuffer();
		for(String str : logContentArr){
			if(str==null || str.trim().length()<=0){
				System.out.println("str为空，str=" +str);
				return ;
			}
			str = str.trim();
			String key = StringUtils.substringBefore(str, ConstantDefine.EQUAL_SIGN);
			String value = StringUtils.substringAfter(str, ConstantDefine.EQUAL_SIGN);

			if(ConstantDefine.KEY_CONSUM_DEVICE_CODE.equalsIgnoreCase(key)){
				//icntv编号
				out.append(value).append("|");
			}else if(ConstantDefine.KEY_EPG_OPERTYPE.equalsIgnoreCase(key)){
				//操作类型
				if("STARTUP".equalsIgnoreCase(value)){
					out.append("1").append("|");
				}else if("SHUTDOWN".equalsIgnoreCase(value)){
					out.append("2").append("|");
				}else if("ACTIVATE".equalsIgnoreCase(value)){
					out.append("3").append("|");
				}
			}else if(ConstantDefine.KEY_DEVICE_OPERATE_DATE.equalsIgnoreCase(key)){
				//操作时间
				Date date = DateUtil.convertStringToDate(ConstantDefine.COMMON_DATE_FORMAT, value);
				value = DateUtil.convertDateToString(ConstantDefine.COMMON_DATE_FORMAT, date);
				out.append(value).append("|");
			}else if(ConstantDefine.KEY_DEVICE_IPADDRESS.equalsIgnoreCase(key)){
				//IP地址
				out.append(value).append("|");
			}else{
				out.append("|");
			}
		}
		//系统来源
		out.append("1").append("|");
		//Fsource
		out.append("1");
		context.write(NullWritable.get(),new Text(out.toString()));
	}





}
