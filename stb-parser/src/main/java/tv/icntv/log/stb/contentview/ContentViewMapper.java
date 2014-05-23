package tv.icntv.log.stb.contentview;/*
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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.log.stb.commons.StringsUtils;
import tv.icntv.log.stb.util.DateUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 15:56
 */
public class ContentViewMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements ContentView {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null == value|| Strings.isNullOrEmpty(value.toString())){
            return;
        }
        String[] values= value.toString().split(SPLIT_T);
        if(null == values || values.length!=16){
            return;
        }
        String content=values[15];
        if(Strings.isNullOrEmpty(content)){
            return;
        }
        if(content.startsWith(CONTENT_PREFIX)){
            content=content.replace(CONTENT_PREFIX,"");
        }

        String[] contentArr = content.split(COMMA_SIGN);

	    if(contentArr==null || contentArr.length<=0){
		    System.out.println("contentArr为空");
		    return ;
	    }

	    StringBuffer stringBuffer=new StringBuffer();

	    //1.CNTVID用户序列号
	    stringBuffer.append(StringsUtils.getEncodeingStr(values[3])).append(SPLIT);

		//2.OperateTtype	操作类型 1:开始 2:结束
        String operateTime = "";
        if (contentArr[4] != null && EMPTY.equals(contentArr[4])) {
            stringBuffer.append(StringsUtils.getEncodeingStr("2")).append(SPLIT);
            operateTime = DateUtil.convertDateToString( "yyyyMMddHHmmss",
                    DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",StringUtils.substringAfter(contentArr[4].trim(), EQUAL_SIGN)));
        }
        if (contentArr[1] != null && EMPTY.equals(contentArr[1])) {
            stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
            operateTime = DateUtil.convertDateToString( "yyyyMMddHHmmss",
                    DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)));
        }

        //3.OperateTime	操作时间,格式是：YYYYMMDDHH24MISS
        if (operateTime == null || EMPTY.equals(operateTime)) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(operateTime)).append(SPLIT);
        }

        //4.ServiceType	业务类型：1:点播；
        stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);

        //5.VideoType	视频类型1：普通视频 2.: 微视频
        stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[6].trim(), EQUAL_SIGN))).append(SPLIT);

        //6.Flux	网络流量  单位（KB）
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //7.EndReason 流媒体服务中断原因
        if (null == contentArr[2].trim() || EMPTY.equals(contentArr[2].trim())) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN))).append(SPLIT);
        }

        //8.ChargeType 计费类型  1：免费；2：收费
        if (null == contentArr[7].trim() || EMPTY.equals(contentArr[7].trim())) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[7].trim(), EQUAL_SIGN))).append(SPLIT);
        }

        //TODO 9.CategoryID	栏目ID
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //10.ProgatherID	节目集ID唯一
        stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[14].trim(), EQUAL_SIGN)+EMPTY)).append(SPLIT);

        //11.ProgramID	节目ID唯一
        stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[8].trim(), EQUAL_SIGN)+EMPTY)).append(SPLIT);

        //12.subjectID 专题id 目前为空
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //13.EPGCode EPG版本编号,见EPGCode版本编号表
        if (contentArr[13].trim().equals("FOUR")){
            stringBuffer.append(StringsUtils.getEncodeingStr("04")).append(SPLIT);
        }else if (contentArr[13].trim().equals("FIVE")) {
            stringBuffer.append(StringsUtils.getEncodeingStr("05")).append(SPLIT);
        }else {
            stringBuffer.append(StringsUtils.getEncodeingStr("06")).append(SPLIT);
        }
        stringBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[13].trim(), EQUAL_SIGN)+EMPTY)).append(SPLIT);

        //14.std_path 目前没有
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //15.VODPathType 收视来源实际路径
        stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);

		//16.DataSource系统来源1：易视腾2：云立方
	    stringBuffer.append(DATA_SOURCE).append(SPLIT);
        //17.Fsource数据来源，见数据来源表
        stringBuffer.append(F_SOURCE).append(SPLIT);

        //18.resolution 视频码率
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //19.Remark1 保留字段
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //20.Remark2 保留字段
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

	    context.write(NullWritable.get(),new Text(stringBuffer.toString()));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	    String s = "014444000014031201405010501059180038    3188    0       014444000014031 :       2720130807163420732     24:69:A5:7B:CE:99       183.12.65.153   2014-05-01 05:01:05 996 2014-05-0\n" +
			    "1 04:58:40 056  2014-05-01 04:58:39 978 2014-05-01 05:01:05 918 1       100     PlayLog content=OperType=05,ProGatherID=942407,ProgramID=6890867,TimeLine=1926120";
        super.cleanup(context);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getKeys() {
        return Lists.newArrayList(KEY_PLAYER_TIMELINE,KEY_PLAYER_OPERTYPE,KEY_PLAYER_PROGATHERID,KEY_PLAYER_PROGRAMID);
    }

}
