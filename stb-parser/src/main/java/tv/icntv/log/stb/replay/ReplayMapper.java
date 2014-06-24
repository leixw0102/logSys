package tv.icntv.log.stb.replay;/*
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
import java.util.List;

/**
 * Author: wangliang
 * Date: 2014/06/04
 * Time: 10:56
 * current version:统分二期需求文档1.5
 */
public class ReplayMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements Replay {
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

	    if(contentArr == null || contentArr.length != 3){
		    System.out.println("contentArr长度错误");
		    return ;
	    }

	    StringBuffer stringBuffer=new StringBuffer();
        String operateType = "";
        String operateTime = "";
        String uuid = "";


        //1.CNTVID用户序列号
        stringBuffer.append(StringsUtils.getEncodeingStr(values[3])).append(SPLIT);

        //2.（回看终端的）IP地址
        if (null == values[7] || EMPTY.equals(values[7])) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(values[7].trim())).append(SPLIT);
        }

        //3.OperateTtype	操作类型 1:开始 2:结束
        operateType = StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN);
        if (null == operateType || EMPTY.equals(operateType)) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        }else if("on".equals(operateType)){
            stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
        }else if("out".equals(operateType)){
            stringBuffer.append(StringsUtils.getEncodeingStr("2")).append(SPLIT);
        }

        // 4.operateTime  操作时间
        operateTime = DateUtil.convertDateToString( "yyyyMMdd HHmmss",DateUtil.convertStringToDate("yyyy-MM-dd HH:mm:ss SSS",values[10].trim()));
        if (operateTime == null || EMPTY.equals(operateTime)) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(operateTime)).append(SPLIT);
        }

        //5.channel频道
        uuid =  StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN);
        if (uuid == null || EMPTY.equals(uuid)) {
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        } else {
            stringBuffer.append(StringsUtils.getEncodeingStr(uuid)).append(SPLIT);
        }

        //6.programName节目名称 目前日志中无此字段
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

        //programId 节目id
//        programId =  StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN);
//        if(!programId.matches("\\d+")){ //节目id只能包含数字否则启用本条日志
//            return;
//        }
//
//        if (programId == null || EMPTY.equals(programId)) {
//            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));
//        } else {
//            stringBuffer.append(StringsUtils.getEncodeingStr(programId));
//        }

        //7.EPGCode EPG版本编号,见EPGCode版本编号表
        stringBuffer.append(StringsUtils.getEncodeingStr("06")).append(SPLIT);

        //8.DataSource系统来源1：易视腾2：云立方
        stringBuffer.append(DATA_SOURCE).append(SPLIT);

        //9.Fsource数据来源，见数据来源表
        stringBuffer.append(F_SOURCE).append(SPLIT);

        //10.resolution 视频码率,目前无此字段
        stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));

        values = null;
        content = null;
        contentArr = null;
        operateTime = null;
        operateType = null;
        uuid = null;

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
