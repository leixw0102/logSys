package tv.icntv.log.stb.cdn;/*
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.log.stb.commons.StringsUtils;

import java.io.IOException;
import java.util.List;

/**
 * Author: wangliang
 * Date: 2014/06/26
 * Time: 15:56
 * current version:统分二期需求文档1.5
 */
@Deprecated
public class CdnModuleMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements CdnModule {
    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try{
            if(null == value|| Strings.isNullOrEmpty(value.toString())){
                return;
            }
            String[] values= value.toString().split(SPLIT_T);
            if(null == values || values.length!=16){
                return;
            }

            if(null == values[14] || !"TaskState".equals(values[14])){
                System.out.println("跳过非TaskStatus");
                return;
            }

            String content=values[15];
            if(Strings.isNullOrEmpty(content)){
                return;
            }

            //过滤异常日志

            String[] contentArr = content.split("\\\\r");

            if(contentArr == null ){
                System.out.println("contentArr长度错误");
                return;
            }
            boolean flag1 = false;
            int i;
            String[] arrRow = null;
            for(i = 0;i<contentArr.length;i++){
                arrRow = contentArr[i].split(COMMA_SIGN);
                if(arrRow.length >= 4){
                    flag1 = true;
                    break;
                }
            }
            if(!flag1){
                return;
            }




            String[] arrTemp;
            arrTemp = arrRow[0].split(":");
            if(!arrTemp[0].trim().matches("\\d+")){
                System.out.println("task 编号错误!!"+arrTemp[0]);
                return;
            }
            if(null == arrTemp[1] || arrTemp[1].trim().length() == 0){
                System.out.println("获取task内容错误!!"+arrTemp[1]);
                return;
            }

            String status = arrTemp[1];
            if("confail".equals(status) || "nofile".equals(status) || "srvclose".equals(status)
                    || "srverr".equals(status) || "timeout".equals(status) || "error".equals(status)){
                return;
            }

            StringBuffer stringBuffer=new StringBuffer();


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

            //8.ConnectResult	节点连接情况 1.成功 2.超时 3.失败 4.302跳转
            stringBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);

            //9.transDomain	302跳转域名
            if(arrRow[3].indexOf("(")>0){
                stringBuffer.append(StringsUtils.getEncodeingStr(arrRow[3].substring(0,arrRow[3].indexOf("(")))).append(SPLIT);
            }else{
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            }

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
            if(arrTemp[1].indexOf("/")>0){
                stringBuffer.append(StringsUtils.getEncodeingStr(arrTemp[1].substring(0,arrTemp[1].indexOf("/")))).append(SPLIT);
            }else{
                stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            }

            //16.Reserved1	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //17.Reserved2	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            //18.Reserved3	保留字段
            stringBuffer.append(StringsUtils.getEncodeingStr(EMPTY));

            values = null;
            content = null;
            contentArr = null;
            arrTemp = null;
            arrRow = null;
            status = null;
            context.write(NullWritable.get(),new Text(stringBuffer.toString()));
        }catch (Exception e){
            return;
        }
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
