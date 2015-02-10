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
package tv.icntv.log.stb.epgoperate;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.log.stb.commons.DateUtils;
import tv.icntv.log.stb.commons.StringsUtils;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangliang
 * Author: wangliang
 * Date: 2014/12/19
 * Time: 10:00
 */
public class EPGOperateMapper extends Mapper<LongWritable,Text,NullWritable,Text> implements EPGOperate {

    /**
     *
     * @param key
     * @param value 字符串例子及对应的字段描述如下（括号中为字段名称及描述）：
     *      010121008091051201406170359240060001（log_id	日志编号）
     *      872（seq_id	请求编号）
     *      0（retry_seqid	重试请求编号）
     *      010121008091051（device_id	设备编号）
     *      4.3.5.13-JX:1（version_id	终端版本号）
     *      JX_E3E420140513001（platform_id	平台号）
     *      0c:c6:55:10:64:73（mac	终端mac）
     *      123.75.39.185（ip	终端ip）
     *      2014-06-17 04:00:36 700（req_time	终端发送日志请求的时间）
     *      2014-06-17 03:59:40 218（cur_time	日志产生时的终端时间）
     *      2014-06-17 03:58:27 524（client_time	日志产生时的服务器时间）(operate time暂取这个)
     *      2014-06-17 03:59:24 006（server_time	接收到日志时的服务器时间）
     *      1（level	日志等级）
     *      106（module	模块）
     *      AppLog（action	操作）
     *      PageName=EPGHistory,EPGID=968228,DatePoint=0（content	日志内容）
     *
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null == value|| Strings.isNullOrEmpty(value.toString())){
            return;
        }
        String[] values= value.toString().split(SPLIT_T);
        if(null == values || values.length!=16){
            return;
        }
        //日志内容，例：PageName=EPGHistory,EPGID=968228,DatePoint=0
        String content=values[15].trim();

        //日志为空字符串的舍去
        if(Strings.isNullOrEmpty(content)){
            return;
        }

        //有些content包含"content="这样的前缀，需要去掉，只保留content内容
        if(content.startsWith(CONTENT_PREFIX)){
            content=content.replace(CONTENT_PREFIX,"");
        }

        String[] contentArr = content.split(COMMA_SIGN);

        if(contentArr==null || contentArr.length<=0 ){
            System.out.println("contentArr为空");
            return ;
        }

        //EPGOperateDomain epgOperateDomain = new EPGOperateDomain();
        StringBuffer resultBuffer = new StringBuffer();
        //--1.cntvid
        resultBuffer.append(values[3].trim()).append(SPLIT);

        if(PAGE_NAME_PANEL.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            if("URL=panelhome".equalsIgnoreCase(contentArr[1].trim())&&contentArr.length==2){
                //--2.PageName
                resultBuffer.append(StringsUtils.getEncodeingStr("panel")).append(SPLIT);
                //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
                resultBuffer.append(StringsUtils.getEncodeingStr("1")).append(SPLIT);
                //--4.action
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--5.url
                resultBuffer.append(StringsUtils.getEncodeingStr("panelhome")).append(SPLIT);
                //--6.categoryID
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--7.progatherID
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--8.subjectId
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--9.subjectName
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--10.programid
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                //--11.Keyword
                resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);

            }else if("Action=OpenUrl".equalsIgnoreCase(contentArr[1].trim())&&contentArr.length==3){
                if(contentArr.length==3&&contentArr[2].indexOf("action=EPGList")>0){
                    //--2.PageName
                    resultBuffer.append(StringsUtils.getEncodeingStr("panel")).append(SPLIT);
                    //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
                    resultBuffer.append(StringsUtils.getEncodeingStr("3")).append(SPLIT);
                    //--4.action
                    resultBuffer.append(StringsUtils.getEncodeingStr("OpenUrl")).append(SPLIT);
                    //--5.url
                    resultBuffer.append(StringsUtils.getEncodeingStr(StringUtils.substringAfter(contentArr[2].trim(), EQUAL_SIGN))).append(SPLIT);
                    //--6.categoryID
                    resultBuffer.append(contentArr[2].substring(contentArr[2].indexOf("action=EPGList&object=")+22)).append(SPLIT);
                    //--7.progatherID
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--8.subjectId
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--9.subjectName
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--10.programid
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--11.Keyword
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                }else if(contentArr.length==3&&contentArr[2].indexOf("action=detail")>0){
                    //--2.PageName
                    resultBuffer.append(StringsUtils.getEncodeingStr("panel")).append(SPLIT);
                    //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
                    resultBuffer.append(StringsUtils.getEncodeingStr("2")).append(SPLIT);
                    //--4.action
                    resultBuffer.append(StringsUtils.getEncodeingStr("OpenUrl")).append(SPLIT);
                    //--5.url
                    resultBuffer.append(contentArr[2].trim().substring(4)).append(SPLIT);
                    //--6.categoryID
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--7.progatherID
                    resultBuffer.append(contentArr[2].substring(contentArr[2].indexOf("action=detail&object=")+21)).append(SPLIT);
                    //--8.subjectId
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--9.subjectName
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--10.programid
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                    //--11.Keyword
                    resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
                } else {
                    //未知的记录舍去
                    return;
                }

            }else{
                //未知的记录舍去
                return;
            }
        }else if(PAGE_NAME_WEB_INDEX.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr("WebIndex")).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("5")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        }else if(PAGE_NAME_EPG_LIST.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr("EPGList")).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("3")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        }else if(PAGE_NAME_EPG_DETAIL.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr("EPGDetail")).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("2")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        }else if(PAGE_NAME_EPG_SEARCH.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr(PAGE_NAME_EPG_SEARCH)).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("6")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)).append(SPLIT);
        }else  if(PAGE_NAME_EPG_HISTORY.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr(PAGE_NAME_EPG_HISTORY)).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("7")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
        }else if(PAGE_NAME_ALBUM.equalsIgnoreCase(StringUtils.substringAfter(contentArr[0].trim(), EQUAL_SIGN))){
            //--2.PageName
            resultBuffer.append(StringsUtils.getEncodeingStr(PAGE_NAME_ALBUM)).append(SPLIT);
            //--3.targetObjectType:1.panel页面,2.节目集详情页,3.栏目,4.面板项,5.网动版首页 (推荐位或列表位)  6．搜索  7.历史记录  8.专题  99.其他
            resultBuffer.append(StringsUtils.getEncodeingStr("8")).append(SPLIT);
            //--4.action
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--5.url
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--6.categoryID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--7.progatherID
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--8.subjectId
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--9.subjectName
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--10.programid
            resultBuffer.append(StringsUtils.getEncodeingStr(EMPTY)).append(SPLIT);
            //--11.Keyword
            resultBuffer.append(StringUtils.substringAfter(contentArr[1].trim(), EQUAL_SIGN)).append(SPLIT);



        }else{

			return;
        }

        //--12.opTime
        resultBuffer.append(DateUtils.getFormatDate(values[10].trim())).append(SPLIT);
        //--13.DataSource
        resultBuffer.append(DATA_SOURCE).append(SPLIT);
        //--14.Fsource
        resultBuffer.append(F_SOURCE);

        context.write(NullWritable.get(),new Text(resultBuffer.toString()));
    }

    @Override
    public List<String> getKeys() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
