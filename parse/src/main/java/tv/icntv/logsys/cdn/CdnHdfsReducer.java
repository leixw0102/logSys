package tv.icntv.logsys.cdn;/*
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

/**
 * Created by leixw   value==ip</>url</>status</>startTime</>endTime</>flow</>ua
 * <p/>
 * Author: leixw
 * Date: 2014/03/03
 * Time: 16:03
 */
public class CdnHdfsReducer extends Reducer<Text,Text,Text,Text> {
    private final String split="</>";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] arrRow = null;
        Long longBeginDateTime = 0L;
        Long longEndDateTime = 0L;

        int rowNum=0;
        Long longDateTimeTemp = 0L;

        Long byteTemp = 0L;

        String ip="",url="",status="",ua="";
        for(Iterator<Text> it=values.iterator();it.hasNext();){
            //合并
            Text text = it.next();
            arrRow = text.toString().split(split);

            //解析日期
            String strDateTime = arrRow[1];
            DateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.UK);
            Date dt2 = null;
            try {
                dt2 = format.parse(strDateTime);
                longDateTimeTemp = dt2.getTime();
            } catch (ParseException e) { e.printStackTrace();}

            //获取分组中最大和最小日期
            if(rowNum==0){
                longBeginDateTime = longDateTimeTemp;
                longEndDateTime = longDateTimeTemp;
                ip=arrRow[0];
                url=arrRow[2];
                status=arrRow[3];
                ua=arrRow[5];
            }else{
                if(longDateTimeTemp < longBeginDateTime){
                    longBeginDateTime = longDateTimeTemp;
                }
                if(longDateTimeTemp > longEndDateTime){
                    longEndDateTime = longDateTimeTemp;
                }
            }
            byteTemp += Long.parseLong(arrRow[4]);//

            rowNum++;
        }
        //value ==  ip</>url</>status</>startTime</>endTime</>flow</>ua
        context.write(key,new Text(ip+split+url+split+status+split+longBeginDateTime+split+longEndDateTime+split+byteTemp+split+ua));
    }
}
