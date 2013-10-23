/* Copyright 2013 Future TV, Inc.
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

package tv.icntv.logsys.mapper;


import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.logsys.Parser.LogMapperParser;
import java.io.IOException;

/**
* Created with IntelliJ IDEA.
* User: lei
* Date: 13-10-17
* Time: 上午11:41
* To change this template use File | Settings | File Templates.
*/
public abstract class IcntvHdfsNoWithReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper implements LogMapperParser {

    protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
        String[] array=getSplit(value);
        if(null == array|| array.length==0){
            //TODO log
            return ;
        }
        parser(array,getConf(),context);
          //To change body of overridden methods use File | Settings | File Templates.
    }

    protected abstract String[] getSplit(Object value);

}
