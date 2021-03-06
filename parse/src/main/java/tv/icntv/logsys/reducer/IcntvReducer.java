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

package tv.icntv.logsys.reducer;

import org.apache.hadoop.hbase.mapreduce.TableReducer;

import tv.icntv.logsys.Parser.LogMapperParser;
import tv.icntv.logsys.Parser.LogReducerParser;

import java.io.IOException;

/**
* Created with IntelliJ IDEA.
* User: lei
* Date: 13-10-17
* Time: 上午11:48
* To change this template use File | Settings | File Templates.
*/
public abstract class IcntvReducer<KEYIN, VALUEIN, KEYOUT> extends TableReducer implements LogReducerParser {
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        parser(key,values,getConf(),context);
    }
}
