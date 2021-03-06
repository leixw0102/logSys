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

package tv.icntv.logsys.Parser;

import org.apache.hadoop.mapreduce.Reducer;
import tv.icntv.logsys.config.LogConfigurable;
import tv.icntv.logsys.xmlObj.XmlLog;

/**
 * Created with IntelliJ IDEA.
 * User: lei
 * Date: 13-10-17
 * Time: 下午1:15
 * To change this template use File | Settings | File Templates.
 */
public interface LogReducerParser extends LogConfigurable{

   // public void parser(Iterable values,XmlLog configuration,Reducer.Context context) ;

    public void parser(Object key,Iterable values,XmlLog configuration,Reducer.Context context) ;
}
