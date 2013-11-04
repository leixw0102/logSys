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

package tv.icntv.logsys.utils;

import org.apache.hadoop.conf.Configuration;
import tv.icntv.logsys.ParserJob;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-10-29
 * Time: 上午11:33
 */
public class ReflectionUtils {

    public static Object newInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(null == className){
            throw new NullPointerException("class name null");
        }
        ClassLoader loader=null;

           loader=Thread.currentThread().getContextClassLoader();
            if(null == loader){
                loader=  ReflectionUtils.class.getClassLoader();
            }
          return loader.loadClass(className).newInstance();

    }
    public static void main(String[]args) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ParserJob job= (ParserJob) newInstance("tv.icntv.logsys.cdn.CdnMapper");
//        job.test();
    }
}
