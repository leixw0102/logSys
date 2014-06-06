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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/06/03
 * Time: 09:30
 */
public class Test {
    static  String str="abc,sdf,sss`ddd,sds`sdf";
    public static void main(String[]args){
       int size= Splitter.on(CharMatcher.is(',').or(CharMatcher.is('`'))).splitToList(str).size();
        System.out.println(size);
        int size1= Splitter.on(CharMatcher.is('`').or(CharMatcher.is(','))).splitToList(str).size() ;
        System.out.println(size1);
    }
}
