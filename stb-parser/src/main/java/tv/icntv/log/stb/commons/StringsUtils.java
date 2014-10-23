package tv.icntv.log.stb.commons;/*
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
import org.apache.commons.lang.StringUtils;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/22
 * Time: 17:30
 */
public class StringsUtils {
    public static String getEncodeingStr(String str){
        if (!Strings.isNullOrEmpty(str)){
            str = str.replace("%", "%25");
            str = str.replace("|", "%7C");
        }
        return str;
    }
    public static String getLogParam(String key, String join, String split, String content) {
        if (StringUtils.isBlank(content) || StringUtils.isBlank(key)) {
            return null;
        }
        String result = StringUtils.substringAfter(content, key.trim() + join);
        if (StringUtils.isBlank(result)) {
            return null;
        }
        if (result.contains(split)) {
            result = StringUtils.substringBefore(result, split);
        }
        if (StringUtils.isNotBlank(result)) {
            result = result.replace("\r","")
                    .replace("\n", "");
        }
        return result;
    }

    public static String firstLetterCapital(String src) {
        if (src.length() == 1) {
            return src.toUpperCase();
        }
        return src.substring(0, 1).toUpperCase() + src.substring(1);
    }
}
