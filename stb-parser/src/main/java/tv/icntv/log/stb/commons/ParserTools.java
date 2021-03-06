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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/05/16
 * Time: 16:06
 */
public class ParserTools {
    public static final Map<String,Object> toArgMap(Object... args) {
        if (args == null) {
            return null;
        }
        if (args.length % 2 != 0) {
            throw new RuntimeException("expected pairs of argName argValue");
        }
        HashMap<String,Object> res = new HashMap<String,Object>();
        for (int i = 0; i < args.length; i += 2) {
            if (args[i + 1] != null) {
                res.put(String.valueOf(args[i]), args[i + 1]);
            }
        }
        return res;
    }
}
