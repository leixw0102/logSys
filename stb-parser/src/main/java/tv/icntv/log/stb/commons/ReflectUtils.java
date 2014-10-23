package tv.icntv.log.stb.commons;/*
 * Copyright 2014 Future TV, Inc.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.icntv.tv/licenses/LICENSE-1.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import tv.icntv.log.stb.cdnModule.CdnStbDomain;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/10/23
 * Time: 14:16
 */
public class ReflectUtils {
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


    public static boolean setFieldValue(Field field, Object obj, String[] values) {
        if (values == null || values.length == 0) {
            return false;
        }
        Class<?> type = field.getType();
        try {
            Method setter = getSetterMethod(field);
            if (type == String.class) {
                setter.invoke(obj, values);
            } else if (type == Integer.class || type == int.class) {
                if("".equals(values[0])){
                    setter.invoke(obj, Integer.valueOf(0));
                }else{
                    setter.invoke(obj, Integer.valueOf(values[0].trim().replace(",","")));
                }
            } else if (type == Long.class || type == long.class) {
                setter.invoke(obj, Long.valueOf(values[0]));
            } else if (type == Float.class || type == float.class) {
                setter.invoke(obj, Float.valueOf(values[0]));
            } else if (type == Double.class || type == double.class) {
                setter.invoke(obj, Double.valueOf(values[0]));
            } else if (type == Date.class) {
                setter.invoke(obj, DATE_FORMAT.parse(values[0]));
            }
        } catch (Exception e) {
            throw new RuntimeException("Set field " + field.getName() + " error, value is '" + values[0] + "'.", e);
        }
        return true;
    }

    /**
     * Get setter method for field from class
     *
     * @param field
     *      original field for which to get setter method
     * @return
     * @throws NoSuchMethodException
     */
    public static Method getSetterMethod(Field field) throws NoSuchMethodException {
        String setter = "set" + StringsUtils.firstLetterCapital(field.getName());
        return field.getDeclaringClass().getDeclaredMethod(setter, field.getType());
    }

    public static void main(String[]args) throws NoSuchFieldException, IllegalAccessException, InstantiationException {
        CdnStbDomain domain = new CdnStbDomain();
        ReflectUtils.setFieldValue(domain.getClass().getDeclaredField("dnsRedList"), domain, new String[]{"dfs"});
        ReflectUtils.setFieldValue(domain.getClass().getDeclaredField("conMinTime"), domain, new String[]{"23"});
        System.out.println(domain.getDnsRedList()+"\t"+domain.getConMinTime());
    }
}
