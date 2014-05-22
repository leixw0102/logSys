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
package tv.icntv.log.stb.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wang.yong
 * Author: wang.yong
 * Date: 2014/05/21
 * Time: 10:11
 */
public class DateUtil {

	public static Date convertStringToDate(String dateFormat, String date){
		SimpleDateFormat sf = new SimpleDateFormat(dateFormat);
		Date d = null;
		if(dateFormat==null || dateFormat.trim().length()<=0 || date==null || date.trim().length()<=0){
			return d;
		}
		try {
			d = sf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return d;
	}

	public static String convertDateToString(String dateFormat, Date date){
		SimpleDateFormat sf = new SimpleDateFormat(dateFormat);
		String dateStr = "";
		if(date==null){
			return dateStr;
		}
		dateStr = sf.format(date);

		return dateStr;
	}
}
