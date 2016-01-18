package com.francis.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	// 获得当前日期的前一天日期
	public static String[] getBeforeDate() {
		String[] strdate = new String[2];
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance(); // 得到日历
		calendar.setTime(new Date());// 把当前时间赋给日历
		calendar.add(Calendar.DAY_OF_MONTH, -1); // 设置为前一天
		Date dBefore = calendar.getTime(); // 得到前一天的时间
		String defaultStartDate = sdf.format(dBefore);
		strdate[0] = defaultStartDate;

		SimpleDateFormat sdff = new SimpleDateFormat("yyyy/MM/dd");
		String d = sdff.format(dBefore);
		strdate[1] = d;

		// 当前时间戳
		SimpleDateFormat sdfcurrent = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		String currentDate = sdfcurrent.format(new Date());
		strdate[2] = currentDate;
		return strdate;
	}

}
