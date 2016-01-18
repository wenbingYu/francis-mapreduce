package com.francis.common;

import java.util.List;

public class ConvertBlackList {


	public static String getsplitCategory(String expr) {

		if (expr == null || "".equals(expr))
			return null;
		if (expr.contains(","))// 老的表达式为，分隔
			return expr;
		List<String> blacks = BlackList.process(expr);
		StringBuilder sb = new StringBuilder();
		for (String name : blacks) {
			if (sb.length() > 0)
				sb.append(";").append(name);
			else
				sb.append(name);
		}
		return sb.toString();

	}
	
	
	public static void main(String[] args){
		System.out.println(ConvertBlackList.getsplitCategory("10110&10116&(10125|10126)&(10131|10130)&10140&15397"));
		
	}

}
