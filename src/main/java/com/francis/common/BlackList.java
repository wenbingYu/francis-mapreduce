/**
 * 
 */
package com.francis.common;

import java.util.List;

/**
 * 人群属性黑名单计算
 * @author lijt
 *
 */
public class BlackList {
	
	private static PolishNotation1 pn = new PolishNotation1();
	
	/** 解析人群属性表达式，计算对应的黑名单
	 * @param expr 人群属性表达式
	 * @return 对应黑名单
	 */
	public static List<String> process(String expr){
		List<String> list = pn.parseExpression(expr);
		return pn.calculatePolishNotation(list);
	}
	
	public static void main(String [] args)
	{
		String expr = "(10059|10093|11281|11379|13793|13496)&(10116|13861|10130)";
		List<String> list = process(expr);
		System.out.println(list.size());
		System.out.println(list.toString());
		
	}
}
