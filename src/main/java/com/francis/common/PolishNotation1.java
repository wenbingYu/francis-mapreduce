package com.francis.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

/**
 * 逆波兰解析人群属性表达式，并返回互斥属性结果集合
 * @author 洪婷婷
 * */

class PolishNotation1 {
	
	//黑名单的互斥规则
	private final static Map<String,String[]> BLACKLIST = 
			new HashMap<String,String[]>();
	static{
		BLACKLIST.put("10110", new String[]{"10111"});
		BLACKLIST.put("10111", new String[]{"10110"});
		BLACKLIST.put("10114", new String[]{"10115","10116","10117","10118","10123","10125","10126","10127","10130","10131","16753","10142","10146","10147","10148","10149","15397","15398","13807","17972","17971","17976","17973","10134","16755","17977","17978","19230","16756","19231","17974","13808","17975"});
		BLACKLIST.put("10115", new String[]{"10114","10116","10117","10118","15397","15398","10131"});
		BLACKLIST.put("10116", new String[]{"10114","10115","10117","10118","15398","10129","10145"});
		BLACKLIST.put("10117", new String[]{"10114","10115","10116","10118","10129","10145","15398","10147","10148"});
		BLACKLIST.put("10118", new String[]{"10114","10115","10116","10117","10129","10145","10147","10148","10149"});
		BLACKLIST.put("10120", new String[]{"10123","10125","10126","10127","10131"});
		BLACKLIST.put("10123", new String[]{"10114","10120","10125","10126","10127","10131"});
		BLACKLIST.put("10125", new String[]{"10114","10120","10123","10126","10127","10129","10145"});
		BLACKLIST.put("10126", new String[]{"10114","10120","10123","10125","10127","10129","10145"});
		BLACKLIST.put("10127", new String[]{"10114","10120","10123","10125","10126","10129","10145"});
		BLACKLIST.put("10129", new String[]{"10116","10117","10118","10130","10131","16753","13807","17972","17971","17976","17973","10134","16755","17977","17978","19230","16756","19231","17974","13808","17975","10147","10148","10149","15397","15398"});
		BLACKLIST.put("10130", new String[]{"10114","10131","16753","15398"});
		BLACKLIST.put("10131", new String[]{"10114","10115","10120","10129","10145","16753","10130","10123"});
		BLACKLIST.put("16753", new String[]{"10114","10129","10145","10131","16754"});
		BLACKLIST.put("10138", new String[]{"10140","10142","10134"});
		BLACKLIST.put("10140", new String[]{"10114","10138","10142"});
		BLACKLIST.put("10142", new String[]{"10114","10138","10140"});
		BLACKLIST.put("10145", new String[]{"10116","10117","10118","10130","10131","16753","13807","17972","17971","17976","17973","10134","16755","17977","17978","19230","16756","19231","17974","13808","17975","10147","10148","10149","15397","15398"});
		BLACKLIST.put("10146", new String[]{"10114","15398"});
		BLACKLIST.put("10147", new String[]{"10114","10117","10118","10129","10145","15398"});
		BLACKLIST.put("10148", new String[]{"10114","10117","10118","10129","10145","15398"});
		BLACKLIST.put("10149", new String[]{"10114","10118","10129","10145","15398"});
		BLACKLIST.put("15397", new String[]{"10114","10115","10129","10145"});
		BLACKLIST.put("15398", new String[]{"10114","10115","10116","10117","10129","10145","10148","10149","10130"});
		BLACKLIST.put("13807", new String[]{"10114","10129","10145","10134","13808","16755","16756","17971","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17972", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17971", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17976", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17975","17977","17978","19230","19231"});
		BLACKLIST.put("17973", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("10134", new String[]{"10114","10129","10145","13807","13808","16755","16756","17971","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("16755", new String[]{"10114","10129","10145","10134","13807","13808","16756","17971","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17977", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17975","17976","17978","19230","19231"});
		BLACKLIST.put("19230", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17975","17976","17977","17978","19231"});
		BLACKLIST.put("16756", new String[]{"10114","10129","10145","10134","13807","13808","16755","17971","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("19231", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17975","17976","17977","17978","19230"});
		BLACKLIST.put("17974", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("13808", new String[]{"10114","10129","10145","10134","13807","16755","16756","17971","17972","17973","17974","17975","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17975", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17976","17977","17978","19230","19231"});
		BLACKLIST.put("17978", new String[]{"10114","10129","10145","10134","13807","13808","16755","16756","17971","17972","17973","17974","17975","17976","17977","19230","19231"});
	}

	public List<String> parseExpression(String expression) {
		//先将表达式分割成操作符与数字并放入temp列表
		StringTokenizer st = new StringTokenizer(expression, "&|()", true);
		List<String> temp = new ArrayList<String>();
		while (st.hasMoreElements()) {
			temp.add((String) st.nextElement());
		}
		List<String> polisthNotationlist = new ArrayList<String>();
		Stack<String> stack = new Stack<String>();
		for (String tempCh : temp) {
			if (isOperator(tempCh)) {
				//栈为空或操作符为(就入栈
				if (stack.getIndex() == -1 || "(".equals(tempCh)) {
					stack.push(tempCh);
				} else {
					if (")".equals(tempCh)) {
						//如果操作符为)就将栈内)之前(之后的操作符弹出并放入逆波兰列表,最后弹出(
						while (!stack.getTop().getContent().equals("(")) {
							String operator = stack.getTop().getContent();
							polisthNotationlist.add(operator);
							stack.pop();
						}
						stack.pop();
					} else {
						//如果栈顶元素为(,操作符入栈
						if (stack.getTop().getContent().equals("(")) {
							stack.push(tempCh);
						} else {
							//当栈不为空,且栈顶元素不为(时,比较操作符的优先级,
							//如果栈顶元素优于当前操作符,那么弹出栈顶元素,
							//直到栈为空或栈顶元素优先级低于当前操作符,则将当前操作符压入栈
							while (stack.getIndex() != -1 
									&& !stack.getTop().getContent().equals("(")){
								if (isPriority(operatorPriority(stack.getTop()
										.getContent()),
										operatorPriority(tempCh))) {
									String operator = stack.getTop().getContent();
									polisthNotationlist.add(operator);
									stack.pop();
									if (stack.getIndex() == -1
											|| !isPriority(
													operatorPriority(stack.
															getTop().
															getContent()),
													operatorPriority(tempCh))) {
										stack.push(tempCh);
										break;
									}
								} else {
									stack.push(tempCh);
									break;
								}
							}
						}
					}
				}
			} else {  
				//非操作符直接入栈
				polisthNotationlist.add(tempCh);
			}
		}
		while (stack.getIndex() != -1) {   //栈中余下的操作符弹出
			polisthNotationlist.add(stack.pop().getContent());
		}
		return polisthNotationlist;
	}

	//计算逆波兰表达式的值
	public List<String> calculatePolishNotation(List<String> polishNotation) {
 		Stack<List<String>> stack = new Stack<List<String>>();
		for (String str : polishNotation) {
			if (!this.isOperator(str)) {
				stack.push(matchDigital(str));
			} else {
				List<String> number1 = stack.pop().getContent();
				List<String> number2 = stack.pop().getContent();
				stack.push(calculate(str, number2, number1));
			}
		}
		return stack.pop().getContent();
	}
	
	private List<String> patternMatch(List<String> Arr1,
			List<String> Arr2,int typeID){ 
		//typeID为0时，求“&”，做并集运算；typeID为1时，求“|”，做交集运算
		ArrayList<String> tmp  = new ArrayList<String>();
		int j ;
//		if (Arr1.size() == 0 && Arr2.size() != 0 ) return Arr2;
//		if (Arr1.size() != 0 && Arr2.size() == 0 ) return Arr1;
		for(int i = 0; i< Arr1.size(); ++i){
			for(j = 0; j < Arr2.size(); ++j){
				if (Arr2.get(j).equals(Arr1.get(i))){
					if(typeID == 1)   //求交集
						tmp.add(Arr2.get(j));
					break;
				}
			}
			if ( j == Arr2.size() && typeID == 0){ 
				//求并集，先取出了Arr1中Arr2没有的元素，即Arr1中除去他们共有的元素
				tmp.add(Arr1.get(i));
			}
		}
		if(typeID == 0){  //求并集，再加上Arr2本身的元素，组成了Arr1和Arr2的并集
			for(int i = 0; i < Arr2.size();++i){
				tmp.add(Arr2.get(i));
			}
		}  
		return tmp;
	}
   	
	private List<String> matchDigital(String pattern){
		List<String> bArr = new ArrayList<String>();
		Iterator<Entry<String, String[]>> iter=BLACKLIST.entrySet().iterator();
		while(iter.hasNext()){
		    Entry<String, String[]> entry=iter.next();
		    String key= entry.getKey();
		    String[] val=entry.getValue();
		    if (!pattern.equals(key.toString())){
		    	continue;
		    }else{
		    	for(int i = 0; i < val.length; i++){
		    		bArr.add(val[i]);		    	
			    }
		    	break;
		    }
		}
		return bArr;
	}
	
	//计算普通表达式
	private List<String> calculate(String operator, List<String> number1,
			List<String> number2) {
		char opt = operator.charAt(0);
		switch (opt) {
		case '&':			
			return patternMatch(number1, number2,0);	
		case '|':
			return patternMatch(number1, number2,1);
		case '!':
			break;
		}
		return null;
	}
	
	//设置操作符优先级，（）设不设无所谓，因为在解析的时候已经有根据（）做判断了
	private int operatorPriority(String operator) { 
		if ("(".equals(operator)) {
			return 1;
		}
		if (")".equals(operator)) {
			return 2;
		}
		if ("|".equals(operator)) {
			return 3;
		}
		if ("&".equals(operator) ){
			return 4;
		}
		return 0;
	}

	//栈顶操作符与列表中操作符比较优先级
	private boolean isPriority(int topOperator, int listOperator) {  
		if (topOperator >= listOperator) { 
			return true;
		}
		return false;
	}

	//判断是否为操作符
	private boolean isOperator(String arg) {
		if ("&|()".indexOf(arg) != -1) {
			return true;
		}
		return false;
	}
}
