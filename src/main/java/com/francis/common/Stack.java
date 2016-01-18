package com.francis.common;

/**
 * 栈的入栈、出栈等操作
 * @author 洪婷婷
 * */

class Stack<T> {	

	private StackNode<T> top;
	private int index = -1;     //假设栈底为-1，同时也表示栈的序列号

	public void push(T content) {    //入栈
		StackNode<T> node = new StackNode<T>();
		node.setContent(content);
		node.setLink(top);
		top = node;
		index++;
	}

	public StackNode<T> pop() {      //出栈
		if (index == -1) {
//			System.out.println("stack hasn't content");
			return null;
		}
		StackNode<T> node = this.getTop();
		top = node.getLink();
		index--;
		return node;
	}

	public StackNode<T> getTop() {     //取栈顶元素
		return top;
	}

	public int getIndex() {       //取栈顶的位置
		return index;
	}

}
