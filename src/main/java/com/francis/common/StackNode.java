package com.francis.common;

/**
 * 定义栈的结构
 * @author 洪婷婷
 * */

class StackNode<T> {

	private T content;
	private StackNode<T> link;

	public T getContent() {
		return content;
	}

	public void setContent(T content) {
		this.content = content;
	}

	public StackNode<T> getLink() {
		return link;
	}

	public void setLink(StackNode<T> link) {
		this.link = link;
	}

}
