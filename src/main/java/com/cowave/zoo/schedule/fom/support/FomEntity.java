package com.cowave.zoo.schedule.fom.support;

import java.util.Collection;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomEntity<T> {

//	public static final int SUCCESS = 200;
//
//	public static final int FAILED = 501;

	private int code;

	private String msg;

	private T data;

	public static <V> FomEntity<V> success(){
        return new FomEntity<>(200);
    }

	public static <V> FomEntity<V> success(V data){
        return new FomEntity<>(200, data);
    }

	public static <V> FomEntity<V> instance(int code, String msg){
		return new FomEntity<>(code, msg, null);
	}

	public FomEntity(int code){
		this.code = code;
	}

	public FomEntity(int code, T data){
		this.code = code;
		this.data = data;
	}

	public FomEntity(int code, String msg, T data){
		this.code = code;
		this.msg = msg;
		this.data = data;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public int getCode() {
		return code;
	}

	public String getMsg() {
		return msg;
	}

	@Override
	public String toString() {
		if(data != null){
			return data.toString();
		}
		return "";
	}

	public static class Page<V> {

		private final int total;

		private Collection<V> list;

		public Page(Collection<V> list, int total){
			this.list = list;
			this.total = total;
		}

		public int getTotal() {
			return total;
		}

		public Collection<V> getList() {
			return list;
		}

		public void setList(Collection<V> list) {
			this.list = list;
		}

		@Override
		public String toString() {
			return "{total=" + total + ", list=" + list + "}";
		}
	}
}
