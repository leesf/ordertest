package com.hust.grid.leesf.ordertest.common;

/**
 * 商品对
 * 
 * @author leesf
 *
 */
public class ItemPair {
	private String item1;
	private String item2;

	public ItemPair(String item1, String item2) {
		this.item1 = item1;
		this.item2 = item2;
	}

	public String getItem1() {
		return item1;
	}

	public String getItem2() {
		return item2;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (this == obj)
			return true;
		if (obj instanceof ItemPair == false)
			return false;

		ItemPair itemPair = (ItemPair) obj;

		return this.item1 != null && this.item1.equals(itemPair.item1) && this.item2 != null
				&& this.item2.equals(itemPair.item2);
	}

	@Override
	public int hashCode() {
		int h1 = 0;
		int h2 = 0;

		if (item1 != null)
			h1 = item1.hashCode();
		if (item2 != null)
			h2 = item2.hashCode();
		return h1 + h2;
	}

	public String toString() {
		return item1 + ":" + item2;
	}
}
