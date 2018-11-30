package cl.cvaldex.rockholiday.lambda.vo;

import java.io.InputStream;

public class TweetVO {
	private int id;
	private String text;
	private String date;
	private String author;
	private InputStream image1;
	private InputStream image2;
	private InputStream image3;
	private InputStream image4;
	
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public InputStream getImage1() {
		return image1;
	}
	public void setImage1(InputStream image1) {
		this.image1 = image1;
	}
	public InputStream getImage2() {
		return image2;
	}
	public void setImage2(InputStream image2) {
		this.image2 = image2;
	}
	public InputStream getImage3() {
		return image3;
	}
	public void setImage3(InputStream image3) {
		this.image3 = image3;
	}
	public InputStream getImage4() {
		return image4;
	}
	public void setImage4(InputStream image4) {
		this.image4 = image4;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 * @to-do refactorizar a StringBuilder
	 */
	public String toString(){
		StringBuilder builder = new StringBuilder();
		
		builder.append(id);
		builder.append(" - ");
		builder.append(date);
		builder.append(" - ");
		builder.append(author);
		builder.append(" - ");
		builder.append(text);
		
		return builder.toString();
	}
	
	public String getAuthor() {
		return author;
	}
	public void setAuthor(String author) {
		this.author = author;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	
}
