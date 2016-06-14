package etc.dataprocess.test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatTest {

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static void main(String[] args) throws Exception {
		DateFormatTest dft = new DateFormatTest();
		
		String dateStr = "2014-01-01 04:02:30";
		System.out.println(dft.formatESDate(dateStr));
	}
	
	private java.util.Date formatESDate(String dateString) throws Exception{
		if(dateString.trim().length() == 0){
			throw new Exception("input date string is empty");
		}
		
		Date dd = new Date();
		try {
			dd = sdf.parse(dateString);
		} catch (Exception e) {
			String err = dateString+", err: "+e.getLocalizedMessage();
			System.err.print(err);
			throw new Exception(err);
		}
		return dd;
		//return sdf.parse(dateString);
	}

}
