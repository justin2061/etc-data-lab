package etc.dataprocess.main;

public class CSVImportMain {

	public static void main(String[] args) throws Exception {
		org.apache.camel.spring.Main main = new org.apache.camel.spring.Main();
		main.setApplicationContextUri("csv-import-context.xml");
		//main.enableHangupSupport();
		main.run();
	}

}
