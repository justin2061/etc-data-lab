package etc.dataprocess.main;

public class InventoryMain {

	public static void main(String[] args) throws Exception {
		org.apache.camel.spring.Main main = new org.apache.camel.spring.Main();
		main.setApplicationContextUri("main-context.xml");
		//main.enableHangupSupport();
		main.run();
	}

}
