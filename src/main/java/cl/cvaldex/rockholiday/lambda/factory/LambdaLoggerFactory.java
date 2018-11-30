package cl.cvaldex.rockholiday.lambda.factory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class LambdaLoggerFactory {
	class DummyLogger implements LambdaLogger{

		public void log(String string) {
			System.out.println(string);
		}
		
	}
	
	public static LambdaLogger getLogger(Context context){
		LambdaLoggerFactory factory = new LambdaLoggerFactory();
		LambdaLogger logger = factory.new DummyLogger();
		
		if(context != null){
			logger = context.getLogger();
		}
		
		return logger;			
	}
}


