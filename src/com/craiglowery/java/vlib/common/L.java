package com.craiglowery.java.vlib.common;

public class L {
	public static boolean LOG = true;
	public static final int D=0;
	public static final int I=1;
	public static final int W=2;
	public static final int E=3;
	
	public static final String ENTER= "ENTER";
	public static final String EXIT= "EXIT";
	public static final String STUB= "STUB";
	
	public static void log(int level,Object sender, String format, Object... arguments) {
		if (LOG) {
			StackTraceElement[] stack = new Exception().getStackTrace();
			String msg = "";
			if (format==ENTER || format==STUB) {  //OK to do object equal here, as we use ENTER consistently in this context
				msg=(String.format("%s (%x): %s from %s",
					stack[1].getMethodName(),sender.hashCode(), format, stack[2].getMethodName()));
			} else {
				msg=(
					String.format("%s (%x): ",stack[1].getMethodName(),sender.hashCode()) +
					String.format(format,arguments)
					);
			}
			switch (level) {
			case D: U_Exception.logger.debug(msg); break;
			case I: U_Exception.logger.info(msg); break;
			case W: U_Exception.logger.warn(msg);break;
			default: U_Exception.logger.error(msg); break;
			}
		}
	}

}
