package fileAccess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteToFile {
	
	public static void writeln(String filename, boolean append, String s) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(new File(filename), append));
			out.write(s);
			out.newLine();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
