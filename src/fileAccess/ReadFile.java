package fileAccess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadFile {

	/**
	 * Returns all lines in the specified file.
	 * @param filename
	 * @return
	 */
	public static List<String> readLines(String filename, String comment) {
		BufferedReader br;
		List<String> lines = new ArrayList<String>();
		String line;
		try {
			br = new BufferedReader(new FileReader(new File(filename)));
			while((line = br.readLine())!=null) {
				if (!line.startsWith(comment)) {
					lines.add(line);
				}
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;
	}
}
