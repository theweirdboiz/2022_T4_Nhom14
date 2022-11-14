package dao;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;

public interface CreateTimeDim {
	public static final String OUT_FILE = "time_dim.csv";
	public static final int HOUR = 24;
	public static final int MINUTE = 60;
	public static final int SECOND = 60;

	public static final String TIME_ZONE = "PST8PDT";
	public File file = new File(OUT_FILE);

	public static boolean create() throws FileNotFoundException {
//		int minute = -1;
//		int hour = 0;
//		int second = -1;
		PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file)));
		int count = 1;
		for (int i = 0; i < 24; i++) {
			for (int j = 0; j < 60; j++) {
				writer.println(count++ + "," + (i + ":" + j));
			}
		}
		writer.flush();
		return file.length() > 0;
	}
}
