
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class ReadTextFileLineByLine {

	public static void main(String[] args) {
		try {
			File file = new File("src/Weather.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			String year;
			String temperature;
			HashMap hm=new HashMap(); 
			while ((line = bufferedReader.readLine()) != null) {
				stringBuffer.append(line);
				stringBuffer.append("\n");
				year=line.substring(15, 19);
			    temperature=line.substring(line.length()-18,line.length()-13);
			    hm.put(year, temperature);
			    System.out.println(year);
			    System.out.println(temperature);
			}
			fileReader.close();
			System.out.println("Contents of file:");
			System.out.println(stringBuffer.toString());
			//Iterator iterator=hm.iterator();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}