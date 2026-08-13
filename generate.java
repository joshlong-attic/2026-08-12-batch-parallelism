import java.io.* ;
import java.util.*;

void main() throws Exception {  
 try (var f =  new BufferedWriter(new OutputStreamWriter( 
 			new FileOutputStream("/Users/jlong/Desktop/data.csv")))) {
 	for (var i = 0; i < 5 * 5 * 1000 * 1000 ; i++) {
 		f.write("" + (Math.random() * System.currentTimeMillis()  )+ 
 					System.lineSeparator());
 	}
 }
}