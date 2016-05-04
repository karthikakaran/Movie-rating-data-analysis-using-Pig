import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE extends EvalFunc <String> {
    @Override
    public String exec(Tuple input) {
                String formattedStr = "";
        try {
            if (input == null || input.size() == 0) {
                return null;
            }
	String str = (String) input.get(0);
        String splitStr[] = str.split("\\|");
            for(int i = 0; i < splitStr.length; i++){
                formattedStr += (i+1)+") "+splitStr[i];
                if(i != 0 && i == splitStr.length - 2){
                        formattedStr += " & ";
                } else if(i == 0 && i == splitStr.length - 2){
                        formattedStr += " & ";
                } else if(i != 0 && i < splitStr.length - 2){
                        formattedStr += ", ";
                } else if (i == 0 && i != splitStr.length - 1){
                        formattedStr += ", ";
                }
            }
	return formattedStr;
        } catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }
	return null;
    }
}

