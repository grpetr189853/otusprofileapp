package twitter.files;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class FileUtils {
    public static void writeToFileFromQueue(File file, BlockingQueue<String> queue){
        if(file.canWrite()){
            try {
                FileWriter writer = new FileWriter(file,true);
                writer.write(queue.take());
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static File createFile(String path){
        File resultFile = new File(path);
        try {
            resultFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultFile;
    }
}
