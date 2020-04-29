package twitter;

import com.twitter.hbc.httpclient.BasicClient;
import twitter.files.FileUtils;
import twitter.tweeutils.ClientSupplier;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class TweeterReader {
    private static final String CONSUMER_KEY = "GPArF3hn3QxTf9v6GrwAczcGO";
    private static final String CONSUMER_SECRET = "7dUNMoPKPWtQZ7TcuhoqWTkuTpkSgRqSGk9vPBfXWPMK8NMXxn";
    private static final String TOKEN = "1908368161-IXatUs3pv25qo6C8gy2Rl7LPWzIz9PsCo9cG45l";
    private static final String SECRET = "S20EJhj1db8MIF5xpv5P9UxvCjBTA5A8JTtreeEZNLKei";
    //Нет смысла в большой очереди - скорость её заполнения напрямую зависит от данных, приходящих по соединению
    public static final int QUEUE_CAPACITY = 50;
    public static final int FILES_COUNT = 50;
    public static final int ROWS_PER_FILE = 100;
    //Делать большое кол-во потоков тоже смысла особого нет, т.к. в итоге всё упирается в плотность потока данных
    //При увеличении кол-ва потоков - увеличивается время простоя каждого, т.е. большую часть времени поток стоит в ожидании данных в очереди
    public static final int CORE_POOL_SIZE = 2;

    public void run() {
        final BlockingQueue<String> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        final BasicClient client = ClientSupplier.prepareAndConnectDefaultClient(CONSUMER_KEY,
                CONSUMER_SECRET,
                TOKEN,
                SECRET,
                queue);
        //Т.к. кол-во файлов жёстко ограничено определённым кол-вом, то есть смысл создать файлы сразу в отдельном потоке,
        // добавив их в очередь
        final BlockingQueue<File> files = new ArrayBlockingQueue<>(FILES_COUNT);
        final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newScheduledThreadPool(CORE_POOL_SIZE);
        executeCreateFilesTask(files, executor);
        for (int i = 0; i < FILES_COUNT; i++) {
            executor.execute(() -> {
                int rowCount = 0;
                try {
                    final File file = files.take();
                    while (!client.isDone() && rowCount < ROWS_PER_FILE) {
                        //Вообще, идея с постоянным открытием/закрытием файла очень сомнительна,
                        // однако при проверке на Ubuntu разницы в производительности при записи в файл
                        // постфактум замечено не было, так что можно оставить и так.
                        FileUtils.writeToFileFromQueue(file,queue);
                        rowCount++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        executor.shutdown();
    }

    private void executeCreateFilesTask(BlockingQueue<File> files, ThreadPoolExecutor executor) {
        executor.execute(() -> {
            for (int i = 0; i < FILES_COUNT; i++) {
                final File file = FileUtils.createFile("/home/alex/profile/result" + i + ".txt");
                files.offer(file);
            }
        });
    }

    public static void main(String[] args) throws Exception {
        TweeterReader reader = new TweeterReader();
        reader.run();
    }
}
