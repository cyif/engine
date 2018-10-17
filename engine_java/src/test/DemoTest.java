import com.alibabacloud.polar_race.engine.common.EngineRace;
import junit.framework.TestCase;

import java.util.List;
import java.util.Random;


public class DemoTest extends TestCase {

    private static final int THREAD_NUM = 64;
    private static final int OPT_NUM_PER_THREAD = 10000;

    private EngineRace engine = new EngineRace();

    public void testWriteAndRead() throws Exception{
        engine.open("data/");
        Thread[] threads = new Thread[THREAD_NUM];

        // Write
        for (int i = 0; i < THREAD_NUM; i++) {
            final long seed = 10000000000L * i;
            threads[i] = new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                for (int i = 0; i < OPT_NUM_PER_THREAD; i++) {
                                    List<byte[]> keyValue = ValueUtil.generateKeyValue(seed + i);
                                    engine.write(keyValue.get(0), keyValue.get(1));
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }

        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].join();
        }

        // Read
        final Random random = new Random();
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i] = new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                for (int i = 0; i < OPT_NUM_PER_THREAD; i++) {
                                    long key = 10000000000L * random.nextInt(THREAD_NUM) + random.nextInt(OPT_NUM_PER_THREAD);
                                    List<byte[]> keyValue = ValueUtil.generateKeyValue(key);
                                    byte[] value = engine.read(keyValue.get(0));
                                    if (!ValueUtil.isEqual(keyValue.get(1), value)) {
                                        System.out.println("Wrong Value");
                                        System.exit(-1);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].join();
        }
    }
}
