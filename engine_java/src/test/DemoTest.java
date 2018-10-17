import com.alibabacloud.polar_race.engine.common.utils.ByteToInt;
import com.alibabacloud.polar_race.engine.common.utils.ConcurrencyHashTable;
import com.alibabacloud.polar_race.engine.common.utils.Key;
import com.alibabacloud.polar_race.engine.common.utils.PutHashSpinLock;

public class DemoTest {
    public static void main(String[] args){

        ConcurrencyHashTable<Key, byte[]> map = new ConcurrencyHashTable<Key, byte[]>(1024, 1024);

        String str1 = "abcdefg";
        String str2 = "acd3231";
        map.put(new Key(str1.getBytes()), ByteToInt.intToByteArray(3333));
        map.put(new Key(str2.getBytes()), ByteToInt.intToByteArray(33323));
        map.put(new Key(str1.getBytes()), ByteToInt.intToByteArray(33133));

        System.out.println(ByteToInt.byteArrayToInt(map.get(new Key(str1.getBytes()))));
        System.out.println(ByteToInt.byteArrayToInt(map.get(new Key(str2.getBytes()))));
        System.out.println(ByteToInt.byteArrayToInt(map.get(new Key(str1.getBytes()))));
    }
}
