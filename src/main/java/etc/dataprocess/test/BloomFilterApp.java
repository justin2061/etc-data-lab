package etc.dataprocess.test;

import java.util.Random;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFilterApp {
    static Random random = new Random();

    
    /**
     * ref: http://eugenedvorkin.com/probabilistic-data-structures-bloom-filter-and-hyperloglog-for-big-data/
     * @param args
     */
    public static void main(String[] args) {
        // convert object into funnel - specific to google implementation
        int totals=100000;
        Funnel<String> funnel = new Funnel<String>() {
            public void funnel(String s, PrimitiveSink primitiveSink) {
                primitiveSink.putString(s, Charsets.UTF_8);
            }
        };

        BloomFilter bloomFilter = BloomFilter.create(funnel, totals);
        for (int i = 0; i < totals; i++) {
            Integer value = random.nextInt(10000);
            // add only even number to bloom filter
            if ((value % 2) == 0) {
                String key = "key" + value;
                //insert only even values into bloom filter
                bloomFilter.put(key);

            }

        }
       // check if key exist in bloom filter
        String key = "key100";
        System.out.println(bloomFilter.mightContain(key));
        assert (bloomFilter.mightContain(key)) == true;

        String key2 = "key5";
        System.out.println(bloomFilter.mightContain(key2));
        assert (bloomFilter.mightContain(key2) == false);
    }
}
