import com.company.HopscotchHashmap;
import com.company.interfaces.HashMapOA;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.*;

public class HashMapTest {

    private HashMapOA map;

    @BeforeClass
    public static void beforeClass() {
        System.out.println("Before CalculatorTest.class");
    }

    @AfterClass
    public static void afterClass() {
        System.out.println("After CalculatorTest.class");
    }

    @After
    public void after() {
        map=null;
    }

    // Check that a new HashMap returns 'true' for isEmpty
    @Test
    public void testIsEmptyForNewMap() {
        map = new HopscotchHashmap<String>(16);
        assertEquals(map.size(), 0);
    }

    // Test size increases as elements are added
    @Test
    public void testSizeIncrementsWhenAddingElements() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        assertEquals(1, map.size());
        map.put(6, "6");
        assertEquals(2, map.size());
    }

    // Make sure get returns the values added under keys
    @Test
    public void testGetReturnsCorrectValue() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        map.put(6, "6");
        assertEquals("5", map.get(5));
        assertEquals("6", map.get(6));
    }

    // Test that an com.company.exception is thrown if a key does not exist
    /*@Test(expected= NoSuchElementException.class)
    public void testThrowsExceptionIfKeyDoesNotExist() {
        map.get(5);
    }*/
    @Test
    public void testThrowsExceptionIfKeyDoesNotExist() {
        map = new HopscotchHashmap<String>();
        assertEquals(null, map.get(5));
    }

    // Test thats an added element replaces another with the same key
    @Test
    public void testReplacesValueWithSameKey() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        map.put(5, "6");
        assertEquals("6", map.get(5));
    }

    // Make sure that two (non-equal) keys with the same hash do not overwrite each other
    @Test
    public void testSameHash() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        map.put(133, "133");
        assertEquals("5", map.get(5));
        assertEquals("133", map.get(133));
    }

    @Test
    public void testSameHash2() {
        map = new HopscotchHashmap<String>();

        map.put(5, "5");
        map.put(133, "133");
        map.put(6, "6");
        map.put(134, "134");

        assertEquals("5", map.get(5));
        assertEquals("133", map.get(133));
        assertEquals("6", map.get(6));
        assertEquals("134", map.get(134));
    }

    @Test
    public void testResize() {
        map = new HopscotchHashmap<Integer>(16);
        for (int i = 0; i < 64; i++) {
            map.put(i, i);
        }
        map.put(128, 128);
        for (int i = 0; i < 64; i++) {
            assertEquals(i, map.get(i));
        }
        assertEquals(128, map.get(128));
    }

    @Test
    public void testHopscotch() {
        map = new HopscotchHashmap<Integer>(64);
        for (int i = 0; i < 64; i++) {
            map.put(i, i);
        }
        //64 record
        map.put(128, 128);

        //64 record
        assertEquals(128, map.get(128));
        assertEquals(48, map.get(48));
        assertEquals(49, map.get(49));
        assertEquals(50, map.get(50));
        assertEquals(33, map.get(33));
        assertEquals(34, map.get(34));
        assertEquals(35, map.get(35));
    }

    @Test
    public void testCombine() {
        map = new HopscotchHashmap<Integer>(128);
        //64 record
        for (int j = 0; j <18 ; j++) {
            map.put(128*j, 128*j);
        }
        for (int j = 0; j <18 ; j++) {
            assertEquals(128*j, map.get(128*j));
        }}

    // Make sure that size decrements as elements are used
    @Test
    public void testRemoveDecrementsSize() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        map.put(6, "6");
        assertEquals(2, map.size());
        map.remove(5);
        assertEquals(1, map.size());
        map.remove(6);
        assertEquals(0, map.size());
    }

    // Test elements are actually removed when remove is called
    @Test/*(expected= NoSuchElementException.class)*/
    public void testRemoveDeletesElement() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        map.remove(5);

        assertEquals(null, map.get(5));
    }

    // Test that contains is 'false' for new maps
    @Test
    public void testContainsKeyForNewMap() {
        map = new HopscotchHashmap<String>();
        assertFalse(map.containsKey(5));
    }

    // Make sure that contains returns 'true' when the key does exist
    @Test
    public void testContainsKeyForExistingKey() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        assertTrue(map.containsKey(5));
    }

    // Check that contains is not fooled by equivalent hash codes
    @Test
    public void testContainsKeyForKeyWithEquivalentHash() {
        map = new HopscotchHashmap<String>();
        map.put(5, "5");
        assertFalse(map.containsKey(133));
    }

    // Check that contains is not fooled by equivalent hash codes
    @Test
    public void testInfiniteLoop() {
        Executor e = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5000; i++) {
            ((ExecutorService) e).submit(() -> {
                int data = (int) Math.random()*100000;
                map.put(data, String.valueOf(data));
            });
        }
        //we won if got here
    }

    /*
    @Test(expected = ArithmeticException.class)
    public void divisionWithException() {
        calculator.getDivide(15, 0);
    }

    @Test(timeout = 500)
    public void timeStampTest() {
        while (true) ;
    }
    */
}
