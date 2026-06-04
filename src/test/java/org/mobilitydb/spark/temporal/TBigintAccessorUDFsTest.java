package org.mobilitydb.spark.temporal;

import org.junit.jupiter.api.*;
import org.mobilitydb.spark.MeosTestBase;

import static functions.GeneratedFunctions.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TBigintAccessorUDFsTest extends MeosTestBase {

    // tbigint instant set with values 1, 5, 3 → min=1, max=5, start=1, end=3
    private static String TBIGINT;

    @BeforeAll
    static void build() throws Exception {
        TBIGINT = temporal_as_hexwkb(
            tbigint_in("{1@2000-01-01, 5@2000-01-02, 3@2000-01-03}"), (byte) 0);
    }

    @Test @Order(1)
    void minValue_returns_one() throws Exception {
        assertEquals(1L, AccessorUDFs.tbigintMinValue.call(TBIGINT));
    }

    @Test @Order(2)
    void maxValue_returns_five() throws Exception {
        assertEquals(5L, AccessorUDFs.tbigintMaxValue.call(TBIGINT));
    }

    @Test @Order(3)
    void startValue_returns_one() throws Exception {
        assertEquals(1L, AccessorUDFs.tbigintStartValue.call(TBIGINT));
    }

    @Test @Order(4)
    void endValue_returns_three() throws Exception {
        assertEquals(3L, AccessorUDFs.tbigintEndValue.call(TBIGINT));
    }

    @Test @Order(5)
    void nullInput_returns_null() throws Exception {
        assertNull(AccessorUDFs.tbigintMinValue.call(null));
    }
}
