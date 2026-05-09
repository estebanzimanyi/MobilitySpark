/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark.temporal;

import org.junit.jupiter.api.*;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpanAlgebraUDFs — span/set topology predicates and algebra.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanAlgebraUDFsTest {

    // Integer spans: [1,10), [5,15), [20,30), [2,5)
    private static String SPAN_1_10;
    private static String SPAN_5_15;
    private static String SPAN_20_30;
    private static String SPAN_2_5;
    private static String SPAN_1_5;
    private static String SPAN_6_10;

    // Integer spansets
    private static String SPANSET_1_5__7_10;
    private static String SPANSET_3_8;

    // Integer sets
    private static String INTSET_1234;
    private static String INTSET_3456;

    // Tstzspan for distance test
    private static String TSTZSPAN_JAN;
    private static String TSTZSPAN_MAR;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        SPAN_1_10  = span_as_hexwkb(intspan_in("[1, 10)"),  (byte) 0);
        SPAN_5_15  = span_as_hexwkb(intspan_in("[5, 15)"),  (byte) 0);
        SPAN_20_30 = span_as_hexwkb(intspan_in("[20, 30)"), (byte) 0);
        SPAN_2_5   = span_as_hexwkb(intspan_in("[2, 5)"),   (byte) 0);
        SPAN_1_5   = span_as_hexwkb(intspan_in("[1, 5)"),   (byte) 0);
        SPAN_6_10  = span_as_hexwkb(intspan_in("[6, 10)"),  (byte) 0);

        SPANSET_1_5__7_10 = spanset_as_hexwkb(intspanset_in("{[1,5),[7,10)}"), (byte) 0);
        SPANSET_3_8       = spanset_as_hexwkb(intspanset_in("{[3,8)}"),         (byte) 0);

        INTSET_1234 = set_as_hexwkb(intset_in("{1, 2, 3, 4}"), (byte) 0);
        INTSET_3456 = set_as_hexwkb(intset_in("{3, 4, 5, 6}"), (byte) 0);

        TSTZSPAN_JAN = span_as_hexwkb(tstzspan_in("[2020-01-01, 2020-01-05)"), (byte) 0);
        TSTZSPAN_MAR = span_as_hexwkb(tstzspan_in("[2020-01-10, 2020-01-15)"), (byte) 0);
    }

    // ── Span topology predicates ───────────────────────────────────────────────

    @Test @Order(1)
    void spanContains_inner_span_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanContains.call(SPAN_1_10, SPAN_2_5));
    }

    @Test @Order(2)
    void spanContains_disjoint_span_returns_false() throws Exception {
        assertFalse(SpanAlgebraUDFs.spanContains.call(SPAN_1_10, SPAN_20_30));
    }

    @Test @Order(3)
    void spanContainedIn_inner_is_contained() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanContainedIn.call(SPAN_2_5, SPAN_1_10));
    }

    @Test @Order(4)
    void spanOverlaps_overlapping_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanOverlaps.call(SPAN_1_10, SPAN_5_15));
    }

    @Test @Order(5)
    void spanOverlaps_disjoint_returns_false() throws Exception {
        assertFalse(SpanAlgebraUDFs.spanOverlaps.call(SPAN_1_5, SPAN_6_10));
    }

    @Test @Order(6)
    void spanAdjacent_adjacent_spans() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanAdjacent.call(SPAN_1_5, SPAN_5_15));
    }

    @Test @Order(7)
    void spanLeft_left_span_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanLeft.call(SPAN_1_5, SPAN_6_10));
    }

    @Test @Order(8)
    void spanRight_right_span_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanRight.call(SPAN_6_10, SPAN_1_5));
    }

    @Test @Order(9)
    void spanOverleft_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanOverleft.call(SPAN_1_10, SPAN_5_15));
    }

    @Test @Order(10)
    void spanOverright_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanOverright.call(SPAN_5_15, SPAN_1_10));
    }

    // ── Span algebra ──────────────────────────────────────────────────────────

    @Test @Order(11)
    void spanUnion_returns_spanset_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spanUnion.call(SPAN_1_5, SPAN_6_10);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(12)
    void spanIntersection_overlapping_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spanIntersection.call(SPAN_1_10, SPAN_5_15);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(13)
    void spanIntersection_disjoint_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.spanIntersection.call(SPAN_1_5, SPAN_20_30));
    }

    @Test @Order(14)
    void spanMinus_returns_spanset_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spanMinus.call(SPAN_1_10, SPAN_5_15);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(15)
    void tstzspanDistance_disjoint_is_positive() throws Exception {
        Double d = SpanAlgebraUDFs.tstzspanDistance.call(TSTZSPAN_JAN, TSTZSPAN_MAR);
        assertNotNull(d);
        assertTrue(d > 0.0, "Gap between Jan and Mar spans should be positive seconds");
    }

    // ── Spanset predicates ────────────────────────────────────────────────────

    @Test @Order(16)
    void spansetContainsSpan_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spansetContainsSpan.call(SPANSET_1_5__7_10, SPAN_2_5));
    }

    @Test @Order(17)
    void spanContainedInSpanset_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spanContainedInSpanset.call(SPAN_2_5, SPANSET_1_5__7_10));
    }

    @Test @Order(18)
    void spansetOverlaps_overlapping_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.spansetOverlaps.call(SPANSET_1_5__7_10, SPANSET_3_8));
    }

    // ── Spanset algebra ───────────────────────────────────────────────────────

    @Test @Order(19)
    void spansetUnion_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spansetUnion.call(SPANSET_1_5__7_10, SPANSET_3_8);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(20)
    void spansetIntersection_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spansetIntersection.call(SPANSET_1_5__7_10, SPANSET_3_8);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(21)
    void spansetMinus_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.spansetMinus.call(SPANSET_1_5__7_10, SPANSET_3_8);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Set predicates ────────────────────────────────────────────────────────

    @Test @Order(22)
    void setContains_superset_returns_true() throws Exception {
        // {1,2,3,4} contains {3,4} subset — use a 2-element set
        String INTSET_34 = set_as_hexwkb(intset_in("{3, 4}"), (byte) 0);
        assertTrue(SpanAlgebraUDFs.setContains.call(INTSET_1234, INTSET_34));
    }

    @Test @Order(23)
    void setOverlaps_overlapping_sets_returns_true() throws Exception {
        assertTrue(SpanAlgebraUDFs.setOverlaps.call(INTSET_1234, INTSET_3456));
    }

    // ── Set algebra ───────────────────────────────────────────────────────────

    @Test @Order(24)
    void setUnion_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.setUnion.call(INTSET_1234, INTSET_3456);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(25)
    void setIntersection_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.setIntersection.call(INTSET_1234, INTSET_3456);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(26)
    void setMinus_returns_hexwkb() throws Exception {
        String result = SpanAlgebraUDFs.setMinus.call(INTSET_1234, INTSET_3456);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(27)
    void null_input_returns_null() throws Exception {
        assertNull(SpanAlgebraUDFs.spanContains.call(null, SPAN_2_5));
        assertNull(SpanAlgebraUDFs.spanUnion.call(null, SPAN_5_15));
        assertNull(SpanAlgebraUDFs.setOverlaps.call(null, INTSET_3456));
    }
}
