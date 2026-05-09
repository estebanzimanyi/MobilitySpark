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
 * Unit tests for AccessorUDFs — temporal accessor and manipulation operations.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AccessorUDFsTest {

    private static String TINT_HEX;
    private static String TINT_HEX2;
    private static String TFLOAT_HEX;
    private static String TBOOL_HEX;
    private static String TTEXT_HEX;
    private static String TPOINT_HEX;
    private static String TSTZSPAN_HEX;
    private static String TSTZSPANSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]"), (byte) 0);
        TINT_HEX2 = temporal_as_hexwkb(
            tint_in("[3@2020-01-03 00:00:00+00, 4@2020-01-04 00:00:00+00]"), (byte) 0);
        TFLOAT_HEX = temporal_as_hexwkb(
            tfloat_in("[1.5@2020-01-01 00:00:00+00, 3.0@2020-01-02 00:00:00+00]"), (byte) 0);
        TBOOL_HEX = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"), (byte) 0);
        TTEXT_HEX = temporal_as_hexwkb(
            ttext_in("[hello@2020-01-01 00:00:00+00, world@2020-01-02 00:00:00+00]"), (byte) 0);
        TPOINT_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 1)@2020-01-02 00:00:00+00]"), (byte) 0);
        TSTZSPAN_HEX = span_as_hexwkb(
            tstzspan_in("[2020-01-01, 2020-01-02)"), (byte) 0);
        TSTZSPANSET_HEX = spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01, 2020-01-02), [2020-01-03, 2020-01-04)}"), (byte) 0);
    }

    @Test @Order(1)
    void numSequences_single_sequence_returns_1() throws Exception {
        assertEquals(1, AccessorUDFs.numSequences.call(TINT_HEX));
    }

    @Test @Order(2)
    void interp_linear_sequence_returns_linear() throws Exception {
        String interp = AccessorUDFs.interp.call(TINT_HEX);
        assertNotNull(interp);
        assertFalse(interp.isEmpty());
    }

    @Test @Order(3)
    void time_returns_spanset_hexwkb() throws Exception {
        String ss = AccessorUDFs.time.call(TINT_HEX);
        assertNotNull(ss);
        assertFalse(ss.isEmpty());
    }

    @Test @Order(4)
    void timespan_returns_span_hexwkb() throws Exception {
        String sp = AccessorUDFs.timespan.call(TINT_HEX);
        assertNotNull(sp);
        assertFalse(sp.isEmpty());
    }

    @Test @Order(5)
    void merge_two_segments_returns_hexwkb() throws Exception {
        String merged = AccessorUDFs.merge.call(TINT_HEX, TINT_HEX2);
        assertNotNull(merged);
        assertFalse(merged.isEmpty());
    }

    @Test @Order(6)
    void shift_by_one_day_returns_hexwkb() throws Exception {
        String shifted = AccessorUDFs.shift.call(TINT_HEX, "1 day");
        assertNotNull(shifted);
        assertFalse(shifted.isEmpty());
    }

    @Test @Order(7)
    void scale_to_two_days_returns_hexwkb() throws Exception {
        String scaled = AccessorUDFs.scale.call(TINT_HEX, "2 days");
        assertNotNull(scaled);
        assertFalse(scaled.isEmpty());
    }

    @Test @Order(8)
    void atSpan_overlapping_returns_hexwkb() throws Exception {
        String result = AccessorUDFs.atSpan.call(TINT_HEX, TSTZSPAN_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(9)
    void atSpanset_overlapping_returns_hexwkb() throws Exception {
        String result = AccessorUDFs.atSpanset.call(TINT_HEX, TSTZSPANSET_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(10)
    void insert_two_segments_returns_hexwkb() throws Exception {
        String result = AccessorUDFs.insert.call(TINT_HEX, TINT_HEX2);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(11)
    void update_with_second_segment_returns_hexwkb() throws Exception {
        String result = AccessorUDFs.update.call(TINT_HEX, TINT_HEX2);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(12)
    void tintStartValue_returns_1() throws Exception {
        assertEquals(Integer.valueOf(1), AccessorUDFs.tintStartValue.call(TINT_HEX));
    }

    @Test @Order(13)
    void tintEndValue_returns_2() throws Exception {
        assertEquals(Integer.valueOf(2), AccessorUDFs.tintEndValue.call(TINT_HEX));
    }

    @Test @Order(14)
    void tintMinValue_returns_1() throws Exception {
        assertEquals(Integer.valueOf(1), AccessorUDFs.tintMinValue.call(TINT_HEX));
    }

    @Test @Order(15)
    void tintMaxValue_returns_2() throws Exception {
        assertEquals(Integer.valueOf(2), AccessorUDFs.tintMaxValue.call(TINT_HEX));
    }

    @Test @Order(16)
    void tfloatStartValue_returns_1_5() throws Exception {
        assertEquals(1.5, AccessorUDFs.tfloatStartValue.call(TFLOAT_HEX), 1e-9);
    }

    @Test @Order(17)
    void tfloatEndValue_returns_3() throws Exception {
        assertEquals(3.0, AccessorUDFs.tfloatEndValue.call(TFLOAT_HEX), 1e-9);
    }

    @Test @Order(18)
    void tfloatMinValue_returns_1_5() throws Exception {
        assertEquals(1.5, AccessorUDFs.tfloatMinValue.call(TFLOAT_HEX), 1e-9);
    }

    @Test @Order(19)
    void tfloatMaxValue_returns_3() throws Exception {
        assertEquals(3.0, AccessorUDFs.tfloatMaxValue.call(TFLOAT_HEX), 1e-9);
    }

    @Test @Order(20)
    void tboolStartValue_returns_true() throws Exception {
        assertTrue(AccessorUDFs.tboolStartValue.call(TBOOL_HEX));
    }

    @Test @Order(21)
    void tboolEndValue_returns_false() throws Exception {
        assertFalse(AccessorUDFs.tboolEndValue.call(TBOOL_HEX));
    }

    @Test @Order(22)
    void ttextStartValue_returns_quoted_hello() throws Exception {
        // MEOS stores ttext values as quoted PostgreSQL text; text_out preserves quotes
        String val = AccessorUDFs.ttextStartValue.call(TTEXT_HEX);
        assertNotNull(val);
        assertEquals("\"hello\"", val);
    }

    @Test @Order(23)
    void ttextEndValue_returns_quoted_world() throws Exception {
        String val = AccessorUDFs.ttextEndValue.call(TTEXT_HEX);
        assertNotNull(val);
        assertEquals("\"world\"", val);
    }

    @Test @Order(24)
    void null_input_returns_null() throws Exception {
        assertNull(AccessorUDFs.numSequences.call(null));
        assertNull(AccessorUDFs.interp.call(null));
        assertNull(AccessorUDFs.tintStartValue.call(null));
        assertNull(AccessorUDFs.merge.call(null, TINT_HEX));
    }
}
