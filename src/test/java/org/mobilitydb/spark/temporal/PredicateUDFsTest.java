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
 * Unit tests for PredicateUDFs — temporal order comparisons and ever/always
 * predicate lifting for tint and tfloat.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PredicateUDFsTest {

    // tint [1@2020-01-01, 2@2020-01-02] — step interpolation; values 1 then 2
    private static String TINT_HEX;
    // tint [3@2020-01-03, 4@2020-01-04] — values 3 then 4, disjoint from TINT_HEX
    private static String TINT_HEX2;
    // tfloat [1.0@2020-01-01, 3.0@2020-01-02] — linear interpolation
    private static String TFLOAT_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]"), (byte) 0);
        TINT_HEX2 = temporal_as_hexwkb(
            tint_in("[3@2020-01-03 00:00:00+00, 4@2020-01-04 00:00:00+00]"), (byte) 0);
        TFLOAT_HEX = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01 00:00:00+00, 3.0@2020-01-02 00:00:00+00]"), (byte) 0);
    }

    // ── Temporal order comparisons ────────────────────────────────────────────

    @Test @Order(1)
    void temporalEq_same_object_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalEq.call(TINT_HEX, TINT_HEX));
    }

    @Test @Order(2)
    void temporalEq_different_objects_returns_false() throws Exception {
        assertFalse(PredicateUDFs.temporalEq.call(TINT_HEX, TINT_HEX2));
    }

    @Test @Order(3)
    void temporalNe_different_objects_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalNe.call(TINT_HEX, TINT_HEX2));
    }

    @Test @Order(4)
    void temporalNe_same_object_returns_false() throws Exception {
        assertFalse(PredicateUDFs.temporalNe.call(TINT_HEX, TINT_HEX));
    }

    @Test @Order(5)
    void temporalLt_first_less_than_second_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalLt.call(TINT_HEX, TINT_HEX2));
    }

    @Test @Order(6)
    void temporalGt_second_greater_than_first_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalGt.call(TINT_HEX2, TINT_HEX));
    }

    @Test @Order(7)
    void temporalLe_same_object_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalLe.call(TINT_HEX, TINT_HEX));
    }

    @Test @Order(8)
    void temporalGe_same_object_returns_true() throws Exception {
        assertTrue(PredicateUDFs.temporalGe.call(TINT_HEX, TINT_HEX));
    }

    // ── ever_eq predicates ────────────────────────────────────────────────────

    @Test @Order(9)
    void everEqTintInt_existing_value_returns_true() throws Exception {
        // [1@t1, 2@t2] ever equals 1 → true
        assertTrue(PredicateUDFs.everEqTintInt.call(TINT_HEX, 1));
    }

    @Test @Order(10)
    void everEqTintInt_absent_value_returns_false() throws Exception {
        // [1@t1, 2@t2] ever equals 99 → false
        assertFalse(PredicateUDFs.everEqTintInt.call(TINT_HEX, 99));
    }

    @Test @Order(11)
    void everEqTfloatFloat_start_value_returns_true() throws Exception {
        assertTrue(PredicateUDFs.everEqTfloatFloat.call(TFLOAT_HEX, 1.0));
    }

    // ── ever_lt predicates ────────────────────────────────────────────────────

    @Test @Order(12)
    void everLtTintInt_has_value_below_threshold_returns_true() throws Exception {
        // [1@t1, 2@t2]: 1 < 5 → true
        assertTrue(PredicateUDFs.everLtTintInt.call(TINT_HEX, 5));
    }

    @Test @Order(13)
    void everLtTintInt_all_values_above_threshold_returns_false() throws Exception {
        // [1@t1, 2@t2] ever < 1 → false (minimum value is 1, not strictly < 1)
        assertFalse(PredicateUDFs.everLtTintInt.call(TINT_HEX, 1));
    }

    // ── always_eq predicates ──────────────────────────────────────────────────

    @Test @Order(14)
    void alwaysEqTintInt_constant_value_returns_true() throws Exception {
        // constant [5@t1, 5@t2]: always equals 5 → true
        String CONST = temporal_as_hexwkb(
            tint_in("[5@2020-01-01 00:00:00+00, 5@2020-01-02 00:00:00+00]"), (byte) 0);
        assertTrue(PredicateUDFs.alwaysEqTintInt.call(CONST, 5));
    }

    @Test @Order(15)
    void alwaysEqTintInt_varying_returns_false() throws Exception {
        // [1@t1, 2@t2] always equals 1 → false (value at t2 is 2)
        assertFalse(PredicateUDFs.alwaysEqTintInt.call(TINT_HEX, 1));
    }

    // ── always_lt predicates ──────────────────────────────────────────────────

    @Test @Order(16)
    void alwaysLtTintInt_all_below_threshold_returns_true() throws Exception {
        // [1@t1, 2@t2]: all values < 5 → true
        assertTrue(PredicateUDFs.alwaysLtTintInt.call(TINT_HEX, 5));
    }

    @Test @Order(17)
    void alwaysLtTintInt_not_all_below_returns_false() throws Exception {
        // [1@t1, 2@t2] always < 2 → false (value at t2 is 2, not < 2)
        assertFalse(PredicateUDFs.alwaysLtTintInt.call(TINT_HEX, 2));
    }

    // ── always_ge predicates ──────────────────────────────────────────────────

    @Test @Order(18)
    void alwaysGeTintInt_all_at_or_above_returns_true() throws Exception {
        // [1@t1, 2@t2] always >= 1 → true
        assertTrue(PredicateUDFs.alwaysGeTintInt.call(TINT_HEX, 1));
    }

    @Test @Order(19)
    void alwaysGeTintInt_not_all_above_returns_false() throws Exception {
        // [1@t1, 2@t2] always >= 3 → false (values 1 and 2 are both < 3)
        assertFalse(PredicateUDFs.alwaysGeTintInt.call(TINT_HEX, 3));
    }

    // ── tfloat ever/always ────────────────────────────────────────────────────

    @Test @Order(20)
    void alwaysLtTfloatFloat_all_below_returns_true() throws Exception {
        // linear [1.0, 3.0] always < 4.0 → true
        assertTrue(PredicateUDFs.alwaysLtTfloatFloat.call(TFLOAT_HEX, 4.0));
    }

    @Test @Order(21)
    void everGtTfloatFloat_has_value_above_returns_true() throws Exception {
        // linear [1.0, 3.0] ever > 2.0 → true (end value is 3.0)
        assertTrue(PredicateUDFs.everGtTfloatFloat.call(TFLOAT_HEX, 2.0));
    }

    // ── null input ────────────────────────────────────────────────────────────

    @Test @Order(22)
    void null_input_returns_null() throws Exception {
        assertNull(PredicateUDFs.temporalEq.call(null, TINT_HEX));
        assertNull(PredicateUDFs.everEqTintInt.call(null, 1));
        assertNull(PredicateUDFs.alwaysLtTfloatFloat.call(null, 1.0));
        assertNull(PredicateUDFs.everEqTintInt.call(TINT_HEX, null));
    }
}
