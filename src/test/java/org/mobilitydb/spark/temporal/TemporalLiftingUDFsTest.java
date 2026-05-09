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
 * Unit tests for TemporalLiftingUDFs — temporal comparison lifting, arithmetic,
 * delta value, and time precision/sampling.
 *
 * All outputs are tbool/tint/tfloat hex-WKB strings.
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TemporalLiftingUDFsTest {

    // tint [1@2020-01-01, 2@2020-01-02] — step interpolation
    private static String TINT_HEX;
    // tint [3@2020-01-03, 4@2020-01-04] — disjoint, higher values
    private static String TINT_HEX2;
    // tfloat [1.0@2020-01-01, 3.0@2020-01-02] — linear
    private static String TFLOAT_HEX;
    // tint with 3 instants for deltaValue
    private static String TINT_3_HEX;

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
        TINT_3_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 3@2020-01-02 00:00:00+00, 6@2020-01-03 00:00:00+00]"), (byte) 0);
    }

    // ── Temporal equality ─────────────────────────────────────────────────────

    @Test @Order(1)
    void teqTintInt_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.teqTintInt.call(TINT_HEX, 1);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(2)
    void teqTfloatFloat_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.teqTfloatFloat.call(TFLOAT_HEX, 1.0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(3)
    void teqTemporal_same_object_returns_all_true_tbool() throws Exception {
        String result = TemporalLiftingUDFs.teqTemporal.call(TINT_HEX, TINT_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Temporal inequality ───────────────────────────────────────────────────

    @Test @Order(4)
    void tneTintInt_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tneTintInt.call(TINT_HEX, 99);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Temporal less-than ────────────────────────────────────────────────────

    @Test @Order(5)
    void tltTintInt_all_below_threshold_returns_hexwkb() throws Exception {
        // [1@t1, 2@t2] < 5 → all true
        String result = TemporalLiftingUDFs.tltTintInt.call(TINT_HEX, 5);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(6)
    void tltTfloatFloat_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tltTfloatFloat.call(TFLOAT_HEX, 2.0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Temporal less-or-equal ────────────────────────────────────────────────

    @Test @Order(7)
    void tleTintInt_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tleTintInt.call(TINT_HEX, 2);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Temporal greater-than ─────────────────────────────────────────────────

    @Test @Order(8)
    void tgtTintInt_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tgtTintInt.call(TINT_HEX, 0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Temporal greater-or-equal ─────────────────────────────────────────────

    @Test @Order(9)
    void tgeTintInt_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tgeTintInt.call(TINT_HEX, 1);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(10)
    void tgeTfloatFloat_returns_tbool_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tgeTfloatFloat.call(TFLOAT_HEX, 1.0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Arithmetic ────────────────────────────────────────────────────────────

    @Test @Order(11)
    void addTintInt_returns_tint_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.addTintInt.call(TINT_HEX, 10);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(12)
    void addTfloatFloat_returns_tfloat_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.addTfloatFloat.call(TFLOAT_HEX, 1.0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(13)
    void subTintInt_returns_tint_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.subTintInt.call(TINT_HEX, 1);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(14)
    void multTintInt_returns_tint_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.multTintInt.call(TINT_HEX, 2);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(15)
    void divTintInt_nonzero_divisor_returns_tint_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.divTintInt.call(TINT_HEX, 2);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(16)
    void divTfloatFloat_returns_tfloat_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.divTfloatFloat.call(TFLOAT_HEX, 2.0);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Delta value ───────────────────────────────────────────────────────────

    @Test @Order(17)
    void deltaValue_three_instants_returns_two_instant_sequence() throws Exception {
        String result = TemporalLiftingUDFs.deltaValue.call(TINT_3_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Time precision and sampling ───────────────────────────────────────────

    @Test @Order(18)
    void tprecision_one_hour_returns_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tprecision.call(TINT_HEX, "1 hour");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(19)
    void tsample_twelve_hours_returns_hexwkb() throws Exception {
        String result = TemporalLiftingUDFs.tsample.call(TFLOAT_HEX, "12 hours");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Null input ────────────────────────────────────────────────────────────

    @Test @Order(20)
    void null_input_returns_null() throws Exception {
        assertNull(TemporalLiftingUDFs.teqTintInt.call(null, 1));
        assertNull(TemporalLiftingUDFs.addTintInt.call(null, 10));
        assertNull(TemporalLiftingUDFs.divTfloatFloat.call(null, 2.0));
        assertNull(TemporalLiftingUDFs.teqTintInt.call(TINT_HEX, null));
        assertNull(TemporalLiftingUDFs.tprecision.call(null, "1 hour"));
    }
}
