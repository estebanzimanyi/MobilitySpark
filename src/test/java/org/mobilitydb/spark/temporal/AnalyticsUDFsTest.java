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
 * Unit tests for AnalyticsUDFs — tfloat math, tnumber scalar aggregates,
 * and tpoint spatial analytics.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnalyticsUDFsTest {

    // Linear tfloat [1.0@2020-01-01, 3.0@2020-01-03] — 2-day span for integral/twavg
    private static String TFLOAT_HEX;
    // Single-instant for round/floor/ceil (value 1.23456)
    private static String TFLOAT_ROUND_HEX;
    // Single-instant in radians (π/2 ≈ 1.5707963267948966)
    private static String TFLOAT_RADIANS_HEX;
    // Single-instant in degrees (90.0)
    private static String TFLOAT_DEGREES_HEX;
    // tgeompoint trip [POINT(0 0)@t1, POINT(3 4)@t2] — 5-unit diagonal
    private static String TRIP_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TFLOAT_HEX = temporal_as_hexwkb(
            tfloat_in("[1.0@2020-01-01 00:00:00+00, 3.0@2020-01-03 00:00:00+00]"), (byte) 0);
        TFLOAT_ROUND_HEX = temporal_as_hexwkb(
            tfloat_in("[1.23456@2020-01-01 00:00:00+00]"), (byte) 0);
        TFLOAT_RADIANS_HEX = temporal_as_hexwkb(
            tfloat_in("[1.5707963267948966@2020-01-01 00:00:00+00]"), (byte) 0);
        TFLOAT_DEGREES_HEX = temporal_as_hexwkb(
            tfloat_in("[90.0@2020-01-01 00:00:00+00]"), (byte) 0);
        TRIP_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(3 4)@2020-01-02 00:00:00+00]"), (byte) 0);
    }

    // ── tfloat math ───────────────────────────────────────────────────────────

    @Test @Order(3)
    void tfloatFloor_returns_hexwkb() throws Exception {
        String result = AnalyticsUDFs.tfloatFloor.call(TFLOAT_ROUND_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(4)
    void tfloatCeil_returns_hexwkb() throws Exception {
        String result = AnalyticsUDFs.tfloatCeil.call(TFLOAT_ROUND_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(5)
    void tfloatDegrees_radians_to_degrees_returns_hexwkb() throws Exception {
        String result = AnalyticsUDFs.tfloatDegrees.call(TFLOAT_RADIANS_HEX, false);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(6)
    void tfloatRadians_degrees_to_radians_returns_hexwkb() throws Exception {
        String result = AnalyticsUDFs.tfloatRadians.call(TFLOAT_DEGREES_HEX);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── tnumber scalar aggregates ─────────────────────────────────────────────

    @Test @Order(7)
    void tnumberIntegral_linear_1_to_3_over_2days_is_positive() throws Exception {
        // MEOS integral is in PostgreSQL epoch microseconds:
        // (1.0 + 3.0) / 2 * 2 * 86400 * 1_000_000 = 3.456E11
        Double integral = AnalyticsUDFs.tnumberIntegral.call(TFLOAT_HEX);
        assertNotNull(integral);
        assertTrue(integral > 0.0, "Integral of positive linear sequence must be positive");
        assertEquals(3.456e11, integral, 1e6);
    }

    @Test @Order(8)
    void tnumberTwavg_linear_1_to_3_is_2() throws Exception {
        // time-weighted average of a linear 1→3 sequence = midpoint = 2.0
        Double twavg = AnalyticsUDFs.tnumberTwavg.call(TFLOAT_HEX);
        assertNotNull(twavg);
        assertEquals(2.0, twavg, 1e-6);
    }

    // ── tpoint spatial analytics ──────────────────────────────────────────────

    @Test @Order(9)
    void tpointLength_diagonal_3_4_returns_5() throws Exception {
        // POINT(0 0) → POINT(3 4): Euclidean distance = 5
        Double length = AnalyticsUDFs.tpointLength.call(TRIP_HEX);
        assertNotNull(length);
        assertEquals(5.0, length, 1e-6);
    }

    @Test @Order(10)
    void tpointSpeed_returns_hexwkb() throws Exception {
        String speed = AnalyticsUDFs.tpointSpeed.call(TRIP_HEX);
        assertNotNull(speed);
        assertFalse(speed.isEmpty());
    }

    @Test @Order(11)
    void tpointAzimuth_returns_hexwkb() throws Exception {
        String azimuth = AnalyticsUDFs.tpointAzimuth.call(TRIP_HEX);
        assertNotNull(azimuth);
        assertFalse(azimuth.isEmpty());
    }

    @Test @Order(12)
    void null_input_returns_null() throws Exception {
        assertNull(AnalyticsUDFs.tfloatDerivative.call(null));
        assertNull(AnalyticsUDFs.tfloatRound.call(null, 2));
        assertNull(AnalyticsUDFs.tnumberTwavg.call(null));
        assertNull(AnalyticsUDFs.tpointLength.call(null));
        assertNull(AnalyticsUDFs.tpointSpeed.call(null));
    }
}
