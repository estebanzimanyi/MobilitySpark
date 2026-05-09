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

import java.util.HexFormat;

import static functions.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SpanUDFs — TemporalParquet binary round-trip for span/spanset.
 *
 * Each xFromBinary UDF converts a Parquet BYTE_ARRAY (produced by MobilityDuck's
 * asBinary()) back to the hex-WKB string used throughout MobilitySpark.
 *
 * Tests simulate Parquet binary values by hex-decoding a span_as_hexwkb() string.
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpanUDFsTest {

    private static byte[] INTSPAN_BYTES;
    private static byte[] FLOATSPAN_BYTES;
    private static byte[] TSTZSPAN_BYTES;
    private static byte[] DATESPAN_BYTES;
    private static byte[] INTSPANSET_BYTES;
    private static byte[] TSTZSPANSET_BYTES;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        HexFormat hf = HexFormat.of();
        INTSPAN_BYTES    = hf.parseHex(span_as_hexwkb(intspan_in("[1, 10)"),             (byte) 0));
        FLOATSPAN_BYTES  = hf.parseHex(span_as_hexwkb(floatspan_in("[1.0, 10.0)"),       (byte) 0));
        TSTZSPAN_BYTES   = hf.parseHex(span_as_hexwkb(tstzspan_in("[2020-01-01, 2020-02-01)"), (byte) 0));
        DATESPAN_BYTES   = hf.parseHex(span_as_hexwkb(datespan_in("[2020-01-01, 2020-01-31)"), (byte) 0));
        INTSPANSET_BYTES = hf.parseHex(spanset_as_hexwkb(intspanset_in("{[1,5),[7,10)}"), (byte) 0));
        TSTZSPANSET_BYTES = hf.parseHex(spanset_as_hexwkb(
            tstzspanset_in("{[2020-01-01,2020-02-01),[2020-03-01,2020-04-01)}"), (byte) 0));
    }

    // ── Span fromBinary ───────────────────────────────────────────────────────

    @Test @Order(1)
    void intspanFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.intspanFromBinary.call(INTSPAN_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(2)
    void floatspanFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.floatspanFromBinary.call(FLOATSPAN_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(3)
    void tstzspanFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.tstzspanFromBinary.call(TSTZSPAN_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(4)
    void datespanFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.datespanFromBinary.call(DATESPAN_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Spanset fromBinary ────────────────────────────────────────────────────

    @Test @Order(5)
    void intspansetFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.intspansetFromBinary.call(INTSPANSET_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test @Order(6)
    void tstzspansetFromBinary_roundtrip_returns_hexwkb() throws Exception {
        String result = SpanUDFs.tstzspansetFromBinary.call(TSTZSPANSET_BYTES);
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // ── Round-trip identity ───────────────────────────────────────────────────

    @Test @Order(7)
    void intspan_binary_roundtrip_is_identity() throws Exception {
        // Decode MEOS hex-WKB → bytes → re-encode: the output hex must match
        String originalHex = span_as_hexwkb(intspan_in("[1, 10)"), (byte) 0);
        byte[] bytes = HexFormat.of().parseHex(originalHex);
        String roundtrip = SpanUDFs.intspanFromBinary.call(bytes);
        assertEquals(originalHex, roundtrip);
    }

    // ── Null input ────────────────────────────────────────────────────────────

    @Test @Order(8)
    void null_input_returns_null() throws Exception {
        assertNull(SpanUDFs.intspanFromBinary.call(null));
        assertNull(SpanUDFs.tstzspanFromBinary.call(null));
        assertNull(SpanUDFs.intspansetFromBinary.call(null));
    }
}
