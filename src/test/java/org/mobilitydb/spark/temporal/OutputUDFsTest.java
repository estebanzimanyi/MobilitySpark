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
 * Unit tests for OutputUDFs — text output, metadata accessors, and
 * instant/sequence navigation.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OutputUDFsTest {

    private static String TINT_HEX;
    private static String TFLOAT_HEX;
    private static String TBOOL_HEX;
    private static String TTEXT_HEX;
    private static String TPOINT_HEX;
    // Two-sequence tint for sequence navigation tests
    private static String TINT_SEQSET_HEX;

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");

        TINT_HEX = temporal_as_hexwkb(
            tint_in("[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00]"), (byte) 0);
        TFLOAT_HEX = temporal_as_hexwkb(
            tfloat_in("[1.5@2020-01-01 00:00:00+00, 3.0@2020-01-02 00:00:00+00]"), (byte) 0);
        TBOOL_HEX = temporal_as_hexwkb(
            tbool_in("[true@2020-01-01 00:00:00+00, false@2020-01-02 00:00:00+00]"), (byte) 0);
        TTEXT_HEX = temporal_as_hexwkb(
            ttext_in("[hello@2020-01-01 00:00:00+00, world@2020-01-02 00:00:00+00]"), (byte) 0);
        TPOINT_HEX = temporal_as_hexwkb(
            tgeompoint_in("[POINT(0 0)@2020-01-01 00:00:00+00, POINT(1 1)@2020-01-02 00:00:00+00]"), (byte) 0);
        TINT_SEQSET_HEX = temporal_as_hexwkb(
            tint_in("{[1@2020-01-01 00:00:00+00, 2@2020-01-02 00:00:00+00],"
                  + "[3@2020-01-03 00:00:00+00, 4@2020-01-04 00:00:00+00]}"), (byte) 0);
    }

    // ── Type-specific text output ──────────────────────────────────────────────

    @Test @Order(1)
    void tintAsText_returns_text_with_at_sign() throws Exception {
        String text = OutputUDFs.tintAsText.call(TINT_HEX);
        assertNotNull(text);
        assertTrue(text.contains("@"), "tint text output must contain '@' timestamp separator");
    }

    @Test @Order(2)
    void tfloatAsText_with_maxdd_returns_text() throws Exception {
        String text = OutputUDFs.tfloatAsText.call(TFLOAT_HEX, 6);
        assertNotNull(text);
        assertTrue(text.contains("1.5"), "tfloat text output must include start value");
    }

    @Test @Order(3)
    void tboolAsText_returns_text_with_t_or_f() throws Exception {
        String text = OutputUDFs.tboolAsText.call(TBOOL_HEX);
        assertNotNull(text);
        assertTrue(text.contains("t") || text.contains("f"), "tbool text output must contain 't' or 'f'");
    }

    @Test @Order(4)
    void ttextAsText_contains_value_string() throws Exception {
        String text = OutputUDFs.ttextAsText.call(TTEXT_HEX);
        assertNotNull(text);
        assertTrue(text.contains("hello"), "ttext output must include the text value");
    }

    // ── Metadata accessors ────────────────────────────────────────────────────

    @Test @Order(7)
    void interpolation_step_sequence_returns_step() throws Exception {
        String interp = OutputUDFs.interpolation.call(TINT_HEX);
        assertNotNull(interp);
        assertEquals("Step", interp);
    }

    @Test @Order(8)
    void interpolation_linear_sequence_returns_linear() throws Exception {
        String interp = OutputUDFs.interpolation.call(TFLOAT_HEX);
        assertNotNull(interp);
        assertEquals("Linear", interp);
    }

    @Test @Order(9)
    void temporalSubtype_sequence_returns_sequence() throws Exception {
        String subtype = OutputUDFs.temporalSubtype.call(TINT_HEX);
        assertNotNull(subtype);
        assertEquals("Sequence", subtype);
    }

    @Test @Order(10)
    void temporalSubtype_sequenceset_returns_sequenceset() throws Exception {
        String subtype = OutputUDFs.temporalSubtype.call(TINT_SEQSET_HEX);
        assertNotNull(subtype);
        assertEquals("SequenceSet", subtype);
    }

    @Test @Order(11)
    void numTimestamps_two_instant_sequence_returns_2() throws Exception {
        assertEquals(Integer.valueOf(2), OutputUDFs.numTimestamps.call(TINT_HEX));
    }

    // ── Instant navigation ────────────────────────────────────────────────────

    @Test @Order(12)
    void startInstant_returns_hexwkb() throws Exception {
        String inst = OutputUDFs.startInstant.call(TINT_HEX);
        assertNotNull(inst);
        assertFalse(inst.isEmpty());
    }

    @Test @Order(13)
    void endInstant_returns_hexwkb() throws Exception {
        String inst = OutputUDFs.endInstant.call(TINT_HEX);
        assertNotNull(inst);
        assertFalse(inst.isEmpty());
    }

    @Test @Order(14)
    void instantN_first_returns_hexwkb() throws Exception {
        String inst = OutputUDFs.instantN.call(TINT_HEX, 1);
        assertNotNull(inst);
        assertFalse(inst.isEmpty());
    }

    // ── Sequence navigation ───────────────────────────────────────────────────

    @Test @Order(15)
    void startSequence_sequenceset_returns_hexwkb() throws Exception {
        String seq = OutputUDFs.startSequence.call(TINT_SEQSET_HEX);
        assertNotNull(seq);
        assertFalse(seq.isEmpty());
    }

    @Test @Order(16)
    void endSequence_sequenceset_returns_hexwkb() throws Exception {
        String seq = OutputUDFs.endSequence.call(TINT_SEQSET_HEX);
        assertNotNull(seq);
        assertFalse(seq.isEmpty());
    }

    @Test @Order(17)
    void sequenceN_second_sequence_returns_hexwkb() throws Exception {
        String seq = OutputUDFs.sequenceN.call(TINT_SEQSET_HEX, 2);
        assertNotNull(seq);
        assertFalse(seq.isEmpty());
    }

    // ── Null input ────────────────────────────────────────────────────────────

    @Test @Order(18)
    void null_input_returns_null() throws Exception {
        assertNull(OutputUDFs.tintAsText.call(null));
        assertNull(OutputUDFs.tfloatAsText.call(null, 6));
        assertNull(OutputUDFs.interpolation.call(null));
        assertNull(OutputUDFs.numTimestamps.call(null));
        assertNull(OutputUDFs.startInstant.call(null));
    }
}
