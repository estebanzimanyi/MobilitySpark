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
 * Unit tests for ConstructorUDFs — text literal → hex-WKB round-trips.
 *
 * Tests run directly against MEOS via JMEOS without a Spark session.
 * MEOS function authority: meos/include/meos.h, meos/include/meos_geo.h
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConstructorUDFsTest {

    @BeforeAll
    static void initMeos() {
        meos_initialize();
        meos_initialize_timezone("UTC");
    }

    @Test @Order(1)
    void tint_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tint.call("[1@2020-01-01, 2@2020-01-02]");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(2)
    void tfloat_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tfloat.call("[1.5@2020-01-01, 2.5@2020-01-02]");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(3)
    void tbool_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tbool.call("[true@2020-01-01, false@2020-01-02]");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(4)
    void ttext_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.ttext.call("[hello@2020-01-01, world@2020-01-02]");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(5)
    void bigintset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.bigintset.call("{100, 200, 300}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(6)
    void tgeogpoint_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tgeogpoint.call("[POINT(4.35 50.85)@2020-01-01, POINT(4.36 50.86)@2020-01-02]");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(7)
    void tstzspan_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tstzspan.call("[2020-01-01, 2020-01-02)");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(8)
    void tstzspanset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tstzspanset.call("{[2020-01-01, 2020-01-02), [2020-03-01, 2020-04-01)}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(9)
    void intspan_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.intspan.call("[1, 10)");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(10)
    void floatspan_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.floatspan.call("[1.0, 10.0)");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(11)
    void datespan_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.datespan.call("[2020-01-01, 2020-01-31)");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(12)
    void datespanset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.datespanset.call("{[2020-01-01, 2020-01-31), [2020-06-01, 2020-06-30)}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(13)
    void intset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.intset.call("{1, 2, 3, 4, 5}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(14)
    void floatset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.floatset.call("{1.1, 2.2, 3.3}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(15)
    void tstzset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tstzset.call("{2020-01-01, 2020-02-01, 2020-03-01}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(16)
    void textset_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.textset.call("{hello, world}");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(17)
    void stbox_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.stbox.call("STBOX X((0,0),(1,1))");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(18)
    void tbox_returns_hexwkb() throws Exception {
        String hex = ConstructorUDFs.tbox.call("TBOX XT((0,1),(2020-01-01,2020-01-02))");
        assertNotNull(hex);
        assertFalse(hex.isEmpty());
    }

    @Test @Order(19)
    void null_input_returns_null() throws Exception {
        assertNull(ConstructorUDFs.tint.call(null));
        assertNull(ConstructorUDFs.tstzspan.call(null));
        assertNull(ConstructorUDFs.intset.call(null));
    }
}
