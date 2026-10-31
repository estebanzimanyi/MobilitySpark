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

package org.mobilitydb.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mobilitydb.spark.generated.GeneratedSpatioTemporalUDFs;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The writers of the UDF surface answer each value by its own type: asText writes an intspan,
 * a tstzspan and an intset as MobilityDB's asText of each, asBinary and asHexWKB write plain
 * WKB, without the SRID, and asEWKB the SRID, each read back by the reader of its format.
 */
class WritersByTypeTest {

    private static SparkSession spark;

    @BeforeAll
    static void setup() {
        spark = SparkSession.builder().appName("writers-by-type").master("local[1]")
                .config("spark.ui.enabled", "false").getOrCreate();
        GeneratedSpatioTemporalUDFs.registerAll(spark);
    }

    @AfterAll
    static void teardown() { if (spark != null) spark.stop(); }

    private static Object scalar(String sql) {
        Row r = spark.sql(sql).collectAsList().get(0);
        return r.isNullAt(0) ? null : r.get(0);
    }

    @Test
    void asTextWritesEachTypeAsItsOwn() {
        assertEquals("[1, 5)", scalar("SELECT asText(intspan_in('[1, 5)'))"));
        assertEquals("{1, 2, 3}", scalar("SELECT asText(intset_in('{1, 2, 3}'))"));
        // the tstzspan is written as tstzspan_out writes it, in the session's time zone
        assertEquals(scalar("SELECT tstzspan_out(tstzspan_in('[2001-01-01, 2001-01-02]'))"),
                scalar("SELECT asText(tstzspan_in('[2001-01-01, 2001-01-02]'))"));
        assertTrue(((String) scalar("SELECT asText(tstzspan_in('[2001-01-01, 2001-01-02]'))"))
                .startsWith("[2001-01-01"));
    }

    @Test
    void asBinaryAndAsHexWkbRoundTrip() {
        assertEquals("[1, 5)", scalar("SELECT asText(intspanFromBinary(asBinary(intspan_in('[1, 5)'))))"));
        assertEquals("[1, 5)", scalar("SELECT asText(intspanFromHexWKB(asHexWKB(intspan_in('[1, 5)'))))"));
    }

    @Test
    void asBinaryCarriesNoSridAndAsEwkbCarriesIt() {
        String trip = "tgeompoint_in('SRID=4326;[Point(1 1)@2001-01-01, Point(2 2)@2001-01-02]')";
        assertFalse(((String) scalar("SELECT asEWKT(tgeompointFromBinary(asBinary(" + trip + ")))"))
                .startsWith("SRID="));
        assertTrue(((String) scalar("SELECT asEWKT(tgeompointFromEWKB(asEWKB(" + trip + ")))"))
                .startsWith("SRID=4326;"));
    }
}
