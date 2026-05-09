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

import functions.functions;
import jnr.ffi.Pointer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Spark SQL UDFs for temporal-lifted comparisons, arithmetic, and time sampling.
 *
 * Temporal comparisons (teq, tlt, …) return a TBOOL temporal value (hex-WKB)
 * — one boolean per instant of the input(s). This mirrors the MobilityDB SQL
 * surface where e.g. "tint #= 5" returns a tbool sequence.
 *
 * Arithmetic operations return a TINT or TFLOAT temporal value (hex-WKB).
 *
 * tprecision / tsample use the pg_interval_in helper for interval arguments.
 *
 * MEOS function authority: meos/include/meos.h
 */
public final class TemporalLiftingUDFs {

    private TemporalLiftingUDFs() {}

    private static Pointer tempPtr(String hex) {
        return hex == null ? null : functions.temporal_from_hexwkb(hex);
    }

    private static Pointer ivPtr(String intervalStr) {
        return intervalStr == null ? null : functions.pg_interval_in(intervalStr, -1);
    }

    // ------------------------------------------------------------------
    // Temporal equality  (→ tbool hex-WKB)
    //
    // MEOS: teq_tint_int / teq_tfloat_float / teq_temporal_temporal
    // ------------------------------------------------------------------

    // teqTintInt("[1@2020-01-01, 2@2020-01-02]", 1) → tbool: "[t@2020-01-01,f@2020-01-02]"
    public static final UDF2<String, Integer, String> teqTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.teq_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> teqTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.teq_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> teqTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.teq_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Temporal inequality  (→ tbool hex-WKB)
    //
    // MEOS: tne_tint_int / tne_tfloat_float / tne_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tneTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tne_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> tneTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tne_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> tneTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tne_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Temporal less-than  (→ tbool hex-WKB)
    //
    // MEOS: tlt_tint_int / tlt_tfloat_float / tlt_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tltTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tlt_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> tltTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tlt_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> tltTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tlt_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Temporal less-or-equal  (→ tbool hex-WKB)
    //
    // MEOS: tle_tint_int / tle_tfloat_float / tle_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tleTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tle_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> tleTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tle_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> tleTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tle_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Temporal greater-than  (→ tbool hex-WKB)
    //
    // MEOS: tgt_tint_int / tgt_tfloat_float / tgt_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tgtTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tgt_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> tgtTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tgt_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> tgtTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tgt_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Temporal greater-or-equal  (→ tbool hex-WKB)
    //
    // MEOS: tge_tint_int / tge_tfloat_float / tge_temporal_temporal
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> tgeTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tge_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> tgeTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.tge_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, String, String> tgeTemporal =
        (s1, s2) -> {
            Pointer p1 = tempPtr(s1), p2 = tempPtr(s2);
            if (p1 == null || p2 == null) return null;
            Pointer r = functions.tge_temporal_temporal(p1, p2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Arithmetic on tnumber  (→ tint/tfloat hex-WKB)
    //
    // MEOS: add_tint_int / add_tfloat_float / sub_tint_int / sub_tfloat_float
    //       mult_tint_int / mult_tfloat_float / div_tint_int / div_tfloat_float
    // ------------------------------------------------------------------

    public static final UDF2<String, Integer, String> addTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.add_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> addTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.add_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Integer, String> subTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.sub_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> subTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.sub_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Integer, String> multTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.mult_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> multTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.mult_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Integer, String> divTintInt =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.div_tint_int(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static final UDF2<String, Double, String> divTfloatFloat =
        (s, v) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || v == null) return null;
            Pointer r = functions.div_tfloat_float(ptr, v);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // tnumber delta value  (→ tnumber hex-WKB)
    //
    // MEOS: tnumber_delta_value(Temporal *) → Temporal *
    // ------------------------------------------------------------------

    // deltaValue("[1@t1, 3@t2, 6@t3]") → "[2@t1, 3@t2]" (consecutive differences)
    public static final UDF1<String, String> deltaValue =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer r = functions.tnumber_delta_value(ptr);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Time precision and sampling  (→ temporal hex-WKB)
    //
    // MEOS: temporal_tprecision / temporal_tsample
    // ------------------------------------------------------------------

    // tprecision(trip, "1 hour") → trip snapped to 1-hour grid (Linear interp)
    // MEOS: temporal_tprecision(Temporal *, Interval *, TimestampTz origin) → Temporal *
    public static final UDF2<String, String, String> tprecision =
        (s, durationStr) -> {
            Pointer ptr = tempPtr(s);
            Pointer dur = ivPtr(durationStr);
            if (ptr == null || dur == null) return null;
            OffsetDateTime epoch = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            Pointer r = functions.temporal_tprecision(ptr, dur, epoch);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    // tsample(trip, "15 minutes") → trip resampled at 15-minute intervals
    // MEOS: temporal_tsample(Temporal *, Interval *, TimestampTz origin, int interp) → Temporal *
    // interp 0 = INTERP_NONE (use original), 1 = STEP, 2 = LINEAR
    public static final UDF2<String, String, String> tsample =
        (s, durationStr) -> {
            Pointer ptr = tempPtr(s);
            Pointer dur = ivPtr(durationStr);
            if (ptr == null || dur == null) return null;
            OffsetDateTime epoch = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            Pointer r = functions.temporal_tsample(ptr, dur, epoch, 2);
            if (r == null) return null;
            return functions.temporal_as_hexwkb(r, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        // Temporal comparison lifting (→ tbool)
        spark.udf().register("teqTintInt",      teqTintInt,      DataTypes.StringType);
        spark.udf().register("teqTfloatFloat",  teqTfloatFloat,  DataTypes.StringType);
        spark.udf().register("teqTemporal",     teqTemporal,     DataTypes.StringType);
        spark.udf().register("tneTintInt",      tneTintInt,      DataTypes.StringType);
        spark.udf().register("tneTfloatFloat",  tneTfloatFloat,  DataTypes.StringType);
        spark.udf().register("tneTemporal",     tneTemporal,     DataTypes.StringType);
        spark.udf().register("tltTintInt",      tltTintInt,      DataTypes.StringType);
        spark.udf().register("tltTfloatFloat",  tltTfloatFloat,  DataTypes.StringType);
        spark.udf().register("tltTemporal",     tltTemporal,     DataTypes.StringType);
        spark.udf().register("tleTintInt",      tleTintInt,      DataTypes.StringType);
        spark.udf().register("tleTfloatFloat",  tleTfloatFloat,  DataTypes.StringType);
        spark.udf().register("tleTemporal",     tleTemporal,     DataTypes.StringType);
        spark.udf().register("tgtTintInt",      tgtTintInt,      DataTypes.StringType);
        spark.udf().register("tgtTfloatFloat",  tgtTfloatFloat,  DataTypes.StringType);
        spark.udf().register("tgtTemporal",     tgtTemporal,     DataTypes.StringType);
        spark.udf().register("tgeTintInt",      tgeTintInt,      DataTypes.StringType);
        spark.udf().register("tgeTfloatFloat",  tgeTfloatFloat,  DataTypes.StringType);
        spark.udf().register("tgeTemporal",     tgeTemporal,     DataTypes.StringType);
        // Arithmetic (→ tint/tfloat)
        spark.udf().register("addTintInt",      addTintInt,      DataTypes.StringType);
        spark.udf().register("addTfloatFloat",  addTfloatFloat,  DataTypes.StringType);
        spark.udf().register("subTintInt",      subTintInt,      DataTypes.StringType);
        spark.udf().register("subTfloatFloat",  subTfloatFloat,  DataTypes.StringType);
        spark.udf().register("multTintInt",     multTintInt,     DataTypes.StringType);
        spark.udf().register("multTfloatFloat", multTfloatFloat, DataTypes.StringType);
        spark.udf().register("divTintInt",      divTintInt,      DataTypes.StringType);
        spark.udf().register("divTfloatFloat",  divTfloatFloat,  DataTypes.StringType);
        // Delta and sampling
        spark.udf().register("deltaValue",      deltaValue,      DataTypes.StringType);
        spark.udf().register("tprecision",      tprecision,      DataTypes.StringType);
        spark.udf().register("tsample",         tsample,         DataTypes.StringType);
    }
}
