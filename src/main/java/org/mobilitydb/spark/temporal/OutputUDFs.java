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

/**
 * Spark SQL UDFs for temporal text output, metadata accessors, and
 * instant/sequence navigation.
 *
 * Type-specific text output UDFs (tintAsText, tfloatAsText, …) mirror
 * the MEOS *_out family. Instant and sequence navigation UDFs return
 * hex-WKB strings so results can be passed back into other UDFs.
 *
 * MEOS function authority: meos/include/meos.h and meos/include/meos_geo.h
 */
public final class OutputUDFs {

    private OutputUDFs() {}

    private static Pointer tempPtr(String hex) {
        return hex == null ? null : functions.temporal_from_hexwkb(hex);
    }

    // ------------------------------------------------------------------
    // Type-specific text output  (hex-WKB → text literal)
    //
    // These are the inverse of the ConstructorUDFs constructors.
    // MEOS: tint_out / tfloat_out / tbool_out / ttext_out / tpoint_out
    // ------------------------------------------------------------------

    // tintAsText("[1@2020-01-01, 2@2020-01-02]") → "[1@2020-01-01 00:00:00+00, ...]"
    // MEOS: tint_out(Temporal *) → char *
    public static final UDF1<String, String> tintAsText =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.tint_out(ptr);
        };

    // tfloatAsText("[1.5@2020-01-01]", 6) → "[1.5@2020-01-01 00:00:00+00]"
    // MEOS: tfloat_out(Temporal *, int maxdd) → char *
    public static final UDF2<String, Integer, String> tfloatAsText =
        (s, maxdd) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || maxdd == null) return null;
            return functions.tfloat_out(ptr, maxdd);
        };

    // tboolAsText("true@2020-01-01") → "t@2020-01-01 00:00:00+00"
    // MEOS: tbool_out(Temporal *) → char *
    public static final UDF1<String, String> tboolAsText =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.tbool_out(ptr);
        };

    // ttextAsText("hello@2020-01-01") → "hello@2020-01-01 00:00:00+00"
    // MEOS: ttext_out(Temporal *) → char *
    public static final UDF1<String, String> ttextAsText =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.ttext_out(ptr);
        };

    // tpointAsText(trip, 6) → "[POINT(4.35 50.85)@2020-01-01 00:00:00+00]"
    // MEOS: tpoint_out(Temporal *, int maxdd) → char *
    public static final UDF2<String, Integer, String> tpointAsText =
        (s, maxdd) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || maxdd == null) return null;
            return functions.tpoint_out(ptr, maxdd);
        };

    // tpointAsWKT(trip, 6) → "[POINT(4.35 50.85)@2020-01-01 00:00:00+00]"
    // MEOS: tpoint_as_text(Temporal *, int maxdd) → char *
    public static final UDF2<String, Integer, String> tpointAsWKT =
        (s, maxdd) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || maxdd == null) return null;
            return functions.tpoint_as_text(ptr, maxdd);
        };

    // ------------------------------------------------------------------
    // Metadata accessors  (hex-WKB → scalar)
    //
    // MEOS: temporal_interp / temporal_subtype
    //       temporal_num_instants / temporal_num_timestamps
    //       temporal_num_sequences
    // ------------------------------------------------------------------

    // interpolation(trip) → "Linear" | "Step" | "Discrete" | "None"
    // MEOS: temporal_interp(Temporal *) → char *
    public static final UDF1<String, String> interpolation =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.temporal_interp(ptr);
        };

    // temporalSubtype(trip) → "Instant" | "Sequence" | "SequenceSet"
    // MEOS: temporal_subtype(Temporal *) → char *
    public static final UDF1<String, String> temporalSubtype =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.temporal_subtype(ptr);
        };

    // numTimestamps(trip) → number of distinct timestamps
    // MEOS: temporal_num_timestamps(Temporal *) → int
    public static final UDF1<String, Integer> numTimestamps =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            return functions.temporal_num_timestamps(ptr);
        };

    // ------------------------------------------------------------------
    // Instant navigation  (hex-WKB → hex-WKB of TInstant)
    //
    // MEOS: temporal_start_instant / temporal_end_instant / temporal_instant_n
    // ------------------------------------------------------------------

    // startInstant(trip) → hex-WKB of the first TInstant
    // MEOS: temporal_start_instant(Temporal *) → TInstant *
    public static final UDF1<String, String> startInstant =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer inst = functions.temporal_start_instant(ptr);
            if (inst == null) return null;
            return functions.temporal_as_hexwkb(inst, (byte) 0);
        };

    // endInstant(trip) → hex-WKB of the last TInstant
    // MEOS: temporal_end_instant(Temporal *) → TInstant *
    public static final UDF1<String, String> endInstant =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer inst = functions.temporal_end_instant(ptr);
            if (inst == null) return null;
            return functions.temporal_as_hexwkb(inst, (byte) 0);
        };

    // instantN(trip, 1) → hex-WKB of the n-th TInstant (1-based)
    // MEOS: temporal_instant_n(Temporal *, int n) → TInstant *
    public static final UDF2<String, Integer, String> instantN =
        (s, n) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || n == null) return null;
            Pointer inst = functions.temporal_instant_n(ptr, n);
            if (inst == null) return null;
            return functions.temporal_as_hexwkb(inst, (byte) 0);
        };

    // ------------------------------------------------------------------
    // Sequence navigation  (hex-WKB → hex-WKB of TSequence)
    //
    // MEOS: temporal_start_sequence / temporal_end_sequence / temporal_sequence_n
    // ------------------------------------------------------------------

    // startSequence(trip) → hex-WKB of the first TSequence
    // MEOS: temporal_start_sequence(Temporal *) → TSequence *
    public static final UDF1<String, String> startSequence =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer seq = functions.temporal_start_sequence(ptr);
            if (seq == null) return null;
            return functions.temporal_as_hexwkb(seq, (byte) 0);
        };

    // endSequence(trip) → hex-WKB of the last TSequence
    // MEOS: temporal_end_sequence(Temporal *) → TSequence *
    public static final UDF1<String, String> endSequence =
        (s) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null) return null;
            Pointer seq = functions.temporal_end_sequence(ptr);
            if (seq == null) return null;
            return functions.temporal_as_hexwkb(seq, (byte) 0);
        };

    // sequenceN(trip, 1) → hex-WKB of the n-th TSequence (1-based)
    // MEOS: temporal_sequence_n(Temporal *, int n) → TSequence *
    public static final UDF2<String, Integer, String> sequenceN =
        (s, n) -> {
            Pointer ptr = tempPtr(s);
            if (ptr == null || n == null) return null;
            Pointer seq = functions.temporal_sequence_n(ptr, n);
            if (seq == null) return null;
            return functions.temporal_as_hexwkb(seq, (byte) 0);
        };

    public static void registerAll(SparkSession spark) {
        // Type-specific text output
        spark.udf().register("tintAsText",       tintAsText,       DataTypes.StringType);
        spark.udf().register("tfloatAsText",      tfloatAsText,     DataTypes.StringType);
        spark.udf().register("tboolAsText",       tboolAsText,      DataTypes.StringType);
        spark.udf().register("ttextAsText",       ttextAsText,      DataTypes.StringType);
        spark.udf().register("tpointAsText",      tpointAsText,     DataTypes.StringType);
        spark.udf().register("tpointAsWKT",       tpointAsWKT,      DataTypes.StringType);
        // Metadata
        spark.udf().register("interpolation",     interpolation,    DataTypes.StringType);
        spark.udf().register("temporalSubtype",   temporalSubtype,  DataTypes.StringType);
        spark.udf().register("numTimestamps",     numTimestamps,    DataTypes.IntegerType);
        // Instant navigation
        spark.udf().register("startInstant",      startInstant,     DataTypes.StringType);
        spark.udf().register("endInstant",        endInstant,       DataTypes.StringType);
        spark.udf().register("instantN",          instantN,         DataTypes.StringType);
        // Sequence navigation
        spark.udf().register("startSequence",     startSequence,    DataTypes.StringType);
        spark.udf().register("endSequence",       endSequence,      DataTypes.StringType);
        spark.udf().register("sequenceN",         sequenceN,        DataTypes.StringType);
    }
}
