/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.read

import org.bdgenomics.utils.misc.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{countDistinct, first, sum, when}
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{RecordGroupDictionary, ReferencePosition}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read._
import org.bdgenomics.adam.sql.{AlignmentRecord => AlignmentRecordSchema}
import org.bdgenomics.formats.avro.{AlignmentRecord, Fragment, Strand}
import org.bdgenomics.adam.rich.RichAlignmentRecord
import htsjdk.samtools.{Cigar, CigarElement, CigarOperator, TextCigarCodec}

import scala.collection.JavaConversions._

private[rdd] object MarkDuplicates extends Serializable with Logging {

  /**
    * Marks alignment records as PCR duplicates.
    * This class marks duplicates all read pairs that have the same pair alignment locations,
    * and all unpaired reads that map to the same sites. Only the highest scoring
    * read/read pair is kept, where the score is the sum of all quality scores in
    * the read that are greater than 15.
    * @param alignmentRecords GenomicRDD of alignment records
    * @return RDD of alignment records with the "duplicateRead" field marked appropriately
    */
  def apply(alignmentRecords: AlignmentRecordRDD): RDD[AlignmentRecord] = {
    import alignmentRecords.dataset.sparkSession.implicits._
    checkRecordGroups(alignmentRecords.recordGroups)

    val fragmentGroupedDf = groupReadsByFragment(alignmentRecords.dataset)
      .join(libraryDf(alignmentRecords.recordGroups, alignmentRecords.dataset.sparkSession), "recordGroupName")
    val duplicatesDf = findDuplicates(fragmentGroupedDf)

    markDuplicates(alignmentRecords.dataset, duplicatesDf)
      .as[AlignmentRecordSchema]
      .rdd.map(_.toAvro)
  }

  /**
    * Marks fragments as duplicates
 *
    * @param fragmentRdd A genomic RDD representing a collection of fragments
    * @return A RDD of fragments each having been specified as duplicate or not
    */
  def apply(fragmentRdd: FragmentRDD): RDD[Fragment] = {
    import fragmentRdd.dataset.sparkSession.implicits._

    // converts a fragment to a tuple suitable for use in DataFrame
    def toFragmentSchema(fragment: Fragment, recordGroups: RecordGroupDictionary) = {
      val bucket = SingleReadBucket(fragment)
      val position = ReferencePositionPair(bucket)

      val recordGroupName: Option[String] = bucket.allReads.headOption.fold(None)(_.getRecordGroupName)
      val library = recordGroups(recordGroupName)

      // reference positions of each read in the fragment
      val read1refPos = position.read1refPos
      val read2refPos = position.read2refPos

      // tuple that will be turned into a row in the DataFrame
      (library , recordGroupName, fragment.getReadName,
        read1refPos.fold()(_.referenceName), read1refPos.fold()(_.pos), read1refPos.fold()(_.strand),
        read2refPos.fold()(_.referenceName), read2refPos.fold()(_.pos), read2refPos.fold()(_.strand),
        scoreBucket(bucket))
    }

    // convert fragments to dataframe wtih reference positions and scores
    val df = fragmentRdd.rdd.map(toFragmentSchema(_, fragmentRdd.recordGroups))
      .toDF("library", "recordGroupName", "readName",
        "read1contigName", "read1fivePrimePosition", "read1strand",
        "read2contigName", "read2fivePrimePosition", "read2strand",
        "score")

    val duplicatesDf = findDuplicates(df)

    fragmentRdd.dataset.join(duplicatesDf, Seq("recordGroupName", "readName"))
      .drop(fragmentRdd.dataset("duplicate"))
      .as[(Fragment, Boolean)]
      .map((f: Fragment, dup: Boolean) => {
        f.getAlignments.forEach()
      })

      // todo thing...
      .as[SingleReadBucket]
      .rdd.map(_.toFragment)

    fragmentRdd.dataset.foreach((fragment: Fragment) =>
      fragment.getAlignments.forEach((alignment: AlignmentRecord) => {
        alignment.setDuplicateRead()
      })
    )

//    markBuckets(rdd.rdd.map(f => SingleReadBucket(f)), rdd.recordGroups)
//      .map(_.toFragment)
  }

  /**
    * Scores a single alignment record by summing all quality scores in the read
    * which are greater than 15.
    * @param record Alignment record containing quality scores
    * @return The "score" of the read, given by the sum of all quality scores greater than 15
    */
  def score(record: AlignmentRecord): Int = {
    record.qualityScores.filter(15 <=).sum
  }

  /**
    * Groups alignment records (reads) by fragment while finding the reference positions and
    * scores of each mapped read in each fragment.
    * @param alignmentRecords Dataset of alignment records
    * @return Dataframe with rows representing fragments made by grouping together the alignment
    *         records by record group name and read name.
    */
  private def groupReadsByFragment(alignmentRecords: Dataset[AlignmentRecordSchema]): DataFrame = {
    import alignmentRecords.sqlContext.implicits._

    // Find the 5' position of all alignment records
    val df = alignmentRecords
      .withColumn("fivePrimePosition",
        fivePrimePositionUDF('readMapped, 'readNegativeStrand, 'cigar, 'start, 'end))

    // Group all fragments, finding read 1 & 2 reference positions and scores
    df.groupBy("recordGroupName", "readName")
      .agg(

        // Read 1 Reference Position
        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true)
          as 'read1contigName,

        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true)
          as 'read1fivePrimePosition,

        first(when('primaryAlignment and 'readInFragment === 0,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true)
          as 'read1strand,

        // Read 2 Reference Position
        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'contigName).otherwise('sequence)),
          ignoreNulls = true)
          as 'read2contigName,

        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped, 'fivePrimePosition).otherwise(0L)),
          ignoreNulls = true)
          as 'read2fivePrimePosition,

        first(when('primaryAlignment and 'readInFragment === 1,
          when('readMapped,
            when('readNegativeStrand, Strand.REVERSE.toString).otherwise(Strand.FORWARD.toString))
            .otherwise(Strand.INDEPENDENT.toString)),
          ignoreNulls = true)
          as 'read2strand,

        // Fragment score
        sum(when('readMapped and 'primaryAlignment, scoreReadUDF('qual)) as 'score))
  }

  /**
    *
    * @param fragmentDf A DataFrame representing genomic fragments
    *
    *                   This DataFrame should have the following schema:
    *                   "library", "recordGroupName", "readName",
    *                   "read1contigName", "read1fivePrimePosition", "read1strand",
    *                   "read2contigName", "read2fivePrimePosition", "read2strand",
    *                   "score"
    * @return A DataFrame with the following schema "recordGroupName", "readName", "duplicateFragment"
    *         indicating all of the fragments which have duplicate reads in them.
    */
  private def findDuplicates(fragmentDf: DataFrame): DataFrame = {
    import fragmentDf.sparkSession.implicits._

    // Window into fragments grouped by left and right reference positions
    val positionWindow = Window
      .partitionBy('library,
        'read1contigName, 'read1fivePrimePosition, 'read1strand,
        'read2contigName, 'read2fivePrimePosition, 'read2strand)
      .orderBy('score.desc)

    // Discard unmapped left position reads
    val filteredDf = fragmentDf
      .filter('read1contigName.isNotNull and 'read1fivePrimePosition.isNotNull and 'read1strand.isNotNull)

    // Count the number of groups of right-position-mapped fragments for each left position
    val groupCountDf = filteredDf
      .groupBy('library, 'read1contigName, 'read1fivePrimePosition, 'read1strand)
      .agg(countDistinct('read2contigName, 'read2fivePrimePosition, 'read2strand)
        as 'groupCount)

    // Join in the group counts for each fragment
    val joinedDf = filteredDf.join(groupCountDf,
      (filteredDf("library") === groupCountDf("library").alias("lib") or
        (filteredDf("library").isNull and groupCountDf("library").isNull)) and
        filteredDf("read1contigName") === groupCountDf("read1contigName").alias("contig") and
        filteredDf("read1fivePrimePosition") === groupCountDf("read1fivePrimePosition").alias("5'") and
        filteredDf("read1strand") === groupCountDf("read1strand").alias("strand"),
      "left")
      .drop(groupCountDf("library")).drop(groupCountDf("read1contigName"))
      .drop(groupCountDf("read1fivePrimePosition")).drop(groupCountDf("read1strand"))

    // Join in the duplicate fragment information
    joinedDf
      .withColumn("duplicateFragment",
        functions.row_number.over(positionWindow) =!= 1 or
          ('read2contigName.isNull and 'read2fivePrimePosition.isNull and 'read2strand.isNull
            and 'groupCount > 0))
      .select("recordGroupName", "readName", "duplicateFragment")
  }

  /**
    * Marks each alignment record in an Dataset as duplicates based on information from a DataFrame
    * which specified which of the alignment records are duplicates
    * @param alignmentRecords Dataset of AlignmentRecords
    * @param duplicatesDf DataFrame containing
    * @return A DataFrame with the same schema as the alignmentRecords Dataset, but with reads having
    *         been marked as duplicates in the "duplicateRead" column in accordance with the duplicatesDf
    *         DataFrame which was provided.
    */
  private def markDuplicates(alignmentRecords: Dataset[AlignmentRecordSchema], duplicatesDf: DataFrame): DataFrame = {
    import alignmentRecords.sparkSession.implicits._
    alignmentRecords
      .drop("duplicateRead")
      .join(duplicatesDf,
        alignmentRecords("recordGroupName") === duplicatesDf("recordGroupName").alias("rgn1") and
          alignmentRecords("readName") === duplicatesDf("readName").alias("rn"))
      .drop(duplicatesDf("recordGroupName"))
      .drop(duplicatesDf("readName"))
      .withColumn("duplicateRead",
        when('duplicateFragment and 'readMapped, true)
          .otherwise(
            when('readMapped and !'primaryAlignment, true).otherwise(false)))
      .drop("duplicateFragment")
  }

  private def scoreReadUDF = functions.udf(scoreRead(_))

  /**
    * Scores a single read based on it's quality.
    * @param qual
    * @return
    */
  private def scoreRead(qual: String): Int = {
    qual.toCharArray.map(q => q - 33).filter(15 <=).sum
  }

  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  private def isClipped(el: CigarElement): Boolean = {
    el.getOperator == CigarOperator.SOFT_CLIP || el.getOperator == CigarOperator.HARD_CLIP
  }

  private def fivePrimePositionUDF = functions.udf(
    (readMapped: Boolean, readNegativeStrand: Boolean, cigar: String, start: Long, end: Long) =>
      fivePrimePosition(readMapped, readNegativeStrand, cigar, start, end))

  private def fivePrimePosition(readMapped: Boolean,
                        readNegativeStrand: Boolean, cigar: String,
                        start: Long, end: Long): Long = {
    if (!readMapped) 0L
    else {
      val samtoolsCigar = TextCigarCodec.decode(cigar)
      val cigarElements = samtoolsCigar.getCigarElements
      math.max(0L,
        if (readNegativeStrand) {
          cigarElements.reverse.takeWhile(isClipped).foldLeft(end)({
            (pos, cigarEl) => pos + cigarEl.getLength
          })
        } else {
          cigarElements.takeWhile(isClipped).foldLeft(start)({
            (pos, cigarEl) => pos - cigarEl.getLength
          })
        })
    }
  }

  /**
    * Checks the record group dictionary that will be used to group reads by position, issuing a
    * warning if there are record groups where the library name is not set. In this case
    * as all record groups without a library will be treated as coming from a single library.
    *
    * @param recordGroupDictionary A mapping from record group name to library
    */
  private def checkRecordGroups(recordGroupDictionary: RecordGroupDictionary) {
    val emptyRgs = recordGroupDictionary.recordGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(recordGroup => {
      log.warn(s"Library ID is empty for record group ${recordGroup.recordGroupName} from sample ${recordGroup.sample}.")
    })

    if (emptyRgs.nonEmpty) {
      log.warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }
  }

  /**
    * Creates a DataFrame with two columns: "recordGroupName" and "library"
    * which maps record group names to library
    *
    * @param recordGroupDictionary A mapping from record group name to library
    * @return A DataFrame with columns "recordGroupName" and "library" representing the
    *         same mapping from record group name to library that was found in the record
    *         group dictionary
    */
  private def libraryDf(recordGroupDictionary: RecordGroupDictionary, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    recordGroupDictionary.recordGroupMap.mapValues(value => {
      val (recordGroup, _) = value
      recordGroup.library
    }).toSeq.toDF("recordGroupName", "library")
  }
}
