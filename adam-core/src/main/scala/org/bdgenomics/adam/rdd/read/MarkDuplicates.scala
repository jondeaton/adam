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
import org.apache.spark.sql.{ DataFrame, Dataset, SQLContext }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{ RecordGroupDictionary, ReferencePosition }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import org.bdgenomics.adam.rdd.read._
import org.bdgenomics.adam.sql
import org.bdgenomics.formats.avro.{ AlignmentRecord, Strand }

import scala.collection.immutable.StringLike
import scala.math
import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator, TextCigarCodec }
import scala.collection.JavaConversions._

private[rdd] object MarkDuplicates extends Serializable with Logging {

  private def markReadsInBucket(bucket: SingleReadBucket, primaryAreDups: Boolean, secondaryAreDups: Boolean) {
    bucket.primaryMapped.foreach(read => {
      read.setDuplicateRead(primaryAreDups)
    })
    bucket.secondaryMapped.foreach(read => {
      read.setDuplicateRead(secondaryAreDups)
    })
    bucket.unmapped.foreach(read => {
      read.setDuplicateRead(false)
    })
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  def score(record: AlignmentRecord): Int = {
    record.qualityScores.filter(15 <=).sum
  }

  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPair, SingleReadBucket)] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read._2, primaryAreDups, secondaryAreDups)
    })
  }

  def apply(alignmentRecords: AlignmentRecordRDD): RDD[AlignmentRecord] = {

    val sqlContext = SQLContext.getOrCreate(alignmentRecords.rdd.context)
    import sqlContext.implicits._

    // Makes a DataFrame mapping "recordGroupName" => "library"
    def libraryDf(rgd: RecordGroupDictionary): DataFrame = {
      rgd.recordGroupMap.mapValues(value => {
        val (recordGroup, _) = value
        recordGroup.library
      }).toSeq.toDF("recordGroupName", "library")
    }

    // UDF for calculating score of a read

    val scoreUDF = functions.udf((qual: String) => {
      qual.toCharArray.map(q => q - 33).filter(15 <=).sum
    })

    import scala.math

    def isClipped(el: CigarElement): Boolean = {
      el.getOperator == CigarOperator.SOFT_CLIP || el.getOperator == CigarOperator.HARD_CLIP
    }

    def fivePrimePosition(readMapped: Boolean,
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

    val fpUDF = functions.udf((readMapped: Boolean, readNegativeStrand: Boolean, cigar: String,
      start: Long, end: Long) => fivePrimePosition(readMapped, readNegativeStrand, cigar, start, end))

    // 1. Find 5' positions for all reads
    val df = alignmentRecords.dataset
      .withColumn("fivePrimePosition",
        fpUDF('readMapped, 'readNegativeStrand, 'cigar, 'start, 'end))

    import org.apache.spark.sql.functions.{ first, when, sum }

    // 2. Group all fragments, finding read 1 & 2 reference positions
    val positionedDf = df
      .groupBy("recordGroupName", "readName")
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

        sum(when('readMapped and 'primaryAlignment, scoreUDF('qual))) as 'score)
      .join(libraryDf(alignmentRecords.recordGroups), "recordGroupName")

    // positionedDf.count = 514,303
    val positionWindow = Window
      .partitionBy('library,
        'read1contigName, 'read1fivePrimePosition, 'read1strand,
        'read2contigName, 'read2fivePrimePosition, 'read2strand)
      .orderBy('score.desc)

    // Unmapped reads
    val filteredDf = positionedDf.filter('read1fivePrimePosition.isNotNull) // count = 232,860 (not anymore)

    // Need to keet track of the ones that
    val unmappedDf = positionedDf.filter('read1fivePrimePosition.isNull)

    val duplicatesDf = filteredDf
      .withColumn("duplicatedRead", functions.row_number.over(positionWindow) =!= 1)
      .filter('duplicatedRead)
      .select("recordGroupName", "readName")

    // Convert to broadcast set
    val duplicateSet = alignmentRecords.rdd.context
      .broadcast(duplicatesDf.collect()
        .map(row => (row(0), row(1))).toSet)

    val unmappedSet = alignmentRecords.rdd.context
      .broadcast(unmappedDf.collect()
        .map(row => (row(0), row(1))).toSet)

    // Mark all the duplicates that have been found
    alignmentRecords.rdd
      .map(read => {
        val fragID = (read.getRecordGroupName, read.getReadName)

        val isdup = duplicateSet.value.contains(fragID) ||
          (
            !unmappedSet.value.contains(fragID) &&
            (read.getReadMapped && !read.primaryAlignment)
          )
        read.setDuplicateRead(isdup)
        read
      })

    // todo: This is the old code
    // markBuckets(alignmentRecords.groupReadsByFragment(), alignmentRecords.recordGroups)
    //  .flatMap(_.allReads)
    // alignmentRecords.rdd
  }

  def apply(rdd: FragmentRDD): RDD[Fragment] = {
    markBuckets(rdd.rdd.map(f => SingleReadBucket(f)), rdd.recordGroups)
      .map(_.toFragment)
  }

  private def checkRecordGroups(recordGroups: RecordGroupDictionary) {
    // do we have record groups where the library name is not set? if so, print a warning message
    // to the user, as all record groups without a library name will be treated as coming from
    // a single library
    val emptyRgs = recordGroups.recordGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(rg => {
      log.warn("Library ID is empty for record group %s from sample %s.".format(rg.recordGroupName,
        rg.sample))
    })

    if (emptyRgs.nonEmpty) {
      log.warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }
  }

  private def markBuckets(rdd: RDD[SingleReadBucket],
                          recordGroups: RecordGroupDictionary): RDD[SingleReadBucket] = {
    checkRecordGroups(recordGroups)

    // Gets the library of a SingleReadBucket
    def getLibrary(singleReadBucket: SingleReadBucket,
                   recordGroups: RecordGroupDictionary): Option[String] = {
      val recordGroupName = singleReadBucket.allReads.head.getRecordGroupName
      recordGroups.recordGroupMap.get(recordGroupName).flatMap(v => {
        val (recordGroup, _) = v
        recordGroup.library
      })
    }

    // Group by library and left position
    def leftPositionAndLibrary(p: (ReferencePositionPair, SingleReadBucket),
                               rgd: RecordGroupDictionary): (Option[ReferencePosition], Option[String]) = {
      val (refPosPair, singleReadBucket) = p
      (refPosPair.read1refPos, getLibrary(singleReadBucket, rgd))
    }

    // Group by right position
    def rightPosition(p: (ReferencePositionPair, SingleReadBucket)): Option[ReferencePosition] = {
      p._1.read2refPos
    }

    rdd.keyBy(ReferencePositionPair(_))
      .groupBy(leftPositionAndLibrary(_, recordGroups))
      .flatMap(kv => PerformDuplicateMarking.time {

        val leftPos: Option[ReferencePosition] = kv._1._1
        val readsAtLeftPos: Iterable[(ReferencePositionPair, SingleReadBucket)] = kv._2

        leftPos match {

          // These are all unmapped reads. There is no way to determine if they are duplicates
          case None =>
            markReads(readsAtLeftPos, areDups = false)

          // These reads have their left position mapped
          case Some(leftPosWithOrientation) =>

            val readsByRightPos = readsAtLeftPos.groupBy(rightPosition)

            // "Group Count" : The number of groups of
            // reads-that-have-the-same-right-position
            // within this group of reads at the same left position
            val groupCount = readsByRightPos.size

            readsByRightPos.foreach(e => {

              val rightPos = e._1
              val reads = e._2

              // todo: how is it possible for a read with null left position
              // to be marked as a duplicate?

              // "Group Is Fragments": This means that this is a group of reads
              // all with the same left position, but which have no right position
              //              val groupIsFragments = rightPos.isEmpty

              // We have no pairs (only fragments) if the current group is a group of fragments
              // and there is only one group in total
              //              val onlyFragments = groupIsFragments && groupCount == 1

              // If there are only fragments then score the fragments. Otherwise, if there are not only
              // fragments (there are pairs as well) mark all fragments as duplicates.
              // If the group does not contain fragments (it contains pairs) then always score it.
              //              if (onlyFragments || !groupIsFragments) {
              // Find the highest-scoring read and mark it as not a duplicate. Mark all the other reads in this group as duplicates.
              val highestScoringRead = reads.max(ScoreOrdering)
              markReadsInBucket(highestScoringRead._2, primaryAreDups = false, secondaryAreDups = true)
              markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
              //              } else {
              //                // This means that when you have a group of fragments and there ARE other pairs
              //                // at that left position... they're all duplicates
              //                markReads(reads, areDups = true)
              //              }
            })
        }
        readsAtLeftPos.map(_._2)
      })
  }

  private object ScoreOrdering extends Ordering[(ReferencePositionPair, SingleReadBucket)] {
    override def compare(x: (ReferencePositionPair, SingleReadBucket), y: (ReferencePositionPair, SingleReadBucket)): Int = {
      // This is safe because scores are Ints
      scoreBucket(x._2) - scoreBucket(y._2)
    }
  }
}
