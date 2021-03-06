/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.feature.serialization

import java.util.Date

/** A collection of [[DatumWriter]]s for writing primitive-like datums.
  *
  */
trait PrimitiveWriter[Writer] {

  def writeString: DatumWriter[Writer, String]
  def writeInt: DatumWriter[Writer, Int]
  def writeLong: DatumWriter[Writer, Long]
  def writeFloat: DatumWriter[Writer, Float]
  def writeDouble: DatumWriter[Writer, Double]
  def writeBoolean: DatumWriter[Writer, Boolean]
  def writeDate: DatumWriter[Writer, Date]
  def writeBytes: DatumWriter[Writer, Array[Byte]]

  /** A [[DatumReader]] for writing an [[Int]] with positive optimization */
  def writePositiveInt: DatumWriter[Writer, Int]
}
