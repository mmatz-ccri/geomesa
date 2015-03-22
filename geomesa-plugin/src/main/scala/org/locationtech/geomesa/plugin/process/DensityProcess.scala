/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.plugin.process

import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.GeoTools
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.factory.{DescribeParameter, DescribeProcess}
import org.geotools.process.vector.{HeatmapProcess, HeatmapSurface}
import org.locationtech.geomesa.core.index.QueryHints
import org.locationtech.geomesa.core.iterators.DensityIterator._
import org.opengis.coverage.grid.GridGeometry
import org.opengis.util.ProgressListener

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

@DescribeProcess(
  title = "Density Map",
  description = "Computes a density map over a set of features stored in Geomesa"
)
class DensityProcess extends HeatmapProcess {
  override def invertQuery(@DescribeParameter(name = "radiusPixels", description = "Radius to use for the kernel", min = 0, max = 1) argRadiusPixels: Integer,
                           @DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output") argOutputEnv: ReferencedEnvelope,
                           @DescribeParameter(name = "outputWidth", description = "Width of the output raster")  argOutputWidth: Integer,
                           @DescribeParameter(name = "outputHeight", description = "Height of the output raster") argOutputHeight: Integer,
                           targetQuery: Query,
                           targetGridGeometry: GridGeometry): Query = {
    val q =
      super.invertQuery(argRadiusPixels,
        argOutputEnv,
        argOutputWidth,
        argOutputHeight,
        targetQuery,
        targetGridGeometry)

    q.getHints.put(QueryHints.BBOX_KEY, argOutputEnv)
    q.getHints.put(QueryHints.DENSITY_KEY, java.lang.Boolean.TRUE)
    q.getHints.put(QueryHints.WIDTH_KEY, argOutputWidth)
    q.getHints.put(QueryHints.HEIGHT_KEY, argOutputHeight)
    q
  }

  override def execute(@DescribeParameter(name = "data", description = "Input features") obsFeatures: SimpleFeatureCollection,
                       @DescribeParameter(name = "radiusPixels", description = "Radius of the density kernel in pixels") argRadiusPixels: Integer,
                       @DescribeParameter(name = "weightAttr", description = "Name of the attribute to use for data point weight", min = 0, max = 1) valueAttr: String,
                       @DescribeParameter(name = "pixelsPerCell", description = "Resolution at which to compute the heatmap (in pixels). Default = 1", defaultValue="1", min = 0, max = 1) argPixelsPerCell: Integer,
                       @DescribeParameter(name = "outputBBOX", description = "Bounding box of the output") argOutputEnv: ReferencedEnvelope,
                       @DescribeParameter(name = "outputWidth", description = "Width of output raster in pixels") argOutputWidth: Integer,
                       @DescribeParameter(name = "outputHeight", description = "Height of output raster in pixels") argOutputHeight: Integer,
                       monitor: ProgressListener): GridCoverage2D = {

    val pixelsPerCell: Int = if (argPixelsPerCell != null && argPixelsPerCell > 1) argPixelsPerCell
    else 1
    val outputWidth = argOutputWidth.toInt
    val outputHeight = argOutputHeight.toInt

    val (gridWidth: Int, gridHeight: Int) = if (pixelsPerCell > 1) (outputWidth / pixelsPerCell, outputHeight/ pixelsPerCell)

    // TODO: Correctly handle transformations
    //    val srcCRS = obsFeatures.getSchema.getCoordinateReferenceSystem
    //    val dstCRS = argOutputEnv.getCoordinateReferenceSystem
    //    val trans = CRS.findMathTransform(srcCRS, dstCRS)

    val radiusCells: Int =
      if (argRadiusPixels != null) argRadiusPixels / pixelsPerCell
      else                         100 / pixelsPerCell

    val hms = new HeatmapSurface(radiusCells, argOutputEnv, gridWidth, gridHeight)

    try {
      extractPoints(obsFeatures, valueAttr, hms)
    }

    val hmg = flipXY(hms.computeSurface)

    // Handle upsampling Later

    val gcf = CoverageFactoryFinder.getGridCoverageFactory(GeoTools.getDefaultHints())

    gcf.create("Process Results", hmg, argOutputEnv)
  }

  def extractPoints(obsFeatures: SimpleFeatureCollection, valueAttr: String, hms: HeatmapSurface) {
    val obsIt = obsFeatures.features()

    while (obsIt.hasNext) {
      val sf = obsIt.next()
      val decodedMap = Try(decodeSparseMatrix(sf.getAttribute(ENCODED_RASTER_ATTRIBUTE).toString))

      decodedMap match {
        case Success(raster) =>
          for {
            (latIdx, col)   <- raster.rowMap
            (lonIdx, count) <- col
          } hms.addPoint(lonIdx, latIdx, count)

        case Failure(e) =>
          println(s"Error expanding encoded raster ${sf.getAttribute(ENCODED_RASTER_ATTRIBUTE)}: ${e.toString}", e)
      }
    }
  }

  def flipXY(grid: Array[Array[Float]]): Array[Array[Float]] = {
    val xsize = grid.length
    val ysize = grid(0).length

    val grid2 = Array.ofDim[Float](ysize, xsize)

    for {
      i <- 0 until xsize
      j <- 0 until ysize
    } grid2(j)(i) = grid(i)(j)
    grid2
  }
}
