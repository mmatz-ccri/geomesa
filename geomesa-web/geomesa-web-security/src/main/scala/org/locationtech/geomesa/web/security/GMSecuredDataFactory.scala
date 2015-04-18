package org.locationtech.geomesa.web.security

import com.google.common.collect.Maps
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility, VisibilityEvaluator}
import org.geoserver.security.WrapperPolicy
import org.geoserver.security.decorators.{DecoratingFeatureSource, DefaultSecureDataFactory}
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.FilteringSimpleFeatureCollection
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.web.security.GMSecureFeatureCollection.FC
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, FilterVisitor}
import org.springframework.security.core.context.SecurityContextHolder

class GMSecuredDataFactory extends DefaultSecureDataFactory with Logging {

  logger.info("Using GMSecuredDataFactory")

  override def secure(o: scala.Any, policy: WrapperPolicy): AnyRef =
    super.secure(o, policy) match {
      case ds: DataStore =>
        new GMSecureDataStore(ds)

      case fs: FeatureSource[SimpleFeatureType, SimpleFeature] =>
        new GMSecureFeatureSource(fs)

      case fc: SimpleFeatureCollection =>
        GMSecureFeatureCollection(fc)

      case so =>
        so
    }


  override def getPriority: Int = 1
}

object GMSecuredDataFactory {
  import scala.collection.JavaConversions._

  def getAuthorizations: Authorizations = {
    val auths = SecurityContextHolder.getContext.getAuthentication.getAuthorities.map(_.getAuthority).toList
    new Authorizations(auths: _*)
  }

  def buildVisibilityEvaluator(): VisibilityEvaluator =
    new VisibilityEvaluator(getAuthorizations)
}

class GMSecureFeatureSource(delegate: FeatureSource[SimpleFeatureType, SimpleFeature])
  extends DecoratingFeatureSource[SimpleFeatureType, SimpleFeature](delegate)
  with Logging {

  logger.info("Secured Feature Source '{}'", delegate.getName)

  override def getFeatures(query: Query): FeatureCollection[SimpleFeatureType, SimpleFeature] = {
    GMSecureFeatureCollection(delegate.getFeatures(query))
  }
}

/** This class serves as a marker to avoid double filtering.
 */
class GMSecureFeatureCollection(delegate: FC, filter: Filter)
  extends FilteringSimpleFeatureCollection(delegate, filter)
  with Logging {

  logger.info("Secured Feature Collection '{}'", delegate.getSchema.getName)
}

object GMSecureFeatureCollection {

  type FC = FeatureCollection[SimpleFeatureType, SimpleFeature]

  def apply(delegate: FC): SimpleFeatureCollection = delegate match {
    case secure: GMSecureFeatureCollection => secure
    case _ =>
      val filter = new VisibilityFilter(GMSecuredDataFactory.buildVisibilityEvaluator())
      new GMSecureFeatureCollection(delegate, filter)
  }
}

class GMSecureDataStore(delegate: DataStore) extends AbstractDataStore with Logging {

  logger.info("Secured Data Store '{}'", delegate.getInfo.getTitle)

  override def getSchema(s: String): SimpleFeatureType = delegate.getSchema(s)

  override def getFeatureReader(s: String): FeatureReader[SimpleFeatureType, SimpleFeature] =
    getFeatureReader(new Query(s, Filter.INCLUDE), Transaction.AUTO_COMMIT)

  override def getFeatureReader(query: Query, transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val delegateReader = delegate.getFeatureReader(query, transaction)
    val filter = new VisibilityFilter(GMSecuredDataFactory.buildVisibilityEvaluator())
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](delegateReader, filter)
  }

  override def getTypeNames: Array[String] = delegate.getTypeNames
}

class VisibilityFilter(ve: VisibilityEvaluator) extends Filter {
  import scala.collection.JavaConversions._
  private val vizCache = Maps.newHashMap[String, java.lang.Boolean]()

  override def evaluate(o: Any): Boolean = {
    val viz = o.asInstanceOf[SimpleFeature].visibility
    viz.exists(v =>
      vizCache.getOrElseUpdate(v, ve.evaluate(new ColumnVisibility(v))))
  }

  override def accept(filterVisitor: FilterVisitor, o: AnyRef): AnyRef = o

}