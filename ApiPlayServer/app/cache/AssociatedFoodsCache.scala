package cache

import play.api.cache.SyncCacheApi

trait AssociatedFoodsCache {
  
  val cache: SyncCacheApi
  
  def associatedFoodsWithInheritedCacheKey(code: String, locale: String) = s"AssociatedFoods.withInherited.$locale.$code"
  def associatedFoodsCacheKey(code: String, locale: String) = s"AssociatedFoods.$locale.$code"
  
  def invalidateAssociatedFoods(code: String, locale: String) =  {
    cache.remove(associatedFoodsCacheKey(code, locale))
    cache.remove(associatedFoodsWithInheritedCacheKey(code, locale))
  }
}
