package com.morpheusdata.upcloud.datasets

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.data.DatasetInfo
import com.morpheusdata.core.data.DatasetQuery
import com.morpheusdata.core.providers.AbstractDatasetProvider
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import io.reactivex.rxjava3.core.Observable

class UpcloudImageDatasetProvider extends AbstractDatasetProvider<VirtualImage, Long>{

    public static final providerName = 'Upcloud Image Dataset Provider'
    public static final providerNamespace = 'upcloud'
    public static final providerKey = 'upcloudImageDataset'
    public static final providerDescription = 'Get images from Upcloud'


    UpcloudImageDatasetProvider(Plugin plugin, MorpheusContext morpheus) {
        this.plugin = plugin
        this.morpheusContext = morpheus
    }

    @Override
    DatasetInfo getInfo() {
        return new DatasetInfo(
                name: providerName,
                namespace: providerNamespace,
                key: providerKey,
                description: providerDescription
        )
    }

    @Override
    Class<VirtualImage> getItemType() {
        return VirtualImage.class
    }

    @Override
    Observable<VirtualImage> list(DatasetQuery datasetQuery) {
        DataQuery query = buildQuery(datasetQuery)
        return morpheus.async.virtualImage.list(query)
    }

    @Override
    Observable<Map> listOptions(DatasetQuery datasetQuery) {
        DataQuery query = buildQuery(datasetQuery)
        morpheus.async.virtualImage.listIdentityProjections(query).map { VirtualImageIdentityProjection item ->
            return [name: item.name, value: item.id]
        }
    }

    @Override
    VirtualImage fetchItem(Object value) {
        def rtn = null
        if(value instanceof Long) {
            rtn = item((Long)value)
        } else if(value instanceof CharSequence) {
            def longValue = value.isNumber() ? value.toLong() : null
            if(longValue) {
                rtn = item(longValue)
            }
        }
        return rtn
    }

    VirtualImage item(Long value) {
        return morpheus.services.virtualImage.get(value)
    }

    @Override
    String itemName(VirtualImage item) {
        return item.name
    }

    @Override
    Long itemValue(VirtualImage item) {
        return item.id
    }

    @Override
    boolean isPlugin() {
        return true
    }

    DataQuery buildQuery(DatasetQuery datasetQuery) {
        Long cloudId = datasetQuery.get("zoneId")?.toLong()
        DataQuery query  = new DatasetQuery().withFilters(
                new DataOrFilter(
                        new DataFilter("visibility", "public"),
                        new DataFilter("accounts.id", datasetQuery.get("accountId")?.toLong()),
                        new DataFilter("owner.id", datasetQuery.get("accountId")?.toLong())
                )
        )
        if(cloudId) {
            query = query.withFilters(
                    new DataOrFilter(
                            new DataAndFilter(
                                    new DataFilter("refType", 'ComputeZone'),
                                    new DataFilter("refId", cloudId.toString())
                            ),
                            new DataFilter("category", "=~","upcloud.image.%")
                    )
            )
        } else {
            query = query.withFilters(
                    new DataFilter("category", "=~","upcloud.image.%")

            )
        }

        return query.withSort("name", DataQuery.SortOrder.asc)
    }
}
