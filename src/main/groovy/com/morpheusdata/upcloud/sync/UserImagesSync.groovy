package com.morpheusdata.upcloud.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.OsType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import com.morpheusdata.upcloud.UpcloudPlugin
import com.morpheusdata.upcloud.services.UpcloudApiService

class UserImagesSync {
    private Cloud cloud
    UpcloudPlugin plugin
    private MorpheusContext morpheusContext

    UserImagesSync(Cloud cloud, UpcloudPlugin plugin) {
        this.cloud = cloud
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            def authConfig = plugin.getAuthConfig(cloud)
            def imageResults = UpcloudApiService.listUserTemplates(authConfig)

            if (imageResults.success == true) {
                def imageRecords = morpheusContext.async.virtualImage.listIdentityProjections(
                        new DataQuery().withFilter("refType", "ComputeZone")
                        .withFilter("refId", cloud.id)
                )

                SyncTask<VirtualImageIdentityProjection, Map, VirtualImage> syncTask = new SyncTask<>(imageRecords, imageResults.imageData as Collection<Map>) as SyncTask<VirtualImageIdentityProjection, Map, VirtualImage>
                syncTask.addMatchFunction { VirtualImageIdentityProjection imageObject, Map cloudItem ->
                    imageObject.externalId == cloudItem?.uuid
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<VirtualImageIdentityProjection, Map>> updateItems ->
                    morpheusContext.async.virtualImage.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.onAdd { itemsToAdd ->
                    addMissingImages(itemsToAdd)
                }.onDelete { removeItems ->
                    removeMissingImages(removeItems)
                }.start()
            } else {
                log.error "Error in getting images: ${imageResults}"
            }
        } catch(e) {
            log.error("cacheUserImages error: ${e}", e)
        }
    }

    private addMissingImages(Collection<Map> addList) {
        def saves = []
        try {
            for (cloudItem in addList) {
                def windowsMatch = cloudItem.title.toLowerCase().indexOf('windows') > -1
                def imageConfig =
                        [
                                category: "upcloud.image.${cloud.id}",
                                owner: cloud.owner,
                                name: cloudItem.title,
                                zoneType: 'upcloud',
                                code: "upcloud.image.${cloud.id}.${cloudItem.uuid}",
                                isCloudInit: false,
                                imageType: 'qcow2',
                                uniqueId: cloudItem.uuid,
                                externalId: cloudItem.uuid,
                                isPublic: cloudItem.false,
                                platform: windowsMatch ? 'windows' : 'linux',
                                minDisk: cloudItem.size,
                                refId: cloud.id.toString(),
                                refType: 'ComputeZone'
                        ]

                def newImage = new VirtualImage(imageConfig)
                saves << newImage
            }
            morpheusContext.async.virtualImage.bulkCreate(saves).blockingGet()
        } catch(e) {
            log.error("addVirtualImage error: ${e}", e)
        }
    }

    private removeMissingImages(List removeList) {
        morpheusContext.async.virtualImage.bulkRemove(removeList).blockingGet()
    }
}
