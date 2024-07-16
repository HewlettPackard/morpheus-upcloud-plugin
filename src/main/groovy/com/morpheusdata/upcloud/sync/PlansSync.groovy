package com.morpheusdata.upcloud.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.OsType
import com.morpheusdata.model.ProvisionType
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.projection.ServicePlanIdentityProjection
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import com.morpheusdata.upcloud.UpcloudPlugin
import com.morpheusdata.upcloud.services.UpcloudApiService
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.AccountPrice
import com.morpheusdata.model.AccountPriceSet
import com.morpheusdata.model.StorageVolumeType

class PlansSync {
    private Cloud cloud
    UpcloudPlugin plugin
    private MorpheusContext morpheusContext
    def priceManagerService

    PlansSync(Cloud cloud, UpcloudPlugin plugin) {
        this.cloud = cloud
        this.plugin = plugin
        this.morpheusContext = plugin.morpheusContext
    }

    def execute() {
        try {
            def authConfig = plugin.getAuthConfig(cloud)
            def planListResults = UpcloudApiService.listPlans(authConfig)

            if (planListResults.success == true) {
                def upcloudProvisionType = new ProvisionType(code:'upcloud')
                def planListRecords = morpheusContext.async.servicePlan.listIdentityProjections(
                        new DataQuery().withFilter("provisionType", upcloudProvisionType)
                        .withFilter('active', true)
                )

                planListResults << getCustomServicePlan()
                SyncTask<ServicePlanIdentityProjection, Map, ServicePlan> syncTask = new SyncTask<>(planListRecords, planListResults as Collection<Map>) as SyncTask<ServicePlanIdentityProjection, Map, ServicePlan>
                syncTask.addMatchFunction { ServicePlanIdentityProjection morpheusItem, Map cloudItem ->
                    morpheusItem.externalId == cloudItem?.name
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ServicePlanIdentityProjection, Map>> updateItems ->
                    morpheusContext.async.servicePlan.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.onAdd { itemsToAdd ->
                    addMissingPlans(itemsToAdd)
                }.onUpdate { List<SyncTask.UpdateItem<ServicePlan, Map>> updateItems ->
                    updateMatchedPlans(updateItems)
                }.onDelete { removeItems ->
                    removeMissingPlans(removeItems)
                }.start()

                cachePrices(planListResults)
            } else {
                log.error "Error in getting plans: ${planListResults}"
            }
        } catch(e) {
            log.error("cachePlans error: ${e}", e)
        }
    }

    private addMissingPlans(Collection<Map> addList) {
        def saves = []
        def upcloudProvisionType = new ProvisionType(code:'upcloud')

        try {
            for (cloudItem in addList) {
                def name = (cloudItem.custom == true) ? cloudItem.name : getNameForPlan(cloudItem)
                def servicePlan = new ServicePlan(
                        code:"upcloud.plan.${cloudItem.name}",
                        provisionType:upcloudProvisionType,
                        description:name,
                        name:name,
                        editable:false,
                        externalId:cloudItem.name,
                        maxCores:cloudItem.core_number,
                        maxMemory:cloudItem.memory_amount.toLong() * ComputeUtility.ONE_MEGABYTE,
                        maxStorage:cloudItem.storage_size.toLong() * ComputeUtility.ONE_GIGABYTE,
                        sortOrder:cloudItem.memory_amount.toLong(),
                        customMaxDataStorage:true,
                        deletable: false,
                        active: cloud.defaultPlanSyncActive,
                        addVolumes:true
                )
                if(cloudItem.custom == true) {
                    servicePlan.deletable = false
                    servicePlan.sortOrder = 131072l
                    servicePlan.customCores = true
                    servicePlan.customMaxStorage = true
                    servicePlan.customMaxMemory = true
                    servicePlan.customMaxDataStorage = true
                    servicePlan.internalId = 'custom'
                }
                saves << servicePlan
            }

            morpheusContext.async.servicePlan.bulkCreate(saves).blockingGet()

        } catch(e) {
            log.error("addMissingPlans error: ${e}", e)
        }
    }

    private updateMatchedPlans(List<SyncTask.UpdateItem<ServicePlan, Map>> updateList) {
        def saves = []

        try {
            for(updateMap in updateList) {
                def matchedItem = updateMap.masterItem
                def plan = updateMap.existingItem
                def name = (matchedItem.custom == true) ? matchedItem.name : getNameForPlan(matchedItem)
                def save = false
                if (plan.name != name) {
                    plan.name = name
                    save = true
                }
                if (plan.description != name) {
                    plan.description = name
                    save = true
                }
                if (plan.maxStorage != matchedItem.storage_size.toLong() * ComputeUtility.ONE_GIGABYTE) {
                    plan.maxStorage = matchedItem.storage_size.toLong() * ComputeUtility.ONE_GIGABYTE
                    save = true
                }
                if (plan.maxMemory != matchedItem.memory_amount.toLong() * ComputeUtility.ONE_MEGABYTE) {
                    plan.maxMemory = matchedItem.memory_amount.toLong() * ComputeUtility.ONE_MEGABYTE
                    save = true
                }

                if (save) {
                    saves << plan
                }
            }
            morpheusContext.async.servicePlan.bulkSave(saves).blockingGet()
        } catch(e) {
            log.error("updateMatchedPlans error: ${e}", e)
        }
    }

    def removeMissingPlans(List removeList) {
        def saves = []
        removeList?.each { ServicePlan it ->
            it.active = false
            it.deleted = true
            saves << it
        }
        morpheusContext.async.servicePlan.bulkSave(saves).blockingGet()
    }

    def cachePrices(Map planList) {
        def authConfig = plugin.getAuthConfig(cloud)
        def priceListResults = UpcloudApiService.listPrices(authConfig)
        def upcloudProvisionType = new ProvisionType(code:'upcloud')

        log.debug("upcloud pricing: {}", priceListResults)
        priceListResults?.data?.prices?.zone?.each { cloudPriceData ->
            def regionCode = cloudPriceData.name
            log.debug("region code: {}", regionCode)
            def regionName = zoneList.find { it.id == regionCode }?.name
            // Add the volume pricing.. for now, just MaxIOPs
            def (storagePrice, storagePriceErrors) = priceManagerService.getOrCreatePrice([
                    name         : "UpCloud - MaxIOPs - (${regionName})",
                    code         : "upcloud.price.storage_maxiops.${regionCode}",
                    priceType    : AccountPrice.PRICE_TYPE.storage,
                    systemCreated: true,
                    incurCharges : 'always',
                    volumeType   : new StorageVolumeType(code:'upcloudVolume'),
                    cost         : new BigDecimal(cloudPriceData["storage_maxiops"]?.price?.toString() ?: '0.0') / new BigDecimal("100.0"),
                    priceUnit    : 'hour']
            )

            // Iterate the preconfigured plans
            planList?.each { cloudPlan ->
                def planName = cloudPlan.name
                ServicePlan currentServicePlan = new ServicePlan(provisionType: upcloudProvisionType, externalId: planName, active: true)
                if (currentServicePlan && currentServicePlan.internalId != 'custom') {
                    def priceSetCode = "upcloud.plan.${planName}.${regionCode}"
                    // Get or create the price set
                    def name = "UpCloud - ${planName} (${regionName})"
                    def priceSet = priceManagerService.getOrCreatePriceSet([
                            code         : priceSetCode,
                            regionCode   : regionCode,
                            name         : name,
                            priceUnit    : 'hour',
                            //resourceType : 'compute',
                            type         : AccountPriceSet.PRICE_SET_TYPE.fixed.toString(),
                            systemCreated: true]
                    )
                    // Get or create the price
                    def priceCode = "upcloud.price.${planName}.${regionCode}"
                    def (price, errors) = priceManagerService.getOrCreatePrice([
                            name         : name,
                            code         : priceCode,
                            priceType    : AccountPrice.PRICE_TYPE.fixed,
                            //priceUnit    : 'hour',
                            systemCreated: true,
                            cost         : new BigDecimal(cloudPriceData["server_plan_${planName}"]?.price?.toString() ?: '0.0') / 100.0,
                            priceUnit    : 'hour']
                    )
                    priceManagerService.addToPriceSet(priceSet, price)
                    // Add the set to the correct service plan
                    priceManagerService.addPriceSetToPlan(currentServicePlan, priceSet)
                    // Add the storage price
                    priceManagerService.addToPriceSet(priceSet, storagePrice)
                }
            }
            ServicePlan customPlan = new ServicePlan(code: 'upcloud.plan.Custom UpCloud', active: true)
            syncCustomPlan(customPlan, cloudPriceData, storagePrice)
        }
    }

    private syncCustomPlan(ServicePlan customPlan, cloudPriceData, AccountPrice storagePrice) {
        def planName = customPlan.name
        def regionCode = cloudPriceData.name
        def regionName = zoneList.find { it.id == regionCode}?.name
        def HOURS_PER_MONTH = 24 * 30

        // Get or create the price set
        def priceSetCode = "upcloud.plan.${planName}.${regionCode}"
        def name = "${planName} (${regionName})"
        def priceSet = priceManagerService.getOrCreatePriceSet([
                code: priceSetCode,
                regionCode: regionCode,
                name: name,
                priceUnit: 'month',
                type: AccountPriceSet.PRICE_SET_TYPE.component.toString(),
                systemCreated: true]
        )

        // Get or create the prices
        // First.. memory
        def priceCode = "upcloud.price.${planName}.${regionCode}.memory"
        def cloudPricePerHour =  new BigDecimal(cloudPriceData["server_memory"]?.price?.toString() ?: '0.0') / 100.0
        def cloudPricePerUnitMB = new BigDecimal(cloudPriceData["server_memory"]?.amount?.toString() ?: '256')
        def cloudPricePerMB = (cloudPricePerHour / cloudPricePerUnitMB ) * HOURS_PER_MONTH
        def (memoryPrice, errors) = priceManagerService.getOrCreatePrice([
                name: "UpCloud - Custom Memory (${regionName})",
                code: priceCode,
                priceType: AccountPrice.PRICE_TYPE.memory,
                systemCreated: true,
                cost: cloudPricePerMB,
                priceUnit: 'month']
        )
        priceManagerService.addToPriceSet(priceSet, memoryPrice)

        // Next.. core
        priceCode = "upcloud.price.${planName}.${regionCode}.core"
        def cloudPricePerCore =  (new BigDecimal(cloudPriceData["server_core"]?.price?.toString() ?: '0.0') / 100.0) * HOURS_PER_MONTH
        def (corePrice, coreErrors) = priceManagerService.getOrCreatePrice([
                name: "UpCloud - Custom Core (${regionName})",
                code: priceCode,
                priceType: AccountPrice.PRICE_TYPE.cores,
                systemCreated: true,
                cost: cloudPricePerCore,
                priceUnit: 'month']
        )
        priceManagerService.addToPriceSet(priceSet, corePrice)

        // Next... stub out a default one for cpu
        priceCode = "upcloud.price.${planName}.${regionCode}.cpu"
        def (cpuPrice, cpuErrors) = priceManagerService.getOrCreatePrice([
                name: "UpCloud - Custom Cpu (${regionName})",
                code: priceCode,
                priceType: AccountPrice.PRICE_TYPE.cpu,
                systemCreated: true,
                cost: new BigDecimal('0.0'),
                priceUnit: 'month']
        )
        priceManagerService.addToPriceSet(priceSet, cpuPrice)

        // Add the storage price
        def (storageMonthPrice, storagePriceErrors) = priceManagerService.getOrCreatePrice([
                name: "UpCloud - MaxIOPs - (${regionName})",
                code: "upcloud.price.storage_maxiops.month.${regionCode}",
                priceType: AccountPrice.PRICE_TYPE.storage,
                systemCreated: true,
                volumeType: StorageVolumeType.findByCode('upcloudVolume'),
                incurCharges: 'always',
                cost: new BigDecimal(((storagePrice?.cost ?: '0.0') * HOURS_PER_MONTH).toString()),
                priceUnit: 'month']
        )
        priceManagerService.addToPriceSet(priceSet, storageMonthPrice)

        // Add the set to the correct service plan
        priceManagerService.addPriceSetToPlan(customPlan, priceSet)
    }

    private static getCustomServicePlan() {
        def rtn = [name:'Custom UpCloud', core_number:1, memory_amount:1024l,
                   storage_size:30l, custom:true]
        return rtn
    }

    private static getNameForPlan(planData) {
        def memoryName = planData.memory_amount < 1000 ? "${planData.memory_amount} MB" : "${planData.memory_amount.div(ComputeUtility.ONE_KILOBYTE)} GB"
        return "UpCloud ${planData.core_number} CPU, ${memoryName} Memory, ${planData.storage_size} GB Storage"
    }

    static zoneList = [
            [id:'de-fra1', name:'Frankfurt #1', available:true],
            [id:'fi-hel1', name:'Helsinki #1', available:true],
            [id:'nl-ams1', name:'Amsterdam #1', available:true],
            [id:'sg-sin1', name:'Singapore #1', available:true],
            [id:'uk-lon1', name:'London #1', available:true],
            [id:'us-chi1', name:'Chicago #1', available:true]
    ]
}
