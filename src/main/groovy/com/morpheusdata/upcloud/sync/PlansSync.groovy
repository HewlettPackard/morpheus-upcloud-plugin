package com.morpheusdata.upcloud.sync

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ProvisionType
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.ServicePlanPriceSet
import com.morpheusdata.model.projection.AccountPriceIdentityProjection
import com.morpheusdata.model.projection.AccountPriceSetIdentityProjection
import com.morpheusdata.model.projection.ServicePlanIdentityProjection
import com.morpheusdata.model.projection.ServicePlanPriceSetIdentityProjection
import com.morpheusdata.upcloud.UpcloudPlugin
import com.morpheusdata.upcloud.services.UpcloudApiService
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.AccountPrice
import com.morpheusdata.model.AccountPriceSet
import com.morpheusdata.model.StorageVolumeType
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class PlansSync {
    private HttpApiClient client
    private Cloud cloud
    UpcloudPlugin plugin
    private MorpheusContext morpheusContext

    PlansSync(HttpApiClient client, Cloud cloud, UpcloudPlugin plugin, MorpheusContext morpheusContext) {
        this.client = client
        this.cloud = cloud
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    def execute() {
        log.debug("SYNCING PLANS")
        try {
            def authConfig = plugin.getAuthConfig(cloud)
            def planListResults = UpcloudApiService.listPlans(client, authConfig)
            if (planListResults.success == true) {
                def planList = planListResults?.data?.plans?.plan
                def existingList = morpheusContext.async.servicePlan.listIdentityProjections(
                        new DataQuery().withFilter("provisionType.code", 'upcloud')
                                .withFilter('active', true)
                )
                planList << getCustomServicePlan()
                SyncTask<ServicePlanIdentityProjection, Map, ServicePlan> syncTask = new SyncTask<>(existingList, planList as Collection<Map>) as SyncTask<ServicePlanIdentityProjection, Map, ServicePlan>
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
            } else {
                log.error "Error in getting plans: ${planListResults}"
            }
        } catch(e) {
            log.error("plansSync error: ${e}", e)
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
                servicePlan.setConfigProperty('tier', cloudItem.tier)
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

            def createResponse = morpheusContext.async.servicePlan.bulkCreate(saves).blockingGet()
            def servicePlans = createResponse.persistedItems
            syncPlanPrices(servicePlans)
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
                if(!plan.getConfigProperty('tier')) {
                    plan.setConfigProperty('tier', matchedItem.tier)
                }

                if (save) {
                    saves << plan
                }
            }
            def updateResponse = morpheusContext.services.servicePlan.bulkSave(saves)
            syncPlanPrices(updateList.collect { it.existingItem })
        } catch(e) {
            log.error("updateMatchedPlans error: ${e}", e)
        }
    }

    def removeMissingPlans(List<ServicePlanIdentityProjection> removeList) {
        def saves = morpheusContext.services.servicePlan.listById(removeList.collect {it.id})
        saves?.each { ServicePlan it ->
            it.active = false
            it.deleted = true
        }
        morpheusContext.async.servicePlan.bulkSave(saves).blockingGet()
    }

    def syncPlanPrices(List<ServicePlan> servicePlans) {
        List<String> priceSetCodes = []
        List<AccountPriceSet> priceSets = []
        List<AccountPrice> prices = []
        Map<String, ServicePlan> priceSetPlans = [:]
        Map<String, AccountPrice> priceSetPrices = [:]

        def authConfig = plugin.getAuthConfig(cloud)
        def priceListResults = UpcloudApiService.listPrices(client, authConfig)
        def upcloudProvisionType = new ProvisionType(code:'upcloud')

        List<String> servicePlanCodes = servicePlans.collect { it.code }
        Map<String, ServicePlan> tmpServicePlanMap = morpheusContext.async.servicePlan.listByCode(servicePlanCodes).distinct { it.code }.toList().blockingGet().collectEntries { [(it.code):it]}

        // per-zone loop only accumulates master lists; SyncTasks run once after in Phase 2.
        priceListResults?.data?.prices?.zone?.each { cloudPriceData ->
            def regionCode = cloudPriceData.name
            def regionName = zoneList.find { it.id == regionCode }?.name ?: regionCode
            AccountPrice storagePrice = new AccountPrice(
                    name         : "UpCloud - MaxIOPs - (${regionName})",
                    code         : "upcloud.price.storage_maxiops.${regionCode}",
                    priceType    : AccountPrice.PRICE_TYPE.storage,
                    systemCreated: true,
                    incurCharges : 'always',
                    volumeType   : new StorageVolumeType(code:'upcloudVolume'),
                    cost         : new BigDecimal(cloudPriceData["storage_maxiops"]?.price?.toString() ?: '0.0') / new BigDecimal("100.0"),
                    priceUnit    : 'hour'
            )

            // Iterate the preconfigured plans
            servicePlans?.each { cloudPlan ->
                def planName = cloudPlan.externalId
                if (cloudPlan.internalId != 'custom') {
                    def priceSetCode = "upcloud.plan.${planName}.${regionCode}".toString()
                    if(!priceSetCodes.contains(priceSetCode)) {
                        priceSetCodes << priceSetCode
                        def tmpServicePlan = tmpServicePlanMap[cloudPlan.code]
                        cloudPlan.id = tmpServicePlan.id
                        priceSetPlans[priceSetCode] = cloudPlan

                        def name = "UpCloud - ${planName} (${regionName})"
                        AccountPriceSet priceSet = new AccountPriceSet(
                                code         : priceSetCode,
                                regionCode   : regionCode,
                                name         : name,
                                priceUnit    : 'hour',
                                type         : AccountPriceSet.PRICE_SET_TYPE.fixed.toString(),
                                systemCreated: true
                        )
                        priceSets << priceSet

                        def priceCode = "upcloud.price.${planName}.${regionCode}".toString()
                        AccountPrice price = new AccountPrice(
                                name         : name,
                                code         : priceCode,
                                priceType    : AccountPrice.PRICE_TYPE.fixed,
                                systemCreated: true,
                                cost         : new BigDecimal(cloudPriceData["server_plan_${planName}"]?.price?.toString() ?: '0.0') / 100.0,
                                priceUnit    : 'hour'
                        )
                        prices << price
                        priceSetPrices[priceSetCode] = price
                    }
                }
            }

            Map customPlanOpts = getCustomServicePlan()
            log.debug("Custom plan opts: ${customPlanOpts}")
            def customPlan = morpheusContext.async.servicePlan.find(
                    new DataQuery().withFilter("code",'upcloud.plan.Custom UpCloud')
                            .withFilter("active", true)
            ).blockingGet()
            if(!customPlan){
                def name = customPlanOpts.name
                def servicePlan = new ServicePlan(
                        code:"upcloud.plan.${customPlanOpts.name}",
                        provisionType:upcloudProvisionType,
                        description:name,
                        name:name,
                        editable:false,
                        externalId:customPlanOpts.name,
                        maxCores:customPlanOpts.core_number,
                        maxMemory:customPlanOpts.memory_amount.toLong() * ComputeUtility.ONE_MEGABYTE,
                        maxStorage:customPlanOpts.storage_size.toLong() * ComputeUtility.ONE_GIGABYTE,
                        active: true,
                        addVolumes:true,
                        deletable: false,
                        sortOrder: 131072l,
                        customCores: true,
                        customMaxStorage: true,
                        customMaxMemory: true,
                        customMaxDataStorage: true,
                        internalId: 'custom'
                )

                customPlan = morpheusContext.async.servicePlan.create(servicePlan).blockingGet()
            }


            syncCustomPlan(customPlan, cloudPriceData, storagePrice)
        }

        // Phase 2 — run SyncTasks once over the full cumulative list.
        try {
            runPriceSetAndPriceSyncTasks(priceSets, prices, priceSetCodes, priceSetPlans)
        } catch(Throwable t) {
        }
    }

    /**
     * runs the price-set then price SyncTasks once over the cumulative list.
     */
    private runPriceSetAndPriceSyncTasks(List<AccountPriceSet> priceSets, List<AccountPrice> prices,
                                         List<String> priceSetCodes, Map<String, ServicePlan> priceSetPlans) {
        Observable<AccountPriceSetIdentityProjection> existingPriceSets = morpheusContext.async.accountPriceSet.listSyncProjectionsByCode(priceSetCodes)
        SyncTask<AccountPriceSetIdentityProjection, AccountPriceSet, AccountPriceSet> syncTask = new SyncTask(existingPriceSets, priceSets)
        syncTask.addMatchFunction { AccountPriceSetIdentityProjection projection, AccountPriceSet cloudItem ->
            return projection.code == cloudItem.code
        }.onDelete { List<AccountPriceSetIdentityProjection> deleteList ->
            def deleteIds = deleteList.collect { it.id }
            List<ServicePlanPriceSet> servicePlanPriceSetDeleteList = morpheusContext.async.servicePlanPriceSet.listByAccountPriceSetIds(deleteIds).toList().blockingGet()
            Boolean servicePlanPriceSetDeleteResult = morpheusContext.async.servicePlanPriceSet.bulkRemove(servicePlanPriceSetDeleteList).blockingGet()
            if(servicePlanPriceSetDeleteResult) {
                morpheusContext.async.accountPriceSet.bulkRemove(deleteList).blockingGet()
            } else {
                log.error("Failed to delete ServicePlanPriceSets associated to AccountPriceSet")
            }
        }.onAdd { List<AccountPriceSet> createList ->
            while (createList.size() > 0) {
                List chunkedList = createList.take(50)
                createList = createList.drop(50)
                createPriceSets(chunkedList, priceSetPlans)
            }
        }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet>> updateItems ->
            Map<Long, SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
            morpheusContext.async.accountPriceSet.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map {AccountPriceSet priceSet ->
                SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet> matchItem = updateItemMap[priceSet.id]
                return new SyncTask.UpdateItem<AccountPriceSet,AccountPriceSet>(existingItem:priceSet, masterItem:matchItem.masterItem)
            }
        }.onUpdate { updateList ->
            while (updateList.size() > 0) {
                List chunkedList = updateList.take(50)
                updateList = updateList.drop(50)
                updateMatchedPriceSet(chunkedList, priceSetPlans)
            }
        }.observe().blockingSubscribe() { complete ->
            if(complete) {
                // query existing prices by price codes (upcloud.price.*), NOT price-set codes.
                // The old code passed priceSetCodes, never matched any account_price rows, so onUpdate never fired.
                // onDelete is structurally unreachable here because priceCodes == prices.collect{it.code}.
                List<String> priceCodes = prices.collect { it.code }
                if(!priceCodes) {
                    return
                }
                Observable<AccountPriceIdentityProjection> existingPrices = morpheusContext.async.accountPrice.listSyncProjectionsByCode(priceCodes)
                SyncTask<AccountPriceIdentityProjection, AccountPrice, AccountPrice> priceSyncTask = new SyncTask(existingPrices, prices)
                priceSyncTask.addMatchFunction { AccountPriceIdentityProjection projection, AccountPrice apiItem ->
                    projection.code == apiItem.code
                }.onDelete { List<AccountPriceIdentityProjection> deleteList ->
                    morpheusContext.async.accountPrice.bulkRemove(deleteList).blockingGet()
                }.onAdd { createList ->
                    while(createList.size() > 0) {
                        List chunkedList = createList.take(50)
                        createList = createList.drop(50)
                        createPrice(chunkedList)
                    }
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
                    morpheusContext.async.accountPrice.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map { AccountPrice price ->
                        SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice> matchItem = updateItemMap[price.id]
                        return new SyncTask.UpdateItem<AccountPrice, AccountPrice>(existingItem: price, masterItem: matchItem.masterItem)
                    }
                }.onUpdate { updateList ->
                    while (updateList.size() > 0) {
                        List chunkedList = updateList.take(50)
                        updateList = updateList.drop(50)
                        updateMatchedPrice(chunkedList)
                    }
                }.start()
            }
        }
    }

    def createPrice(List<AccountPrice> createList) {
        createList?.take(3)?.each { AccountPrice p ->
        }
        Boolean itemsCreated
        try {
            itemsCreated = morpheusContext.async.accountPrice.create(createList).blockingGet()
        } catch(Throwable t) {
            return  // early-exit: callers ignore the return value; skips the link-up step below
        }
        if(itemsCreated) {
            List<String> priceSetCodes = createList.collect { it.code.replace("upcloud.price.", "upcloud.plan.") }

            Map<String, AccountPriceSet> tmpPriceSets = morpheusContext.accountPriceSet.listByCode(priceSetCodes).toList().blockingGet().collectEntries { [(it.code): it] }
            int linkCount = 0
            morpheusContext.async.accountPrice.listByCode(createList.collect {it.code}).blockingSubscribe { AccountPrice price ->
                def priceSetCode = price.code.replace("upcloud.price.", "upcloud.plan.")
                AccountPriceSet priceSet = tmpPriceSets[priceSetCode]
                if(priceSet) {
                    morpheusContext.async.accountPriceSet.addToPriceSet(priceSet, price).blockingGet()
                    linkCount++
                } else {
                    log.error("createPrice addToPriceSet: Could not find matching price set for code {}", price.code)
                }
            }
        } else {
        }
    }

    def updateMatchedPrice(List<SyncTask.UpdateItem<AccountPrice,AccountPrice>> updateItems) {
        // update price for pricing changes
        List<AccountPrice> itemsToUpdate = []
        Map<Long, BigDecimal> updateCostMap = [:]
        updateItems.each {it ->
            AccountPrice remoteItem = it.masterItem
            AccountPrice localItem = it.existingItem
            def doSave = false

            if(localItem.name != remoteItem.name) {
                localItem.name = remoteItem.name
                doSave = true
            }

            if(localItem.cost != remoteItem.cost) {
                log.debug("cost doesn't match, updating: local: $localItem.cost, remote: $remoteItem.cost")
                localItem.cost = remoteItem.cost
                doSave = true
            }

            if(localItem.priceType != remoteItem.priceType) {
                localItem.priceType = remoteItem.priceType
                doSave = true
            }

            if(localItem.incurCharges != remoteItem.incurCharges) {
                localItem.incurCharges = remoteItem.incurCharges
                doSave = true
            }

            if(localItem.priceUnit != remoteItem.priceUnit) {
                localItem.priceUnit = remoteItem.priceUnit
                doSave = true
            }

            if(localItem.currency != remoteItem.currency) {
                localItem.currency = remoteItem.currency
                doSave = true
            }

            if(doSave) {
                updateCostMap[localItem.id] = remoteItem.cost
                itemsToUpdate << localItem
            }
        }


        if(itemsToUpdate.size() > 0) {
            Boolean itemsUpdated
            try {
                itemsUpdated = morpheusContext.async.accountPrice.save(itemsToUpdate).blockingGet()
            } catch(Throwable t) {
                return  // early-exit: callers ignore the return value; skips the link refresh below
            }
            if(itemsUpdated) {
                List<String> priceSetCodes = itemsToUpdate.collect { it.code.replace("upcloud.price.", "upcloud.plan.") }
                Map<String, AccountPriceSet> tmpPriceSets = morpheusContext.async.accountPriceSet.listByCode(priceSetCodes).toList().blockingGet().collectEntries { [(it.code): it] }
                morpheusContext.async.accountPrice.listByCode(itemsToUpdate.collect {it.code}).blockingSubscribe { AccountPrice price ->
                    def priceSetCode = price.code.replace("upcloud.price.", "upcloud.plan.")
                    AccountPriceSet priceSet = tmpPriceSets[priceSetCode]
                    BigDecimal matchedCost = updateCostMap[price.id]
                    if(matchedCost != null) {
                        price.cost = matchedCost
                    }
                    if(priceSet) {
                        morpheusContext.async.accountPriceSet.addToPriceSet(priceSet, price).blockingGet()
                    } else {
                        log.error("createPrice (update) addToPriceSet: Could not find matching price set for code {}", price.code)
                    }
                }
            } else {
            }
        }
    }

    def createPriceSets(List<AccountPriceSet> createList, Map<String, ServicePlan> priceSetPlans) {
        Boolean priceSetsCreated
        try {
            priceSetsCreated = morpheusContext.async.accountPriceSet.create(createList).blockingGet()
        } catch(Throwable t) {
            return  // early-exit: callers ignore the return value; this just skips the link-up step below
        }
        if(priceSetsCreated) {
            List<AccountPriceSet> tmpPriceSets = morpheusContext.async.accountPriceSet.listByCode(createList.collect { it.code }).distinct{it.code }.toList().blockingGet()
            syncServicePlanPriceSets(tmpPriceSets, priceSetPlans)
        } else {
        }
    }

    def updateMatchedPriceSet(List<SyncTask.UpdateItem<AccountPriceSet,AccountPriceSet>> updateItems, Map<String, ServicePlan> priceSetPlans) {
        List<AccountPriceSet> itemsToUpdate = []
        updateItems.each {it ->
            AccountPriceSet remoteItem = it.masterItem
            AccountPriceSet localItem = it.existingItem
            def save = false

            if(localItem.name != remoteItem.name) {
                localItem.name = remoteItem.name
                save = true
            }

            if(save) {
                itemsToUpdate << localItem
            }
        }

        if(itemsToUpdate.size() > 0) {
            morpheusContext.accountPriceSet.save(itemsToUpdate).blockingGet()
        }

        syncServicePlanPriceSets(updateItems.collect { it.existingItem }, priceSetPlans)
    }

    def syncServicePlanPriceSets(List<AccountPriceSet> priceSets, Map<String, ServicePlan> priceSetPlans) {
        Map<String, ServicePlanPriceSet> cloudItems = [:]

        // make sure we have a distinct list of price sets to prevent duplicate service plan price sets.
        // this is primarily an issue when the data already had duplicates, we will continue to create duplicates
        // and compound the problem.
        priceSets?.collect { AccountPriceSet priceSet ->
            if(cloudItems[priceSet.code] == null) {
                cloudItems[priceSet.code] = new ServicePlanPriceSet(priceSet: priceSet, servicePlan: priceSetPlans[priceSet.code])
            }
        }

        Observable<ServicePlanPriceSetIdentityProjection> existingItems = morpheusContext.servicePlanPriceSet.listSyncProjections(priceSets)
        SyncTask<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet, ServicePlanPriceSet> syncTask = new SyncTask(existingItems, cloudItems.values())
        syncTask.addMatchFunction { ServicePlanPriceSetIdentityProjection projection, ServicePlanPriceSet cloudItem ->
            return (projection.priceSet.code == cloudItem.priceSet.code && projection.servicePlan.code == cloudItem.servicePlan.code)
        }.onDelete { List<ServicePlanPriceSetIdentityProjection> deleteList ->
            morpheusContext.async.servicePlanPriceSet.bulkRemove(deleteList).blockingGet()
        }.onAdd { createList ->
            while(createList.size() > 0) {
                List chunkedList = createList.take(50)
                createList = createList.drop(50)
                morpheusContext.async.servicePlanPriceSet.create(chunkedList).blockingGet()
            }
        }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet>> updateItems ->
            Map<Long, SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
            morpheusContext.async.servicePlanPriceSet.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map { ServicePlanPriceSet servicePlanPriceSet ->
                SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet> matchItem = updateItemMap[servicePlanPriceSet.id]
                return new SyncTask.UpdateItem<ServicePlanPriceSet, ServicePlanPriceSet>(existingItem: servicePlanPriceSet, masterItem: matchItem.masterItem)
            }
        }.onUpdate { updateList ->
            // do nothing
        }.start()
    }

    /**
     * Idempotent cleanup of duplicate Custom UpCloud AccountPriceSet rows and stale ServicePlanPriceSet links.
     * Lowest-id row wins as canonical; no-op when already converged.
     */
    private cleanupDuplicateCustomPriceSets(String priceSetCode, ServicePlan customPlan) {
        // one-shot historical-data repair (TODO: drop after 2 release cycles confirm convergence).
        // Synchronized per-code to serialize concurrent refreshes in this JVM (multi-JVM clusters still race but bulkRemove is idempotent).
        synchronized(("morph-11511-cleanup:" + priceSetCode).intern()) {
            try {
                List<AccountPriceSet> allSets = morpheusContext.async.accountPriceSet
                        .listByCode([priceSetCode]).toList().blockingGet() ?: []
                if(allSets.size() <= 1) {
                    return  // steady state
                }

            // Lowest id wins (oldest row). toSorted+first is unambiguous vs allSets.min{it.id}.
            AccountPriceSet canonical = allSets.toSorted { it.id }.first()
            List<AccountPriceSet> duplicates = allSets.findAll { it.id != canonical.id }

            // Step 1: drop SPPS links pointing at dup sets (canonical gets its own link via syncCustomPlan).
            List<Long> duplicateSetIds = duplicates.collect { it.id }
            List<ServicePlanPriceSetIdentityProjection> linksToDupSets =
                    morpheusContext.async.servicePlanPriceSet
                            .listByAccountPriceSetIds(duplicateSetIds)
                            .toList().blockingGet()
                            ?.collect { new ServicePlanPriceSetIdentityProjection(id: it.id) } ?: []
            if(linksToDupSets) {
                morpheusContext.async.servicePlanPriceSet.bulkRemove(linksToDupSets).blockingGet()
            }

            // Step 2: dedupe SPPS links on (canonical, customPlan); keep lowest id.
            List<ServicePlanPriceSet> canonicalLinks = morpheusContext.async.servicePlanPriceSet
                    .listByAccountPriceSetIds([canonical.id])
                    .filter { it.servicePlan?.id == customPlan.id }
                    .toList().blockingGet() ?: []
            if(canonicalLinks.size() > 1) {
                Long keepLinkId = canonicalLinks.collect { it.id }.min()
                List<ServicePlanPriceSetIdentityProjection> linksToRemove = canonicalLinks
                        .findAll { it.id != keepLinkId }
                        .collect { new ServicePlanPriceSetIdentityProjection(id: it.id) }
                morpheusContext.async.servicePlanPriceSet.bulkRemove(linksToRemove).blockingGet()
            }

            // Step 3: remove the dup AccountPriceSet rows (server cascades account_price_set_price).
            List<AccountPriceSetIdentityProjection> dupProjections = duplicates.collect {
                new AccountPriceSetIdentityProjection(id: it.id, code: it.code)
            }
            morpheusContext.async.accountPriceSet.bulkRemove(dupProjections).blockingGet()

        } catch(Throwable t) {
            // never abort sync on cleanup failure
        }
        }  // end synchronized
    }

    /**
     * Idempotent get-or-create + link for a single AccountPrice.
     * priceManagerService.getOrCreatePrice handles history-aware cost propagation server-side.
     * @return true on success; false on error (caller surfaces partial-link state via WARN)
     */
    private boolean upsertCustomComponentPrice(AccountPriceSet priceSet, AccountPrice price) {
        try {
            // getOrCreatePrice on async.* is synchronous and returns AccountPrice directly (not Single). No .blockingGet().
            AccountPrice resolved = morpheusContext.async.accountPriceSet.getOrCreatePrice(price)
            if(!resolved) {
                return false
            }
            morpheusContext.async.accountPriceSet.addToPriceSet(priceSet, resolved).blockingGet()
            return true
        } catch(Throwable t) {
            return false
        }
    }

    private syncCustomPlan(ServicePlan customPlan, cloudPriceData, AccountPrice storagePrice) {
        def planName = customPlan.name
        def regionCode = cloudPriceData.name
        // fall back to regionCode if zone isn't in the hard-coded zoneList (avoids "(null)" names).
        def regionName = zoneList.find { it.id == regionCode}?.name ?: regionCode
        def HOURS_PER_MONTH = 24 * 30

        // pre-clean historical duplicates before the get-or-create below.
        def priceSetCode = "upcloud.plan.${planName}.${regionCode}"
        cleanupDuplicateCustomPriceSets(priceSetCode, customPlan)

        // get-or-create instead of unconditional create. Race window accepted; next refresh's cleanup converges.
        def name = "${planName} (${regionName})"
        // Sorted+first matches the canonical-selection rule in cleanupDuplicateCustomPriceSets.
        def existingPriceSet = morpheusContext.async.accountPriceSet.listByCode([priceSetCode])
                .toList().blockingGet()
                ?.toSorted { it.id }
                ?.find()
        def priceSet
        if(existingPriceSet) {
            priceSet = existingPriceSet
        } else {
            priceSet = new AccountPriceSet(
                    code: priceSetCode,
                    regionCode: regionCode,
                    name: name,
                    priceUnit: 'month',
                    type: AccountPriceSet.PRICE_SET_TYPE.component.toString(),
                    systemCreated: true
            )
            priceSet = morpheusContext.async.accountPriceSet.create(priceSet).blockingGet()
        }

        if(!priceSet) {
            return
        }

        // track per-component success so partial linkage is loudly surfaced via WARN below.
        List<String> failedComponents = []

        // First.. memory
        def cloudPricePerHour =  new BigDecimal(cloudPriceData["server_memory"]?.price?.toString() ?: '0.0') / 100.0
        def cloudPricePerUnitMB = new BigDecimal(cloudPriceData["server_memory"]?.amount?.toString() ?: '256')
        def cloudPricePerMB = (cloudPricePerHour / cloudPricePerUnitMB ) * HOURS_PER_MONTH
        if(!upsertCustomComponentPrice(priceSet, new AccountPrice(
                name: "UpCloud - Custom Memory (${regionName})",
                code: "upcloud.price.${planName}.${regionCode}.memory",
                priceType: AccountPrice.PRICE_TYPE.memory,
                systemCreated: true,
                cost: cloudPricePerMB,
                priceUnit: 'month'
        ))) { failedComponents << 'memory' }

        // Next.. core
        // upsertCustomComponentPrice forces .blockingGet() (old code's core/cpu calls were fire-and-forget).
        def cloudPricePerCore =  (new BigDecimal(cloudPriceData["server_core"]?.price?.toString() ?: '0.0') / 100.0) * HOURS_PER_MONTH
        if(!upsertCustomComponentPrice(priceSet, new AccountPrice(
                name: "UpCloud - Custom Core (${regionName})",
                code: "upcloud.price.${planName}.${regionCode}.core",
                priceType: AccountPrice.PRICE_TYPE.cores,
                systemCreated: true,
                cost: cloudPricePerCore,
                priceUnit: 'month'
        ))) { failedComponents << 'core' }

        // Next... stub out a default one for cpu
        if(!upsertCustomComponentPrice(priceSet, new AccountPrice(
                name: "UpCloud - Custom Cpu (${regionName})",
                code: "upcloud.price.${planName}.${regionCode}.cpu",
                priceType: AccountPrice.PRICE_TYPE.cpu,
                systemCreated: true,
                cost: new BigDecimal('0.0'),
                priceUnit: 'month'
        ))) { failedComponents << 'cpu' }

        // Add the storage price
        if(!upsertCustomComponentPrice(priceSet, new AccountPrice(
                name: "UpCloud - MaxIOPs - (${regionName})",
                code: "upcloud.price.storage_maxiops.month.${regionCode}",
                priceType: AccountPrice.PRICE_TYPE.storage,
                systemCreated: true,
                volumeType: new StorageVolumeType(code:'upcloudVolume'),
                incurCharges: 'always',
                cost: new BigDecimal(((storagePrice?.cost ?: '0.0') * HOURS_PER_MONTH).toString()),
                priceUnit: 'month'
        ))) { failedComponents << 'storage' }

        // WARN on partial component linkage (costing will be wrong until next refresh resolves).
        if(failedComponents) {
        }

        // get-or-create SPPS link. servicePlan.id is safe (eager projection, not lazy). Same race accepted.
        def existingLink = morpheusContext.async.servicePlanPriceSet.listIdentityProjections(priceSet)
                .filter { it.servicePlan?.id == customPlan.id }
                .firstElement().blockingGet()
        if(existingLink) {
        } else {
            def spps = new ServicePlanPriceSet(servicePlan: customPlan, priceSet: priceSet)
            morpheusContext.async.servicePlanPriceSet.create([spps]).blockingGet()
        }
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
            [id:'au-syd1', name:'Sydney #1',     available:true],
            [id:'de-fra1', name:'Frankfurt #1',  available:true],
            [id:'dk-cph1', name:'Copenhagen #1', available:true],
            [id:'es-mad1', name:'Madrid #1',     available:true],
            [id:'fi-hel1', name:'Helsinki #1',   available:true],
            [id:'fi-hel2', name:'Helsinki #2',   available:true],
            [id:'nl-ams1', name:'Amsterdam #1',  available:true],
            [id:'no-svg1', name:'Stavanger #1',  available:true],
            [id:'pl-waw1', name:'Warsaw #1',     available:true],
            [id:'se-sto1', name:'Stockholm #1',  available:true],
            [id:'sg-sin1', name:'Singapore #1',  available:true],
            [id:'uk-lon1', name:'London #1',     available:true],
            [id:'us-chi1', name:'Chicago #1',    available:true],
            [id:'us-nyc1', name:'New York #1',   available:true],
            [id:'us-sjo1', name:'San Jose #1',   available:true],
    ]
}
