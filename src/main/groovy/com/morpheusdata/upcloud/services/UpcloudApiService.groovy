package com.morpheusdata.upcloud.services

import com.morpheusdata.model.StorageVolume
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.upcloud.*
import com.morpheusdata.core.util.*
import com.morpheusdata.upcloud.util.*
import com.morpheusdata.core.util.HttpApiClient.RequestOptions
import groovy.json.JsonOutput
import groovy.util.logging.Commons
import groovy.util.logging.Slf4j
import org.apache.http.*
import org.apache.http.client.*
import org.apache.http.client.methods.*
import org.apache.http.client.utils.*
import org.apache.http.entity.*
import org.apache.http.util.*

import javax.imageio.spi.ServiceRegistry
import java.lang.String
import java.lang.ref.ReferenceQueue

@Slf4j
class UpcloudApiService {
    static upCloudEndpoint = 'https://api.upcloud.com'
    static upcloudApiVersion = '1.3'
    static requestTimeout = 300000 //5 minutes?

    static listZones(Map authConfig) {
        def rtn = [success:false]
        try {
            Map callOpts = [:]
            String callPath = '/zone'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listZones: ${e}", e
            rtn.success = false
        }
        return ServiceResponse.create(rtn)
    }

    static listPlans(Map authConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = '/plan'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if (callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listPlans: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static listPrices(Map authConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = '/price'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if (callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listPrices: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static listPublicTemplates(Map authConfig) {
        def rtn = [success:false, data:[storages:[storage:[]]]]
        try {
            def callOpts = [:]
            def callPath = '/storage/template'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            log.debug("callResults: ${callResults}")
            if (callResults.success == true) {
                def imageData = callResults.data
                imageData?.storages?.storage?.each { image ->
                    if(image.access == 'public')
                        rtn.data.storages.storage << image
                }
                //rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listPublicTemplates: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static listUserTemplates(Map authConfig) {
        def rtn = [success:false, data:[storages:[storage:[]]]]
        try {
            def callOpts = [:]
            def callPath = '/storage/template'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if (callResults.success == true) {
                def imageData = callResults.data
                imageData?.storages?.storage?.each { image ->
                    if(image.access == 'private')
                        rtn.data.storages.storage << image
                }
                //rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listUserTemplates: ${e}", e
            rtn.success = false
        }
        log.debug("USER TEMPLATES: ${rtn.data}")
        return rtn
    }

    /*static listUserTemplates(Map authConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = '/storage/template'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if (callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listPublicTemplates: ${e}", e
            rtn.success = false
        }
        return rtn
    }*/

    static getStorageDetails(Map authConfig, String storageId) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage/${storageId}"
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.storage = callResults.data?.storage
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listStorageDetails: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static removeStorage(Map authConfig, String storageId) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage/${storageId}"
            def callResults = callApi(authConfig, callPath, callOpts, 'DELETE')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on removeStorage: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static getServerDetail(Map authConfig, String serverId) {
        log.info("getting server detail for server: ${serverId}")
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}"
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.server = rtn.data?.server
                rtn.volumes = getVmVolumes(rtn.data?.server?.'storage_devices'?.'storage_device')
                rtn.networks = getVmNetworks(rtn.data?.server?.'ip_addresses'?.'ip_address')
                rtn.success = true
            } else {
                rtn.success = false
				rtn.errorCode = callResults.errorCode
            }
        } catch (e) {
            log.error "Error on getServerDetail: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static getVmVolumes(storageDevices) {
        def rtn = []
        try {
            storageDevices?.eachWithIndex { storageDevice, index ->
                if(storageDevice.type == 'disk') {
                    def newDisk = [address:storageDevice.address, size:storageDevice.'storage_size',
                                   description:storageDevice.'storage_title', name:storageDevice.'storage_title',
                                   type:'disk', storageId:storageDevice.storage, index:index, deviceName:storageDevice.address]
                    rtn << newDisk
                }
            }
        } catch(e) {
            log.error("getVmVolumes error: ${e}")
        }
        return rtn
    }

    static getVmNetworks(networkDevices) {
        def rtn = []
        try {
            def counter = 0
            networkDevices?.each { networkDevice ->
                def newNic = [access:networkDevice.access, family:networkDevice.family, address:networkDevice.address,
                              row:counter]
                rtn << newNic
                counter++
            }
        } catch(e) {
            log.error("getVmNetworks error: ${e}")
        }
        return rtn
    }

    static listServers(Map authConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = '/server'
            def callResults = callApi(authConfig, callPath, callOpts, 'GET')
            if (callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on listServers: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static createServer(Map authConfig, Map serverConfig) {
        log.debug("serverConfig: ${serverConfig}")
        log.info("account: ${serverConfig.account.id}")
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server"
            def rootVolume = serverConfig.rootVolume
            def maxStorage = (int)rootVolume.maxStorage.div(ComputeUtility.ONE_GIGABYTE)
            callOpts.body = [
                    server: [
                            zone:serverConfig.zoneRef,
                            title:serverConfig.name,
                            hostname:serverConfig.hostname ?: '',
                            storage_devices: [
                                    storage_device: [
                                            [
                                                    action:'clone',
                                                    size:maxStorage,
                                                    storage: serverConfig.cloneImageId ?: serverConfig.imageRef,
                                                    tier: (rootVolume.diskType == 'upcloudHddVolume') ? 'hdd' : serverConfig.tier,
                                                    title:serverConfig.name + ' ' + (rootVolume.name ?: 'disk')
                                            ]
                                    ]
                            ],
                            /*ip_addresses : [
                      ip_address : [
                        [access:'private', family:'IPv4'],
                        [access:'public', family:'IPv4'],
                        [access:'public', family:'IPv6']
                                ]
                            ],*/
                            login_user: [
                                    username:(serverConfig.userConfig?.sshUsername ?: serverConfig.username ?: 'root'),
                                    ssh_keys: [
                                            ssh_key: []
                                    ]
                            ]
                    ]
            ]
            if(serverConfig.planRef) {
                callOpts.body.server.plan = serverConfig.planRef
            } else {
                callOpts.body.server.core_number = (serverConfig.maxCores ?: 1)
                callOpts.body.server.memory_amount = (serverConfig.maxMemory ?: ComputeUtility.ONE_GIGABYTE) / ComputeUtility.ONE_MEGABYTE
            }
            //data disks?
            if(serverConfig.dataDisks) {
                serverConfig.dataDisks?.eachWithIndex { StorageVolume dataDisk, index ->
                    log.info("dataDisk: ${dataDisk}")
                    log.info("dataDisk: ${dataDisk.dump()}")
                    def diskSize = (int)dataDisk.maxStorage.div(ComputeUtility.ONE_GIGABYTE)
                    def diskData = [action: 'create',
                                    size:diskSize,
                                    tier:(dataDisk.diskType == 'upcloudHddVolume') ? 'hdd' : 'maxiops',
                                    title:serverConfig.name + ' ' + (dataDisk.name ?: 'disk ' + (index + 1))
                    ]

                    if(dataDisk.getConfigProperty("snapshotUUID")) {
                        diskData.action = 'clone'
                        diskData.storage = dataDisk.getConfigProperty("snapshotUUID")
                    }

                    callOpts.body.server.storage_devices.storage_device << diskData
                }
            }
            //ssh key
            if(serverConfig.userConfig?.cloudInitUsers) {
                serverConfig.userConfig.cloudInitUsers.each { user ->
                    user.keys.each { key ->
                        callOpts.body.server.login_user.ssh_keys.ssh_key << key
                    }
                }
            } else if(serverConfig.userConfig?.createUsers) {
                serverConfig.userConfig.createUsers.each { user ->
                    user.keys.each { key ->
                        callOpts.body.server.login_user.ssh_keys.ssh_key << key
                    }
                }
            } else if(serverConfig.userConfig?.primaryKey?.publicKey) {
                callOpts.body.server.login_user.ssh_keys.ssh_key << serverConfig.userConfig.primaryKey.publicKey
            }
            //user data
            callOpts.body.server.metadata = 'yes'
            if(serverConfig.cloudConfig)
                callOpts.body.server.user_data = serverConfig.cloudConfig
            //create server
            log.debug("callOpts: ${callOpts}")
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            log.info("call api results: ${callResults}")
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.server = rtn.data?.server
                rtn.externalId = rtn.data?.server?.uuid
                rtn.success = true
            } else {
                rtn.err = callResults.data?.error?.error_message
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on createServer: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static waitForServerExists(Map authConfig, String serverId) {
        def rtn = [success:false]
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                def serverDetail = getServerDetail(authConfig, serverId)
                log.info("wait for server exists get server detail: ${serverDetail}")
                if(serverDetail.success == true && serverDetail?.server?.state == 'started') {
                    def privateIp = serverDetail.server?.'ip_addresses'?.'ip_address'?.find{ it.access == 'utility' && it.family == 'IPv4' }
                    if(privateIp?.address) {
                        rtn.success = true
                        rtn.results = serverDetail.server
                        break
                    }
                } else if(attempts > 3 && serverDetail.success == false && serverDetail.data?.error?.error_message == 'SERVER_NOT_FOUND') {
                    rtn.error = true
                    rtn.results = serverDetail.data
                    pending = false
                }
                attempts ++
                if(attempts > 250) {
                    pending = false
                } else {
                    sleep(1000l * 5l)
                }
            }
        } catch(e) {
            log.error(e)
        }
        return rtn
    }

    static waitForServerStatus(Map authConfig, String serverId, String status) {
        def rtn = [success:false]
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                def serverDetail = getServerDetail(authConfig, serverId)
                log.debug("serverDetail: ${serverDetail}")
                if(serverDetail.success == true && serverDetail?.server?.state == status) {
                    log.info("SERVER STATUS: ${serverDetail?.server?.state}")
                    rtn.success = true
                    rtn.results = serverDetail.server
                    pending = false
                    break
                } else if(serverDetail.success == false && serverDetail.errorCode == '404') {
					rtn.success = false
					rtn.errorCode = serverDetail.errorCode
					pending false
					break
				}
                attempts ++
                if(attempts > 100) {
                    pending = false
                } else {
                    sleep(1000l * 5l)
                }
            }
        } catch(e) {
            log.error(e)
        }
        return rtn
    }

    static waitForServerNotStatus(Map authConfig, String serverId, String status) {
        def rtn = [success:false]
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                def serverDetail = getServerDetail(authConfig, serverId)
				if(serverDetail.success == true && serverDetail?.server?.state != status) {
                    rtn.success = true
                    rtn.results = serverDetail.server
                    pending = false
                    break
                } else if(serverDetail.success == false && serverDetail.errorCode == '404') {
					rtn.success = false
					rtn.errorCode = serverDetail.errorCode
					pending = false
					break
				}
                attempts ++
                if(attempts > 100) {
                    pending = false
                } else {
                    sleep(1000l * 5l)
                }
            }
        } catch(e) {
            log.error(e)
        }
        return rtn
    }

    static checkServerReady(Map authConfig, String serverId) {
        def rtn = [success:false]
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                sleep(1000l * 5l)
                def serverDetail = getServerDetail(authConfig, serverId)
                if(serverDetail.success == true && serverDetail?.server?.state) {
                    def tmpState = serverDetail.server.state
                    if(tmpState == 'started') {
                        rtn.success = true
                        rtn.results = serverDetail.server
                        pending = false
                    } else if(tmpState == 'failed') {
                        rtn.error = true
                        rtn.results = serverDetail.server
                        rtn.success = true
                        pending = false
                    }
                }
                attempts ++
                if(attempts > 30)
                    pending = false
            }
        } catch(e) {
            log.error("error waiting for upcloud server: ${e.message}",e)
        }
        return rtn
    }

    static checkStorageReady(Map authConfig, String storageId) {
        def rtn = [success:false]
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                sleep(1000l * 5l)
                def storageDetail = getStorageDetails(authConfig, storageId)
                if(storageDetail.success == true && storageDetail?.storage?.state) {
                    def tmpState = storageDetail.storage.state
                    if(tmpState == 'online') {
                        rtn.success = true
                        rtn.results = storageDetail.storage
                        pending = false
                    } else if(tmpState == 'failed') {
                        rtn.error = true
                        rtn.results = storageDetail.storage
                        rtn.success = true
                        pending = false
                    }
                }
                attempts ++
                if(attempts > 30)
                    pending = false
            }
        } catch(e) {
            log.error("error waiting for upcloud storage: ${e.message}",e)
        }
        return rtn
    }

    static attachStorage(Map authConfig, String serverId, String storageId, Integer index) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}/storage/attach"
            def diskAddress = 'virtio:' + index //gonna need a way to handle multiple types
            callOpts.body = [
                    storage_device:[
                            type:'disk',
                            address:diskAddress,
                            storage:storageId
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.address = diskAddress
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on attachStorage: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static detachStorage(Map authConfig, String serverId, String address) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}/storage/detach"
            callOpts.body = [
                    storage_device:[
                            address:address
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on detachStorage: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    /*static uploadKeypair(apiUrl, apiKey, keyName, publicKey) {
        def rtn = [success:false]
        //query = query.encodeAsJSON()
        def headers = ['Content-Type':'application/json']
        def body = [public_key:publicKey, name:keyName]
        log.debug("body: ${body.encodeAsJSON()}")
        def results = callApi(apiUrl, '/v2/account/keys', apiKey, [headers:headers, body:body, requestContentType:ContentType.JSON], Method.POST)
        log.debug("got: ${results}")
        rtn.success = results?.success
        if(rtn.success == true) {
            rtn.results = new groovy.json.JsonSlurper().parseText(results.content)
        }
        return rtn
    }

    static listKeypairs(apiUrl, apiKey) {
        def rtn = [success:false]
        //query = query.encodeAsJSON()
        def headers = ['Content-Type':'application/json']
        log.debug("listCreateOptions")
        def pageNum = 1
        def perPage = 10
        def query = [per_page:"${perPage}", page:"${pageNum}"]
        def results = callApi(apiUrl, '/v2/account/keys', apiKey, [headers:headers, query:query], Method.GET)
        log.debug("got: ${results}")
        rtn.success = results?.success
        if(rtn.success == true) {
            rtn.results = new groovy.json.JsonSlurper().parseText(results.content)
            log.debug("total ssh keys: ${rtn.results.meta.total}")
            def theresMore = rtn.results.links?.pages?.next ? true: false
            while (theresMore) {
                pageNum++
                query.page = "${pageNum}"
                def moreResults = callApi(apiUrl, '/v2/account/keys', apiKey, [headers:headers, query:query, requestContentType:ContentType.JSON], Method.GET)
                def r = new groovy.json.JsonSlurper().parseText(moreResults.content)
                rtn.results.ssh_keys += r.ssh_keys
                theresMore = r.links?.pages?.next ? true: false
            }
        }
        return rtn
    }

    static findOrUploadKeypair(apiUrl, apiKey, keyName, publicKey) {
        def rtn
        def keyList = UpcloudComputeUtility.listKeypairs(apiUrl, apiKey)
        if(keyList.success == true) {
            def match = keyList.results.ssh_keys.find{publicKey.startsWith(it.public_key)}
            log.debug("match: ${match} - list: ${keyList.results}")
            if(match)
                rtn = match.id
        }
        if(!rtn) {
            def keyResults = UpcloudComputeUtility.uploadKeypair(apiUrl, apiKey, keyName, publicKey)
            if(keyResults.success == true) {
                rtn = keyResults.results.ssh_key.id
            } else {

            }
        }
        return rtn
    }*/

    static startServer(Map authConfig, String serverId) {
        log.info("calling upcloud api service start server")
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}/start"
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            log.info("call results: ${callResults}")
            if(callResults.success == true || (callResults.success == false && callResults.errors.error.endsWith("Read timed out"))) {
                log.info("read timed out error caught")
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on startServer: ${e}", e
            rtn.success = false
        }
        log.info("start server rtn: ${rtn}")
        return rtn
    }

    static stopServer(Map authConfig, String serverId) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}/stop"
            callOpts.body = [
                    'stop_server':[
                            'stop_type':'soft',
                            'timeout':'60'
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
				rtn.errorCode = callResults.errorCode
            }
        } catch (e) {
            log.error "Error on stopServer: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static powerOffServer(Map authConfig, String serverId) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}/stop"
            callOpts.body = [
                    'stop_server':[
                            'stop_type':'hard',
                            'timeout':'60'
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on stopServer: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static removeServer(Map authConfig, String serverId) {
		log.info("removing server with id: ${serverId}")
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}"
            def callResults = callApi(authConfig, callPath, callOpts, 'DELETE')
            if(callResults.success == true || callResults.errorCode == '404') {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on removeServer: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static resizeServer(Map authConfig, String serverId, Map serverConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/server/${serverId}"
            callOpts.body = [
                    server:[:]
            ]
            if(serverConfig.planRef) {
                callOpts.body.server.plan = serverConfig.planRef
            }
            if(serverConfig.containsKey('maxMemory')) {
                callOpts.body.server.core_number = (serverConfig.maxCores ?: 1)
                callOpts.body.server.memory_amount = (serverConfig.maxMemory ?: ComputeUtility.ONE_GIGABYTE) / ComputeUtility.ONE_MEGABYTE
            }
            def callResults = callApi(authConfig, callPath, callOpts, 'PUT')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on resizeServer: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static resizeStorage(Map authConfig, String storageId, Map storageConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage/${storageId}"
            def maxStorage = (int)storageConfig.maxStorage.div(ComputeUtility.ONE_GIGABYTE)
            callOpts.body = [
                    storage: [
                            size:maxStorage
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'PUT')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on resizeStorage: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static createStorage(Map authConfig, Map storageConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage"
            def maxStorage = (int)storageConfig.maxStorage.div(ComputeUtility.ONE_GIGABYTE)
            callOpts.body = [
                    storage: [
                            size:maxStorage,
                            tier:storageConfig.tier ?: 'maxiops',
                            title:storageConfig.serverName + ' ' + (storageConfig.name ?: 'disk ' + (storageConfig.index ?: 1)),
                            zone:storageConfig.zoneRef
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on resizeStorage: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static createSnapshot(Map authConfig, String stroageId, Map storageConfig) {
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage/${stroageId}/backup"
            callOpts.body = [
                    storage: [
                            title:storageConfig.snapshotName
                    ]
            ]
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on createSnapshot: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    static waitForStorageStatus(Map authConfig, String storageId, String status, Map opts=[:]) {
        def rtn = [success:false]
        def maxAttempts = opts?.maxAttempts ?: 100
        def retryInterval = opts?.retryInterval ?: (1000l * 5l)
        try {
            def pending = true
            def attempts = 0
            while(pending) {
                def serverDetail = getStorageDetails(authConfig, storageId)
                if(serverDetail.success == true && serverDetail?.storage?.state == status) {
                    rtn.success = true
                    rtn.results = serverDetail.storage
                    pending = false
                    break
                }
                attempts ++
                if(attempts > maxAttempts) {
                    pending = false
                } else {
                    sleep(retryInterval)
                }
            }
        } catch(e) {
            log.error(e)
        }
        return rtn
    }

    static restoreSnapshot(Map authConfig, String storageId) {
        log.info("restoring snapshot with storage id: ${storageId}")
        def rtn = [success:false]
        try {
            def callOpts = [:]
            def callPath = "/storage/${storageId}/restore"
            def callResults = callApi(authConfig, callPath, callOpts, 'POST')
            if(callResults.success == true) {
                rtn.data = callResults.data
                rtn.success = true
            } else {
                rtn.success = false
            }
        } catch (e) {
            log.error "Error on restoreSnapshot: ${e}", e
            rtn.success = false
        }
        return rtn
    }

    /*static listSnapshots(apiUrl, apiKey, opts){
        def rtn = [success:false]
        def headers = ['Content-Type':'application/json']
        def results = callApi(apiUrl, '/v2/droplets/' + opts.server.externalId + '/snapshots', apiKey, [headers:headers, requestContentType:ContentType.JSON], Method.GET)
        log.debug("got: ${results}")
        rtn.success = results?.success
        if(rtn.success == true) {
            rtn.results = new groovy.json.JsonSlurper().parseText(results.content)
            rtn.action = rtn.results?.action
        }
        return rtn
    }*/

    static ServiceResponse callApi(Map authConfig, String path, Map opts = [:], String method) {
        log.info("in call api")
        def apiUrl = authConfig.apiUrl.toString()
        def username = authConfig.username.toString()
        def password = authConfig.password.toString()
        def apiVersion = authConfig.apiVersion ?: upcloudApiVersion
        def apiPath = "${apiVersion}${path}".toString()
        //log.info("calling to: ${apiUrl}; path: ${apiVersion}${path}, opts: ${JsonOutput.prettyPrint(JsonOutput.toJson(opts + [password: '*******']))}")
        log.info("calling to: ${apiUrl}; path: ${apiVersion}${path}, opts: ${opts}")

        RequestOptions requestOptions = new RequestOptions(headers: [:])
        if(opts.body) {
           requestOptions.body = opts.body
        }

        if(opts.query) {
            opts.query?.each { k, v ->
                requestOptions.queryParams.(k) = v
            }
        }

        if(opts.headers) {
            opts.headers?.each { k, v ->
                requestOptions.headers.(k) = v
            }
        }

        requestOptions.headers['Content-Type'] = 'application/json'

        HttpApiClient client = new HttpApiClient()
        log.info("about to call json api")
        log.info("apiUrl: ${apiUrl}")
        log.info("apiPath: ${apiPath}")
        log.info("username: ${username}")
        log.info("requestOptions: ${requestOptions.body}, ${requestOptions.queryParams}, ${requestOptions.headers}")
        log.info("method: ${method}")

        ServiceResponse response = client.callJsonApi(apiUrl, apiPath, username, password, requestOptions, method)
        log.info("called json api: ${response}")
        return response
    }
}
