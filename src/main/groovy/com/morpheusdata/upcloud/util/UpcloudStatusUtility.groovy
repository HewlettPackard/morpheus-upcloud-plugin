package com.morpheusdata.upcloud.util

import com.morpheusdata.*
import groovy.json.JsonOutput
import groovy.util.logging.Commons
import org.apache.http.*
import org.apache.http.client.*
import org.apache.http.client.methods.*
import org.apache.http.client.utils.*
import org.apache.http.entity.*
import org.apache.http.util.*

@Commons
class UpcloudStatusUtility {
    static testConnection(Map authConfig) {
        def rtn = [success:false, invalidLogin:false]
        try {
            def results = listZones(authConfig)
            rtn.success = results.success
        } catch(e) {
            log.error("testConnection to upcloud: ${e}")
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
                    rtn.success = true
                    rtn.results = serverDetail.server
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

    static validateServerConfig(Map opts=[:]){
        def rtn = [success: true, errors: []]
        if(opts.containsKey('nodeCount') && !opts.nodeCount){
            rtn.errors += [field:'nodeCount', msg:'Cannot be blank']
            rtn.errors += [field:'config.nodeCount', msg:'Cannot be blank']
        }
        rtn.success = (rtn.errors.size() == 0)
        return rtn
    }
}
