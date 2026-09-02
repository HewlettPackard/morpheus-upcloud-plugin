package com.morpheusdata.upcloud

import com.morpheusdata.core.MorpheusAsyncServices
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.MorpheusServices
import com.morpheusdata.core.MorpheusVirtualImageService
import com.morpheusdata.core.synchronous.compute.MorpheusSynchronousComputeServerService
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Instance
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.Workload
import com.morpheusdata.model.provisioning.HostRequest
import com.morpheusdata.model.provisioning.WorkloadRequest
import io.reactivex.rxjava3.core.Maybe
import spock.lang.Specification

/**
 * Regression coverage for MORPH-6828 (marketplace plugin): a newly added UpCloud cloud could
 * accept a VM submission before service plans were cached, leaving server.plan null and causing
 * an NPE deep in provisioning instead of a clean validation error.
 */
class UpcloudProvisionProviderPlanGuardSpec extends Specification {

	UpcloudPlugin plugin
	MorpheusContext context
	UpcloudProvisionProvider provider

	def setup() {
		plugin = Stub(UpcloudPlugin) {
			getAuthConfig(_ as Cloud) >> [:]
		}
		context = Stub(MorpheusContext)
		provider = new UpcloudProvisionProvider(plugin, context)
	}

	private VirtualImage buildVirtualImage() {
		new VirtualImage(id: 1L, isCloudInit: false, platform: 'linux')
	}

	private ComputeServer buildServer() {
		def rootVolume = new StorageVolume(rootVolume: true, maxStorage: 25L * 1024 * 1024 * 1024)
		new ComputeServer(
				id: 100L,
				cloud: new Cloud(id: 1L),
				account: new Account(id: 1L),
				sourceImage: buildVirtualImage(),
				plan: null,
				volumes: [rootVolume],
				maxMemory: 1024L * 1024 * 1024
		)
	}

	def "runWorkload rejects a workload with no service plan instead of NPEing"() {
		given:
		def server = buildServer()
		def instance = new Instance(id: 10L, plan: null)
		def workload = new Workload(id: 5L, server: server, instance: instance)

		def computeServerService = Stub(MorpheusSynchronousComputeServerService) {
			get(_ as Long) >> server
		}
		def services = Stub(MorpheusServices) {
			getComputeServer() >> computeServerService
		}
		def viService = Stub(MorpheusVirtualImageService) {
			get(_ as Long) >> Maybe.just(server.sourceImage)
		}
		def async = Stub(MorpheusAsyncServices) {
			getVirtualImage() >> viService
		}
		context.getServices() >> services
		context.getAsync() >> async

		when:
		def result = provider.runWorkload(workload, new WorkloadRequest(), [:])

		then:
		!result.success
		result.msg == 'No service plan was specified for this server'
	}

	def "runHost rejects a host with no service plan instead of NPEing"() {
		given:
		def server = buildServer()

		when:
		def result = provider.runHost(server, new HostRequest(), [:])

		then:
		!result.success
		result.msg == 'No service plan was specified for this server'
	}
}
