package com.morpheusdata.upcloud.services

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.response.ServiceResponse
import spock.lang.Specification

/**
 * Regression coverage for MORPH-9765 (marketplace plugin): cloud-init user_data was never
 * sent to UpCloud because createServer() read serverConfig.cloudConfig while the provision
 * provider only ever populated serverConfig.userData.
 */
class UpcloudApiServiceCreateServerSpec extends Specification {

	def "createServer sends userData as the UpCloud user_data field"() {
		given:
		def client = Mock(HttpApiClient)
		def authConfig = [apiUrl: 'https://api.upcloud.com', username: 'user', password: 'pass']
		def rootVolume = Mock(StorageVolume) {
			getMaxStorage() >> (25L * 1024 * 1024 * 1024)
		}
		def serverConfig = [
				name      : 'test-server',
				zoneRef   : 'uk-lon1',
				rootVolume: rootVolume,
				imageRef  : 'template-uuid',
				planRef   : '1xCPU-1GB',
				userData  : '#cloud-config\ntimezone: America/Denver\n'
		]

		when:
		def result = UpcloudApiService.createServer(client, authConfig, serverConfig)

		then:
		1 * client.callJsonApi(*_) >> { args ->
			def requestOptions = args[4]
			assert requestOptions.body.server.user_data == serverConfig.userData
			new ServiceResponse(success: true, data: [server: [uuid: 'abc-123']])
		}
		result.success
	}

	def "createServer omits user_data when none was built"() {
		given:
		def client = Mock(HttpApiClient)
		def authConfig = [apiUrl: 'https://api.upcloud.com', username: 'user', password: 'pass']
		def rootVolume = Mock(StorageVolume) {
			getMaxStorage() >> (25L * 1024 * 1024 * 1024)
		}
		def serverConfig = [
				name      : 'test-server',
				zoneRef   : 'uk-lon1',
				rootVolume: rootVolume,
				imageRef  : 'template-uuid',
				planRef   : '1xCPU-1GB'
		]

		when:
		UpcloudApiService.createServer(client, authConfig, serverConfig)

		then:
		1 * client.callJsonApi(*_) >> { args ->
			def requestOptions = args[4]
			assert !requestOptions.body.server.containsKey('user_data')
			new ServiceResponse(success: true, data: [server: [uuid: 'abc-123']])
		}
	}
}
