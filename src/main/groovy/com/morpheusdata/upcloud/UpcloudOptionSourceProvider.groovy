package com.morpheusdata.upcloud

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import groovy.util.logging.Slf4j

@Slf4j
class UpcloudOptionSourceProvider extends AbstractOptionSourceProvider{
    UpcloudPlugin plugin
    MorpheusContext morpheusContext

    UpcloudOptionSourceProvider(UpcloudPlugin plugin, MorpheusContext context) {
        this.plugin = plugin
        this.morpheusContext = context
    }

    @Override
    MorpheusContext getMorpheus() {
        return this.morpheusContext
    }

    @Override
    Plugin getPlugin() {
        return this.plugin
    }

    @Override
    String getCode() {
        return 'upcloud-option-source'
    }

    @Override
    String getName() {
        return 'Upcloud Option Source'
    }

    @Override
    List<String> getMethodNames() {
        return new ArrayList<String>(['upcloudPluginInventoryLevels'])
    }

    def upcloudPluginInventoryLevels(args) {
        [
            [name:'Basic', value:'basic'],
            [name:'Full', value:'full']
        ]
    }
}
