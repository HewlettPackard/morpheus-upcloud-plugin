# Morpheus UpCloud Plugin

This plugin provides a full integration between [UpCloud](https://upcloud.com) and [Morpheus](https://morpheusdata.com). It enables cloud inventory sync, server provisioning, VM power and resize actions, and backup/restore from within the Morpheus platform.

## 📑 Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Repository structure](#repository-structure)
- [Building the plugin](#building-the-plugin)
- [License](#license)
- [Installing](#installing)
- [Detailed Usage Step](#detailed-usage-step)
- [API Endpoints](#api-endpoints)

---

## Features

### Cloud Sync

The following resources are discovered and kept in sync from UpCloud:

- **Zones** — available UpCloud zones/regions
- **Plans** — UpCloud server plans and pricing data
- **Public Templates** — UpCloud public OS templates available for provisioning
- **User Images** — private/user-created templates available for provisioning
- **Virtual Machines** — UpCloud servers, including power state and network details

Any additions, updates, and removals in UpCloud are automatically reflected in Morpheus on the next sync cycle.

### Provisioning

Virtual machines can be provisioned into UpCloud directly from Morpheus using standard instance types and layouts. Supported operations include:

- Create, start, stop, and delete servers
- Resize servers to a different UpCloud plan
- Attach and detach storage volumes (MaxIOPS, HDD, and Standard volume types)
- Use UpCloud public/user templates and uploaded SSH keys during provisioning
- Provision Linux, Windows, Docker host, and Kubernetes node server types

### Backups

UpCloud server backups are supported via the Morpheus backup framework. Supported operations include:

- Execute backup jobs through Morpheus
- Restore backups to existing workloads

## Requirements

| Component | Minimum Version |
|-----------|----------------|
| Morpheus | 8.1.2 |

## Repository structure

- `src/main/groovy` - Source code for the plugin
  - `UpcloudPlugin.groovy` - Plugin entry point; registers all providers and resolves API credentials
  - `UpcloudCloudProvider.groovy` - Defines the UpCloud cloud/zone type, network types, storage volume types, and compute server types
  - `UpcloudProvisionProvider.groovy` - Handles provisioning, resizing, starting/stopping, and removal of UpCloud servers
  - `UpcloudOptionSourceProvider.groovy` - Supplies option lists (e.g. zones, plans) for Morpheus UI forms
  - `UpcloudBackupProvider.groovy`, `UpcloudBackupExecutionProvider.groovy`, `UpcloudBackupRestoreProvider.groovy`, `UpcloudBackupTypeProvider.groovy` - Backup, backup execution, and restore support for UpCloud servers
  - `datasets/` - Dataset providers (`UpcloudCloudRegionDatasetProvider`, `UpcloudImageDatasetProvider`) used to populate region and image selection lists
  - `services/UpcloudApiService.groovy` - Wraps calls to the UpCloud REST API (zones, plans, templates, servers, storage)
  - `sync/` - Synchronization tasks that keep Morpheus in sync with UpCloud (`PlansSync`, `PublicTemplatesSync`, `UserImagesSync`, `VirtualMachinesSync`)
  - `util/` - Utility classes (`UpcloudComputeUtility`, `UpcloudStatusUtility`) used for API communication and server status handling
- `src/assets` - Plugin assets, including the UpCloud logo (`assets/upcloud.svg`) used in the Morpheus UI
- `build.gradle` and `gradle.properties` - Build configuration and dependency/version properties for the plugin

## Building the plugin

Run the following command to compile and package the plugin jar:

```bash
./gradlew shadowJar
```

The plugin JAR will be written to `build/libs/`.

To execute tests, use the following command:

```bash
./gradlew test
```

## License

Copyright 2022 Morpheus Data, LLC. Licensed under the [Apache License, Version 2.0](LICENSE).

## Installing

1. Download the latest `.jar` from the [Releases](https://github.com/HewlettPackard/morpheus-upcloud-plugin/releases) page, or [build it yourself](#building-the-plugin).
2. In Morpheus, navigate to **Administration → Integrations → Plugins**.
3. Click **Browse** and upload the `.jar` file.
4. The **UpCloud** cloud type will appear after the plugin loads.

## Detailed Usage Step

When adding an UpCloud cloud in Morpheus (**Infrastructure → Clouds → Add Cloud**), provide the following:

| Field | Description |
|-------|-------------|
| **Credentials** | Select local credentials or a stored username/password credential |
| **Username** | UpCloud account username |
| **Password** | UpCloud account password |
| **Zone** | UpCloud zone/region to add to Morpheus |
| **Inventory** | Inventory level for discovering existing UpCloud servers |

Credentials can also be stored as a Morpheus [Credential](https://docs.morpheusdata.com/en/latest/administration/credentials/credentials.html) and selected at cloud setup time.

Once the cloud is added, Morpheus will trigger an initial sync of zones, plans, templates, images, and existing virtual machines. From there, instances can be provisioned into UpCloud using standard Morpheus instance types and layouts, and backups can be configured through the Morpheus backup framework.

## API Endpoints

The plugin communicates with the UpCloud REST API at `https://api.upcloud.com` (API version `1.3`):

- `/zone` (GET) - List available UpCloud zones/regions
- `/plan` (GET) - List available server plans
- `/price` (GET) - List pricing information for zones, plans, and resources
- `/storage/template` (GET) - List public and private (user) storage templates/images
- `/storage` (POST) - Create a new storage volume
- `/storage/{storageId}` (GET, PUT, DELETE) - Retrieve, resize, or delete a specific storage volume
- `/storage/{storageId}/backup` (POST) - Create a snapshot/backup of a storage volume
- `/storage/{storageId}/restore` (POST) - Restore a storage volume from a snapshot/backup
- `/server` (GET, POST) - List servers and create a new server
- `/server/{serverId}` (GET, PUT, DELETE) - Retrieve, resize/modify, or delete a specific server
- `/server/{serverId}/start` (POST) - Start a server
- `/server/{serverId}/stop` (POST) - Stop or power off a server
- `/server/{serverId}/storage/attach` (POST) - Attach a storage volume to a server
- `/server/{serverId}/storage/detach` (POST) - Detach a storage volume from a server
