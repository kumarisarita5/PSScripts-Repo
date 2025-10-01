param(
    [string] $VaultSubscriptionId = "1f847e97-a9e0-4335-9e1a-531c116d57e0",
    [string] $VaultResourceGroupName = "lab-sark",
    [string] $VaultName ="lab-rsv",
    [string] $Region ="canadacentral",
    [string] $PolicyName = '24-hour-retention-policy',
    [string] $SourceVmARMIdsCSV = "/subscriptions/1f847e97-a9e0-4335-9e1a-531c116d57e0/resourceGroups/lab-sark/providers/Microsoft.Compute/virtualMachines/lab-sark-vm1",
    [string] $TargetResourceGroupId = "/subscriptions/1f847e97-a9e0-4335-9e1a-531c116d57e0/resourceGroups/lab-sark-asr",
    [string] $TargetVirtualNetworkId = "/subscriptions/1f847e97-a9e0-4335-9e1a-531c116d57e0/resourceGroups/lab-sark/providers/Microsoft.Network/virtualNetworks/vnet-canadacentral-2",
    [string] $SourceAvailabilityZone = "1",
    [string] $TargetAvailabilityZone = "2",
    [string] $PrimaryStagingStorageAccount = "/subscriptions/1f847e97-a9e0-4335-9e1a-531c116d57e0/resourceGroups/lab-sark/providers/Microsoft.Storage/storageAccounts/contosoappsa1",
    [string] $RecoveryReplicaDiskAccountType = 'Standard_LRS',
    [string] $RecoveryTargetDiskAccountType = 'Standard_LRS'
)

$CRLF = "`r`n"

# Initialize the designated output of deployment script
$DeploymentScriptOutputs = @{}
$sourceVmARMIds = New-Object System.Collections.ArrayList
foreach ($sourceId in $SourceVmARMIdsCSV.Split(',')) {
    $sourceVmARMIds.Add($sourceId.Trim())
}

$message = 'Enable zonal replication will be triggered for following {0} VMs' -f $sourceVmARMIds.Count
foreach ($sourceVmArmId in $sourceVmARMIds) {
    $message += "`n$sourceVmArmId"
}
Write-Output $message
# Get vault directly - the managed identity already has the correct subscription context
try {
    $vault = Get-AzRecoveryServicesVault -ResourceGroupName $VaultResourceGroupName -Name $VaultName
    if ($null -eq $vault) {
        throw "Vault $VaultName not found in resource group $VaultResourceGroupName"
    }
    Write-Output "Found vault: $($vault.Name)"
} catch {
    Write-Output "Error getting vault: $($_.Exception.Message)"
    throw "Failed to get Recovery Services Vault. Ensure the vault exists and the managed identity has proper permissions."
}

try {
    Set-AzRecoveryServicesAsrVaultContext -vault $vault
    $message = 'Vault context set.'
    Write-Output $message
} catch {
    Write-Output "Error setting vault context: $($_.Exception.Message)"
    throw "Failed to set vault context. Ensure the managed identity has Site Recovery permissions."
}
Write-Output $CRLF

# Get existing fabrics
	 
$azureFabrics = Get-AzRecoveryServicesAsrFabric
Write-Output "Found $($azureFabrics.Count) existing fabrics"

# Setup the fabric for the region
$fabric = $azureFabrics | Where-Object {$_.FabricSpecificDetails.Location -eq $Region}
if ($null -eq $fabric) {
    Write-Output "Fabric does not exist. Creating Fabric for region $Region."
		 
    $job = New-AzRecoveryServicesAsrFabric -Azure -Name $Region -Location $Region
    do {
        Start-Sleep -Seconds 50
        $job = Get-AzRecoveryServicesAsrJob -Job $job
																   
    } while ($job.State -ne 'Succeeded' -and $job.State -ne 'Failed' -and $job.State -ne 'CompletedWithInformation')

    if ($job.State -eq 'Failed') {
        $message = 'Job {0} failed for {1}' -f $job.DisplayName, $job.TargetObjectName
        Write-Output $message
        foreach ($er in $job.Errors) {
            foreach ($pe in $er.ProviderErrorDetails) {
                $pe
            }
            foreach ($se in $er.ServiceErrorDetails) {
                $se
            }
        }
															   
									  
			 
																	 
        throw $message
    }
    $fabric = Get-AzRecoveryServicesAsrFabric -Name $Region
    Write-Output 'Created Fabric.'
}

$message = 'Using Fabric {0}' -f $fabric.Id
Write-Output $message
Write-Output $CRLF

# Setup the Protection Container
$containers = Get-AzRecoveryServicesAsrProtectionContainer -Fabric $fabric
if ($null -eq $containers -or $containers.Count -eq 0) {
    Write-Output 'Protection container does not exist. Creating Protection Container.'
    $job = New-AzRecoveryServicesAsrProtectionContainer -Name $fabric.Name -Fabric $fabric
    do {
        Start-Sleep -Seconds 50
        $job = Get-AzRecoveryServicesAsrJob -Job $job
    } while ($job.State -ne 'Succeeded' -and $job.State -ne 'Failed' -and $job.State -ne 'CompletedWithInformation')

    if ($job.State -eq 'Failed') {
        $message = 'Job {0} failed for {1}' -f $job.DisplayName, $job.TargetObjectName
        Write-Output $message
        foreach ($er in $job.Errors) {
            foreach ($pe in $er.ProviderErrorDetails) {
                $pe
            }
            foreach ($se in $er.ServiceErrorDetails) {
                $se
            }
        }
        throw $message
    }
    $container = Get-AzRecoveryServicesAsrProtectionContainer -Name $fabric.Name -Fabric $fabric
    Write-Output 'Created Protection Container.'
}
else {
    # For zonal ASR, we use the same container for both source and target
    $container = $containers[0]
}

$message = 'Using Protection Container {0}' -f $container.Id
Write-Output $message
Write-Output $CRLF

# Create or get replication policy
$policy = Get-AzRecoveryServicesAsrPolicy -Name $PolicyName
if ($null -eq $policy) {
    Write-Output 'Replication policy does not exist. Creating Replication policy.'
    $job = New-AzRecoveryServicesAsrPolicy -AzureToAzure -Name $PolicyName -RecoveryPointRetentionInHours 24 -ApplicationConsistentSnapshotFrequencyInHours 4
    do {
        Start-Sleep -Seconds 50
        $job = Get-AzRecoveryServicesAsrJob -Job $job
    } while ($job.State -ne 'Succeeded' -and $job.State -ne 'Failed' -and $job.State -ne 'CompletedWithInformation')

    if ($job.State -eq 'Failed') {
        $message = 'Job {0} failed for {1}' -f $job.DisplayName, $job.TargetObjectName
        Write-Output $message
        foreach ($er in $job.Errors) {
            foreach ($pe in $er.ProviderErrorDetails) {
                $pe
            }
            foreach ($se in $er.ServiceErrorDetails) {
                $se
            }
        }
        throw $message
    }
    $policy = Get-AzRecoveryServicesAsrPolicy -Name $PolicyName
    Write-Output 'Created Replication policy.'
}

$message = 'Using Policy {0}' -f $policy.Id
Write-Output $message
Write-Output $CRLF

# For zonal replication, first check if any protection container mappings exist
# If not, this indicates zone-to-zone replication might need to be set up differently
Write-Output "Checking for existing protection container mappings..."

try {
    # Get all fabrics and containers
    $allFabrics = Get-AzRecoveryServicesAsrFabric
    Write-Output "Found $($allFabrics.Count) fabrics total"

    # Get all existing mappings across all containers
    $allMappings = @()
    foreach ($fabric in $allFabrics) {
        $containers = Get-AzRecoveryServicesAsrProtectionContainer -Fabric $fabric
        foreach ($container in $containers) {
            try {
                $mappings = Get-AzRecoveryServicesAsrProtectionContainerMapping -ProtectionContainer $container -ErrorAction SilentlyContinue
                if ($mappings) {
                    $allMappings += $mappings
                }
            } catch {
                # Continue if no mappings found for this container
                Write-Output "No mappings found for container: $($container.Name)"
            }
        }
    }

    Write-Output "Found $($allMappings.Count) total protection container mappings"

    # Find a mapping that uses our policy
    $compatibleMappings = $allMappings | Where-Object { $_.PolicyId -eq $policy.Id }

    if ($null -eq $compatibleMappings -or $compatibleMappings.Count -eq 0) {
        Write-Output "No existing mapping found for policy $($policy.Name)"
        Write-Output "For zone-to-zone replication, creating a mapping between different containers if available..."

        # For zone-to-zone, we need containers that can map to each other
        # Try to find if we have multiple fabrics or containers
        $canadaCentralFabrics = $allFabrics | Where-Object {$_.FabricSpecificDetails.Location -eq $Region}

        if ($canadaCentralFabrics.Count -ge 1) {
            $primaryFabric = $canadaCentralFabrics[0]
            $primaryContainers = Get-AzRecoveryServicesAsrProtectionContainer -Fabric $primaryFabric

            if ($primaryContainers.Count -ge 1) {
                $primaryContainer = $primaryContainers[0]

                # For zone-to-zone within same region, try using the same container
                # but with a different approach - check if we can create without explicit mapping
                Write-Output "Attempting zone-to-zone replication setup..."
                Write-Output "This may require the target to be set up via Azure Portal first for zone-to-zone scenarios"

                # Use the primary container as both source and target for zone-to-zone
                $containerMapping = $primaryContainer  # We'll modify the replication call
                $useContainerDirectly = $true
            } else {
                throw "No protection containers found in fabric $($primaryFabric.Name)"
            }
        } else {
            throw "No fabrics found for region $Region"
        }
    } else {
        # Use the first compatible mapping
        $containerMapping = $compatibleMappings | Select-Object -First 1
        $useContainerDirectly = $false
        Write-Output "Using existing compatible mapping: $($containerMapping.Name)"
    }

} catch {
    Write-Output "Error checking protection container mappings: $($_.Exception.Message)"
    throw "Unable to set up protection container mapping for zone-to-zone replication. You may need to configure this via Azure Portal first."
}

if (-not $useContainerDirectly) {
    $message = 'Using Protection Container Mapping {0}' -f $containerMapping.Id
    Write-Output $message
} else {
    $message = 'Using Protection Container directly for zone-to-zone: {0}' -f $containerMapping.Id
    Write-Output $message
}
Write-Output $CRLF

# Start enabling replication for all the VMs
$enableReplicationJobs = New-Object System.Collections.ArrayList
foreach ($sourceVmArmId in $sourceVmARMIds) {
    # Parse VM information
    $vmIdTokens = $sourceVmArmId.Split('/')
    $vmName = $vmIdTokens[8]
    $vmResourceGroupName = $vmIdTokens[4]
    $message = 'Enable zonal protection to be triggered for {0} using VM name {1}.' -f $sourceVmArmId, $vmName
    $vm = Get-AzVM -ResourceGroupName $vmResourceGroupName -Name $vmName
    Write-Output $message

    # Create disk replication configuration
    $diskList = New-Object System.Collections.ArrayList

    # OS Disk configuration
    $osDisk = New-AzRecoveryServicesAsrAzureToAzureDiskReplicationConfig -DiskId $vm.StorageProfile.OsDisk.ManagedDisk.Id -LogStorageAccountId $PrimaryStagingStorageAccount -ManagedDisk -RecoveryReplicaDiskAccountType $RecoveryReplicaDiskAccountType -RecoveryResourceGroupId $TargetResourceGroupId -RecoveryTargetDiskAccountType $RecoveryTargetDiskAccountType
    $diskList.Add($osDisk)

    # Data Disks configuration
    foreach($dataDisk in $vm.StorageProfile.DataDisks) {
        $disk = New-AzRecoveryServicesAsrAzureToAzureDiskReplicationConfig -DiskId $dataDisk.ManagedDisk.Id -LogStorageAccountId $PrimaryStagingStorageAccount -ManagedDisk -RecoveryReplicaDiskAccountType $RecoveryReplicaDiskAccountType -RecoveryResourceGroupId $TargetResourceGroupId -RecoveryTargetDiskAccountType $RecoveryTargetDiskAccountType
        $diskList.Add($disk)
    }

    # Create the replication protected item with zonal configuration
    $message = 'Enable zonal protection being triggered.'
    Write-Output $message

    try {
        if ($useContainerDirectly) {
            # For zone-to-zone scenarios where we use container directly
            # This approach may not work and might require Azure Portal setup
            Write-Output "Attempting direct container approach (experimental for zone-to-zone)"
            throw "Zone-to-zone replication via PowerShell requires pre-configured protection container mappings. Please set up zone-to-zone replication via Azure Portal first, then this script can manage existing replicated items."
        } else {
            # Standard approach with protection container mapping
            $job = New-AzRecoveryServicesAsrReplicationProtectedItem -Name $vmName -ProtectionContainerMapping $containerMapping -AzureVmId $vm.ID -AzureToAzureDiskReplicationConfiguration $diskList -RecoveryResourceGroupId $TargetResourceGroupId -RecoveryAzureNetworkId $TargetVirtualNetworkId -RecoveryAvailabilityZone $TargetAvailabilityZone
        }
        
        if ($null -ne $job) {
            $enableReplicationJobs.Add($job)
            Write-Output "Successfully initiated replication job for $vmName"
        } else {
            Write-Output "Failed to create replication job for $vmName - job is null"
        }
    }
    catch {
        Write-Output "Error enabling replication for $vmName : $($_.Exception.Message)"
        throw $_
    }
}

Write-Output $CRLF

# Monitor each enable replication job
$protectedItemArmIds = New-Object System.Collections.ArrayList
foreach ($job in $enableReplicationJobs) {
    if ($null -eq $job) {
        Write-Output "Skipping null job"
        continue
    }
    
    Write-Output "Monitoring job: $($job.Name)"
    do {
        Start-Sleep -Seconds 50
        $job = Get-AzRecoveryServicesAsrJob -Job $job
        Write-Output "Job State: $($job.State)"
    } while ($job.State -ne 'Succeeded' -and $job.State -ne 'Failed' -and $job.State -ne 'CompletedWithInformation')

    if ($job.State -eq 'Failed') {
        $message = 'Job {0} failed for {1}' -f $job.DisplayName, $job.TargetObjectName
        Write-Output $message
        foreach ($er in $job.Errors) {
            foreach ($pe in $er.ProviderErrorDetails) {
                Write-Output "Provider Error: $pe"
            }
            foreach ($se in $er.ServiceErrorDetails) {
                Write-Output "Service Error: $se"
            }
        }
        throw $message
    }

    $targetObjectName = $job.TargetObjectName
    $message = 'Enable protection completed for {0}. Waiting for IR.' -f $targetObjectName
    Write-Output $message

    # Wait for Initial Replication completion
    $startTime = $job.StartTime
    $irFinished = $false
    $irWaitCount = 0
    do {
        $irJobs = Get-AzRecoveryServicesAsrJob | Where-Object {$_.JobType -like '*IrCompletion' -and $_.TargetObjectName -eq $targetObjectName -and $_.StartTime -gt $startTime} | Sort-Object StartTime -Descending | Select-Object -First 2
        if ($null -ne $irJobs -and $irJobs.Length -ne 0) {
            $secondaryIrJob = $irJobs | Where-Object {$_.JobType -like 'SecondaryIrCompletion'}
            if ($null -ne $secondaryIrJob -and $secondaryIrJob.Length -ge 1) {
                $irFinished = $secondaryIrJob.State -eq 'Succeeded' -or $secondaryIrJob.State -eq 'Failed'
            }
            else {
                $irFinished = $irJobs[0].State -eq 'Failed'
            }
        }

        if (-not $irFinished) {
            Start-Sleep -Seconds 50
            $irWaitCount++
            if ($irWaitCount -gt 20) {  # Limit waiting time
                Write-Output "IR wait time exceeded for $targetObjectName, continuing..."
                $irFinished = $true
            }
        }
    } while (-not $irFinished)

    $message = 'IR wait completed for {0}.' -f $targetObjectName
    Write-Output $message

    try {
        $rpi = Get-AzRecoveryServicesAsrReplicationProtectedItem -Name $targetObjectName -ProtectionContainer $container
        $message = 'Enable zonal replication completed for {0}.' -f $rpi.ID
        Write-Output $message
        $protectedItemArmIds.Add($rpi.Id)
    }
    catch {
        Write-Output "Could not retrieve protected item for $targetObjectName : $($_.Exception.Message)"
        # Continue with other VMs
    }
}

$DeploymentScriptOutputs['ProtectedItemArmIds'] = $protectedItemArmIds -join ','

# Log consolidated output
Write-Output 'Infrastructure Details'
foreach ($key in $DeploymentScriptOutputs.Keys) {
    $message = '{0} : {1}' -f $key, $DeploymentScriptOutputs[$key]
    Write-Output $message
}

