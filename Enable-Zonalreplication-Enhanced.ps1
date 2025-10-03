# Enhanced Enable-ZonalReplication.ps1 with automatic protection container mapping creation
param(
    [string] $VaultSubscriptionId,
    [string] $VaultResourceGroupName,
    [string] $VaultName,
    [string] $Region,
    [string] $PolicyName,
    [string] $SourceVmARMIdsCSV,
    [string] $TargetResourceGroupId,
    [string] $TargetVirtualNetworkId,
    [string] $SourceAvailabilityZone = "2",
    [string] $TargetAvailabilityZone = "1",
    [string] $PrimaryStagingStorageAccount,
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
Write-Output $CRLF

# Setup the vault context
$message = 'Setting Vault context using vault {0} under resource group {1} in subscription {2}.' -f $VaultName, $VaultResourceGroupName, $VaultSubscriptionId
Write-Output $message
Select-AzSubscription -SubscriptionId $VaultSubscriptionId
$vault = Get-AzRecoveryServicesVault -ResourceGroupName $VaultResourceGroupName -Name $VaultName
Set-AzRecoveryServicesAsrVaultContext -vault $vault
$message = 'Vault context set.'
Write-Output $message
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
} else {
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

# Enhanced: Create protection container mapping for zone-to-zone replication
Write-Output "Setting up protection container mapping for zone-to-zone replication..."

$mappingName = "mapping-zone$SourceAvailabilityZone-to-zone$TargetAvailabilityZone"
$containerMapping = $null

try {
    # Check if mapping already exists
    $containerMapping = Get-AzRecoveryServicesAsrProtectionContainerMapping -Name $mappingName -ProtectionContainer $container -ErrorAction SilentlyContinue

    if ($null -eq $containerMapping) {
        Write-Output "Creating new protection container mapping: $mappingName"

        # For zone-to-zone replication within the same region, use the same container as both source and target
        $mappingJob = New-AzRecoveryServicesAsrProtectionContainerMapping -Name $mappingName -Policy $policy -PrimaryProtectionContainer $container -RecoveryProtectionContainer $container

        # Wait for mapping creation to complete
        do {
            Start-Sleep -Seconds 30
            $mappingJob = Get-AzRecoveryServicesAsrJob -Job $mappingJob
            Write-Output "Mapping creation job state: $($mappingJob.State)"
        } while ($mappingJob.State -ne 'Succeeded' -and $mappingJob.State -ne 'Failed' -and $mappingJob.State -ne 'CompletedWithInformation')

        if ($mappingJob.State -eq 'Failed') {
            $message = 'Protection container mapping creation failed: {0}' -f $mappingJob.DisplayName
            Write-Output $message
            foreach ($er in $mappingJob.Errors) {
                foreach ($pe in $er.ProviderErrorDetails) {
                    Write-Output "Provider Error: $pe"
                }
                foreach ($se in $er.ServiceErrorDetails) {
                    Write-Output "Service Error: $se"
                }
            }
            throw $message
        }

        # Get the created mapping
        $containerMapping = Get-AzRecoveryServicesAsrProtectionContainerMapping -Name $mappingName -ProtectionContainer $container
        Write-Output "Successfully created protection container mapping: $($containerMapping.Name)"
    } else {
        Write-Output "Using existing protection container mapping: $($containerMapping.Name)"
    }

} catch {
    Write-Output "Error with protection container mapping: $($_.Exception.Message)"
    # Try to find any existing compatible mapping as fallback
    try {
        $allMappings = Get-AzRecoveryServicesAsrProtectionContainerMapping -ProtectionContainer $container
        $compatibleMapping = $allMappings | Where-Object { $_.PolicyId -eq $policy.Id } | Select-Object -First 1

        if ($null -ne $compatibleMapping) {
            Write-Output "Using existing compatible mapping as fallback: $($compatibleMapping.Name)"
            $containerMapping = $compatibleMapping
        } else {
            throw "No compatible protection container mapping found and unable to create new one"
        }
    } catch {
        throw "Unable to set up protection container mapping for zone-to-zone replication: $($_.Exception.Message)"
    }
}

$message = 'Using Protection Container Mapping {0}' -f $containerMapping.Id
Write-Output $message
Write-Output $CRLF

# Continue with the rest of the replication logic...
# (VM replication setup code follows here - same as original script)

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
        # Use the protection container mapping we created/found
        $job = New-AzRecoveryServicesAsrReplicationProtectedItem -Name $vmName -ProtectionContainerMapping $containerMapping -AzureVmId $vm.ID -AzureToAzureDiskReplicationConfiguration $diskList -RecoveryResourceGroupId $TargetResourceGroupId -RecoveryAzureNetworkId $TargetVirtualNetworkId -RecoveryAvailabilityZone $TargetAvailabilityZone

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

# Continue with monitoring jobs and final output...
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
