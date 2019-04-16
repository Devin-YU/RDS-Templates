[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ErrorActionPreference = "Stop"

#region Functions and other supporting elements

<#
.SYNOPSIS
Function for creating a variable from XML
#>
function Set-ScriptVariable ($Name, $Value) {
    Invoke-Expression ("`$Script:" + $Name + " = `"" + $Value + "`"")
}

#region StorageTableFunctions

#region Enums
enum LogLevel
{
    Informational
    Warning
    Error
}

enum HAStatuses
{
    Running
    Completed
    Failed
}
#endregion

function Add-TableLog
{
    <#
        .SYNOPSIS
            Add a log entry into storage table
    #>
    param
    (
        [string]$EntityName,
        [string]$Message,
        [logLevel]$Level,
        [string]$ActivityId,
        $LogTable
    )

    $LogTimeStampUTC = ([System.DateTime]::UtcNow)

    # Creating job submission information
    $logEntryId = [guid]::NewGuid().Guid
    [hashtable]$logProps = @{ "LogTimeStampUTC"=$LogTimeStampUTC;
                              "ActivityId"=$ActivityId;
                              "EntityName"=$EntityName;
                              "message"=$message;
                              "logLevel"=$level.ToString()}

    Add-AzTableRow -table $logTable -partitionKey $ActivityId -rowKey $logEntryId -property $logProps
}

#endregion
#endregion


$CurrentPath = Split-Path $script:MyInvocation.MyCommand.Path

##### XML path #####
$XMLPath = "$CurrentPath\..\Config.xml"

###### Verify XML file ######
if (Test-Path $XMLPath) {
    Write-Verbose "Found $XMLPath"
    Write-Verbose "Validating file..."
    try {
        $Variable = [xml](Get-Content $XMLPath)
    }
    catch {
        $Validate = $false
        Write-Error "$XMLPath is invalid. Check XML syntax - Unable to proceed"
        exit 1
    }
}
else {
    $Validate = $false
    Write-Error "Missing $XMLPath - Unable to proceed"
    exit 1
}

##### Load XML configuration values as variables #####
Write-Verbose "loading values from Config.xml"
$Variable = [xml](Get-Content "$XMLPath")
$Variable.RDMIScale.Azure | ForEach-Object { $_.Variable } | Where-Object { $_.Name -ne $null } | ForEach-Object { Set-ScriptVariable -Name $_.Name -Value $_.Value }
$Variable.RDMIScale.RdmiScaleSettings | ForEach-Object { $_.Variable } | Where-Object { $_.Name -ne $null } | ForEach-Object { Set-ScriptVariable -Name $_.Name -Value $_.Value }
$Variable.RDMIScale.Deployment | ForEach-Object { $_.Variable } | Where-Object { $_.Name -ne $null } | ForEach-Object { Set-ScriptVariable -Name $_.Name -Value $_.Value }

##### Load functions/module #####
Import-Module AzTable
Import-Module Microsoft.RdInfra.RdPowershell
. $CurrentPath\..\Functions-PSStoredCredentials.ps1

##### Login with delegated access in WVD tenant #####
$Credential = Get-StoredCredential -UserName $Username

##### Authenticating to Azure #####
$appCreds = Get-StoredCredential -UserName $AADApplicationId
try
{
    try
    {
        Add-AzAccount -SubscriptionId $currentAzureSubscriptionId -Credential $appCreds
    }
    catch
    {
        if ($error.exception.HResult -eq -2146233087)
        {
            Add-AzAccount -SubscriptionId $currentAzureSubscriptionId -Credential $appCreds -ServicePrincipal -Tenant $AADTenantId
        }
        else
        {
            throw $error.exception
        }
    }
}
catch {
    exit 1
}

##### select the current Azure subscription specified in the config #####
Select-AzSubscription -SubscriptionId $currentAzureSubscriptionId

#### PMC
#### HA - Decision to exit if running or take ownership
$ScalingHATableName = "WVDScalingHA"

# Get tables
$ScalingHATable = Get-AzTableTable -resourceGroup $StorageAccountRG -TableName $ScalingHATableName -storageAccountName $StorageAccountName

if ($ScalingHATable -eq $null) 
{
    throw "An error ocurred trying to obtain table $ScalingHATableName in Storage Account $StorageAccountName at Resource Group $StorageAccountRG"
}

# Get owner record
$PartitionKey = "ScalingOwnership"
$RowKey = "ScalingOwnerEntity"

for ($i=0;$i -le 100;$i++)
{
    Write-Verbose -Verbose "Iteration: $i"
    $Seconds = ((Get-Random -Minimum $TakeOverThresholdMin -Maximum $LongRunningTakeOverThresholdMin)*60)
    Write-Verbose -Verbose "Completing current task in $Seconds seconds"
    
    Start-Sleep -Seconds $Seconds

    # Initializing owner record if it does not exist yet
    $OwnerRecord = Get-AzTableRow -Table $ScalingHATable -PartitionKey $PartitionKey -RowKey $RowKey

    $OwnerRecord | Out-String

    if ($OwnerRecord.Owner -eq $HAOwnerName)
    {
        $RecordProps = @{"Owner"=$OwnerRecord.Owner;
                         "LastUpdateUTC"=([System.DateTime]::UtcNow);
                         "Status"=([HAStatuses]::Completed).ToString();
                         "TakeOverThresholdMin"=$OwnerRecord.TakeOverThresholdMin;
                         "LongRunningTakeOverThresholdMin"=$OwnerRecord.LongRunningTakeOverThresholdMin;
                         "CurrentActivityId"=$OwnerRecord.CurrentActivityId}

        Add-AzTableRow -table $ScalingHATable -partitionKey $PartitionKey -rowKey $RowKey -property $RecordProps -UpdateExisting
    }
}