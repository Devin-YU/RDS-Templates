
<#
.SYNOPSIS
	ScalingScriptHACoreHelper.ps1 - Script module that provides all functions used for HA implementation
.DESCRIPTION
  	ScalingScriptHACoreHelper.ps1 - Script module that provides all functions used for HA implementation
#>
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$ErrorActionPreference = "Stop"

enum LogLevel
{
    Informational
    Warning
    Error
}

enum HAStatuses
{
    Initializing
    Running
    Completed
    Failed
}

enum ExecCodes
{
    ExecInProgressByOwner
    ExecInProgressByMe
    TakeOverThresholdLongRun
    TakeOverThreshold
    OwnershipRenewal
    ExitOwnerWithinThreshold
    UpdateFromOnwer
    NoLongerOwner
}

class PsOwnerToken
{
    [string]$Owner = [string]::Empty
    [datetime]$LastUpdateUTC=([System.DateTime]::UtcNow)
    [HAStatuses]$Status=[HAStatuses]::Initializing
    [int]$TakeOverThresholdMin=0
    [int]$LongRunningTakeOverThresholdMin=0
    [string]$CurrentActivityId=$ActivityId
    [bool]$ShouldExit=$false

    PsOwnerToken() {}

    PsOwnerToken([string]$Owner, [datetime]$LastUpdateUTC, [HAStatuses]$Status=[HAStatuses]::Initializing, [int]$TakeOverThresholdMin, [int]$LongRunningTakeOverThresholdMin, [string]$CurrentActivityId)
    {
        $this.Owner = $Owner
        $this.LastUpdateUTC = $LastUpdateUTC
        $this.Status = $Status
        $this.TakeOverThresholdMin = $TakeOverThresholdMin
        $this.LongRunningTakeOverThresholdMin = $LongRunningTakeOverThresholdMin
        $this.CurrentActivityId = $CurrentActivityId
    }

    [object] GetPropertiesAsHashTable() {
        return @{ "Owner"=$this.Owner;
                  "LastUpdateUTC"=$this.LastUpdateUTC;
                  "Status"=($this.Status).ToString();
                  "TakeOverThresholdMin"=$this.TakeOverThresholdMin
                  "LongRunningTakeOverThresholdMin"=$this.LongRunningTakeOverThresholdMin
                  "CurrentActivityId"=$this.CurrentActivityId}
    }
}

function RandomizeStartDelay
{
    
    # Randomizing start
    [int]$TicksSubset = (Get-Date).Ticks.Tostring().Substring((Get-Date).Ticks.ToString().Length-9)
    [int]$PSProcessId = (Get-Process powershell | Sort-Object cpu -Descending )[0].id
    [int]$RandomMs = (Get-Random -SetSeed ($TicksSubset+$PSProcessId) -Minimum 500 -Maximum 2000)
    Start-Sleep -Milliseconds $RandomMs
}

function Add-TableLog
{
    <#
    .SYNOPSIS
        Add a log entry into storage table
    #>
    param
    (
        [string]$EntityName,
        [string]$OwnerStatus,
        [string]$ExecCode,
        [string]$Message,
        [logLevel]$Level,
        [string]$ActivityId,
        $LogTable
    )

    $LogTimeStampUTC = ([System.DateTime]::UtcNow)

    # Creating job submission information
    $logEntryId = [guid]::NewGuid().Guid
    [hashtable]$logProps = @{ "LogTimeStampUTC"=$LogTimeStampUTC;
                              "OwnerStatus"=$OwnerStatus;
                              "ExecCode"=$ExecCode;
                              "ActivityId"=$ActivityId;
                              "EntityName"=$EntityName;
                              "Message"=$message;
                              "LogLevel"=$level.ToString()}

    Add-AzTableRow -table $logTable -partitionKey $ActivityId -rowKey $logEntryId -property $logProps | Out-null
}


function GetHaOwnerTokenInfo
{
    <#
    .SYNOPSIS
        Returns current values of OwnerToken
    #>
    param
    (
        $HaTable,
        [string]$PartitionKey,
        [string]$RowKey,
        [string]$Owner
    )

    # Initializing owner record if it does not exist yet
    $OwnerToken = $nul
    $OwnerRow = Get-AzTableRow -Table $HaTable -PartitionKey $PartitionKey -RowKey $RowKey

    if ($OwnerRow -ne $null)
    {
        $OwnerToken = [PSOwnerToken]::new($OwnerRow.Owner,$OwnerRow.LastUpdateUTC,$OwnerRow.Status,$OwnerRow.TakeOverThresholdMin,$OwnerRow.LongRunningTakeOverThresholdMin,$OwnerRow.CurrentActivityId)
    }

    return $OwnerToken
}

function GetHaOwnerToken
{
    <#
    .SYNOPSIS
        Returns the OwnerToken, upadtes the ha table and set value of ShouldExit
    #>
    param
    (
        $HaTable,
        $LogTable,
        [string]$PartitionKey,
        [string]$RowKey,
        [string]$Owner,
        [int]$TakeOverMin,
        [int]$LongRunningTakeOverMin,
        [string]$ActivityId
    )

    RandomizeStartDelay

    # Initializing owner record if it does not exist yet
    $OwnerRow = Get-AzTableRow -Table $HaTable -PartitionKey $PartitionKey -RowKey $RowKey

    if ($OwnerRow -eq $null)
    {
        $OwnerToken = [PSOwnerToken]::new($Owner,([datetime]::UtcNow),[HAStatuses]::Running,$TakeOverMin,$LongRunningTakeOverMin,$ActivityId)
            
        Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::UpdateFromOnwer) -Message "Setting up new owner record" -EntityName $OwnerToken.Owner -Level ([LogLevel]::Informational) -ActivityId $OwnerToke.ActivityId -LogTable $LogTable | Out-Null
        Write-Log 3 "Setting up new owner record" "Info"
        Add-AzTableRow -table $HaTable -partitionKey $PartitionKey -rowKey $RowKey -property $OwnerToken.GetPropertiesAsHashTable() | Out-Null
    }
    else
    {
        $OwnerToken = [PSOwnerToken]::new($OwnerRow.Owner,$OwnerRow.LastUpdateUTC,$OwnerRow.Status,$OwnerRow.TakeOverThresholdMin,$OwnerRow.LongRunningTakeOverThresholdMin,$OwnerRow.CurrentActivityId)
    }

    # Deciding whether or not move forward, get ownership or exit
    $LastUpdateInMinutes = (([System.DateTime]::UtcNow).Subtract($OwnerToken.LastUpdateUTC).TotalMinutes) 

    Write-Verbose -Verbose "LastUpdateInMinutes: $LastUpdateInMinutes"
    Write-Log 3 "LastUpdateInMinutes: $LastUpdateInMinutes"

    Write-Log 3 "Testing values" "Info"
    Write-Log 3 "---------------" "Info"
    Write-Log 3 "From Record" "Info"
    Write-Log 3 "    OwnerToken.Owner                           => $($OwnerToken.Owner)" "Info"
    Write-Log 3 "    OwnerToken.CurrentActivityId               => $($OwnerToken.CurrentActivityId)" "Info"
    Write-Log 3 "    OwnerToken.TakeOverThresholdMin            => $($OwnerToken.TakeOverThresholdMin)" "Info"
    Write-Log 3 "    OwnerToken.Status                          => $($OwnerToken.Status)" "Info"
    Write-Log 3 "    OwnerToken.LongRunningTakeOverThresholdMin => $($OwnerToken.LongRunningTakeOverThresholdMin)" "Info"
    Write-Log 3 "    OwnerToken.LastUpdateUTC                   => $($OwnerToken.LastUpdateUTC)" "Info"
    Write-Log 3 "From Host" "Info"
    Write-Log 3 "    Owner                         => $Owner" "Info"
    Write-Log 3 "    ActivityId                    => $ActivityId" "Info"
    Write-Log 3 "    LastUpdateInMinutes           => $LastUpdateInMinutes" "Info"

    if ($OwnerToken.Status -eq [HAStatuses]::Running)
    {
        Write-Log 3 "`($($OwnerToken.Status)`) Execution in progress case..." "Info"
        if(($OwnerToken.Owner -ne $Owner) -and ($LastUpdateInMinutes -lt $OwnerToken.LongRunningTakeOverThresholdMin))
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::ExecInProgressByOwner) -Message "Exiting due to execution in progress by another owner `($($OwnerToken.Owner)`)" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
            Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::ExecInProgressByOwner)`) Exiting due to execution in progress by another owner `($($OwnerToken.Owner)`)" "Info"
            $OwnerToken.ShouldExit = $true
        }
        elseif (($OwnerToken.Owner -eq $Owner) -and ($OwnerToken.CurrentActivityId -ne $ActivityId) -and ($LastUpdateInMinutes -lt $OwnerToken.LongRunningTakeOverThresholdMin)) 
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::ExecInProgressByMe) -Message "Exiting due to execution in progress by same owner `($($OwnerToken.Owner)`) and this is a new process" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
            Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::ExecInProgressByMe)`) Exiting due to execution in progress by same owner `($($OwnerToken.Owner)`) and this is a new process" "Info"
            $OwnerToken.ShouldExit = $true
        }
        elseif ($LastUpdateInMinutes -gt $OwnerToken.LongRunningTakeOverThresholdMin)
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::TakeOverThresholdLongRun) -Message "Taking over from current owner $($OwnerToken.Owner) due to staleness and last update being greater than long running threshold $($OwnerToken.LongRunningTakeOverThresholdMin)" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
            Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::TakeOverThresholdLongRun)`) Taking over from current owner $($OwnerToken.Owner) due to staleness and last update being greater than long running threshold $($OwnerToken.LongRunningTakeOverThresholdMin)" "Info"
            $OwnerToken.Status = [HAStatuses]::Running
            $OwnerToken.LastUpdateUTC = [System.DateTime]::UtcNow
            $OwnerToken.CurrentActivityId = $ActivityId
            Add-AzTableRow -table $HaTable -partitionKey $PartitionKey -rowKey $RowKey -property $OwnerToken.GetPropertiesAsHashTable() -UpdateExisting | Out-Null
        }
    }
    elseif ($LastUpdateInMinutes -gt $OwnerToken.TakeOverThresholdMin) 
    {
        if ($OwnerToken.Owner -ne $Owner)
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::TakeOverThreshold) -Message "Taking over from current owner $($OwnerToken.Owner) due to last update being greater than threshold $($OwnerToken.TakeOverThresholdMin)" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
            Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::TakeOverThreshold)`) Taking over from current owner $($OwnerToken.Owner) due to last update being greater than threshold $($OwnerToken.TakeOverThresholdMin)" "Info"
        }
        else
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::OwnershipRenewal) -Message "Renewing ownership of $($OwnerToken.Owner) due to last update being greater than threshold $($OwnerToken.TakeOverThresholdMin)" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
            Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::OwnershipRenewal)`) Renewing ownership of $($OwnerToken.Owner) due to last update being greater than threshold $($OwnerToken.TakeOverThresholdMin)" "Info"
        }
        
        $OwnerToken.Status = [HAStatuses]::Running
        $OwnerToken.LastUpdateUTC = [System.DateTime]::UtcNow
        $OwnerToken.CurrentActivityId = $ActivityId
        Add-AzTableRow -table $HaTable -partitionKey $PartitionKey -rowKey $RowKey -property $OwnerToken.GetPropertiesAsHashTable() -UpdateExisting | Out-Null
    }
    else
    {
        Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::ExitOwnerWithinThreshold) -Message "Exiting due to last update from current owner $($OwnerToken.Owner) is still within threshold ($LastUpdateInMinutes) in minutes" -EntityName $Owner -Level ([LogLevel]::Informational) -ActivityId $ActivityId -LogTable $LogTable | Out-Null
        Write-Log 3 "`($($OwnerToken.Status)`) `($([ExecCodes]::ExitOwnerWithinThreshold)`) Exiting due to last update from current owner $($OwnerToken.Owner) is still within threshold ($LastUpdateInMinutes) in minutes" "Info"
        $OwnerToken.ShouldExit = $true
    }

    return $OwnerToken
}

function UpateOwnerToken
{
    <#
    .SYNOPSIS
        Updates OwnerToken if still owner
    #>
    param
    (
        $HaTable,
        $LogTable,
        [string]$PartitionKey,
        [string]$RowKey,
        [PsOwnerToken]$OwnerToken
    )

    $LatestOwnerToken = GetHaOwnerTokenINfo -PartitionKey $PartitionKey -RowKey $RowKey -HaTable $ScalingHATable

    if ($LatestOwnerToken -ne $null)
    {
        if ($LatestOwnerToken.Owner -eq $OwnerToken.Owner)
        {
            $OwnerToken.LastUpdateUTC = [System.DateTime]::UtcNow
            $OwnerToken.Status =  $OwnerToken.Status
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::UpdateFromOnwer) -Message "* `($($OwnerToken.Owner)`) Completed execution, updating owner info...." -EntityName $OwnerToken.Owner -Level ([LogLevel]::Informational) -ActivityId $OwnerToken.ActivityId -LogTable $ScalingLogTable | Out-null
            Write-Log 3 "`($($OwnerToke.Status)`) `($([ExecCodes]::UpdateFromOnwer)`) $($OwnerToken.Owner) Completed execution, updating owner info...." "Info"
            
            Add-AzTableRow -table $ScalingHATable -partitionKey $PartitionKey -rowKey $RowKey -property $OwnerToken.GetPropertiesAsHashTable() -UpdateExisting | Out-null
        }
        else
        {
            Add-TableLog -OwnerStatus $OwnerToken.Status -ExecCode ([ExecCodes]::NoLongerOwner) -Message "* `($($OwnerToken.Owner)`) completed execution but no longer owner, current owner is $($LatestOwnerToken.Owner), will not update Ha Table." -EntityName $OwnerToken.Owner -Level ([LogLevel]::Informational) -ActivityId $OwnerToken.ActivityId -LogTable $ScalingLogTable | Out-null
        }
    }
}