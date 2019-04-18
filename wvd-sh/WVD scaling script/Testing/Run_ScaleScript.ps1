[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

for ($i=0;$i -le 500;$i++)
{
    Write-Verbose -Verbose "Iteration: $i"
    ..\basicScale.ps1
    $seconds = (Get-random -Minimum (15*60) -Maximum (120*60))
    Write-Verbose -Verbose "Sleeping $seconds seconds before next basicScale.ps1 execution"
    Start-Sleep -Seconds $seconds
}