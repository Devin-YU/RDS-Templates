[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

for ($i=0;$i -le 500;$i++)
{
    ..\basicScale.ps1
    Start-Sleep -Seconds (Get-random -Maximum 120 -Minimum 60)
}