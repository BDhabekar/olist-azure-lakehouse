<#
  Runs the control database's deployment script against one environment's Azure SQL database.

  It must be started inside an AzureCLI task: it asks the Azure CLI, which that task has signed in
  as the pipeline's deployment identity, for a short-lived token for Azure SQL. No password exists.
  The identity must already be a user of the database (sql/control-db/deploy/dev_bootstrap_users.sql or prod_bootstrap_users.sql,
  run once by the server's Microsoft Entra admin).
#>
param(
    [Parameter(Mandatory)] [string] $Server,       # e.g. sql-olist-prod-bd.database.windows.net
    [Parameter(Mandatory)] [string] $Database,     # the control database of that environment
    [Parameter(Mandatory)] [string] $FactoryUser,  # the Data Factory whose managed identity reads schema etl
    [Parameter(Mandatory)] [string] $ScriptPath    # sql/control-db/deploy/ctl_deploy.sql
)
$ErrorActionPreference = 'Stop'

if (-not (Get-Module -ListAvailable -Name SqlServer)) {
    Write-Host 'Installing the PowerShell module SqlServer for this user...'
    Install-Module -Name SqlServer -Scope CurrentUser -Force -AllowClobber
}
Import-Module SqlServer

$token = az account get-access-token --resource https://database.windows.net/ --query accessToken --output tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'No access token for Azure SQL. Is this script running inside an AzureCLI task?'
}

# The control database is serverless: it pauses itself when nobody uses it, and the first
# connection after a pause wakes it. Waking takes up to a minute - longer than a connection
# normally waits - so the first deployment of a quiet day would fail on a timeout. The script
# therefore knocks first and waits until the database answers. Only "not awake yet" is waited
# for; any other error (a missing user, a blocked address) stops the run at once.
$attempts = 8
for ($i = 1; $i -le $attempts; $i++) {
    try {
        Invoke-Sqlcmd -ServerInstance $Server -Database $Database -AccessToken $token `
            -Query 'SELECT 1 AS awake' -ConnectionTimeout 60 -ErrorAction Stop | Out-Null
        Write-Host "The database answers (attempt $i of $attempts)."
        break
    }
    catch {
        $message = "$($_.Exception.Message)"
        $notAwakeYet = $message -match 'Timeout|timed out|not currently available|40613|paused|resum'
        if (-not $notAwakeYet -or $i -eq $attempts) { throw }
        Write-Host "No answer yet (attempt $i of $attempts) - the database is waking up. Waiting 15 seconds."
        Start-Sleep -Seconds 15
    }
}

Write-Host "Deploying $ScriptPath to $Server / $Database"
$rows = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -AccessToken $token `
            -ConnectionTimeout 60 `
            -InputFile $ScriptPath `
            -Variable @("ExpectedDb=$Database", "FactoryUser=$FactoryUser") `
            -AbortOnError -ErrorAction Stop

$rows | Format-Table -AutoSize | Out-String -Width 220 | Write-Host
Write-Host 'Control database deployed.'
