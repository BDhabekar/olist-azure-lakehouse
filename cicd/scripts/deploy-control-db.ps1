<#
  Runs the control database's deployment script against one environment's Azure SQL database.

  It must be started inside an AzureCLI task: it asks the Azure CLI, which that task has signed in
  as the pipeline's deployment identity, for a short-lived token for Azure SQL. No password exists.
  The identity must already be a user of the database (sql/control-db/deploy/prod_bootstrap_users.sql,
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

Write-Host "Deploying $ScriptPath to $Server / $Database"
$rows = Invoke-Sqlcmd -ServerInstance $Server -Database $Database -AccessToken $token `
            -InputFile $ScriptPath `
            -Variable @("ExpectedDb=$Database", "FactoryUser=$FactoryUser") `
            -AbortOnError -ErrorAction Stop

$rows | Format-Table -AutoSize | Out-String -Width 220 | Write-Host
Write-Host 'Control database deployed.'
