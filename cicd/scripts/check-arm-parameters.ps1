<#
  Fails the build when a value that must differ between dev and prod is not a parameter of the
  generated ARM template.

  Why: the deployment replaces dev's values with prod's by overriding template parameters. If a
  property is not a parameter, there is nothing to override, the deployment still succeeds, and
  prod quietly keeps dev's value - for example prod's pipeline writing into dev's storage account.
  This check turns that silent mistake into a red build.
#>
param(
    [Parameter(Mandatory)] [string]   $ParametersFile,
    [Parameter(Mandatory)] [string[]] $Required
)
$ErrorActionPreference = 'Stop'

$names = @((Get-Content -Path $ParametersFile -Raw | ConvertFrom-Json).parameters.PSObject.Properties.Name)

Write-Host "Parameters of the generated template ($($names.Count)):"
$names | Sort-Object | ForEach-Object { Write-Host "  $_" }

$missing = @($Required | Where-Object { $_ -notin $names })
if ($missing.Count -gt 0) {
    Write-Host "##vso[task.logissue type=error]Not a parameter of the template: $($missing -join ', '). A deployment now would leave prod on dev's value."
    exit 1
}
Write-Host "OK: all $($Required.Count) values that differ between dev and prod are template parameters."
