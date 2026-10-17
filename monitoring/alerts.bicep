// =====================================================================================
// The failure alert of one environment - deployed into dev and into prod by
// cicd/templates/deploy-environment.yml, step 4.
// -------------------------------------------------------------------------------------
// WHAT IT MAKES (in the environment's own resource group)
//   1. An action group: who is told, and how - one mail address.
//   2. A metric alert rule on the environment's Data Factory: every minute Azure Monitor
//      adds up the factory's metric "Failed pipeline runs metrics" (PipelineFailedRuns)
//      over the last 5 minutes; if the total is greater than 0 the alert fires and the
//      action group sends a mail. When the total is back to 0 it resolves (second mail).
//
// WHAT IT CANNOT SEE
//   - a run that never started (schedule stopped, the laptop's runtime off);
//   - a run that succeeded with wrong data (the quarantine and audit tables cover that);
//   - debug runs: Data Factory emits this metric only for completed, triggered runs.
//
// The mail says THAT a run failed. WHAT failed is in the control database's etl.run_log,
// where every failed table run writes a row with the error text.
//
// The receiving address is not in this repository: it is the secret pipeline variable
// alertEmail, set in Azure DevOps, passed in as the parameter alertEmail.
// =====================================================================================

@description('dev or prod - used in the names of the two resources')
@allowed([
  'dev'
  'prod'
])
param env string

@description('Name of the environment\'s Data Factory, in this resource group')
param factoryName string

@description('Mail address that receives the alert (secret pipeline variable alertEmail)')
@secure()
param alertEmail string

@description('0 = critical ... 4 = verbose. prod 1, dev 3')
@minValue(0)
@maxValue(4)
param severity int

resource factory 'Microsoft.DataFactory/factories@2018-06-01' existing = {
  name: factoryName
}

resource actionGroup 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: 'ag-olist-${env}-failures'
  location: 'global'
  properties: {
    enabled: true
    groupShortName: 'olist-${env}' // at most 12 characters: olist-prod = 10
    emailReceivers: [
      {
        name: 'data-engineer-mail'
        emailAddress: alertEmail
        useCommonAlertSchema: true
      }
    ]
  }
}

resource failedRunsAlert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: 'alert-olist-${env}-pipeline-failed'
  location: 'global'
  properties: {
    description: 'A pipeline run of ${factoryName} failed. The error text is in etl.run_log of the ${env} control database.'
    severity: severity
    enabled: true
    scopes: [
      factory.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    autoMitigate: true
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'failed-pipeline-runs'
          metricNamespace: 'Microsoft.DataFactory/factories'
          metricName: 'PipelineFailedRuns'
          operator: 'GreaterThan'
          threshold: 0
          timeAggregation: 'Total'
        }
      ]
    }
    actions: [
      {
        actionGroupId: actionGroup.id
      }
    ]
  }
}

output alertRuleId string = failedRunsAlert.id
output actionGroupId string = actionGroup.id
