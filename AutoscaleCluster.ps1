<#
    .DESCRIPTION
     Auto Scale HDInsight Cluster based on load
#>

$connectionName = "AzureRunAsConnection"
$resourceGroupName ="resourcegroup"  2
$clusterName = "clustername"

#Make sure the runbook stops on error
$ErrorActionPreference = "Stop"

try
{
    # Get the connection "AzureRunAsConnection "
    $servicePrincipalConnection=Get-AutomationConnection -Name $connectionName         

    "Logging in to Azure..."
    Add-AzureRmAccount 
        -ServicePrincipal `
        -TenantId $servicePrincipalConnection.TenantId `
        -ApplicationId $servicePrincipalConnection.ApplicationId `
        -CertificateThumbprint $servicePrincipalConnection.CertificateThumbprint 
}
catch {
    if (!$servicePrincipalConnection)
    {
        $ErrorMessage = "Connection $connectionName not found."
        throw $ErrorMessage
    } else{
        Write-Error -Message $_.Exception
        throw $_.Exception
    }
}


#HDI Cluster credentials - Cluster password can be stored as an encrypted Azure Runbook variable.
$pass = Get-AutomationVariable –Name 'ClusterPwdProd'
$encodedCreds = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$('admin'):$($pass)"))
$basicAuthValue = "Basic $encodedCreds"

$Headers = @{
        Authorization = $basicAuthValue
}

#Set Parameter Configuration
$MinNodes = 6           #Minumum number of nodes that the cluster can have (Stay over 4 for stability)      
$MaxNodes = 14          #Max numbre of nodes that the cluster can have
$MaxCapacity = 95       #Threshold of cluster usage to trigger a rescaling
$IncreaseAmount = 2     #Amount of nodes to increase each run
$DecreaseAmount = 2     #Amount of nodes to decrease each run

#Get the number of Nodes the cluster currently has
$resp = Invoke-RestMethod -Uri "https://$clusterName.azurehdinsight.net/yarnui/ws/v1/cluster/metrics" -Headers $Headers -UseBasicParsing
$totalNodes = $resp.clusterMetrics.totalNodes

#Get the cluster capacity 
$resp = Invoke-RestMethod -Uri "https://$clusterName.azurehdinsight.net/yarnui/ws/v1/cluster/scheduler" -Headers $Headers -UseBasicParsing
$currentCapacity = $resp.scheduler.schedulerInfo.usedCapacity

"Total Nodes: $totalNodes"
"Yarn current capacity: $currentCapacity"
# check that there's actually data, sometimes the api returns empty and that causes scaling issues. 
if ($totalNodes) 
{
    if ($currentCapacity) 
    {
        if ($currentCapacity -gt $MaxCapacity)
        {
            "Over Capacity Threshold - Increasing Capacity"
            "Current Nodes $totalNodes" 
            $newNodes = $totalNodes + $IncreaseAmount
            "New Nodes: $newNodes"
            if ($newNodes -le $MaxNodes)
            {
            "Upscaling cluster"
            Set-AzureRmHDInsightClusterSize -ClusterName "$clusterName" -TargetInstanceCount $newNodes -ResourceGroupName "$resourceGroupName"
            }
            else 
            {
                "Already at maximum cluster size"
            }

        }
        else
        {
            "Under Capacity - Reducing Cluster Size"
            "Current Nodes $totalNodes" 
            $newNodes = $totalNodes - $DecreaseAmount
            "New Nodes: $newNodes"
            if ($newNodes -ge $MinNodes)
            {
                "Downscaling cluster"
                Set-AzureRmHDInsightClusterSize -ClusterName "$clusterName" -TargetInstanceCount $newNodes -ResourceGroupName "$resourceGroupName"
            }
            else
            {
                "Already at minimum cluster size"
            }
        }
    }
}
