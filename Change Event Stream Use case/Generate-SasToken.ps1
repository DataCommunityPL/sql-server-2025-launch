
function Generate-SasToken {
 
    # Provide values for the following resources:
    $resourceGroupName  = "SQLServerLaunch2025"
    $namespaceName      = "bialykruk2008"
    $eventHubName       = "eh1"
    $policyName         = "ces-demo-policy"
 
    # Login to Azure and select the Azure Subscription
    Connect-AzAccount -InformationAction SilentlyContinue | Out-Null
 
    # Validate the existence of the specified resource group, event hub namespace, and event hub
    Get-AzResourceGroup -Name $resourceGroupName -ErrorAction Stop | Out-Null
    Get-AzEventHubNamespace -ResourceGroupName $resourceGroupName -Name $namespaceName -ErrorAction Stop | Out-Null
    Get-AzEventHub -ResourceGroupName $resourceGroupName -NamespaceName $namespaceName -Name $eventHubName -ErrorAction Stop | Out-Null
 
    # Get the event hub authorization policy (it must have Manage rights)
    $policy = Get-AzEventHubAuthorizationRule -ResourceGroupName $resourceGroupName -NamespaceName $namespaceName -EventHubName $eventHubName -AuthorizationRuleName $policyName -ErrorAction SilentlyContinue
 
    if (-not ("Manage" -in $policy.Rights)) {
        throw "Authorization rule '$policyName' does not exist, or is missing the required 'Manage' right"
    }
 
    # Get the Primary Key of the Shared Access Policy
    $keys = Get-AzEventHubKey -ResourceGroupName $resourceGroupName -NamespaceName $namespaceName -EventHubName $eventHubName -AuthorizationRuleName $policyName
 
    if (-not $keys) {
        throw "Could not obtain Azure Event Hub Key"
    }
 
    if (-not $keys.PrimaryKey) {
        throw "Could not obtain Primary Key"
    }
 
    $primaryKey = ($keys.PrimaryKey) 
 
    # Define a function to create the SAS token
    function Create-SasToken {
        param ([string]$resourceUri, [string]$keyName, [string]$key)
 
        $sinceEpoch = [datetime]::UtcNow - [datetime]"1970-01-01"
        $expiry = [int]$sinceEpoch.TotalSeconds + (60 * 60 * 24 * 31 * 6)  # 6 months
        $stringToSign = [System.Web.HttpUtility]::UrlEncode($resourceUri) + "`n" + $expiry
        $hmac = New-Object System.Security.Cryptography.HMACSHA256
        $hmac.Key = [Text.Encoding]::UTF8.GetBytes($key)
        $signature = [Convert]::ToBase64String($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($stringToSign)))
        $sasToken = "SharedAccessSignature sr=$([System.Web.HttpUtility]::UrlEncode($resourceUri))&sig=$([System.Web.HttpUtility]::UrlEncode($signature))&se=$expiry&skn=$keyName"
 
        return $sasToken
    }
 
    # Construct the resource URI for the SAS token
    $resourceUri = "https://$namespaceName.servicebus.windows.net/$eventHubName"
 
    # Generate the SAS token using the primary key from the new policy
    $sasToken = Create-SasToken -resourceUri $resourceUri -keyName $policyName -key $primaryKey
 
    # Output the SAS token
    Write-Host "`n-- Generated SAS Token --" -ForegroundColor Gray
    Write-Host $sasToken -ForegroundColor White
    Write-Host "-- End of generated SAS Token --`n" -ForegroundColor Gray
 
    # Copy the SAS token to the clipboard
    $sasToken | Set-Clipboard
    Write-Host "The generated SAS token has been copied to the clipboard." -ForegroundColor Green
}
 
Generate-SasToken