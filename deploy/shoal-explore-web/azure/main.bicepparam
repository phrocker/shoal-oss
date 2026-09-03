// Example parameters for deploy/shoal-explore-web/azure/main.bicep.
//
// Every value here is an OBVIOUS PLACEHOLDER. Copy this file, replace the
// placeholders with your environment's real values, and deploy with:
//
//   az deployment group create \
//     --resource-group <your-rg> \
//     --template-file main.bicep \
//     --parameters main.bicepparam
//
// No secrets belong in this file. The container needs no client secret (the
// Entra authenticator validates inbound tokens only), and the Azure Files key
// is resolved at deploy time by the template's listKeys() call.

using './main.bicep'

param location = 'eastus'
param namePrefix = 'shoal-explore'

// Replace with your registry/repository:tag. Use a pinned tag, never :latest,
// so a single-writer redeploy is a deliberate, reviewable change.
param containerImage = 'REGISTRY-PLACEHOLDER.azurecr.io/shoal-explore-web:REPLACE-WITH-PINNED-TAG'

param containerPort = 8098
param useAcrManagedIdentity = true
param planSkuName = 'P1v3'

// Placeholder identifiers. Replace with your tenant and application (client) IDs.
param entraTenantId = '00000000-0000-0000-0000-000000000000'
param entraClientId = '11111111-1111-1111-1111-111111111111'

// Entra app-role values you define on the app registration and assign to users
// or groups. A caller with no mapped role authenticates but sees no corpus.
param entraReaderRoles = 'Shoal.Reader'
param entraContributorRoles = 'Shoal.Contributor'

// Leave empty to use OIDC discovery defaults derived from the tenant.
param entraIssuer = ''
param entraJwksUri = ''

param stateShareQuotaGiB = 100
param storageSkuName = 'Standard_LRS'
