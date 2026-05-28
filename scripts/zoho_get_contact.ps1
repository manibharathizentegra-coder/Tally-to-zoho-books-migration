param(
    [Parameter(Mandatory = $true)]
    [string]$ContactId
)

$ErrorActionPreference = "Stop"

$envPath = Join-Path $PSScriptRoot "..\.env"
if (!(Test-Path $envPath)) {
    Write-Host "ERROR: .env not found at $envPath"
    exit 1
}

$lines = Get-Content $envPath
$kv = @{}
foreach ($l in $lines) {
    if ($l -match '^\s*#') { continue }
    if ($l -match '^\s*$') { continue }
    $parts = $l -split '=',2
    if ($parts.Length -eq 2) {
        $key = $parts[0].Trim()
        $val = $parts[1].Trim()
        $kv[$key] = $val
    }
}

$org = $kv['ORGANIZATION_ID']
$clientId = $kv['CLIENT_ID']
$clientSecret = $kv['CLIENT_SECRET']
$refreshToken = $kv['REFRESH_TOKEN']

$token = $null

if (-not $org -or -not $clientId -or -not $clientSecret -or -not $refreshToken) {
    Write-Host "ERROR: ORGANIZATION_ID / CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN missing in .env"
    exit 1
}

$authUrl = "https://accounts.zoho.in/oauth/v2/token"
$authBody = @{
    refresh_token = $refreshToken
    client_id = $clientId
    client_secret = $clientSecret
    grant_type = "refresh_token"
}

try {
    $authRes = Invoke-RestMethod -Method POST -Uri $authUrl -Body $authBody -TimeoutSec 30
    $token = $authRes.access_token
    if (-not $token) {
        Write-Host "ERROR: Failed to refresh access token"
        $authRes | ConvertTo-Json -Depth 6
        exit 1
    }
} catch {
    Write-Host "ERROR: Token refresh failed: $($_.Exception.Message)"
    if ($_.Exception.Response) {
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
            if ($body) { Write-Host $body }
        } catch {}
    }
    exit 1
}

$uri = "https://www.zohoapis.in/books/v3/contacts/$ContactId?organization_id=$org"
$headers = @{ Authorization = "Zoho-oauthtoken $token" }

try {
    $res = Invoke-RestMethod -Method GET -Uri $uri -Headers $headers -TimeoutSec 30
    $contact = $res.contact
    $out = [ordered]@{
        contact_id = $contact.contact_id
        contact_name = $contact.contact_name
        phone = $contact.phone
        mobile = $contact.mobile
        contact_persons = $contact.contact_persons
    }
    $out | ConvertTo-Json -Depth 6
} catch {
    Write-Host "ERROR: $($_.Exception.Message)"
    if ($_.Exception.Response) {
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
            if ($body) { Write-Host $body }
        } catch {}
    }
    exit 1
}
