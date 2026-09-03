# PowerShell counterpart of ci/github-git-auth.sh -- see that file for why the
# Authorization header has to go out on the first request rather than through a
# credential helper. Dot-source it (". ci/github-git-auth.ps1") in any pwsh step
# that runs git against github.com, with GH_TOKEN mapped into the step's env.
$header = ""
if ($env:GH_TOKEN -match '^[A-Za-z0-9_-]+$') {
    # Azure masks the raw token, but not its base64 form -- register that too.
    $basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("x-access-token:$env:GH_TOKEN"))
    Write-Host "##vso[task.setsecret]$basic"
    $header = "AUTHORIZATION: basic $basic"
    Write-Host "github.com git requests will be authenticated"
} else {
    Write-Host "No usable token; github.com git requests stay anonymous"
}
$env:GIT_CONFIG_COUNT = "1"
$env:GIT_CONFIG_KEY_0 = "http.https://github.com/.extraheader"
$env:GIT_CONFIG_VALUE_0 = $header
