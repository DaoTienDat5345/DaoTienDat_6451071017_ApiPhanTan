$WebhookUrl = "http://localhost:3001/webhook"
$Total = 500

Write-Host "Sending $Total events to webhook..."

$success = 0
$failed = 0

for ($i = 1; $i -le $Total; $i++) {
    $body = @{
        object = "page"
        entry = @(
            @{
                id = "test_page_001"
                time = [int][double]::Parse((Get-Date -UFormat %s))
                changes = @(
                    @{
                        field = "feed"
                        value = @{
                            item = "comment"
                            verb = "add"
                            comment_id = "burst_comment_$i"
                            post_id = "test_post_001"
                            from = @{
                                id = "user_$i"
                                name = "Test User $i"
                            }
                            message = "Test burst comment number $i"
                            created_time = [int][double]::Parse((Get-Date -UFormat %s))
                        }
                    }
                )
            }
        )
    } | ConvertTo-Json -Depth 10

    try {
        Invoke-RestMethod `
            -Uri $WebhookUrl `
            -Method POST `
            -Body $body `
            -ContentType "application/json" | Out-Null

        $success++
    }
    catch {
        $failed++
        Write-Host ("Failed event {0}: {1}" -f $i, $_.Exception.Message) -ForegroundColor Red
    }
}

Write-Host "Done."
Write-Host "Success: $success"
Write-Host "Failed : $failed"