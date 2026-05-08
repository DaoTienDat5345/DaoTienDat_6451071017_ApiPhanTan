param(
  [string]$BaseUri = "http://localhost:3001/webhook",
  [string]$PageId = "1164574796735265",
  [string]$PostId = "post_test_001"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-UnixTimeSeconds {
  return [int][double]((Get-Date).ToUniversalTime() - [datetime]"1970-01-01").TotalSeconds
}

function New-WebhookBody {
  param(
    [string]$CommentId,
    [string]$UserId,
    [string]$Message
  )

  return @{
    object = "page"
    entry = @(
      @{
        id = $PageId
        time = Get-UnixTimeSeconds
        changes = @(
          @{
            field = "feed"
            value = @{
              item = "comment"
              verb = "add"
              comment_id = $CommentId
              post_id = $PostId
              message = $Message
              from = @{
                id = $UserId
              }
            }
          }
        )
      }
    )
  } | ConvertTo-Json -Depth 10
}

function Send-Case {
  param(
    [string]$Name,
    [string]$CommentId,
    [string]$UserId,
    [string]$Message,
    [string]$Expected
  )

  Write-Host ""
  Write-Host "==> Sending case: $Name" -ForegroundColor Cyan
  Write-Host "    comment_id: $CommentId"
  Write-Host "    expected:   $Expected"

  $body = New-WebhookBody -CommentId $CommentId -UserId $UserId -Message $Message
  $response = Invoke-RestMethod -Method Post -Uri $BaseUri -ContentType "application/json" -Body $body

  Write-Host "    response:   $($response.message) (eventCount=$($response.eventCount))" -ForegroundColor Green
}

$cases = @(
  @{
    Name = "ask_price"
    CommentId = "cmt_price_001"
    UserId = "user_price_001"
    Message = "Shop oi gia bao nhieu?"
    Expected = "intent=ask_price, sentiment=neutral, route=auto_reply"
  },
  @{
    Name = "complaint"
    CommentId = "cmt_complaint_001"
    UserId = "user_complaint_001"
    Message = "Minh chua nhan duoc hang"
    Expected = "intent=complaint, sentiment=negative, route=manual_review"
  },
  @{
    Name = "praise"
    CommentId = "cmt_praise_001"
    UserId = "user_praise_001"
    Message = "Bai viet hay qua, san pham nhin xinh that"
    Expected = "intent=praise, sentiment=positive, route=auto_reply"
  }
)

foreach ($case in $cases) {
  Send-Case @case
}

Write-Host ""
Write-Host "==> Sending spam burst (3 events)" -ForegroundColor Yellow

1..3 | ForEach-Object {
  $index = $_
  $body = New-WebhookBody -CommentId ("cmt_spam_ai_00{0}" -f $index) -UserId "user_spam_ai_001" -Message "Inbox telegram de nhan uu dai"
  $response = Invoke-RestMethod -Method Post -Uri $BaseUri -ContentType "application/json" -Body $body
  Write-Host "    spam event ${index}: $($response.message) (eventCount=$($response.eventCount))" -ForegroundColor Green
}

Write-Host ""
Write-Host "Done. Check data/events/ for:" -ForegroundColor Cyan
Write-Host "  - cmt_price_001"
Write-Host "  - cmt_complaint_001"
Write-Host "  - cmt_praise_001"
Write-Host "  - cmt_spam_ai_001..003"
Write-Host ""
Write-Host "Suggested checks:" -ForegroundColor Cyan
Write-Host "  - ask_price -> classification.intent=ask_price"
Write-Host "  - complaint -> classification.intent=complaint"
Write-Host "  - praise -> classification.intent=praise"
Write-Host "  - spam burst -> spamAnalysis.isSpam=true on the later events"
