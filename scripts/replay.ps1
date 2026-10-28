<#
.SYNOPSIS
  Re-processes the WHOLE topic from offset 0 (every message is delivered to the consumer again)
  and proves that MongoDB still has exactly one document per SQL row.
#>
. "$PSScriptRoot\common.ps1"

$before = ("$(Invoke-Mongo 'print(db.products.countDocuments({}))')").Trim()
Write-Step "MongoDB documents before replay: $before"

Write-Step 'Stop consumer and rewind consumer group to the beginning of the topic'
Invoke-Native { docker compose stop consumer }
Invoke-KafkaTool 'kafka-consumer-groups.sh' @('--group', $ConsumerGroup, '--describe')
Invoke-KafkaTool 'kafka-consumer-groups.sh' @(
    '--group', $ConsumerGroup, '--topic', $Topic, '--reset-offsets', '--to-earliest', '--execute'
)

Write-Step 'Start consumer: all messages are consumed again'
Invoke-Native { docker compose start consumer }
Start-Sleep -Seconds 12
Show-ServiceLogs 'consumer' '20s' 'Consumed'

$after = ("$(Invoke-Mongo 'print(db.products.countDocuments({}))')").Trim()
Write-Step "MongoDB documents after replay: $after (before: $before)"
Invoke-Mongo @'
const dups = db.products.aggregate([{ $group: { _id: '$sku', n: { $sum: 1 } } }, { $match: { n: { $gt: 1 } } }]).toArray();
print('Duplicated SKUs: ' + dups.length);
'@
Invoke-KafkaTool 'kafka-consumer-groups.sh' @('--group', $ConsumerGroup, '--describe')
