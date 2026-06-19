# Helper functions to query DBs
function Query-Source ($query) {
    docker exec -i postgres-source psql -U postgres -d postgres_source -t -A -c $query
}

function Query-Target ($query) {
    docker exec -i postgres-target psql -U postgres -d postgres_target -t -A -c $query
}

Write-Host "=== Starting PowerShell Verification Script ==="

# Reset source and target databases to initial state (idempotency support)
Write-Host "Resetting source and target databases to clean initial state..."
$null = Query-Source "DELETE FROM public.patent WHERE id = '10000005';"
$null = Query-Source "INSERT INTO public.patent (id, title, num_claims) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8) ON CONFLICT (id) DO NOTHING;"
$null = Query-Source "UPDATE public.patent SET title = 'Method and system for blockchain based smart contracts' WHERE id = '10000000';"

$null = Query-Target "DELETE FROM public.patent_current_state WHERE id = '10000005';"
$null = Query-Target "DELETE FROM public.patent_history WHERE id = '10000005';"
$null = Query-Target "INSERT INTO public.patent_current_state (id, title, num_claims) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, num_claims = EXCLUDED.num_claims;"
$null = Query-Target "DELETE FROM public.patent_history WHERE id = '10000001';"
$null = Query-Target "INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to) VALUES ('10000001', 'System and method for real-time data streaming and processing', 8, NOW(), NULL);"
$null = Query-Target "UPDATE public.patent_current_state SET title = 'Method and system for blockchain based smart contracts' WHERE id = '10000000';"
$null = Query-Target "DELETE FROM public.patent_history WHERE id = '10000000';"
$null = Query-Target "INSERT INTO public.patent_history (id, title, num_claims, valid_from, valid_to) VALUES ('10000000', 'Method and system for blockchain based smart contracts', 15, NOW(), NULL);"

Write-Host "Waiting 3 seconds for CDC pipeline to stabilize..."
Start-Sleep -Seconds 3

# 1. Wait for target tables to be populated with initial snapshot (5 records)
Write-Host "Checking initial snapshot state..."
$maxRetries = 20
$retryCount = 0
$snapshotPassed = $false

while ($true) {
    $sourceCount = (Query-Source "SELECT COUNT(*) FROM public.patent;").Trim()
    $targetCurrentCount = (Query-Target "SELECT COUNT(*) FROM public.patent_current_state;").Trim()
    $targetActiveHistory = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NULL;").Trim()

    Write-Host "Current counts -> Source: $sourceCount, Target Current: $targetCurrentCount, Target Active History: $targetActiveHistory"

    if ($sourceCount -eq $targetCurrentCount -and $sourceCount -eq $targetActiveHistory -and $sourceCount -gt 0) {
        Write-Host "Initial snapshot successfully synchronized!"
        $snapshotPassed = $true
        break
    }

    $retryCount++
    if ($retryCount -ge $maxRetries) {
        break
    }
    Start-Sleep -Seconds 3
}

if (-not $snapshotPassed) {
    Write-Error "Timed out waiting for initial snapshot synchronization."
    exit 1
}

# Verify all history records have valid_to as NULL
$nullValCount = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE valid_to IS NOT NULL;").Trim()
if ($nullValCount -ne "0") {
    Write-Error "Assertion Failed: Some snapshot history records have valid_to set to non-null value!"
    exit 1
}
Write-Host "Initial snapshot assertions passed."

# 2. Test INSERT operation
Write-Host "Testing INSERT operation..."
$null = Query-Source "INSERT INTO public.patent (id, title, num_claims) VALUES ('10000005', 'Method and apparatus for quantum error correction', 10);"

# Wait for replication
$maxRetries = 15
$retryCount = 0
$insertPassed = $false

while ($true) {
    $insertedCurrent = (Query-Target "SELECT COUNT(*) FROM public.patent_current_state WHERE id = '10000005';").Trim()
    $insertedHistory = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000005' AND valid_to IS NULL;").Trim()

    if ($insertedCurrent -eq "1" -and $insertedHistory -eq "1") {
        Write-Host "INSERT verification passed."
        $insertPassed = $true
        break
    }

    $retryCount++
    if ($retryCount -ge $maxRetries) {
        break
    }
    Start-Sleep -Seconds 1
}

if (-not $insertPassed) {
    Write-Error "Assertion Failed: Inserted row '10000005' not found in target tables after timeout!"
    exit 1
}

# 3. Test UPDATE operation (SCD Type 2)
Write-Host "Testing UPDATE operation (SCD Type 2)..."
$null = Query-Source "UPDATE public.patent SET title = 'Method and system for blockchain based smart contracts v2' WHERE id = '10000000';"

# Wait for replication
$maxRetries = 15
$retryCount = 0
$updatePassed = $false

while ($true) {
    $updatedTitle = (Query-Target "SELECT title FROM public.patent_current_state WHERE id = '10000000';").Trim()
    $histTotalCount = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000';").Trim()
    $histClosedCount = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000' AND valid_to IS NOT NULL;").Trim()
    $histActiveCount = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000000' AND valid_to IS NULL;").Trim()

    if ($updatedTitle -eq "Method and system for blockchain based smart contracts v2" -and
        $histTotalCount -eq "2" -and
        $histClosedCount -eq "1" -and
        $histActiveCount -eq "1") {
        
        $histActiveTitle = (Query-Target "SELECT title FROM public.patent_history WHERE id = '10000000' AND valid_to IS NULL;").Trim()
        if ($histActiveTitle -eq "Method and system for blockchain based smart contracts v2") {
            Write-Host "UPDATE verification passed."
            $updatePassed = $true
            break
        }
    }

    $retryCount++
    if ($retryCount -ge $maxRetries) {
        break
    }
    Start-Sleep -Seconds 1
}

if (-not $updatePassed) {
    Write-Error "Assertion Failed: Update test timed out or assertions failed!"
    exit 1
}

# 4. Test DELETE operation
Write-Host "Testing DELETE operation..."
$null = Query-Source "DELETE FROM public.patent WHERE id = '10000001';"

# Wait for replication
$maxRetries = 15
$retryCount = 0
$deletePassed = $false

while ($true) {
    $deletedCurrent = (Query-Target "SELECT COUNT(*) FROM public.patent_current_state WHERE id = '10000001';").Trim()
    $deletedActiveHist = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000001' AND valid_to IS NULL;").Trim()
    $deletedClosedHist = (Query-Target "SELECT COUNT(*) FROM public.patent_history WHERE id = '10000001' AND valid_to IS NOT NULL;").Trim()

    if ($deletedCurrent -eq "0" -and $deletedActiveHist -eq "0" -and $deletedClosedHist -eq "1") {
        Write-Host "DELETE verification passed."
        $deletePassed = $true
        break
    }

    $retryCount++
    if ($retryCount -ge $maxRetries) {
        break
    }
    Start-Sleep -Seconds 1
}

if (-not $deletePassed) {
    Write-Error "Assertion Failed: Delete test timed out or assertions failed!"
    exit 1
}

Write-Host "=== All CDC Pipeline Assertions Passed Successfully! ==="
exit 0
