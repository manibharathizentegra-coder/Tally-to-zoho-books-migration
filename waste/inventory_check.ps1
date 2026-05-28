param(
  [Parameter(Mandatory)][string]$ZohoReportPath,
  [Parameter(Mandatory)][string]$AdjustmentPath,
  [decimal]$ZohoDisplayedTotal,
  [string]$OutPath = (Join-Path -Path $PWD -ChildPath 'inventory_diff_report.txt')
)

$ErrorActionPreference = 'Stop'

function Convert-ExcelColumnLettersToNumber {
  param([Parameter(Mandatory)][string]$Letters)
  $n = 0
  foreach ($ch in $Letters.ToUpperInvariant().ToCharArray()) {
    if ($ch -lt 'A' -or $ch -gt 'Z') { continue }
    $n = ($n * 26) + ([int][char]$ch - [int][char]'A' + 1)
  }
  return $n
}

function Get-CellRefColumnLetters {
  param([Parameter(Mandatory)][string]$CellRef)
  $m = [regex]::Match($CellRef, '^[A-Za-z]+')
  if (-not $m.Success) { return $null }
  return $m.Value
}

function Get-XlsxSharedStrings {
  param([Parameter(Mandatory)]$Zip)
  $entry = $Zip.GetEntry('xl/sharedStrings.xml')
  if (-not $entry) { return @() }

  $strings = New-Object System.Collections.Generic.List[string]
  $stream = $entry.Open()
  try {
    $settings = New-Object System.Xml.XmlReaderSettings
    $settings.IgnoreComments = $true
    $settings.IgnoreWhitespace = $true
    $reader = [System.Xml.XmlReader]::Create($stream, $settings)
    try {
      while ($reader.Read()) {
        if ($reader.NodeType -ne [System.Xml.XmlNodeType]::Element -or $reader.LocalName -ne 'si') { continue }
        $sb = New-Object System.Text.StringBuilder
        $sub = $reader.ReadSubtree()
        try {
          $sub.Read() | Out-Null
          while ($sub.Read()) {
            if ($sub.NodeType -eq [System.Xml.XmlNodeType]::Element -and $sub.LocalName -eq 't') {
              $sb.Append($sub.ReadElementContentAsString()) | Out-Null
            }
          }
        } finally {
          $sub.Dispose()
        }
        $strings.Add($sb.ToString())
      }
    } finally {
      $reader.Dispose()
    }
  } finally {
    $stream.Dispose()
  }
  return ,$strings.ToArray()
}

function Get-XlsxSheets {
  param([Parameter(Mandatory)]$Zip)

  $workbookEntry = $Zip.GetEntry('xl/workbook.xml')
  if (-not $workbookEntry) { throw "Invalid xlsx: missing xl/workbook.xml" }
  $relsEntry = $Zip.GetEntry('xl/_rels/workbook.xml.rels')
  if (-not $relsEntry) { throw "Invalid xlsx: missing xl/_rels/workbook.xml.rels" }

  function Read-ZipEntryText {
    param([Parameter(Mandatory)]$Entry)
    $s = $Entry.Open()
    try { return (New-Object System.IO.StreamReader($s)).ReadToEnd() } finally { $s.Dispose() }
  }

  $workbookXml = [xml](Read-ZipEntryText -Entry $workbookEntry)
  $relsXml = [xml](Read-ZipEntryText -Entry $relsEntry)

  $ridToTarget = @{}
  $relsNs = New-Object System.Xml.XmlNamespaceManager($relsXml.NameTable)
  $relsNs.AddNamespace('pr', 'http://schemas.openxmlformats.org/package/2006/relationships') | Out-Null
  $relNodes = $relsXml.SelectNodes('/pr:Relationships/pr:Relationship', $relsNs)
  foreach ($rel in @($relNodes)) {
    $id = $rel.GetAttribute('Id')
    $target = $rel.GetAttribute('Target')
    if ($id -and $target) { $ridToTarget[$id] = $target }
  }

  $wbNs = New-Object System.Xml.XmlNamespaceManager($workbookXml.NameTable)
  $wbNs.AddNamespace('s', 'http://schemas.openxmlformats.org/spreadsheetml/2006/main') | Out-Null
  $wbNs.AddNamespace('r', 'http://schemas.openxmlformats.org/officeDocument/2006/relationships') | Out-Null
  $sheetNodes = $workbookXml.SelectNodes('/s:workbook/s:sheets/s:sheet', $wbNs)

  $sheets = @()
  foreach ($sheet in @($sheetNodes)) {
    $name = $sheet.GetAttribute('name')
    $sheetId = $sheet.GetAttribute('sheetId')
    $rid = $sheet.GetAttribute('id', 'http://schemas.openxmlformats.org/officeDocument/2006/relationships')
    if (-not $rid) { continue }
    $target = $ridToTarget[$rid]
    if (-not $target) { continue }
    $path = if ($target.StartsWith('/')) { $target.TrimStart('/') } else { "xl/$target" }
    $sheets += [pscustomobject]@{
      Name = [string]$name
      SheetId = [int]$sheetId
      Path = $path -replace '\\', '/'
    }
  }
  return ,$sheets
}

function Get-WorksheetAnalysis {
  param(
    [Parameter(Mandatory)]$Zip,
    [string[]]$SharedStrings = @(),
    [Parameter(Mandatory)][string]$SheetPath,
    [Parameter(Mandatory)][string]$FileLabel,
    [int]$HeaderSearchRows = 60
  )

  $entry = $Zip.GetEntry($SheetPath)
  if (-not $entry) { throw "Missing worksheet entry: $SheetPath" }

  $result = [ordered]@{
    File = $FileLabel
    SheetPath = $SheetPath
    HeaderRow = $null
    HeaderMap = @{} # colNum -> headerText
    AssetValueCols = @() # colNum
    IdCols = @() # colNum
    TotalsByCol = @{} # colNum -> decimal
    TotalAllAssetCols = [decimal]0
    TotalAllAssetCols_VisibleRowsOnly = [decimal]0
    Rows = New-Object System.Collections.Generic.List[object]
    Notes = New-Object System.Collections.Generic.List[string]
  }

  $stream = $entry.Open()
  try {
    $settings = New-Object System.Xml.XmlReaderSettings
    $settings.IgnoreComments = $true
    $settings.IgnoreWhitespace = $true
    $reader = [System.Xml.XmlReader]::Create($stream, $settings)

    $headerRow = $null
    $headerMap = @{}
    $assetCols = @()
    $idCols = @()

    function Read-CellValueFromElement {
      param(
        [Parameter(Mandatory)][System.Xml.XmlElement]$Cell
      )
      $type = $Cell.GetAttribute('t')
      $vNode = $Cell.SelectSingleNode('*[local-name()="v"]')
      if ($type -eq 's') {
        if (-not $vNode) { return $null }
        $idx = 0
        if (-not [int]::TryParse($vNode.InnerText, [ref]$idx)) { return $null }
        if ($idx -ge 0 -and $idx -lt $SharedStrings.Length) { return $SharedStrings[$idx] }
        return $null
      }
      if ($type -eq 'inlineStr') {
        $tNodes = $Cell.SelectNodes('.//*[local-name()="is"]//*[local-name()="t"]')
        if (-not $tNodes -or $tNodes.Count -eq 0) { return $null }
        return ($tNodes | ForEach-Object { $_.InnerText }) -join ''
      }
      if ($type -eq 'str') {
        if ($vNode) { return [string]$vNode.InnerText }
        return $null
      }
      if ($vNode) { return [string]$vNode.InnerText }
      return $null
    }

    try {
      while ($reader.Read()) {
        if ($reader.NodeType -ne [System.Xml.XmlNodeType]::Element -or $reader.LocalName -ne 'row') { continue }

        $rowXmlText = $reader.ReadOuterXml()
        $rowDoc = [xml]$rowXmlText
        $rowEl = [System.Xml.XmlElement]$rowDoc.DocumentElement
        if (-not $rowEl) { continue }

        $rText = $rowEl.GetAttribute('r')
        $r = 0
        [int]::TryParse($rText, [ref]$r) | Out-Null

        $isHidden = $false
        $hiddenText = $rowEl.GetAttribute('hidden')
        if ($hiddenText -eq '1') { $isHidden = $true }

        $cells = $rowEl.SelectNodes('*[local-name()="c"]')
        if (-not $cells -or $cells.Count -eq 0) { continue }

        $rowVals = @{}
        foreach ($cNode in $cells) {
          $c = [System.Xml.XmlElement]$cNode
          $cellRef = [string]$c.GetAttribute('r')
          if (-not $cellRef) { continue }
          $letters = Get-CellRefColumnLetters -CellRef $cellRef
          if (-not $letters) { continue }
          $colNum = Convert-ExcelColumnLettersToNumber -Letters $letters
          $val = Read-CellValueFromElement -Cell $c
          $rowVals[$colNum] = $val
        }

        if (-not $headerRow -and $r -le $HeaderSearchRows) {
          $maybeHeaders = $rowVals.GetEnumerator() | ForEach-Object { [string]$_.Value } | Where-Object { $_ }
          $hit = $maybeHeaders | Where-Object { $_ -match 'Inventory\s*Asset\s*Value' } | Select-Object -First 1
          if ($hit) {
            $headerRow = $r
            foreach ($kv in $rowVals.GetEnumerator()) {
              $hdr = ([string]$kv.Value).Trim()
              if (-not $hdr) { continue }
              $headerMap[[int]$kv.Key] = $hdr
            }
            $assetCols = @($headerMap.GetEnumerator() | Where-Object { $_.Value -match 'Inventory\s*Asset\s*Value' } | ForEach-Object { [int]$_.Key } | Sort-Object)
            $idCols = @($headerMap.GetEnumerator() | Where-Object { $_.Value -match '^(Item|Item\s*Name|SKU|Item\s*Code|Warehouse|Location)$' } | ForEach-Object { [int]$_.Key } | Sort-Object)
            if (-not $idCols) {
              # Fallback: commonly useful identifiers
              $idCols = @($headerMap.GetEnumerator() | Where-Object { $_.Value -match '(Item|SKU|Warehouse|Location)' } | ForEach-Object { [int]$_.Key } | Sort-Object)
            }
            $result.Notes.Add("Detected header row $headerRow") | Out-Null
          }
          continue
        }

        if (-not $headerRow) { continue }
        if ($r -le $headerRow) { continue }

        # Build row object for later investigation (only if any asset col has data)
        $assetValues = @{}
        $rowAssetSum = [decimal]0
        $hasAsset = $false
        foreach ($col in $assetCols) {
          $raw = $rowVals[$col]
          if ($null -eq $raw -or [string]::IsNullOrWhiteSpace([string]$raw)) { continue }
          $s = ([string]$raw).Trim()
          # Cope with formatted strings like "1,234.56"
          $sNorm = $s -replace ',', ''
          [decimal]$d = 0
          if ([decimal]::TryParse($sNorm, [ref]$d)) {
            $assetValues[$col] = $d
            $rowAssetSum += $d
            $hasAsset = $true
            if (-not $result.TotalsByCol.ContainsKey($col)) { $result.TotalsByCol[$col] = [decimal]0 }
            $result.TotalsByCol[$col] += $d
            if (-not $isHidden) {
              $key = "vis:$col"
              if (-not $result.Contains($key)) { $result[$key] = [decimal]0 }
              $result[$key] += $d
            }
          } else {
            # Non-numeric in asset column
            $result.Notes.Add("Non-numeric asset value at row $r col ${col}: '$s'") | Out-Null
          }
        }

        if (-not $hasAsset) { continue }

        $id = [ordered]@{}
        foreach ($col in $idCols) {
          $hdr = $headerMap[$col]
          $id[$hdr] = $rowVals[$col]
        }

        $result.Rows.Add([pscustomobject]@{
          Row = $r
          Hidden = $isHidden
          Id = $id
          AssetValues = $assetValues
          AssetSum = $rowAssetSum
        }) | Out-Null
      }
    } finally {
      $reader.Dispose()
    }
  } finally {
    $stream.Dispose()
  }

  $result.HeaderRow = $headerRow
  $result.HeaderMap = $headerMap
  $result.AssetValueCols = $assetCols
  $result.IdCols = $idCols
  $result.TotalAllAssetCols = ($result.TotalsByCol.GetEnumerator() | ForEach-Object { [decimal]$_.Value } | Measure-Object -Sum).Sum
  $visibleSum = [decimal]0
  foreach ($col in $assetCols) {
    $k = "vis:$col"
    if ($result.Contains($k)) { $visibleSum += [decimal]$result[$k] }
  }
  $result.TotalAllAssetCols_VisibleRowsOnly = $visibleSum

  return [pscustomobject]$result
}

function Find-SubsetMatch {
  param(
    [Parameter(Mandatory)][object[]]$Rows,
    [Parameter(Mandatory)][decimal]$Target,
    [int]$MaxRows = 2000,
    [int]$MaxSubsetSize = 4,
    [decimal]$Tolerance = 0.05
  )
  # Heuristic: try to find 1..N rows whose AssetSum matches Target (within tolerance).
  $candidates = $Rows | Where-Object { $_.AssetSum -ne 0 } | Sort-Object { [math]::Abs([decimal]$_.AssetSum - $Target) } | Select-Object -First $MaxRows
  $cand = @($candidates)

  for ($k = 1; $k -le $MaxSubsetSize; $k++) {
    if ($k -eq 1) {
      foreach ($r in $cand) {
        if ([math]::Abs([decimal]$r.AssetSum - $Target) -le $Tolerance) { return @($r) }
      }
      continue
    }
    if ($k -eq 2) {
      for ($i=0; $i -lt $cand.Count; $i++) {
        for ($j=$i+1; $j -lt $cand.Count; $j++) {
          $s = [decimal]$cand[$i].AssetSum + [decimal]$cand[$j].AssetSum
          if ([math]::Abs($s - $Target) -le $Tolerance) { return @($cand[$i], $cand[$j]) }
        }
      }
      continue
    }
    if ($k -eq 3) {
      for ($i=0; $i -lt $cand.Count; $i++) {
        for ($j=$i+1; $j -lt $cand.Count; $j++) {
          for ($m=$j+1; $m -lt $cand.Count; $m++) {
            $s = [decimal]$cand[$i].AssetSum + [decimal]$cand[$j].AssetSum + [decimal]$cand[$m].AssetSum
            if ([math]::Abs($s - $Target) -le $Tolerance) { return @($cand[$i], $cand[$j], $cand[$m]) }
          }
        }
      }
      continue
    }
    if ($k -eq 4) {
      for ($i=0; $i -lt $cand.Count; $i++) {
        for ($j=$i+1; $j -lt $cand.Count; $j++) {
          for ($m=$j+1; $m -lt $cand.Count; $m++) {
            for ($n=$m+1; $n -lt $cand.Count; $n++) {
              $s = [decimal]$cand[$i].AssetSum + [decimal]$cand[$j].AssetSum + [decimal]$cand[$m].AssetSum + [decimal]$cand[$n].AssetSum
              if ([math]::Abs($s - $Target) -le $Tolerance) { return @($cand[$i], $cand[$j], $cand[$m], $cand[$n]) }
            }
          }
        }
      }
    }
  }
  return @()
}

function Analyze-Workbook {
  param(
    [Parameter(Mandatory)][string]$Path,
    [Parameter(Mandatory)][string]$Label
  )
  Add-Type -AssemblyName System.IO.Compression.FileSystem | Out-Null
  $zip = [System.IO.Compression.ZipFile]::OpenRead($Path)
  try {
    $shared = Get-XlsxSharedStrings -Zip $zip
    $sheets = Get-XlsxSheets -Zip $zip
    $analyses = @()
    foreach ($s in $sheets) {
      $analyses += Get-WorksheetAnalysis -Zip $zip -SharedStrings $shared -SheetPath $s.Path -FileLabel $Label
    }
    return ,$analyses
  } finally {
    $zip.Dispose()
  }
}

function Format-Id {
  param([hashtable]$Id)
  if (-not $Id) { return '' }
  return ($Id.GetEnumerator() | Sort-Object Name | ForEach-Object { "{0}={1}" -f $_.Name,$_.Value }) -join '; '
}

$zoho = Analyze-Workbook -Path $ZohoReportPath -Label 'ZOHO_REPORT'
$adj = Analyze-Workbook -Path $AdjustmentPath -Label 'ADJUSTMENT'

$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine("Inventory check run: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
$null = $sb.AppendLine("Zoho report: $ZohoReportPath")
$null = $sb.AppendLine("Adjustment file: $AdjustmentPath")
$null = $sb.AppendLine('')

function Append-AnalysisSummary {
  param([object]$a)
  $null = $sb.AppendLine("[$($a.File)] Sheet: $($a.SheetPath)")
  if (-not $a.HeaderRow) {
    $null = $sb.AppendLine('  Header row: NOT FOUND (no "Inventory Asset Value" header detected)')
    $null = $sb.AppendLine('')
    return
  }
  $null = $sb.AppendLine("  Header row: $($a.HeaderRow)")
  $null = $sb.AppendLine("  Asset cols: $([string]::Join(',', $a.AssetValueCols))")
  $null = $sb.AppendLine("  Total (all asset cols): $([decimal]$a.TotalAllAssetCols)")
  $null = $sb.AppendLine("  Total (visible rows only): $([decimal]$a.TotalAllAssetCols_VisibleRowsOnly)")
  foreach ($kv in $a.TotalsByCol.GetEnumerator() | Sort-Object Name) {
    $hdr = $a.HeaderMap[[int]$kv.Name]
    $null = $sb.AppendLine("    Col $($kv.Name) [$hdr] sum: $([decimal]$kv.Value)")
  }
  $null = $sb.AppendLine('')
}

foreach ($a in $zoho) { Append-AnalysisSummary -a $a }
foreach ($a in $adj) { Append-AnalysisSummary -a $a }

if ($ZohoDisplayedTotal) {
  $zohoTotals = @($zoho | Where-Object { $_.HeaderRow } | ForEach-Object { [decimal]$_.TotalAllAssetCols })
  $zohoTotalSum = ($zohoTotals | Measure-Object -Sum).Sum
  $delta = [decimal]$ZohoDisplayedTotal - [decimal]$zohoTotalSum
  $null = $sb.AppendLine("Zoho displayed total provided: $ZohoDisplayedTotal")
  $null = $sb.AppendLine("Computed total from xlsx (sum of all 'Inventory Asset Value' cols across detected sheets): $zohoTotalSum")
  $null = $sb.AppendLine("Delta (displayed - computed): $delta")
  $null = $sb.AppendLine('')

  # Try to find culprit rows in the main zoho sheet that sum to delta
  $main = $zoho | Where-Object { $_.HeaderRow } | Sort-Object { $_.Rows.Count } -Descending | Select-Object -First 1
  if ($main -and $main.Rows.Count -gt 0) {
    $matches = Find-SubsetMatch -Rows @($main.Rows) -Target $delta -MaxSubsetSize 4 -Tolerance 0.05
    if ($matches.Count -gt 0) {
      $null = $sb.AppendLine('Potential culprit rows (asset sum ~= delta):')
      foreach ($m in $matches) {
        $null = $sb.AppendLine("  Row $($m.Row) hidden=$($m.Hidden) assetSum=$([decimal]$m.AssetSum) id={$(Format-Id -Id $m.Id)}")
      }
      $null = $sb.AppendLine('')
    } else {
      $null = $sb.AppendLine('No small subset of rows matched the delta (searched subset size up to 4).')
      $null = $sb.AppendLine('')
    }
  }
}

if ($zoho.Count -gt 0) {
  $notes = @($zoho.Notes + $adj.Notes) | Where-Object { $_ } | Select-Object -Unique
  if ($notes.Count -gt 0) {
    $null = $sb.AppendLine('Notes:')
    foreach ($n in $notes) { $null = $sb.AppendLine("  - $n") }
    $null = $sb.AppendLine('')
  }
}

[System.IO.File]::WriteAllText($OutPath, $sb.ToString(), [System.Text.Encoding]::UTF8)
Write-Host "Wrote report to: $OutPath"
