/* global Papa */

(() => {
  const $ = (sel) => document.querySelector(sel);
  const filesEl = $("#files");
  const resultsEl = $("#results");
  const progressEl = $("#progress");
  const progressBarEl = $("#progressBar");

  const state = {
    files: [],
    primaryId: null,
    running: false,
  };

  const encodings = ["utf-8", "windows-1251", "iso-8859-1"];

  function uid() {
    return Math.random().toString(16).slice(2) + Date.now().toString(16);
  }

  function setProgress(text, pct) {
    progressEl.textContent = text;
    const p = Math.max(0, Math.min(100, pct || 0));
    progressBarEl.style.width = `${p}%`;
  }

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function csvCell(v) {
    const s = v == null ? "" : String(v);
    if (/[",\n\r]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
    return s;
  }

  function makeCsv(headers, rows) {
    const out = [];
    out.push(headers.map(csvCell).join(","));
    for (const r of rows) {
      out.push(headers.map((h) => csvCell(r[h])).join(","));
    }
    return out.join("\n");
  }

  function downloadLink(label, filename, content, mime = "text/csv;charset=utf-8") {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    return { label, filename, url };
  }

  function normalizeStatus(raw) {
    const s = (raw || "").trim();
    return s ? s.toLowerCase() : null;
  }

  function pow10BigInt(n) {
    let x = 1n;
    for (let i = 0; i < n; i++) x *= 10n;
    return x;
  }

  function parseAmountScaled(raw, scale, decimalComma) {
    let s = (raw || "").trim();
    if (!s) return null;

    let neg = false;
    if (s.startsWith("(") && s.endsWith(")")) {
      neg = true;
      s = s.slice(1, -1).trim();
    }

    s = s.replaceAll("\u00a0", "").replaceAll(" ", "");

    if (decimalComma) {
      // Comma is decimal separator; dots are thousands separators.
      // Only strip dots when a comma is also present (unambiguous case).
      // If no comma is present, the dot might itself be the decimal separator.
      if (s.includes(",")) {
        s = s.replaceAll(".", "").replaceAll(",", ".");
      }
    } else {
      if (s.includes(",") && s.includes(".")) {
        s = s.replaceAll(",", "");
      } else if (s.includes(",") && !s.includes(".")) {
        s = s.replaceAll(",", ".");
      }
    }

    // Keep only digits, dot, sign.
    s = s.replace(/[^\d.+-]/g, "");
    if (!s || s === "-" || s === "+" || s === "." || s === "-." || s === "+.") return null;

    let sign = 1n;
    if (s.startsWith("-")) {
      sign = -1n;
      s = s.slice(1);
    } else if (s.startsWith("+")) {
      s = s.slice(1);
    }
    if (!s) return null;

    const parts = s.split(".");
    if (parts.length > 2) return null;
    let intPart = parts[0] || "0";
    let fracPart = parts[1] || "";

    intPart = intPart.replace(/^0+(?=\d)/, "");
    if (!/^\d+$/.test(intPart)) return null;
    if (fracPart && !/^\d+$/.test(fracPart)) return null;

    const scaleN = Number(scale);
    if (!Number.isInteger(scaleN) || scaleN < 0 || scaleN > 18) return null;

    const factor = pow10BigInt(scaleN);
    let scaledAbs = BigInt(intPart) * factor;

    if (scaleN === 0) {
      if (fracPart && fracPart[0] >= "5") scaledAbs += 1n;
    } else {
      if (fracPart.length <= scaleN) {
        const fracPadded = (fracPart + "0".repeat(scaleN)).slice(0, scaleN);
        if (fracPadded) scaledAbs += BigInt(fracPadded);
      } else {
        const main = fracPart.slice(0, scaleN);
        const nextDigit = fracPart[scaleN] || "0";
        if (main) scaledAbs += BigInt(main);
        if (nextDigit >= "5") scaledAbs += 1n;
      }
    }

    if (neg) sign = -sign;
    return sign * scaledAbs;
  }

  function guessColumn(header, candidates) {
    const norm = header.map((h) => String(h || "").trim().toLowerCase());
    for (const c of candidates) {
      const idx = norm.indexOf(c);
      if (idx >= 0) return idx;
    }
    return -1;
  }

  async function parseHeaderPreview(file, delimiter) {
    return new Promise((resolve, reject) => {
      Papa.parse(file, {
        delimiter: delimiter || "",
        preview: 1,
        skipEmptyLines: true,
        complete: (res) => {
          const header = (res.data && res.data[0]) || [];
          resolve(header.map((x) => String(x ?? "").trim()));
        },
        error: (err) => reject(err),
      });
    });
  }

  function fileCardHtml(f, idx) {
    const header = f.header || [];
    const options = header
      .map((h, i) => `<option value="${i}">${escapeHtml(h)} (index ${i})</option>`)
      .join("");

    const encOpts = encodings
      .map((e) => `<option value="${escapeHtml(e)}"${f.encoding === e ? " selected" : ""}>${escapeHtml(e)}</option>`)
      .join("");

    const isPrimary = state.primaryId === f.id;

    return `
      <div class="file-card" data-file-id="${escapeHtml(f.id)}">
        <div class="row row-between row-wrap gap">
          <h3>Файл ${idx + 1}</h3>
          <div class="row gap">
            <label class="row gap muted" style="user-select:none;">
              <input type="radio" name="primary" value="${escapeHtml(f.id)}" ${isPrimary ? "checked" : ""} />
              primary
            </label>
            <button class="btn btn-danger" type="button" data-action="remove-file">Удалить</button>
          </div>
        </div>

        <div class="grid grid-3">
          <label class="field">
            <div class="label">Имя (для отчётов)</div>
            <input data-field="name" value="${escapeHtml(f.name)}" placeholder="например: bank" />
          </label>

          <label class="field">
            <div class="label">CSV файл</div>
            <input data-field="file" type="file" accept=".csv,text/csv" />
            <div class="hint">${f.file ? escapeHtml(f.file.name) : "Файл не выбран"}</div>
          </label>

          <label class="field">
            <div class="label">Разделитель</div>
            <input data-field="delimiter" value="${escapeHtml(f.delimiter)}" placeholder="например: , или ;" maxlength="1" />
          </label>
        </div>

        <div class="grid grid-3" style="margin-top: 10px;">
          <label class="field">
            <div class="label">Кодировка (если нужно)</div>
            <select data-field="encoding">
              ${encOpts}
            </select>
            <div class="hint">CSV будет декодироваться этой кодировкой перед парсингом.</div>
          </label>

          <label class="field">
            <div class="label">Десятичная запятая</div>
            <select data-field="decimalComma">
              <option value="0"${!f.decimalComma ? " selected" : ""}>Нет (обычно 12.34)</option>
              <option value="1"${f.decimalComma ? " selected" : ""}>Да (обычно 12,34)</option>
            </select>
          </label>

          <label class="field">
            <div class="label">Сохранить колонки (keep_cols, через запятую)</div>
            <input data-field="keepCols" value="${escapeHtml(f.keepColsText)}" placeholder="например: created_at, merchant" />
          </label>
        </div>

        <div class="grid grid-3" style="margin-top: 10px;">
          <label class="field">
            <div class="label">transaction_id</div>
            <select data-field="idCol">
              <option value="-1">(выберите колонку)</option>
              ${options}
            </select>
          </label>
          <label class="field">
            <div class="label">amount</div>
            <select data-field="amountCol">
              <option value="-1">(выберите колонку)</option>
              ${options}
            </select>
          </label>
          <label class="field">
            <div class="label">status</div>
            <select data-field="statusCol">
              <option value="-1">(выберите колонку)</option>
              ${options}
            </select>
          </label>
        </div>

        <div class="hint">
          ${header.length ? `Заголовок прочитан: <code>${escapeHtml(header.join(" | "))}</code>` : "Выберите файл, чтобы прочитать заголовок."}
        </div>
      </div>
    `;
  }

  function renderFiles() {
    filesEl.innerHTML = state.files.map((f, i) => fileCardHtml(f, i)).join("");

    // Set selected values for selects after innerHTML (to avoid escaping issues).
    for (const f of state.files) {
      const root = filesEl.querySelector(`[data-file-id="${CSS.escape(f.id)}"]`);
      if (!root) continue;
      const setSel = (field, val) => {
        const el = root.querySelector(`[data-field="${field}"]`);
        if (el && typeof val === "number") el.value = String(val);
      };
      setSel("idCol", f.idCol);
      setSel("amountCol", f.amountCol);
      setSel("statusCol", f.statusCol);
    }
  }

  function addFile(initial = {}) {
    const f = {
      id: uid(),
      name: initial.name || `file${state.files.length + 1}`,
      file: initial.file || null,
      delimiter: initial.delimiter ?? ",",
      encoding: initial.encoding || "utf-8",
      decimalComma: Boolean(initial.decimalComma),
      header: initial.header || [],
      idCol: initial.idCol ?? -1,
      amountCol: initial.amountCol ?? -1,
      statusCol: initial.statusCol ?? -1,
      keepColsText: initial.keepColsText || "",
    };
    state.files.push(f);
    if (!state.primaryId) state.primaryId = f.id;
    renderFiles();
  }

  function removeFile(fileId) {
    state.files = state.files.filter((x) => x.id !== fileId);
    if (state.primaryId === fileId) state.primaryId = state.files[0]?.id || null;
    renderFiles();
  }

  function getGlobalSettings() {
    const amountScale = Number($("#amountScale").value);
    const amountTolerance = Number($("#amountTolerance").value);
    const reportLimit = Number($("#reportLimit").value);
    return {
      amountScale,
      amountTolerance,
      reportLimit: Number.isFinite(reportLimit) ? Math.max(0, reportLimit) : 0,
    };
  }

  function validate() {
    const errs = [];
    if (state.files.length < 2) errs.push("Нужно минимум 2 файла.");
    if (!state.primaryId) errs.push("Выберите primary файл.");
    for (const f of state.files) {
      if (!f.file) errs.push(`Файл не выбран: ${f.name}`);
      if (!f.delimiter || String(f.delimiter).length !== 1) errs.push(`Неверный разделитель у ${f.name}`);
      if (!f.header || !f.header.length) errs.push(`Не удалось прочитать заголовок у ${f.name}`);
      if (f.idCol < 0 || f.amountCol < 0 || f.statusCol < 0) errs.push(`Выберите id/amount/status колонки у ${f.name}`);
    }
    const names = state.files.map((f) => f.name);
    if (new Set(names).size !== names.length) errs.push("Имена файлов (для отчётов) должны быть уникальны.");
    const gs = getGlobalSettings();
    if (!Number.isInteger(gs.amountScale) || gs.amountScale < 0 || gs.amountScale > 18) errs.push("amount_scale должен быть 0..18");
    if (!Number.isInteger(gs.amountTolerance) || gs.amountTolerance < 0) errs.push("amount_tolerance должен быть >= 0");
    return errs;
  }

  async function decodeFileToText(file, encoding) {
    const buf = await file.arrayBuffer();
    const dec = new TextDecoder(encoding || "utf-8", { fatal: false });
    return dec.decode(buf);
  }

  async function parseCsvToMaps(fileSpec, globalSettings, progressBasePct, progressSpanPct) {
    const { amountScale } = globalSettings;

    const keepCols = fileSpec.keepColsText
      ? fileSpec.keepColsText.split(",").map((x) => x.trim()).filter(Boolean)
      : [];

    // Map keep col name -> index (case-insensitive)
    const keepIdx = [];
    const headerNorm = fileSpec.header.map((h) => h.trim().toLowerCase());
    for (const kc of keepCols) {
      const idx = headerNorm.indexOf(kc.trim().toLowerCase());
      if (idx >= 0) keepIdx.push({ name: kc, idx });
    }

    const text = await decodeFileToText(fileSpec.file, fileSpec.encoding);
    const records = new Map(); // txid -> record (first occurrence)
    const counts = new Map(); // txid -> count
    const dupPushed = new Set(); // txid pushed previous first row into duplicates already
    const duplicates = []; // rows for export
    let rowsTotal = 0;
    let rowsBadId = 0;
    let rowsBadAmount = 0;

    const reportRowFor = (rec) => {
      const row = {
        txid: rec.txid,
        amount_raw: rec.amountRaw ?? "",
        amount_scaled: rec.amountScaled == null ? "" : rec.amountScaled.toString(),
        status_raw: rec.statusRaw ?? "",
        status_norm: rec.statusNorm ?? "",
        rownum: rec.rownum,
      };
      for (const k of keepCols) {
        row[`keep__${k}`] = rec.keep?.[k] ?? "";
      }
      return row;
    };

    setProgress(`Чтение ${fileSpec.name}…`, progressBasePct);

    let rownum = 0; // 1-based; first parsed row is header
    await new Promise((resolve, reject) => {
      Papa.parse(text, {
        delimiter: fileSpec.delimiter,
        skipEmptyLines: true,
        header: false,
        worker: false,
        step: (res) => {
          const row = res.data || [];
          rownum += 1;
          if (rownum === 1) return; // header

          rowsTotal += 1;

          const txid = String(row[fileSpec.idCol] ?? "").trim();
          if (!txid) {
            rowsBadId += 1;
            return;
          }

          const c = (counts.get(txid) || 0) + 1;
          counts.set(txid, c);

          const amountRaw = String(row[fileSpec.amountCol] ?? "");
          const amountScaled = parseAmountScaled(amountRaw, amountScale, fileSpec.decimalComma);
          if (amountRaw && amountScaled == null) rowsBadAmount += 1;
          const statusRaw = String(row[fileSpec.statusCol] ?? "");
          const statusNorm = normalizeStatus(statusRaw);

          const keep = {};
          for (const { name, idx } of keepIdx) keep[name] = String(row[idx] ?? "");

          if (c === 1) {
            records.set(txid, { txid, amountRaw, amountScaled, statusRaw, statusNorm, rownum, keep });
          } else {
            const first = records.get(txid);
            if (first && !dupPushed.has(txid)) {
              duplicates.push(reportRowFor(first));
              dupPushed.add(txid);
            }
            duplicates.push(reportRowFor({ txid, amountRaw, amountScaled, statusRaw, statusNorm, rownum, keep }));
          }

          if (rowsTotal % 5000 === 0) {
            const pct = progressBasePct + Math.min(progressSpanPct, (rowsTotal / 500000) * progressSpanPct);
            setProgress(`Чтение ${fileSpec.name}… строк: ${rowsTotal.toLocaleString("ru-RU")}`, pct);
          }
        },
        complete: () => resolve(),
        error: (err) => reject(err),
      });
    });

    setProgress(`Готово: ${fileSpec.name}. Строк: ${rowsTotal.toLocaleString("ru-RU")}`, progressBasePct + progressSpanPct);

    return {
      name: fileSpec.name,
      header: fileSpec.header,
      keepCols,
      records,
      counts,
      duplicates,
      rowsTotal,
      rowsBadId,
      rowsBadAmount,
    };
  }

  function statusTotals(mapPack) {
    const totals = new Map(); // status_norm -> {count, sumScaled BigInt}
    for (const [txid, rec] of mapPack.records.entries()) {
      if ((mapPack.counts.get(txid) || 0) !== 1) continue;
      const key = rec.statusNorm || "";
      const cur = totals.get(key) || { tx_count: 0, amount_scaled_sum: 0n };
      cur.tx_count += 1;
      if (rec.amountScaled != null) cur.amount_scaled_sum += rec.amountScaled;
      totals.set(key, cur);
    }
    const rows = [];
    const keys = Array.from(totals.keys()).sort((a, b) => a.localeCompare(b));
    for (const k of keys) {
      const v = totals.get(k);
      rows.push({
        status_norm: k,
        tx_count: v.tx_count,
        amount_scaled_sum: v.amount_scaled_sum.toString(),
      });
    }
    return { headers: ["status_norm", "tx_count", "amount_scaled_sum"], rows };
  }

  function clampRows(rows, limit) {
    if (!limit || limit <= 0) return { rows, truncated: false };
    if (rows.length <= limit) return { rows, truncated: false };
    return { rows: rows.slice(0, limit), truncated: true };
  }

  function reconcilePair(basePack, otherPack, globalSettings) {
    const tol = BigInt(globalSettings.amountTolerance || 0);
    const reportLimit = globalSettings.reportLimit || 0;

    const baseUnique = new Set();
    for (const [txid, c] of basePack.counts.entries()) if (c === 1) baseUnique.add(txid);
    const otherUnique = new Set();
    for (const [txid, c] of otherPack.counts.entries()) if (c === 1) otherUnique.add(txid);

    const missingInBase = [];
    for (const txid of otherUnique) {
      if (!baseUnique.has(txid)) {
        const rec = otherPack.records.get(txid);
        if (rec) missingInBase.push(rec);
      }
    }

    const missingInOther = [];
    for (const txid of baseUnique) {
      if (!otherUnique.has(txid)) {
        const rec = basePack.records.get(txid);
        if (rec) missingInOther.push(rec);
      }
    }

    const keepCols = Array.from(new Set([...(basePack.keepCols || []), ...(otherPack.keepCols || [])]));

    const mismatches = [];
    for (const txid of baseUnique) {
      if (!otherUnique.has(txid)) continue;
      const b = basePack.records.get(txid);
      const o = otherPack.records.get(txid);
      if (!b || !o) continue;

      const amountParseError = b.amountScaled == null || o.amountScaled == null;
      const amountMismatch = !amountParseError && ((b.amountScaled > o.amountScaled ? b.amountScaled - o.amountScaled : o.amountScaled - b.amountScaled) > tol);
      const statusMismatch = (b.statusNorm || "") !== (o.statusNorm || "");
      if (!(amountParseError || amountMismatch || statusMismatch)) continue;

      let mismatchType = "status_mismatch";
      if (amountParseError) mismatchType = "amount_parse_error";
      else if (amountMismatch && statusMismatch) mismatchType = "amount_and_status_mismatch";
      else if (amountMismatch) mismatchType = "amount_mismatch";

      const row = {
        txid,
        mismatch_type: mismatchType,
        base_amount_raw: b.amountRaw ?? "",
        base_amount_scaled: b.amountScaled == null ? "" : b.amountScaled.toString(),
        other_amount_raw: o.amountRaw ?? "",
        other_amount_scaled: o.amountScaled == null ? "" : o.amountScaled.toString(),
        amount_diff_scaled: (o.amountScaled != null && b.amountScaled != null) ? (o.amountScaled - b.amountScaled).toString() : "",
        base_status_raw: b.statusRaw ?? "",
        base_status_norm: b.statusNorm ?? "",
        other_status_raw: o.statusRaw ?? "",
        other_status_norm: o.statusNorm ?? "",
        base_rownum: b.rownum,
        other_rownum: o.rownum,
      };
      for (const k of keepCols) {
        row[`base__${k}`] = b.keep?.[k] ?? "";
        row[`other__${k}`] = o.keep?.[k] ?? "";
      }
      mismatches.push(row);
    }

    const missingHeaders = (pack) => {
      const hs = ["txid", "amount_raw", "amount_scaled", "status_raw", "status_norm", "rownum"];
      for (const k of pack.keepCols || []) hs.push(`keep__${k}`);
      return hs;
    };

    const baseRow = (rec, pack) => {
      const r = {
        txid: rec.txid,
        amount_raw: rec.amountRaw ?? "",
        amount_scaled: rec.amountScaled == null ? "" : rec.amountScaled.toString(),
        status_raw: rec.statusRaw ?? "",
        status_norm: rec.statusNorm ?? "",
        rownum: rec.rownum,
      };
      for (const k of pack.keepCols || []) r[`keep__${k}`] = rec.keep?.[k] ?? "";
      return r;
    };

    const baseMissingRows = missingInOther.map((rec) => baseRow(rec, basePack));
    const otherMissingRows = missingInBase.map((rec) => baseRow(rec, otherPack));

    const mismatchHeaders = [
      "txid",
      "mismatch_type",
      "base_amount_raw",
      "base_amount_scaled",
      "other_amount_raw",
      "other_amount_scaled",
      "amount_diff_scaled",
      "base_status_raw",
      "base_status_norm",
      "other_status_raw",
      "other_status_norm",
      "base_rownum",
      "other_rownum",
      ...keepCols.flatMap((k) => [`base__${k}`, `other__${k}`]),
    ];

    const limited = {
      missing_in_base: clampRows(otherMissingRows, reportLimit),
      missing_in_other: clampRows(baseMissingRows, reportLimit),
      mismatches: clampRows(mismatches, reportLimit),
      duplicates_base: clampRows(basePack.duplicates, reportLimit),
      duplicates_other: clampRows(otherPack.duplicates, reportLimit),
    };

    return {
      counts: {
        missing_in_base: otherMissingRows.length,
        missing_in_other: baseMissingRows.length,
        mismatches: mismatches.length,
        duplicates_base_rows: basePack.duplicates.length,
        duplicates_other_rows: otherPack.duplicates.length,
      },
      reports: {
        missing_in_base: { headers: missingHeaders(otherPack), rows: limited.missing_in_base.rows, truncated: limited.missing_in_base.truncated },
        missing_in_other: { headers: missingHeaders(basePack), rows: limited.missing_in_other.rows, truncated: limited.missing_in_other.truncated },
        mismatches: { headers: mismatchHeaders, rows: limited.mismatches.rows, truncated: limited.mismatches.truncated },
        duplicates_base: { headers: missingHeaders(basePack), rows: limited.duplicates_base.rows, truncated: limited.duplicates_base.truncated },
        duplicates_other: { headers: missingHeaders(otherPack), rows: limited.duplicates_other.rows, truncated: limited.duplicates_other.truncated },
      },
    };
  }

  function renderResults(summary, downloads) {
    const blocks = [];
    blocks.push(`
      <div class="result-block">
        <div class="result-title">
          <strong>Сводка</strong>
          <span class="muted mono">${escapeHtml(new Date().toLocaleString("ru-RU"))}</span>
        </div>
        <div class="kpi">
          <div class="item"><div class="value">${escapeHtml(summary.primary)}</div><div class="name">primary</div></div>
          <div class="item"><div class="value">${escapeHtml(String(summary.settings.amount_scale))}</div><div class="name">amount_scale</div></div>
          <div class="item"><div class="value">${escapeHtml(String(summary.settings.amount_tolerance_scaled))}</div><div class="name">tolerance (scaled)</div></div>
          <div class="item"><div class="value">${escapeHtml(String(summary.files_count))}</div><div class="name">файлов</div></div>
        </div>
        <div class="links">
          ${downloads.summary ? `<a class="link" href="${downloads.summary.url}" download="${escapeHtml(downloads.summary.filename)}">summary.json</a>` : ""}
        </div>
      </div>
    `);

    for (const pair of summary.pairs) {
      blocks.push(`
        <div class="result-block">
          <div class="result-title">
            <strong>${escapeHtml(pair.base)} vs ${escapeHtml(pair.other)}</strong>
            <span class="muted mono">${escapeHtml(pair.key)}</span>
          </div>
          <div class="kpi">
            <div class="item"><div class="value">${escapeHtml(String(pair.counts.missing_in_base))}</div><div class="name">missing_in_base</div></div>
            <div class="item"><div class="value">${escapeHtml(String(pair.counts.missing_in_other))}</div><div class="name">missing_in_other</div></div>
            <div class="item"><div class="value">${escapeHtml(String(pair.counts.mismatches))}</div><div class="name">mismatches</div></div>
            <div class="item"><div class="value">${escapeHtml(String(pair.counts.duplicates_other_rows))}</div><div class="name">duplicates_other_rows</div></div>
          </div>
          <div class="links">
            ${pair.links
              .map((l) => `<a class="link" href="${l.url}" download="${escapeHtml(l.filename)}">${escapeHtml(l.label)}</a>`)
              .join("")}
          </div>
          ${pair.notes.length ? `<div class="hint">${pair.notes.map(escapeHtml).join("<br/>")}</div>` : ""}
        </div>
      `);
    }

    blocks.push(`
      <div class="result-block">
        <div class="result-title">
          <strong>Агрегаты по статусам</strong>
          <span class="muted">для каждого файла (уникальные txid)</span>
        </div>
        <div class="links">
          ${downloads.statusTotals.map((l) => `<a class="link" href="${l.url}" download="${escapeHtml(l.filename)}">${escapeHtml(l.label)}</a>`).join("")}
        </div>
      </div>
    `);

    resultsEl.innerHTML = `<div class="results">${blocks.join("")}</div>`;
  }

  async function run() {
    if (state.running) return;
    const errs = validate();
    if (errs.length) {
      resultsEl.innerHTML = `<div class="mono" style="color: var(--danger)">${escapeHtml(errs.join("\n"))}</div>`;
      return;
    }
    if (!window.Papa) {
      resultsEl.innerHTML = `<div class="mono" style="color: var(--danger)">PapaParse не загрузился (проверьте интернет/блокировки CDN).</div>`;
      return;
    }

    state.running = true;
    resultsEl.textContent = "Идёт сверка…";
    setProgress("Старт…", 0);

    try {
      const gs = getGlobalSettings();
      const primary = state.files.find((f) => f.id === state.primaryId) || state.files[0];
      const others = state.files.filter((f) => f.id !== primary.id);

      // Heuristic: distribute progress across files.
      setProgress(`Подготовка…`, 1);

      const basePack = await parseCsvToMaps(primary, gs, 2, 28);
      const statusTotalsLinks = [];

      const baseTotals = statusTotals(basePack);
      statusTotalsLinks.push(
        downloadLink(
          `status_totals__${primary.name}.csv`,
          `status_totals__${primary.name}.csv`,
          makeCsv(baseTotals.headers, baseTotals.rows)
        )
      );

      const pairs = [];
      let pairIdx = 0;
      for (const other of others) {
        pairIdx += 1;
        const otherPack = await parseCsvToMaps(other, gs, 30 + (pairIdx - 1) * (50 / others.length), 22);

        const otherTotals = statusTotals(otherPack);
        statusTotalsLinks.push(
          downloadLink(
            `status_totals__${other.name}.csv`,
            `status_totals__${other.name}.csv`,
            makeCsv(otherTotals.headers, otherTotals.rows)
          )
        );

        setProgress(`Сравнение ${primary.name} vs ${other.name}…`, 60 + (pairIdx - 1) * (30 / others.length));
        const pair = reconcilePair(basePack, otherPack, gs);

        const links = [];
        const notes = [];
        const mk = (label, filename, rep) => {
          const suffix = rep.truncated ? " (truncated)" : "";
          if (rep.truncated) notes.push(`${label}: отчёт ограничен лимитом строк (см. настройку “Ограничение строк в отчётах”)`);
          links.push(downloadLink(`${label}${suffix}`, filename, makeCsv(rep.headers, rep.rows)));
        };

        mk("mismatches.csv", `${primary.name}__vs__${other.name}__mismatches.csv`, pair.reports.mismatches);
        mk("missing_in_base.csv", `${primary.name}__vs__${other.name}__missing_in_base.csv`, pair.reports.missing_in_base);
        mk("missing_in_other.csv", `${primary.name}__vs__${other.name}__missing_in_other.csv`, pair.reports.missing_in_other);
        mk("duplicates_base.csv", `${primary.name}__vs__${other.name}__duplicates_base.csv`, pair.reports.duplicates_base);
        mk("duplicates_other.csv", `${primary.name}__vs__${other.name}__duplicates_other.csv`, pair.reports.duplicates_other);

        pairs.push({
          key: `${primary.name}__vs__${other.name}`,
          base: primary.name,
          other: other.name,
          counts: pair.counts,
          links,
          notes,
        });
      }

      const summary = {
        created_at: new Date().toISOString(),
        primary: primary.name,
        files_count: state.files.length,
        settings: {
          amount_scale: gs.amountScale,
          amount_tolerance_scaled: gs.amountTolerance,
          report_limit: gs.reportLimit,
        },
        files: state.files.map((f) => ({
          name: f.name,
          filename: f.file?.name || "",
          delimiter: f.delimiter,
          encoding: f.encoding,
          decimal_comma: Boolean(f.decimalComma),
          columns: {
            id_index: f.idCol,
            amount_index: f.amountCol,
            status_index: f.statusCol,
          },
          keep_cols: f.keepColsText
            ? f.keepColsText.split(",").map((x) => x.trim()).filter(Boolean)
            : [],
        })),
        pairs: pairs.map((p) => ({ key: p.key, counts: p.counts })),
      };

      const downloads = {
        summary: downloadLink("summary.json", "summary.json", JSON.stringify(summary, null, 2), "application/json;charset=utf-8"),
        statusTotals: statusTotalsLinks,
      };

      setProgress("Готово.", 100);
      renderResults({ ...summary, pairs }, downloads);
    } catch (e) {
      setProgress("Ошибка.", 0);
      resultsEl.innerHTML = `<div class="mono" style="color: var(--danger)">${escapeHtml(e?.stack || String(e))}</div>`;
    } finally {
      state.running = false;
    }
  }

  async function refreshHeaderForFile(fileId) {
    const f = state.files.find((x) => x.id === fileId);
    if (!f || !f.file) return;
    try {
      setProgress(`Чтение заголовка: ${f.name}…`, 0);
      // Decode a small slice for preview to respect encoding.
      const slice = f.file.slice(0, Math.min(512 * 1024, f.file.size));
      const text = await decodeFileToText(slice, f.encoding);
      const header = await new Promise((resolve, reject) => {
        Papa.parse(text, {
          delimiter: f.delimiter || "",
          preview: 1,
          skipEmptyLines: true,
          complete: (res) => resolve(((res.data && res.data[0]) || []).map((x) => String(x ?? "").trim())),
          error: (err) => reject(err),
        });
      });
      f.header = header;

      // Auto-guess columns.
      if (f.idCol < 0) {
        const idx = guessColumn(header, ["transaction_id", "txid", "txn", "transactionid", "id"]);
        if (idx >= 0) f.idCol = idx;
      }
      if (f.amountCol < 0) {
        const idx = guessColumn(header, ["amount", "sum", "total", "value"]);
        if (idx >= 0) f.amountCol = idx;
      }
      if (f.statusCol < 0) {
        const idx = guessColumn(header, ["status", "state"]);
        if (idx >= 0) f.statusCol = idx;
      }

      renderFiles();
      setProgress(`Заголовок прочитан: ${f.name}`, 0);
    } catch (e) {
      f.header = [];
      renderFiles();
      setProgress(`Не удалось прочитать заголовок: ${f.name}`, 0);
      resultsEl.innerHTML = `<div class="mono" style="color: var(--danger)">Ошибка чтения заголовка ${escapeHtml(f.name)}: ${escapeHtml(String(e))}</div>`;
    }
  }

  // Events
  $("#addFileBtn").addEventListener("click", () => addFile());
  $("#runBtn").addEventListener("click", () => run());

  filesEl.addEventListener("click", (ev) => {
    const btn = ev.target.closest("button[data-action]");
    if (!btn) return;
    const root = btn.closest("[data-file-id]");
    if (!root) return;
    const fileId = root.getAttribute("data-file-id");
    if (btn.getAttribute("data-action") === "remove-file") removeFile(fileId);
  });

  filesEl.addEventListener("change", async (ev) => {
    const root = ev.target.closest("[data-file-id]");
    if (!root) return;
    const fileId = root.getAttribute("data-file-id");
    const f = state.files.find((x) => x.id === fileId);
    if (!f) return;

    if (ev.target.name === "primary") {
      state.primaryId = fileId;
      renderFiles();
      return;
    }

    const field = ev.target.getAttribute("data-field");
    if (!field) return;

    if (field === "file") {
      f.file = ev.target.files && ev.target.files[0] ? ev.target.files[0] : null;
      if (f.file) await refreshHeaderForFile(fileId);
      renderFiles();
      return;
    }

    if (field === "encoding") {
      f.encoding = ev.target.value;
      if (f.file) await refreshHeaderForFile(fileId);
      return;
    }

    if (field === "decimalComma") {
      f.decimalComma = ev.target.value === "1";
      return;
    }

    if (field === "idCol" || field === "amountCol" || field === "statusCol") {
      f[field] = Number(ev.target.value);
      return;
    }
  });

  filesEl.addEventListener("input", async (ev) => {
    const root = ev.target.closest("[data-file-id]");
    if (!root) return;
    const fileId = root.getAttribute("data-file-id");
    const f = state.files.find((x) => x.id === fileId);
    if (!f) return;
    const field = ev.target.getAttribute("data-field");
    if (!field) return;

    if (field === "name") {
      f.name = ev.target.value.trim() || f.name;
      return;
    }
    if (field === "delimiter") {
      const d = ev.target.value;
      f.delimiter = d ? d[0] : "";
      if (f.file && f.delimiter.length === 1) await refreshHeaderForFile(fileId);
      return;
    }
    if (field === "keepCols") {
      f.keepColsText = ev.target.value;
      return;
    }
  });

  // init
  addFile({ name: "primary" });
  addFile({ name: "other" });
})();

