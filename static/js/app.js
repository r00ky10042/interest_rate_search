/* ── State ──────────────────────────────── */
let _sort = { col: "monthly_12m", dir: "desc" };
let _scrapeTimer = null;

/* ── Elements ───────────────────────────── */
const searchInput  = document.getElementById("searchInput");
const regionSelect = document.getElementById("regionSelect");
const onlyMonthly  = document.getElementById("onlyMonthly");
const tbody        = document.getElementById("tbody");
const countBadge   = document.getElementById("countBadge");
const scrapeBtn    = document.getElementById("scrapeBtn");
const progressWrap = document.getElementById("progressWrap");
const progressFill = document.getElementById("progressFill");
const progressText = document.getElementById("progressText");
const statsBar     = document.getElementById("statsBar");
const visitBar     = document.getElementById("visitBar");

/* ── Fetch & Render ─────────────────────── */
function loadRates() {
  const params = new URLSearchParams({
    q:            searchInput.value.trim(),
    r1:           regionSelect.value,
    only_monthly: onlyMonthly.checked,
    sort:         _sort.col,
    dir:          _sort.dir,
  });

  fetch("/api/rates?" + params)
    .then(r => r.json())
    .then(({ data, total }) => {
      renderTable(data);
      countBadge.textContent = total.toLocaleString() + "건";
    });
}

function rateVal(s) {
  if (!s) return -1;
  const m = s.match(/[\d.]+/);
  return m ? parseFloat(m[0]) : -1;
}

function rateClass(s, maxRate) {
  const v = rateVal(s);
  if (v < 0) return "rate-none";
  if (v >= maxRate - 0.05) return "rate-high";
  if (v >= maxRate - 0.5)  return "rate-mid";
  return "rate-low";
}

function renderTable(rows) {
  const maxRate = Math.max(...rows.map(r => rateVal(r.monthly_12m)));

  tbody.innerHTML = rows.map((r, i) => {
    const isTop = rateVal(r.monthly_12m) >= maxRate - 0.05 && maxRate > 0;
    const rc    = rateClass(r.monthly_12m, maxRate);
    const mc    = rateClass(r.maturity_12m, maxRate);
    return `<tr class="${isTop ? "highlight" : ""}">
      <td>${r.r1 || ""}</td>
      <td>${r.r2 || ""}</td>
      <td style="text-align:left">${r.name || ""}</td>
      <td>${r.div_nm || ""}</td>
      <td class="tl" title="${r.addr || ""}">${r.addr || ""}</td>
      <td class="${rc}">${r.monthly_12m || "-"}</td>
      <td class="${mc}">${r.maturity_12m || "-"}</td>
    </tr>`;
  }).join("");
}

/* ── Stats ──────────────────────────────── */
function loadStats() {
  fetch("/api/stats")
    .then(r => r.json())
    .then(s => {
      const updated = s.last_updated
        ? `마지막 수집: ${s.last_updated}`
        : "아직 수집된 데이터가 없습니다.";
      statsBar.textContent =
        `총 ${(s.total || 0).toLocaleString()}개 금고 | ` +
        `월지급식 제공: ${(s.monthly_count || 0).toLocaleString()}개 | ${updated}`;
    });
}

/* ── Sort ───────────────────────────────── */
document.querySelectorAll("th.sortable").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (_sort.col === col) {
      _sort.dir = _sort.dir === "desc" ? "asc" : "desc";
    } else {
      _sort.col = col;
      _sort.dir = col.includes("12m") ? "desc" : "asc";
    }
    document.querySelectorAll("th").forEach(h => {
      h.classList.remove("sorted-asc", "sorted-desc");
    });
    th.classList.add(_sort.dir === "asc" ? "sorted-asc" : "sorted-desc");
    loadRates();
  });
});

// 기본 정렬 표시
document.querySelector(`th[data-col="monthly_12m"]`).classList.add("sorted-desc");

/* ── Filters ────────────────────────────── */
let _debounce;
searchInput.addEventListener("input", () => {
  clearTimeout(_debounce);
  _debounce = setTimeout(loadRates, 300);
});
regionSelect.addEventListener("change", loadRates);
onlyMonthly.addEventListener("change", loadRates);

/* ── Password Modal ─────────────────────── */
const pwModal      = document.getElementById("pwModal");
const pwInput      = document.getElementById("pwInput");
const pwError      = document.getElementById("pwError");
const pwCancelBtn  = document.getElementById("pwCancelBtn");
const pwConfirmBtn = document.getElementById("pwConfirmBtn");

function openPwModal() {
  pwModal.classList.remove("hidden");
  pwInput.value = "";
  pwError.classList.add("hidden");
  setTimeout(() => pwInput.focus(), 50);
}

function closePwModal() {
  pwModal.classList.add("hidden");
}

pwCancelBtn.addEventListener("click", closePwModal);
pwModal.addEventListener("click", e => { if (e.target === pwModal) closePwModal(); });
pwInput.addEventListener("keydown", e => { if (e.key === "Enter") pwConfirmBtn.click(); });

pwConfirmBtn.addEventListener("click", () => {
  const pw = pwInput.value;
  if (!pw) return;
  pwConfirmBtn.disabled = true;
  pwConfirmBtn.textContent = "확인 중...";
  fetch("/api/scrape", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ password: pw })
  })
    .then(r => r.json())
    .then(res => {
      pwConfirmBtn.disabled = false;
      pwConfirmBtn.textContent = "수집 시작";
      if (res.error) {
        pwError.textContent = res.error;
        pwError.classList.remove("hidden");
        return;
      }
      closePwModal();
      scrapeBtn.disabled = true;
      scrapeBtn.innerHTML = '<span class="spinner"></span>수집 중...';
      progressWrap.classList.remove("hidden");
      _scrapeTimer = setInterval(pollScrape, 1000);
    });
});

/* ── Scrape ─────────────────────────────── */
scrapeBtn.addEventListener("click", () => {
  if (scrapeBtn.disabled) return;
  openPwModal();
});

function pollScrape() {
  fetch("/api/scrape/status")
    .then(r => r.json())
    .then(s => {
      const pct = s.total > 0 ? Math.round(s.done / s.total * 100) : 0;
      progressFill.style.width = pct + "%";
      progressText.textContent =
        s.total > 0
          ? `${s.done.toLocaleString()} / ${s.total.toLocaleString()} (${pct}%)`
          : (s.log.slice(-1)[0] || "준비 중...");

      if (!s.running) {
        clearInterval(_scrapeTimer);
        scrapeBtn.disabled = false;
        scrapeBtn.textContent = "🔄 금리 재수집";
        progressWrap.classList.add("hidden");
        progressFill.style.width = "0%";
        loadRates();
        loadStats();
      }
    });
}

/* ── Visits ─────────────────────────────── */
function loadVisits() {
  fetch("/api/visits")
    .then(r => r.json())
    .then(v => {
      visitBar.textContent = `오늘 방문자: ${v.today.toLocaleString()}명 | 누적 방문자: ${v.total.toLocaleString()}명`;
    });
}

/* ── Init ───────────────────────────────── */
loadRates();
loadStats();
loadVisits();
