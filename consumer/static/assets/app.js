/* ── State ────────────────────────────────────────────────────────────────── */
const PAGE_SIZE = 50;
let currentOffset = 0;
let currentTotal  = 0;
let currentFilter = { action: '', from: '', to: '' };
let ws = null;

/* ── Init ─────────────────────────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  loadStats();
  loadLogs();
  connectWS();

  document.getElementById('btn-apply').addEventListener('click', () => {
    currentFilter = {
      action: document.getElementById('filter-action').value,
      from:   document.getElementById('filter-from').value,
      to:     document.getElementById('filter-to').value,
    };
    currentOffset = 0;
    loadLogs();
    loadStats();
  });

  document.getElementById('btn-clear').addEventListener('click', () => {
    currentFilter = { action: '', from: '', to: '' };
    currentOffset = 0;
    document.getElementById('filter-action').value = '';
    document.getElementById('filter-from').value   = '';
    document.getElementById('filter-to').value     = '';
    loadLogs();
    loadStats();
  });

  document.getElementById('btn-prev').addEventListener('click', () => {
    if (currentOffset > 0) {
      currentOffset = Math.max(0, currentOffset - PAGE_SIZE);
      loadLogs();
    }
  });

  document.getElementById('btn-next').addEventListener('click', () => {
    if (currentOffset + PAGE_SIZE < currentTotal) {
      currentOffset += PAGE_SIZE;
      loadLogs();
    }
  });
});

/* ── WebSocket ────────────────────────────────────────────────────────────── */
function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);

  ws.onopen = () => {
    setLiveStatus(true);
  };

  ws.onclose = () => {
    setLiveStatus(false);
    setTimeout(connectWS, 3000);
  };

  ws.onerror = () => {
    setLiveStatus(false);
  };

  ws.onmessage = (e) => {
    try {
      const event = JSON.parse(e.data);
      // Only prepend row when no active filter (or action matches)
      const filterActive = currentFilter.action || currentFilter.from || currentFilter.to;
      if (!filterActive || currentFilter.action === '' || currentFilter.action === event.action) {
        if (currentOffset === 0) {
          prependRow(event);
        }
      }
      updateStatsBump(event.action);
      showToast(event);
    } catch (_) {}
  };
}

function setLiveStatus(connected) {
  const badge = document.getElementById('live-badge');
  const text  = document.getElementById('live-text');
  if (connected) {
    badge.className = 'live-badge';
    text.textContent = 'LIVE';
  } else {
    badge.className = 'live-badge disconnected';
    text.textContent = 'OFFLINE';
  }
}

/* ── API ──────────────────────────────────────────────────────────────────── */
async function loadStats() {
  try {
    const res = await fetch('/api/stats');
    if (res.status === 401) { window.location.href = '/login.html'; return; }
    if (!res.ok) return;
    const stats = await res.json();
    animateStat('stat-total',    stats.total    ?? 0);
    animateStat('stat-inserts',  stats.inserts  ?? 0);
    animateStat('stat-updates',  stats.updates  ?? 0);
    animateStat('stat-deletes',  stats.deletes  ?? 0);
  } catch (_) {}
}

async function loadLogs() {
  const tbody = document.getElementById('logs-tbody');
  tbody.innerHTML = `<tr><td colspan="6" class="empty-state"><div class="empty-state-icon">⏳</div>Loading...</td></tr>`;

  const params = buildParams();
  try {
    const res = await fetch(`/api/audit-logs?${params}`);
    if (res.status === 401) { window.location.href = '/login.html'; return; }
    if (!res.ok) throw new Error('Network error');
    const data = await res.json();
    currentTotal = data.total ?? 0;
    renderTable(data.data ?? []);
    renderPagination();
  } catch (err) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty-state"><div class="empty-state-icon">⚠️</div>Failed to load logs</td></tr>`;
  }
}

function buildParams() {
  const p = new URLSearchParams({ limit: PAGE_SIZE, offset: currentOffset });
  if (currentFilter.action) p.set('action', currentFilter.action);
  if (currentFilter.from)   p.set('from',   currentFilter.from);
  if (currentFilter.to)     p.set('to',     currentFilter.to);
  return p.toString();
}

/* ── Render ───────────────────────────────────────────────────────────────── */
function renderTable(logs) {
  const tbody = document.getElementById('logs-tbody');
  tbody.innerHTML = '';

  if (!logs.length) {
    tbody.innerHTML = `
      <tr><td colspan="6" class="empty-state">
        <div class="empty-state-icon">📭</div>
        No events found. Try adjusting your filters.
      </td></tr>`;
    return;
  }

  logs.forEach(log => tbody.appendChild(createRow(log)));
}

function createRow(log, isNew = false) {
  const tr = document.createElement('tr');
  if (isNew) tr.classList.add('row-new');

  const time = formatTime(log.event_time);
  const table = log.table_name ? log.table_name.split('.').pop() : '—';
  const recordId = log.record_id || '—';

  tr.innerHTML = `
    <td class="td-id">#${log.id}</td>
    <td class="td-time" title="${log.event_time}">${time}</td>
    <td class="td-table" title="${log.table_name}">${escHtml(table)}</td>
    <td>${actionBadge(log.action)}</td>
    <td class="td-record">${escHtml(recordId)}</td>
    <td><button class="btn-detail" onclick="openDetailModal(${log.id})">↗ Detail</button></td>
  `;
  return tr;
}

function prependRow(log) {
  const tbody = document.getElementById('logs-tbody');
  // Remove the empty-state row if present
  const empty = tbody.querySelector('.empty-state');
  if (empty) tbody.innerHTML = '';

  // Remove oldest row if table is too long (keep 50 rows max in live view)
  while (tbody.children.length >= PAGE_SIZE) {
    tbody.removeChild(tbody.lastChild);
  }

  tbody.prepend(createRow(log, true));
}

function renderPagination() {
  const from = currentTotal === 0 ? 0 : currentOffset + 1;
  const to   = Math.min(currentOffset + PAGE_SIZE, currentTotal);

  document.getElementById('pagination-info').textContent =
    currentTotal === 0 ? 'No events' : `${from}–${to} of ${currentTotal.toLocaleString()} events`;

  document.getElementById('btn-prev').disabled = currentOffset === 0;
  document.getElementById('btn-next').disabled = currentOffset + PAGE_SIZE >= currentTotal;
}

/* ── Stats animation ──────────────────────────────────────────────────────── */
function animateStat(id, target) {
  const el = document.getElementById(id);
  if (!el) return;
  const current = parseInt(el.textContent.replace(/,/g, ''), 10) || 0;
  if (current === target) return;

  const duration = 600;
  const start = performance.now();
  function tick(now) {
    const progress = Math.min((now - start) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    el.textContent = Math.round(current + (target - current) * eased).toLocaleString();
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

function updateStatsBump(action) {
  const map = {
    INSERT:   ['stat-total', 'stat-inserts'],
    UPDATE:   ['stat-total', 'stat-updates'],
    DELETE:   ['stat-total', 'stat-deletes'],
    SNAPSHOT: ['stat-total'],
  };
  const ids = map[action] || ['stat-total'];
  ids.forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    const val = (parseInt(el.textContent.replace(/,/g, ''), 10) || 0) + 1;
    el.textContent = val.toLocaleString();
    el.classList.remove('bump');
    void el.offsetWidth; // reflow
    el.classList.add('bump');
  });
}

/* ── Toast ────────────────────────────────────────────────────────────────── */
function showToast(event) {
  const icons = { INSERT: '🟢', UPDATE: '🟡', DELETE: '🔴', SNAPSHOT: '🔵' };
  const icon = icons[event.action] || '⚪';
  const table = event.table_name ? event.table_name.split('.').pop() : 'unknown';
  const id    = event.record_id  ? ` · ID ${event.record_id}` : '';

  const toast = document.createElement('div');
  toast.className = 'toast';
  toast.innerHTML = `
    <span class="toast-icon">${icon}</span>
    <div class="toast-body">
      <div class="toast-title">${event.action} on ${escHtml(table)}${escHtml(id)}</div>
      <div class="toast-desc">${formatTime(event.event_time)}</div>
    </div>
  `;
  const container = document.getElementById('toast-container');
  container.appendChild(toast);
  setTimeout(() => {
    toast.classList.add('removing');
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

/* ── Helpers ──────────────────────────────────────────────────────────────── */
function actionBadge(action) {
  const a = (action || '').toUpperCase();
  return `<span class="action-badge badge-${a}"><span class="action-badge-dot"></span>${a}</span>`;
}

function formatTime(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString('id-ID', {
      day: '2-digit', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false,
    });
  } catch (_) { return iso; }
}

function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/* ── Modal Detail ────────────────────────────────────────────────────────── */
async function openDetailModal(id) {
  const modal = document.getElementById('detail-modal');
  modal.classList.add('visible');
  document.getElementById('modal-title').innerHTML = `Log Detail #${id} <span id="modal-badge-text" class="modal-title-badge">...</span>`;
  
  const tbodyBefore = document.getElementById('diff-before-body');
  const tbodyAfter = document.getElementById('diff-after-body');
  
  tbodyBefore.innerHTML = `<tr><td colspan="2" class="pl-md py-[2px] null-state">Loading...</td></tr>`;
  tbodyAfter.innerHTML = `<tr><td colspan="2" class="pl-md py-[2px] null-state">Loading...</td></tr>`;
  
  try {
    const res = await fetch(`/api/audit-logs/${id}`);
    if (res.status === 401) { window.location.href = '/login.html'; return; }
    if (!res.ok) throw new Error('Failed to load');
    
    const log = await res.json();
    renderModalDiff(log);
  } catch (e) {
    tbodyBefore.innerHTML = `<tr><td colspan="2" class="pl-md py-[2px] text-error">Error loading data</td></tr>`;
    tbodyAfter.innerHTML = `<tr><td colspan="2" class="pl-md py-[2px] text-error">Error loading data</td></tr>`;
  }
}

function closeDetailModal() {
  document.getElementById('detail-modal').classList.remove('visible');
}

// Close on escape or outside click
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closeDetailModal();
});
document.getElementById('detail-modal')?.addEventListener('click', (e) => {
  if (e.target === document.getElementById('detail-modal')) closeDetailModal();
});

function renderModalDiff(log) {
  const dot = document.getElementById('modal-badge-dot');
  const badge = document.getElementById('modal-badge-text');
  
  const a = (log.action || '').toUpperCase();
  badge.textContent = a;
  
  if (a === 'INSERT') {
    dot.style.background = 'var(--clr-insert)';
    badge.style.color = 'var(--clr-insert)';
    badge.style.borderColor = 'var(--clr-insert)';
  } else if (a === 'UPDATE') {
    dot.style.background = 'var(--clr-update)';
    badge.style.color = 'var(--clr-update)';
    badge.style.borderColor = 'var(--clr-update)';
  } else if (a === 'DELETE') {
    dot.style.background = 'var(--clr-delete)';
    badge.style.color = 'var(--clr-delete)';
    badge.style.borderColor = 'var(--clr-delete)';
  } else {
    dot.style.background = 'var(--clr-primary)';
    badge.style.color = 'var(--clr-primary)';
    badge.style.borderColor = 'var(--clr-primary)';
  }

  const beforeStr = log.before_data ? JSON.stringify(log.before_data, null, 2) : 'null';
  const afterStr = log.after_data ? JSON.stringify(log.after_data, null, 2) : 'null';
  
  const beforeLines = beforeStr.split('\n');
  const afterLines = afterStr.split('\n');
  
  // Simple line-by-line diff
  let htmlBefore = '';
  let htmlAfter = '';
  
  const maxLines = Math.max(beforeLines.length, afterLines.length);
  
  let adds = 0;
  let rems = 0;

  for (let i = 0; i < maxLines; i++) {
    const b = beforeLines[i] !== undefined ? beforeLines[i] : null;
    const aLine = afterLines[i] !== undefined ? afterLines[i] : null;
    
    let bClass = '';
    let aClass = '';
    
    if (b !== null && aLine !== null && b !== aLine) {
      bClass = 'diff-removed';
      aClass = 'diff-added';
      rems++; adds++;
    } else if (b !== null && aLine === null) {
      bClass = 'diff-removed';
      rems++;
    } else if (b === null && aLine !== null) {
      aClass = 'diff-added';
      adds++;
    }
    
    if (b !== null) {
      htmlBefore += `<tr class="${bClass}"><td class="gutter-number">${i+1}</td><td class="pl-md py-[2px]">${highlightJson(b)}</td></tr>`;
    }
    if (aLine !== null) {
      htmlAfter += `<tr class="${aClass}"><td class="gutter-number">${i+1}</td><td class="pl-md py-[2px]">${highlightJson(aLine)}</td></tr>`;
    }
  }
  
  document.getElementById('diff-before-body').innerHTML = htmlBefore || `<tr><td colspan="2" class="pl-md py-[2px] null-state">No Data</td></tr>`;
  document.getElementById('diff-after-body').innerHTML = htmlAfter || `<tr><td colspan="2" class="pl-md py-[2px] null-state">No Data</td></tr>`;

  document.getElementById('diff-stats').innerHTML = `
    <div class="diff-stat-item"><span class="diff-stat-dot rem"></span> ${rems} Removals</div>
    <div class="diff-stat-item"><span class="diff-stat-dot add"></span> ${adds} Additions</div>
  `;
}

function highlightJson(line) {
  if (line === 'null') return '<span class="json-null">null</span>';
  return line
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
      let cls = 'json-number';
      if (/^"/.test(match)) {
        if (/:$/.test(match)) {
          cls = 'json-key';
        } else {
          cls = 'json-string';
        }
      } else if (/true|false/.test(match)) {
        cls = 'json-boolean';
      } else if (/null/.test(match)) {
        cls = 'json-null';
      }
      return '<span class="' + cls + '">' + match + '</span>';
    });
}
