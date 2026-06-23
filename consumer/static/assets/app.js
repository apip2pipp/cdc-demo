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
    <td><a href="/detail.html?id=${log.id}" class="btn-detail">↗ Detail</a></td>
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
