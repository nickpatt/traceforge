// TraceForge UI. Reads the query API (same origin) and draws services, dependencies,
// trace lists and a waterfall. No framework, no build step.
(() => {
  const $ = (s) => document.querySelector(s);
  const el = (tag, props = {}, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(props)) {
      if (k === 'style') n.style.cssText = v;
      else if (k.startsWith('aria-') || k === 'role' || k.startsWith('data-')) n.setAttribute(k, v);
      else n[k] = v;
    }
    n.append(...kids.filter((k) => k != null));
    return n;
  };
  const SVG = 'http://www.w3.org/2000/svg';
  const svg = (tag, attrs = {}, ...kids) => {
    const n = document.createElementNS(SVG, tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
    n.append(...kids);
    return n;
  };

  // Service colours: assigned in order of first sight, so a service keeps its colour everywhere.
  const palette = ['#2563eb', '#0d9488', '#d97706', '#db2777', '#16a34a', '#0891b2', '#b45309', '#64748b'];
  const colours = new Map();
  const colourOf = (svc) => {
    if (!colours.has(svc)) colours.set(svc, palette[colours.size % palette.length]);
    return colours.get(svc);
  };

  const fmtMs = (ms) => ms === 0 ? '0' : ms >= 1000 ? (ms / 1000).toFixed(ms >= 10000 ? 0 : 2) + ' s'
    : ms >= 10 ? ms.toFixed(0) + ' ms' : ms >= 1 ? ms.toFixed(1) + ' ms' : (ms * 1000).toFixed(0) + ' µs';
  const fmtN = (n) => new Intl.NumberFormat('en-US').format(n);
  const api = async (path) => {
    const r = await fetch('api/' + path);
    if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
    return r.json();
  };

  const state = { window: '15m', mode: 'recent', trace: null };

  // ---- Services ----------------------------------------------------------------------
  async function loadServices() {
    const body = $('#svc-body');
    let rows;
    try { rows = (await api(`services?since=${state.window}`)) || []; } catch (e) { body.replaceChildren(el('tr', {}, el('td', { colSpan: 7, className: 'empty', textContent: `Could not load services: ${e.message}` }))); return; }
    if (!rows.length) { body.replaceChildren(el('tr', {}, el('td', { colSpan: 7, className: 'empty', textContent: 'No spans in this window yet. Run `make loadgen` to send some.' }))); $('#svc-scale').textContent = ''; return; }
    rows.forEach((r) => colourOf(r.service));
    // Shared log scale across services so the bars compare.
    const lo = Math.max(0.01, Math.min(...rows.map((r) => r.p50_ms || 0.01)));
    const hi = Math.max(lo * 10, ...rows.map((r) => r.p99_ms));
    const pos = (v) => (Math.log10(Math.max(v, lo)) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo)) * 100;
    body.replaceChildren(...rows.map((r) => {
      const errPct = r.spans ? (r.errors / r.spans) * 100 : 0;
      return el('tr', { style: `--c:${colourOf(r.service)}` },
        el('th', { scope: 'row' }, el('span', { className: 'svc' }, el('i', { className: 'dot', 'aria-hidden': 'true' }), r.service)),
        el('td', { className: 'num', textContent: fmtN(r.spans) }),
        el('td', { className: 'num' + (r.errors ? ' err-rate' : ''), textContent: r.errors ? `${errPct.toFixed(1)}%` : '0' }),
        el('td', { className: 'num', textContent: fmtMs(r.p50_ms) }),
        el('td', { className: 'num', textContent: fmtMs(r.p95_ms) }),
        el('td', { className: 'num', textContent: fmtMs(r.p99_ms) }),
        el('td', { 'aria-hidden': 'true' }, el('div', { className: 'range' },
          el('i', { style: `left:${pos(r.p50_ms)}%;width:${Math.max(1, pos(r.p99_ms) - pos(r.p50_ms))}%` }),
          el('b', { style: `left:${pos(r.p95_ms)}%` }))));
    }));
    $('#svc-scale').replaceChildren(el('span', { textContent: fmtMs(lo) }), el('span', { textContent: fmtMs(hi) }));
  }

  // ---- Dependencies --------------------------------------------------------------------
  async function loadDeps() {
    const box = $('#graph'), list = $('#graph-list');
    let edges;
    try { edges = (await api(`dependencies?since=${state.window}`)) || []; } catch (e) { box.textContent = `Could not load dependencies: ${e.message}`; return; }
    edges = edges.filter((e) => e.parent !== e.child);
    if (!edges.length) { box.replaceChildren(el('p', { className: 'empty', textContent: 'No cross-service calls in this window.' })); list.replaceChildren(); return; }
    // Layered layout: depth = longest path from a service nobody calls.
    const nodes = [...new Set(edges.flatMap((e) => [e.parent, e.child]))];
    const depth = new Map(nodes.map((n) => [n, 0]));
    for (let i = 0; i < nodes.length; i++) for (const e of edges) depth.set(e.child, Math.max(depth.get(e.child), Math.min(depth.get(e.parent) + 1, nodes.length)));
    const cols = new Map();
    nodes.sort((a, b) => a.localeCompare(b)).forEach((n) => { const d = depth.get(n); cols.set(d, [...(cols.get(d) || []), n]); });
    const W = 120, H = 30, GX = 70, GY = 22, pad = 8;
    const ncol = Math.max(...cols.keys()) + 1, nrow = Math.max(...[...cols.values()].map((c) => c.length));
    const width = pad * 2 + ncol * W + (ncol - 1) * GX, height = pad * 2 + nrow * H + (nrow - 1) * GY;
    const at = new Map();
    for (const [d, col] of cols) col.forEach((n, i) => {
      const off = (nrow - col.length) * (H + GY) / 2;
      at.set(n, { x: pad + d * (W + GX), y: pad + off + i * (H + GY) });
    });
    const g = svg('svg', { viewBox: `0 0 ${width} ${height}`, width, 'aria-hidden': 'true' });
    for (const e of edges) {
      const a = at.get(e.parent), b = at.get(e.child);
      const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x, y2 = b.y + H / 2, mx = (x1 + x2) / 2;
      const back = x2 <= x1;
      const d = back ? `M${x1} ${y1} C${x1 + 40} ${y1 - 40}, ${x2 - 40} ${y2 - 40}, ${x2} ${y2}` : `M${x1} ${y1} C${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`;
      const label = fmtN(e.calls);
      const lw = label.length * 6.8 + 8;
      g.append(svg('g', { class: 'edge' + (e.errors ? ' has-err' : '') },
        svg('path', { d, 'stroke-width': Math.min(4, 1 + Math.log10(e.calls + 1) / 1.5) }),
        svg('rect', { class: 'lbl-bg', x: mx - lw / 2, y: (y1 + y2) / 2 - 8, width: lw, height: 16, rx: 3 }),
        svg('text', { x: mx, y: (y1 + y2) / 2 + 4, 'text-anchor': 'middle' }, label)));
    }
    for (const n of nodes) {
      const p = at.get(n);
      g.append(svg('g', { class: 'node' },
        svg('rect', { x: p.x, y: p.y, width: W, height: H, rx: 7 }),
        svg('rect', { class: 'swatch', x: p.x + 9, y: p.y + H / 2 - 4.5, width: 9, height: 9, rx: 3, style: `fill:${colourOf(n)}` }),
        svg('text', { x: p.x + 25, y: p.y + H / 2 + 4 }, n)));
    }
    box.replaceChildren(g);
    list.replaceChildren(...edges.map((e) => el('li', { textContent: `${e.parent} calls ${e.child}: ${fmtN(e.calls)} calls, ${fmtN(e.errors)} errors` })));
  }

  // ---- Trace list ----------------------------------------------------------------------
  async function loadTraces() {
    const ol = $('#trace-list');
    const q = state.mode === 'slowest' ? '&sort=slowest' : state.mode === 'errors' ? '&errors=1' : '';
    let ts;
    try { ts = (await api(`traces?since=${state.window}&limit=40${q}`)) || []; } catch (e) { ol.replaceChildren(el('li', { className: 'empty', textContent: `Could not load traces: ${e.message}` })); return; }
    if (!ts.length) { ol.replaceChildren(el('li', { className: 'empty', textContent: state.mode === 'errors' ? 'No failed traces in this window.' : 'No traces in this window yet.' })); return; }
    ol.replaceChildren(...ts.map((t) => {
      const when = new Date(t.start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      const b = el('button', { type: 'button', 'aria-current': String(t.trace_id === state.trace), style: `--c:${colourOf(t.service)}` },
        el('span', { className: 't-name' }, el('i', { className: 'dot', 'aria-hidden': 'true' }), el('span', { textContent: `${t.service} · ${t.name}` }), t.status === 'ERROR' ? el('span', { className: 'badge', textContent: 'error' }) : null),
        el('span', { className: 't-dur', textContent: fmtMs(t.duration_ms) }),
        el('span', { className: 't-sub', textContent: `${when} · ${t.trace_id}` }));
      b.addEventListener('click', () => openTrace(t.trace_id));
      return el('li', {}, b);
    }));
  }

  // ---- Waterfall -----------------------------------------------------------------------
  async function openTrace(id) {
    state.trace = id;
    history.replaceState(null, '', '#trace=' + id);
    document.querySelectorAll('#trace-list button').forEach((b) => b.setAttribute('aria-current', String(b.querySelector('.t-sub').textContent.endsWith(id))));
    const wf = $('#wf'), meta = $('#wf-meta'), detail = $('#detail');
    let t;
    try { t = await api('traces/' + encodeURIComponent(id)); } catch (e) { meta.textContent = `Could not load trace: ${e.message}`; wf.replaceChildren(); return; }
    const flat = [];
    const walk = (n, d) => { flat.push({ n, d }); (n.children || []).forEach((c) => walk(c, d + 1)); };
    (t.roots || []).forEach((r) => walk(r, 0));
    (t.orphans || []).forEach((r) => walk(r, 0));
    const total = t.total_ms || Math.max(...flat.map(({ n }) => n.offset_ms + n.duration_ms), 1);
    const errs = flat.filter(({ n }) => n.status === 'ERROR').length;
    meta.textContent = `${t.span_count} spans · ${fmtMs(total)}${errs ? ` · ${errs} failed` : ''} · ${t.trace_id}`;
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => el('span', { style: `left:${f * 100}%`, textContent: fmtMs(total * f) }));
    const rows = flat.map(({ n, d }) => {
      const left = (n.offset_ms / total) * 100, width = Math.max(0.2, (n.duration_ms / total) * 100);
      const durRight = left + width > 82;
      const btn = el('button', { type: 'button', className: 'wf-row' + (n.status === 'ERROR' ? ' is-err' : ''), 'aria-pressed': 'false', style: `--c:${colourOf(n.service)};--depth:${d}`,
        'aria-label': `${n.service} ${n.name}, starts at ${fmtMs(n.offset_ms)}, takes ${fmtMs(n.duration_ms)}${n.status === 'ERROR' ? ', failed' : ''}` },
        el('span', { className: 'wf-label' }, el('i', { className: 'dot', 'aria-hidden': 'true' }), el('span', { className: 'svc-name', textContent: n.service }), el('span', { className: 'span-name', textContent: n.name })),
        el('span', { className: 'wf-track', 'aria-hidden': 'true' },
          el('span', { className: 'wf-bar', style: `left:${left}%;width:${width}%` }),
          el('span', { className: 'wf-dur', style: durRight ? `right:${100 - left}%` : `left:calc(${left + width}% + 2px)`, textContent: fmtMs(n.duration_ms) })));
      btn.addEventListener('click', () => {
        wf.querySelectorAll('.wf-row').forEach((r) => r.setAttribute('aria-pressed', String(r === btn)));
        detail.replaceChildren(el('dl', {}, ...[['Service', n.service], ['Span', n.name], ['Kind', n.kind], ['Status', n.status], ['Starts at', fmtMs(n.offset_ms)], ['Duration', fmtMs(n.duration_ms)], ['Span ID', n.span_id]]
          .map(([k, v]) => el('div', {}, el('dt', { textContent: k }), el('dd', { textContent: v })))));
      });
      return el('li', {}, btn);
    });
    wf.classList.remove('is-in'); void wf.offsetWidth; wf.classList.add('is-in');
    wf.replaceChildren(el('div', { className: 'wf-axis', 'aria-hidden': 'true' }, el('span', { textContent: 'Span' }), el('span', { className: 'ticks' }, ...ticks)), el('ol', { className: 'wf-rows', 'aria-label': 'Spans in this trace' }, ...rows));
    detail.replaceChildren();
  }

  // ---- Wiring --------------------------------------------------------------------------
  async function refresh() {
    await loadServices();
    await Promise.all([loadDeps(), loadTraces()]);
    $('#stamp').textContent = 'Updated ' + new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    if (!state.trace) {
      const first = document.querySelector('#trace-list button');
      const fromHash = location.hash.match(/trace=([0-9a-f]+)/)?.[1];
      if (fromHash) openTrace(fromHash); else if (first) first.click();
    }
  }
  $('#window').addEventListener('change', (e) => { state.window = e.target.value; refresh(); });
  $('#refresh').addEventListener('click', refresh);
  document.querySelectorAll('.tabs [role=tab]').forEach((tab) => tab.addEventListener('click', () => {
    state.mode = tab.dataset.mode;
    document.querySelectorAll('.tabs [role=tab]').forEach((t) => t.setAttribute('aria-selected', String(t === tab)));
    loadTraces();
  }));
  refresh();
})();
