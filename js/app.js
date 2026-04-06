// ===== LOGIC =====

let DATA, META, DATES; // populated async by fetch('data.json') in DOMContentLoaded
let itemMode='customer',trendChart=null,diffChart=null,filtered=[];
let excelUnit='unit',excelPage=1;
const PER_PAGE=50;

// ===== INIT =====
function buildIndexes(){
  // Lightweight pass — ensures DATA is accessible
  console.log('[init] buildIndexes: '+DATA.length+' rows, '+DATES.length+' dates');
}
function buildAggregations(){
  // Aggregations are computed on-demand per update function
  console.log('[init] buildAggregations: ready');
}
function renderDashboard(){
  // ── Guard: ensure DATA is populated from fresh fetch ──────────────
  if (!DATA || !DATA.length || !DATES || !DATES.length) {
    console.error('[Render] ❌ DATA or DATES is empty — cannot render');
    return;
  }

  const di=getDateIdxs(),idxs=di.map(x=>x.i),labels=di.map(x=>x.d);
  const labelsShort=labels.map(l=>l.slice(5));
  const d=filtered.length?filtered:DATA;
  _lastFilteredRows=d; _lastIdxs=idxs; _lastLabels=labels;

  // KPI/Charts/Ranking use FG Summary rows only (Layer2 FG, Unit column)
  const kpiData = getKPIData();

  // ── Data verification: prove render uses fresh data ────────────────
  const fingerprint = DATA.length + '|' + DATES.length + '|' + DATES[DATES.length-1];
  const totalUnit = kpiData.reduce((s,r) => s + (Array.isArray(r[5]) ? r[5].reduce((a,v) => a+(v||0), 0) : 0), 0);
  console.log('[Render] ✅ RENDER DATA:', {
    source: 'DATA (from fetch)',
    fingerprint: fingerprint,
    generatedAt: META?.generatedAt || 'N/A',
    totalRows: DATA.length,
    filteredRows: d.length,
    kpiRows: kpiData.length,
    dateRange: DATES[0] + ' → ' + DATES[DATES.length-1],
    dateIdxCount: idxs.length,
    totalUnit: totalUnit
  });

  // ── Single-pass KPI aggregation: ONE source of truth ──────────────
  // Compute KPI totals once, reuse everywhere (Exec Summary, Stress, etc.)
  let _kpiTotOrd=0, _kpiTotAct=0, _kpiTotDiff=0, _kpiNeg=0;
  kpiData.forEach(r=>{
    if(skipTotal(r))return;
    const o=sumArr(r[5],idxs), a=sumArr(r[6],idxs);
    _kpiTotOrd+=o; _kpiTotAct+=a; _kpiTotDiff+=a-o;
    idxs.forEach(i=>{if(diffU(r,i)<0)_kpiNeg++;});
  });
  const _kpiAgg = { totOrd:_kpiTotOrd, totAct:_kpiTotAct, totDiff:_kpiTotDiff, neg:_kpiNeg };
  console.log('[Render] 🎯 SINGLE KPI AGG:', _kpiAgg);

  updateKPI(_kpiAgg);
  updateCharts(kpiData,idxs,labelsShort);
  updateRanking(kpiData,idxs);
  updateRootCause(kpiData,idxs);
  updateSmartNotes(kpiData,idxs);
  // Table uses full filtered data (shows all rows including detail)
  updateTable(d,idxs);
  // Executive Summary & Operational use SAME kpiData + precomputed agg
  updateExecSummary(kpiData,idxs,_kpiAgg);
  updateOperational(kpiData,idxs,_kpiAgg);
  // AI insights disabled — updateAIInsights() removed from core flow
}

// ═══════════════════════════════════════════════════════════════════════════
// ★ DATA LOADING — SINGLE SOURCE OF TRUTH (Fixed 2026-03-23)
// ═══════════════════════════════════════════════════════════════════════════
// Architecture: data.json → fetch → Dashboard
// NO fallback to data.js / RAW_EMBEDDED / hardcoded data
// If data.json fails → Dashboard shows error (no silent stale data)
// ═══════════════════════════════════════════════════════════════════════════

const DATA_PATH = 'data/data.json'

async function loadDashboardData() {
  const url = DATA_PATH + '?v=' + Date.now();
  console.log('[DataSource] 📡 Fetching:', url);

  // ★ cache: 'no-store' = FORCE fresh fetch, bypass ALL browser caches
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error('HTTP ' + res.status + ' — ไม่พบ data.json');

  const RAW = await res.json();

  // ── Validate structure ──────────────────────────────────────────────
  if (!RAW || typeof RAW !== 'object')
    throw new Error('data.json — ไม่ใช่ JSON object');
  if (!Array.isArray(RAW.r) || RAW.r.length === 0)
    throw new Error('data.json: key "r" หายหรือว่างเปล่า (row data)');
  if (!RAW.m || typeof RAW.m !== 'object')
    throw new Error('data.json: key "m" (metadata) หาย');
  if (!Array.isArray(RAW.m.d) || RAW.m.d.length === 0)
    throw new Error('data.json: key "m.d" (dates) หายหรือว่างเปล่า');

  // ── Data checksum: quick hash of row content to detect changes ──────
  const checksum = RAW.r.length + ':' + RAW.m.d.length + ':'
    + RAW.r.slice(0,3).map(r => (r[5]||[]).slice(0,5).join(',')).join('|');

  console.log('[DataSource] ✅ DATA SOURCE: data.json (fresh — cache:no-store)', {
    generatedAt: RAW.m.generatedAt,
    rows: RAW.r.length,
    dates: RAW.m.d.length,
    lastDate: RAW.m.d[RAW.m.d.length - 1],
    checksum: checksum
  });

  return RAW;
}

function initDashboard(RAW) {
  // ═══════════════════════════════════════════════════════════════════
  // ★ CRITICAL: Wire ALL globals from the FRESH fetch result
  //   Every variable below MUST reference RAW from loadDashboardData()
  //   NO legacy data, NO cached data, NO fallback
  // ═══════════════════════════════════════════════════════════════════

  // ── Step 1: Clear any stale state ─────────────────────────────────
  DATA = null; META = null; DATES = null; filtered = [];
  window.RAW = null; window.DATES = null;
  window.DEMAND = null; window.DELIVERED = null;

  // ── Step 2: Wire fresh data from fetch result ─────────────────────
  window.RAW = RAW;
  DATA  = RAW.r;
  META  = RAW.m;
  DATES = META.d;

  // ── Step 3: Verify wiring is correct ──────────────────────────────
  console.log('[Init] 📦 DATA wired from RAW:', {
    'DATA === RAW.r': DATA === RAW.r,
    'META === RAW.m': META === RAW.m,
    'DATES === META.d': DATES === META.d,
    rows: DATA.length,
    dates: DATES.length,
    generatedAt: META.generatedAt
  });

  // Expose full-length aggregated arrays (computed from fresh DATA)
  window.DATES     = DATES;
  window.DEMAND    = DATES.map((_,i) => DATA.reduce((s,r) => s + ((Array.isArray(r[5]) && r[5][i]) || 0), 0));
  window.DELIVERED = DATES.map((_,i) => DATA.reduce((s,r) => s + ((Array.isArray(r[6]) && r[6][i]) || 0), 0));

  // ── Debug: sample first non-zero data point to prove data is fresh ─
  let sampleDate = '', sampleVal = 0;
  for (let i = DATES.length - 1; i >= 0; i--) {
    if (window.DEMAND[i] > 0) { sampleDate = DATES[i]; sampleVal = window.DEMAND[i]; break; }
  }
  console.log('[Init] 📊 DEMAND sample: last non-zero =', sampleDate, '→', sampleVal);
  console.log('[Init] DATES:', DATES.length, '| range:', DATES[0], '→', DATES[DATES.length-1]);

  // Show last date on screen (debug element)
  const _dbg = document.getElementById('debugDate');
  if (_dbg) _dbg.innerText = DATES[DATES.length - 1];

  // Set date-range filter inputs to cover full dataset
  const _elDateTo = document.getElementById('fDateTo');
  if (_elDateTo && DATES.length) _elDateTo.value = DATES[DATES.length - 1];
  const _elDateFrom = document.getElementById('fDateFrom');
  if (_elDateFrom && DATES.length) _elDateFrom.value = DATES[0];

  // ── Step 4: Update header timestamp (ONLY generatedAt) ────────────
  updateDataTimestamp(RAW);

  // ── Step 5: Init chain — all use fresh DATA/DATES/META ─────────────
  buildIndexes();
  buildAggregations();
  initFilters();
  applyFilters();         // rebuilds filtered[] from fresh DATA
  updateRMIntelligence();
  renderDashboard();      // renders KPI + Exec + Operational from ONE source
  buildExcelTab();
  buildRMPlanWeekOptions();

  // ── Step 6: Start auto-refresh watcher ─────────────────────────────
  startDataWatcher();

  // Delayed Lucide re-init
  setTimeout(()=>{ if(typeof lucide!=='undefined') lucide.createIcons(); }, 300);
  setTimeout(()=>{ if(typeof lucide!=='undefined') lucide.createIcons(); }, 800);

  console.log('[Init] ✅ Dashboard initialized with FRESH data (generatedAt:', META.generatedAt, ')');
}

document.addEventListener('DOMContentLoaded', () => {
  loadDashboardData()
    .then(RAW => initDashboard(RAW))
    .catch(err => {
      // ★ NO FALLBACK — fail loudly so stale data never shows
      console.error('[DataSource] ❌ FAILED TO LOAD data.json:', err.message);
      console.error('[DataSource] NO FALLBACK — dashboard will not render stale data');
      document.body.style.cssText = 'margin:0;font-family:sans-serif;background:#fff';
      document.body.innerHTML =
        '<div style="max-width:540px;margin:80px auto;padding:40px;border:1px solid #fca5a5;'
        + 'border-radius:12px;background:#fff5f5;text-align:center">'
        + '<div style="font-size:40px;margin-bottom:16px">❌</div>'
        + '<h2 style="font-size:20px;color:#b91c1c;margin:0 0 12px">ไม่สามารถโหลดข้อมูลได้</h2>'
        + '<p style="color:#374151;font-size:14px;line-height:1.8;margin-bottom:16px">'
        + 'ไม่สามารถโหลด <strong>data/data.json</strong> ได้<br>'
        + 'ต้องเปิดผ่าน HTTP server (ไม่ใช่ file://)<br>'
        + 'ใช้ Live Server หรือ <code>python -m http.server</code></p>'
        + '<code style="display:block;background:#fee2e2;padding:10px 14px;border-radius:6px;'
        + 'font-size:12px;color:#991b1b;margin-bottom:16px;word-break:break-all">'
        + err.message + '</code>'
        + '<p style="color:#9ca3af;font-size:11px">กด F12 → Console เพื่อดูรายละเอียดเพิ่มเติม</p>'
        + '</div>';
    });
});

// ===== TABS =====
const _TAB_IDS=['exec','trend','root','ops','impact','fgshortage','excel','rm','rmreport','rmplan'];
let _activeTab='exec';
let _lastFilteredRows=null,_lastIdxs=null,_lastLabels=null;
/**
 * SINGLE SOURCE OF TRUTH — always use these instead of re-deriving.
 * Both return the exact same data/indices renderDashboard() committed to screen.
 * Falls back gracefully before first render or if state is stale.
 */
function getFilteredData() {
  // Prefer _lastFilteredRows (set by renderDashboard — exactly what screen shows)
  if (_lastFilteredRows && _lastFilteredRows.length) return _lastFilteredRows;
  // Fallback: re-derive (first load / before first render)
  return (typeof filtered !== 'undefined' && filtered.length) ? filtered : DATA;
}
function getCurrentIdxs() {
  // Prefer _lastIdxs (committed by renderDashboard)
  if (_lastIdxs && _lastIdxs.length) return _lastIdxs;
  // Fallback
  return getDateIdxs().map(x => x.i);
}
function switchTab(t){
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.toggle('active',b.dataset.tab===t));
  _TAB_IDS.forEach(id=>{
    const cap=id.charAt(0).toUpperCase()+id.slice(1);
    const el=document.getElementById('tab'+cap);
    if(el) el.classList.toggle('active',id===t);
  });
  _activeTab=t;
  if(t==='excel') buildExcelTab();
  if(t==='fgshortage') renderFGShortage();
  if(t==='rmplan') buildRMPlanWeekOptions();
  if((t==='rm'||t==='rmreport'||t==='rmplan')&&_lastFilteredRows){
    const di=getDateIdxs(),idxs=di.map(x=>x.i),labels=di.map(x=>x.d);
    if(t==='rm') updateRMTab(_lastFilteredRows,idxs,labels);
    if(t==='rmreport') updateRMReportTab(_lastFilteredRows,idxs,labels);
    if(t==='rmplan') runRMPlanning();
  }
}
let _sidebarCollapsed=false;
function toggleSidebar(){
  _sidebarCollapsed=!_sidebarCollapsed;
  const s=document.getElementById('sidebar');
  if(s) s.classList.toggle('collapsed',_sidebarCollapsed);
  // Update toggle icon
  const icon=s&&s.querySelector('.sidebar-toggle-btn i');
  if(icon) icon.setAttribute('data-lucide',_sidebarCollapsed?'panel-left-open':'menu');
  if(typeof lucide!=='undefined') lucide.createIcons();
}

// ===== FILTERS =====
function initFilters(){
const gs=document.getElementById('fGroup');
META.g.filter(g=>g!=='Total').forEach(g=>{const o=document.createElement('option');o.value=g;o.textContent=g;gs.appendChild(o)});
updateCustomerDD();updateItemDD();updateItemCodeDD();
function _refresh(){applyFilters();updateRMIntelligence();renderDashboard();}
gs.addEventListener('change',()=>{updateCustomerDD();updateItemDD();updateItemCodeDD();_refresh()});
document.getElementById('fCustomer').addEventListener('change',()=>{updateItemDD();updateItemCodeDD();_refresh()});
document.getElementById('fItem').addEventListener('change',()=>{updateItemDisplay();_refresh()});
document.getElementById('fItemCode').addEventListener('change',()=>{_refresh()});
document.getElementById('fDateFrom').addEventListener('change',()=>{_refresh()});
document.getElementById('fDateTo').addEventListener('change',()=>{_refresh()});
}
function updateCustomerDD(){
const g=document.getElementById('fGroup').value,s=document.getElementById('fCustomer');
s.innerHTML='<option value="">ทั้งหมด</option>';
let c=new Set();if(g&&g!=='Total'&&META.gc[g])META.gc[g].forEach(x=>c.add(x));else META.c.forEach(x=>c.add(x));
[...c].sort().forEach(x=>{const o=document.createElement('option');o.value=x;o.textContent=x;s.appendChild(o)});
}
function updateItemDD(){
const g=document.getElementById('fGroup').value,c=document.getElementById('fCustomer').value,s=document.getElementById('fItem');
s.innerHTML='<option value="">ทั้งหมด</option>';let its=new Set();
DATA.forEach(r=>{if(g&&r[0]!==g)return;if(c&&r[1]!==c)return;const v=itemMode==='customer'?r[2]:r[3];if(v)its.add(v)});
[...its].sort().forEach(x=>{const o=document.createElement('option');o.value=x;o.textContent=x;s.appendChild(o)});
}
function updateItemCodeDD(){
const g=document.getElementById('fGroup').value,c=document.getElementById('fCustomer').value,s=document.getElementById('fItemCode');
s.innerHTML='<option value="">ทั้งหมด</option>';let codes=new Set();
DATA.forEach(r=>{if(g&&r[0]!==g)return;if(c&&r[1]!==c)return;if(r[4])codes.add(r[4])});
[...codes].sort().forEach(x=>{const o=document.createElement('option');o.value=x;o.textContent=x+(META.ic[x]?' ('+META.ic[x]+')':'');s.appendChild(o)});
}
function setItemMode(m){
itemMode=m;document.getElementById('togCust').classList.toggle('active',m==='customer');
document.getElementById('togInt').classList.toggle('active',m==='internal');
document.getElementById('itemLabel').textContent=m==='customer'?'รายการลูกค้า':'รายการภายใน';
updateItemDD();document.getElementById('itemDisplay').textContent='';applyFilters();renderDashboard();
}
function updateItemDisplay(){
const v=document.getElementById('fItem').value,d=document.getElementById('itemDisplay');
if(!v){d.textContent='';return}
if(itemMode==='customer'){const m=META.im[v];d.textContent=m?'→ '+m:''}
else{const m=META.imr[v];d.textContent=m?'→ '+m:''}
}

// ===== FILTER & COMPUTE =====
// NOTE: DATA_MIN/MAX computed dynamically inside getDateIdxs() — NOT module-level constants.
// They must use DATES after the async fetch populates it.
function getDateIdxs(){
  // Compute bounds dynamically from current DATES (populated after fetch)
  const DATA_MIN = (typeof DATES !== 'undefined' && DATES && DATES.length)
    ? new Date(DATES[0]) : new Date('2026-01-01');
  const DATA_MAX = (typeof DATES !== 'undefined' && DATES && DATES.length)
    ? new Date(DATES[DATES.length-1]) : new Date('2099-12-31');
  const dfRaw = document.getElementById('fDateFrom').value;
  const dtRaw = document.getElementById('fDateTo').value;
  const df = dfRaw ? new Date(Math.max(new Date(dfRaw), DATA_MIN)) : DATA_MIN;
  const dt = dtRaw ? new Date(Math.min(new Date(dtRaw), DATA_MAX)) : DATA_MAX;
  return DATES.map((d,i) => ({d,i}))
              .filter(x => {
                const xd = new Date(x.d);
                return xd >= df && xd <= dt;
              });
}
function applyFilters(){
// ── Guard: DATA must exist (from fresh fetch) ─────────────────────
if (!DATA || !DATA.length) {
  console.error('[Filter] ❌ DATA is empty — cannot filter');
  filtered = [];
  return;
}
const g=document.getElementById('fGroup').value;
const c=document.getElementById('fCustomer').value;
const it=document.getElementById('fItem').value,ic=document.getElementById('fItemCode').value;
filtered=DATA.filter(r=>{
  if(g&&r[0]!==g)return false;
  // Layer selection — prevents double counting
  if(!c){
    if(r[1]!=='')return false;      // All customers → Layer1 only (r[1]==="")
  }else{
    if(r[1]!==c)return false;       // Specific customer → Layer2 only
  }
  if(it){const v=itemMode==='customer'?r[2]:r[3];if(v!==it)return false}
  if(ic&&r[4]!==ic)return false;
  return true;
});
console.log('[Filter] 📋 Applied:', {
  sourceRows: DATA.length,
  filteredRows: filtered.length,
  group: g||'ALL', customer: c||'ALL',
  generatedAt: META?.generatedAt
});
}
function sumArr(a,ix){return ix.reduce((s,i)=>s+(a[i]||0),0)}
function fN(n){return n===0?'0':Math.round(n).toLocaleString('th-TH')}
function fN2(n){return n===0?'0':n.toLocaleString('th-TH',{maximumFractionDigits:2})}

// ===== KPI =====
function riskBadge(pct){
if(pct>=98)return'<span class="risk-badge risk-green">ปกติ</span>';
if(pct>=95)return'<span class="risk-badge risk-yellow">เฝ้าระวัง</span>';
return'<span class="risk-badge risk-red">เสี่ยง</span>';
}
function diffU(r,i){return(r[6][i]||0)-(r[5][i]||0)}
function diffK(r,i){return(r[9][i]||0)-(r[8][i]||0)}
function updateKPI(agg){
// agg = precomputed { totOrd, totAct, totDiff, neg } from renderDashboard
const tO=agg.totOrd, tA=agg.totAct, tD=agg.totDiff, neg=agg.neg;
const pct=tO>0?(tA/tO*100):0;
// ── KPI proof: log exact values displayed ─────────────────────────
console.log('[KPI] 📊 VALUES:', {
  generatedAt: META?.generatedAt,
  PO_Unit: tO.toLocaleString(),
  ACT_Unit: tA.toLocaleString(),
  Diff: tD.toLocaleString(),
  FillRate: pct.toFixed(1)+'%',
  NegCount: neg
});
setKPI('kpiOrder',fN(tO),'หน่วย','neutral');
setKPI('kpiActual',fN(tA),'หน่วย',tA>=tO?'positive':'negative');
setKPI('kpiDiff',fN(tD),'หน่วย',tD>=0?'positive':'negative');
const pcls=pct>=98?'positive':pct>=95?'warning':'negative';
document.getElementById('kpiPct').innerHTML=pct.toFixed(1)+'%'+riskBadge(pct);
document.getElementById('kpiPctSub').textContent=tO>0?'เทียบยอดสั่ง':'ไม่มีข้อมูล';
document.getElementById('kpiPct').closest('.kpi-card').className='kpi-card '+pcls;
setKPI('kpiNeg',fN(neg),'รายการ-วัน',neg===0?'positive':'negative');
}
function setKPI(id,v,s,c){document.getElementById(id).textContent=v;document.getElementById(id+'Sub').textContent=s;document.getElementById(id).closest('.kpi-card').className='kpi-card '+c}

// ===== CHARTS =====
function updateCharts(data,idxs,labels){
const d=data.filter(r=>!skipTotal(r));
const oD=new Array(idxs.length).fill(0),aD=new Array(idxs.length).fill(0),dD=new Array(idxs.length).fill(0);
d.forEach(r=>{idxs.forEach((di,li)=>{oD[li]+=r[5][di]||0;aD[li]+=r[6][di]||0;dD[li]+=diffU(r,di)})});
// ── Chart proof: log actual values being rendered ─────────────────
const oSum=oD.reduce((a,v)=>a+v,0), aSum=aD.reduce((a,v)=>a+v,0);
const nonZero=oD.filter(v=>v>0).length;
console.log('[Chart] 🔥 CHART DATA (actual values sent to Chart.js):', {
  generatedAt: META?.generatedAt,
  inputRows: d.length,
  datePoints: idxs.length,
  'PO_Unit total': oSum.toLocaleString(),
  'ACT_Unit total': aSum.toLocaleString(),
  'Non-zero dates': nonZero + '/' + idxs.length,
  'First 5 PO': oD.slice(0,5).map(v=>Math.round(v)),
  'Last 5 PO': oD.slice(-5).map(v=>Math.round(v))
});
if(trendChart)trendChart.destroy();
trendChart=new Chart(document.getElementById('trendChart').getContext('2d'),{type:'line',data:{labels,datasets:[
{label:'ยอดสั่ง',data:oD,borderColor:'#2563eb',backgroundColor:'rgba(37,99,235,.1)',fill:true,tension:.3,pointRadius:3,borderWidth:2},
{label:'ยอดส่งจริง',data:aD,borderColor:'#16a34a',backgroundColor:'rgba(22,163,74,.1)',fill:true,tension:.3,pointRadius:3,borderWidth:2}
]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'top',labels:{font:{size:11},usePointStyle:true}},tooltip:{mode:'index',intersect:false}},scales:{y:{beginAtZero:true,ticks:{font:{size:10},callback:v=>v.toLocaleString()}},x:{ticks:{font:{size:10},maxRotation:45}}},interaction:{mode:'nearest',axis:'x',intersect:false}}});
if(diffChart)diffChart.destroy();
diffChart=new Chart(document.getElementById('diffChart').getContext('2d'),{type:'bar',data:{labels,datasets:[{label:'Diff',data:dD,backgroundColor:dD.map(v=>v>=0?'rgba(22,163,74,.7)':'rgba(220,38,38,.7)'),borderColor:dD.map(v=>v>=0?'#16a34a':'#dc2626'),borderWidth:1,borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>'Diff: '+c.parsed.y.toLocaleString()+' หน่วย'}}},scales:{y:{ticks:{font:{size:10},callback:v=>v.toLocaleString()}},x:{ticks:{font:{size:10},maxRotation:45}}}}});
}

// ===== RANKING =====
function updateRanking(data,idxs){
const d=data.filter(r=>!skipTotal(r));
const cd={};d.forEach(r=>{if(!r[1])return;if(!cd[r[1]])cd[r[1]]=0;idxs.forEach(i=>{cd[r[1]]+=diffU(r,i)})});
const sc=Object.entries(cd).filter(e=>e[1]<0).sort((a,b)=>a[1]-b[1]).slice(0,5);
document.getElementById('rankCust').innerHTML=sc.length?sc.map((e,i)=>`<div class="rank-item"><div><span class="rank-num rank-${i+1}">${i+1}</span>${e[0]}</div><span class="neg">${fN(e[1])}</span></div>`).join(''):'<div style="color:var(--gray);font-size:12px">ไม่มีข้อมูล Diff ติดลบ</div>';
const id={};d.forEach(r=>{const nm=r[3]||r[2]||'';if(!nm)return;if(!id[nm])id[nm]=0;idxs.forEach(i=>{id[nm]+=diffU(r,i)})});
const si=Object.entries(id).filter(e=>e[1]<0).sort((a,b)=>a[1]-b[1]).slice(0,5);
document.getElementById('rankItem').innerHTML=si.length?si.map((e,i)=>`<div class="rank-item"><div><span class="rank-num rank-${i+1}">${i+1}</span><span title="${e[0]}">${e[0].length>25?e[0].slice(0,25)+'…':e[0]}</span></div><span class="neg">${fN(e[1])}</span></div>`).join(''):'<div style="color:var(--gray);font-size:12px">ไม่มีข้อมูล</div>';
const dd={};idxs.forEach(i=>{dd[DATES[i]]=0;d.forEach(r=>{dd[DATES[i]]+=diffU(r,i)})});
const sd=Object.entries(dd).sort((a,b)=>a[1]-b[1]).slice(0,5);
document.getElementById('rankDate').innerHTML=sd.map((e,i)=>`<div class="rank-item"><div><span class="rank-num rank-${i+1}">${i+1}</span>${e[0]}</div><span class="${e[1]<0?'neg':'pos'}">${fN(e[1])}</span></div>`).join('');
}

// ===== TABLE =====
function updateTable(data,idxs){
const thead=document.querySelector('#dataTable thead tr'),tbody=document.getElementById('tableBody');
let h='<th>Group</th><th>Customer</th><th>รายการ</th><th>รายการภายใน</th>';
idxs.forEach(i=>{const d=DATES[i].slice(5);h+=`<th class="num">สั่ง<br>${d}</th><th class="num">ส่ง<br>${d}</th><th class="num">Diff<br>${d}</th>`});
h+='<th class="num">รวมสั่ง</th><th class="num">รวมส่ง</th><th class="num">รวม Diff</th><th class="num">%</th>';
thead.innerHTML=h;
const disp=data.slice(0,200);let rows='';
disp.forEach(r=>{let tO=0,tA=0,tD=0,neg=false,c='';
idxs.forEach(i=>{const o=r[5][i]||0,a=r[6][i]||0,d=a-o;tO+=o;tA+=a;tD+=d;if(d<0)neg=true;
c+=`<td class="num">${fN(o)}</td><td class="num">${fN(a)}</td><td class="num ${d<0?'neg':d>0?'pos':''}">${fN(d)}</td>`});
const p=tO>0?(tA/tO*100).toFixed(1):'-';
rows+=`<tr class="${neg?'neg-diff':''}"><td>${r[0]}</td><td>${r[1]||'-'}</td><td>${r[2]||'-'}</td><td>${r[3]||'-'}</td>${c}<td class="num" style="font-weight:700">${fN(tO)}</td><td class="num" style="font-weight:700">${fN(tA)}</td><td class="num ${tD<0?'neg':tD>0?'pos':''}" style="font-weight:700">${fN(tD)}</td><td class="num" style="font-weight:700">${p}%</td></tr>`});
tbody.innerHTML=rows;
document.getElementById('tableCount').textContent=`(แสดง ${disp.length} จาก ${data.length} รายการ)`;
}

// ===== ROOT CAUSE =====
function updateRootCause(data,idxs){
const el=document.getElementById('rootCause');const notes=[];
const d=data.filter(r=>!skipTotal(r));
if(!d.length||idxs.length<3){el.innerHTML='<div class="rc-item info"><span class="rc-icon">ℹ️</span>ต้องมีข้อมูลอย่างน้อย 3 วันเพื่อวิเคราะห์</div>';return}
const oByD=[],aByD=[];
idxs.forEach(i=>{let o=0,a=0;d.forEach(r=>{o+=r[5][i]||0;a+=r[6][i]||0});oByD.push(o);aByD.push(a)});
let oInc=0,aFlat=0;
for(let i=1;i<oByD.length;i++){if(oByD[i]>oByD[i-1]*1.05)oInc++;const chg=aByD[i-1]>0?Math.abs(aByD[i]-aByD[i-1])/aByD[i-1]:0;if(chg<0.1)aFlat++}
if(oInc>=idxs.length*0.4&&aFlat>=idxs.length*0.4){
notes.push({t:'capacity',i:'🏭',m:'<strong>Capacity Risk:</strong> ยอดสั่งมีแนวโน้มเพิ่มขึ้นต่อเนื่อง แต่ยอดส่งจริงคงที่ → อาจเกิดปัญหา capacity การผลิตไม่เพียงพอ'})
}
let consDays=0,maxCons=0;
for(let i=0;i<idxs.length;i++){
if(aByD[i]<oByD[i]*0.95&&oByD[i]>0){consDays++;if(consDays>maxCons)maxCons=consDays}else consDays=0;
}
if(maxCons>=3){notes.push({t:'fulfillment',i:'📦',m:`<strong>Fulfillment Risk:</strong> ส่งสินค้าไม่ถึงเป้าต่อเนื่อง ${maxCons} วัน → ควรตรวจสอบสต็อก การผลิต หรือปัญหาโลจิสติกส์`})}
const dByD=[];idxs.forEach(i=>{let dv=0;d.forEach(r=>{dv+=diffU(r,i)});dByD.push(dv)});
const avgDiff=dByD.reduce((s,v)=>s+Math.abs(v),0)/dByD.length;
const anomalies=[];
dByD.forEach((v,i)=>{if(Math.abs(v)>avgDiff*2&&avgDiff>0)anomalies.push({d:DATES[idxs[i]],v})});
if(anomalies.length>0){
const aList=anomalies.slice(0,3).map(a=>`${a.d} (${fN(a.v)})`).join(', ');
notes.push({t:'anomaly',i:'🔍',m:`<strong>Anomaly Detected:</strong> พบ ${anomalies.length} วันที่ Diff สูงผิดปกติ (>2x ค่าเฉลี่ย): ${aList}`})
}
if(!notes.length)notes.push({t:'success',i:'✅',m:'ไม่พบสัญญาณความเสี่ยงที่สำคัญในช่วงเวลาที่เลือก'});
el.innerHTML=notes.map(n=>`<div class="rc-item ${n.t}"><span class="rc-icon">${n.i}</span><span>${n.m}</span></div>`).join('');
}

// ===== SMART NOTES =====
function updateSmartNotes(data,idxs){
const el=document.getElementById('smartNotes');const notes=[];
const d=data.filter(r=>!skipTotal(r));
if(!d.length){el.innerHTML='<div class="rc-item info"><span class="rc-icon">ℹ️</span>ไม่พบข้อมูลตามเงื่อนไขที่เลือก</div>';return}
let tO=0,tA=0,tD=0;
d.forEach(r=>{idxs.forEach(i=>{const o=r[5][i]||0,a=r[6][i]||0;tO+=o;tA+=a;tD+=a-o})});
const pct=tO>0?(tA/tO*100):0;
if(pct>=98)notes.push({t:'success',i:'✅',m:`อัตราส่งสินค้า ${pct.toFixed(1)}% — บรรลุเป้าหมายระดับดีเยี่ยม`});
else if(pct>=95)notes.push({t:'warn',i:'⚠️',m:`อัตราส่งสินค้า ${pct.toFixed(1)}% — ใกล้เป้าหมาย แต่ยังต้องเฝ้าระวัง`});
else notes.push({t:'danger',i:'🔴',m:`อัตราส่งสินค้า ${pct.toFixed(1)}% — ต่ำกว่าเป้าหมาย ต้องแก้ไขเร่งด่วน`});
const half=Math.floor(idxs.length/2),fh=idxs.slice(0,half),sh=idxs.slice(half);
let fhA=0,shA=0;d.forEach(r=>{fh.forEach(i=>{fhA+=r[6][i]||0});sh.forEach(i=>{shA+=r[6][i]||0})});
if(shA>fhA&&fhA>0)notes.push({t:'info',i:'📈',m:`ยอดส่งช่วงหลังเพิ่มขึ้น ${((shA-fhA)/fhA*100).toFixed(1)}% — แนวโน้มดีขึ้น`});
else if(shA<fhA&&fhA>0)notes.push({t:'warn',i:'📉',m:`ยอดส่งช่วงหลังลดลง ${((fhA-shA)/fhA*100).toFixed(1)}% — ควรติดตาม`});
let negCnt=0;d.forEach(r=>{let allN=true,has=false;idxs.forEach(i=>{if(r[5][i]>0){has=true;if(diffU(r,i)>=0)allN=false}});if(has&&allN)negCnt++});
if(negCnt>0)notes.push({t:'danger',i:'🔻',m:`พบ <strong>${negCnt}</strong> รายการ Diff ติดลบทุกวัน → ควรเร่งปรับแผนการผลิต`});
el.innerHTML=notes.map(n=>`<div class="rc-item ${n.t}"><span class="rc-icon">${n.i}</span><span>${n.m}</span></div>`).join('');
}

// ===== EXCEL TAB =====
function setExcelUnit(u){
excelUnit=u;document.getElementById('exUnitBtn').classList.toggle('active',u==='unit');
document.getElementById('exKgBtn').classList.toggle('active',u==='kg');buildExcelTab();
}
function buildExcelTab(){
const idxs=getDateIdxs().map(x=>x.i);
const d=filtered.length?filtered:DATA;
const total=d.length;const pages=Math.ceil(total/PER_PAGE);
const start=(excelPage-1)*PER_PAGE,end=Math.min(start+PER_PAGE,total);
const slice=d.slice(start,end);
let h='<th class="frozen" style="left:0;min-width:60px">Group</th><th class="frozen" style="left:60px;min-width:100px">Customer</th><th class="frozen" style="left:160px;min-width:140px">รายการลูกค้า</th><th class="frozen" style="left:300px;min-width:140px">รายการภายใน</th>';
idxs.forEach(i=>{const dt=DATES[i].slice(5);h+=`<th class="num" style="min-width:70px">สั่ง ${dt}</th><th class="num" style="min-width:70px">ส่ง ${dt}</th><th class="num" style="min-width:70px">Diff ${dt}</th>`});
h+='<th class="num" style="min-width:80px">รวมสั่ง</th><th class="num" style="min-width:80px">รวมส่ง</th><th class="num" style="min-width:80px">รวม Diff</th>';
document.getElementById('excelHead').innerHTML=h;
const oi=excelUnit==='kg'?8:5,ai=excelUnit==='kg'?9:6;
const diFn=excelUnit==='kg'?diffK:diffU;
const fmt=excelUnit==='kg'?fN2:fN;
let b='';
slice.forEach(r=>{
let tO=0,tA=0,tD=0,neg=false,c='';
c+=`<td class="frozen" style="left:0;min-width:60px">${r[0]}</td><td class="frozen" style="left:60px;min-width:100px">${r[1]||'-'}</td><td class="frozen" style="left:160px;min-width:140px">${r[2]||'-'}</td><td class="frozen" style="left:300px;min-width:140px">${r[3]||'-'}</td>`;
idxs.forEach(i=>{const o=r[oi][i]||0,a=r[ai][i]||0,d2=diFn(r,i);tO+=o;tA+=a;tD+=d2;if(d2<0)neg=true;
c+=`<td class="num">${fmt(o)}</td><td class="num">${fmt(a)}</td><td class="num ${d2<0?'neg':d2>0?'pos':''}">${fmt(d2)}</td>`});
c+=`<td class="num" style="font-weight:700">${fmt(tO)}</td><td class="num" style="font-weight:700">${fmt(tA)}</td><td class="num ${tD<0?'neg':tD>0?'pos':''}" style="font-weight:700">${fmt(tD)}</td>`;
b+=`<tr class="${neg?'neg-diff':''}">${c}</tr>`});
document.getElementById('excelBody').innerHTML=b;
document.getElementById('excelInfo').textContent=`แสดง ${start+1}-${end} จาก ${total} (${excelUnit.toUpperCase()})`;
let pg='';
if(pages>1){
pg+=`<button onclick="excelGo(1)" ${excelPage===1?'disabled':''}>«</button>`;
pg+=`<button onclick="excelGo(${Math.max(1,excelPage-1)})" ${excelPage===1?'disabled':''}>‹</button>`;
const s=Math.max(1,excelPage-2),e=Math.min(pages,excelPage+2);
for(let i=s;i<=e;i++)pg+=`<button class="${i===excelPage?'active':''}" onclick="excelGo(${i})">${i}</button>`;
pg+=`<button onclick="excelGo(${Math.min(pages,excelPage+1)})" ${excelPage===pages?'disabled':''}>›</button>`;
pg+=`<button onclick="excelGo(${pages})" ${excelPage===pages?'disabled':''}>»</button>`;
pg+=`<span>หน้า ${excelPage}/${pages}</span>`;
}
document.getElementById('excelPagination').innerHTML=pg;
}
function excelGo(p){excelPage=p;buildExcelTab()}

// ===== EXPORT CSV =====
function exportCSV(){
const idxs=getDateIdxs().map(x=>x.i);
const d=filtered.length?filtered:DATA;
const oi=excelUnit==='kg'?8:5,ai=excelUnit==='kg'?9:6;
const diFn=excelUnit==='kg'?diffK:diffU;
let csv='\uFEFF"Group","Customer","รายการลูกค้า","รายการภายใน"';
idxs.forEach(i=>{const dt=DATES[i];csv+=`,"สั่ง ${dt}","ส่ง ${dt}","Diff ${dt}"`});
csv+=',"รวมสั่ง","รวมส่ง","รวม Diff"\n';
d.forEach(r=>{
csv+=`"${r[0]}","${r[1]||''}","${r[2]||''}","${r[3]||''}"`;
let tO=0,tA=0,tD=0;
idxs.forEach(i=>{const o=r[oi][i]||0,a=r[ai][i]||0,dd=diFn(r,i);tO+=o;tA+=a;tD+=dd;csv+=`,${o},${a},${dd}`});
csv+=`,${tO},${tA},${tD}\n`});
const blob=new Blob([csv],{type:'text/csv;charset=utf-8;'});
const url=URL.createObjectURL(blob),a=document.createElement('a');
a.href=url;a.download=`PO_Actual_${excelUnit}_${new Date().toISOString().slice(0,10)}.csv`;a.click();URL.revokeObjectURL(url);
}

// ===== AI AGENT (lightweight query engine) =====
function askQ(q){document.getElementById('chatInput').value=q;sendChat()}
function sendChat(){
const inp=document.getElementById('chatInput'),q=inp.value.trim();if(!q)return;inp.value='';
addMsg(q,'user');setTimeout(()=>addMsg(processQuery(q),'bot'),200);
}
function addMsg(t,tp){const b=document.getElementById('chatBox'),d=document.createElement('div');d.className='chat-msg '+tp;d.innerHTML='<div class="bubble">'+t+'</div>';b.appendChild(d);b.scrollTop=b.scrollHeight}
function processQuery(q){
  const ql = q.toLowerCase();
  // ── SINGLE SOURCE OF TRUTH ──────────────────────────────────────────
  // Use getFilteredData()/getCurrentIdxs() so results ALWAYS match screen.
  const _data = getFilteredData();  // row-filtered: same as renderDashboard used
  const _idxs = getCurrentIdxs();  // date-filtered: same indices as screen
  const _rows  = _data.filter(r => !skipTotal(r));

  // Guard: no data
  if (!_rows.length || !_idxs.length) {
    return `ไม่พบข้อมูลใน filter ปัจจุบัน (rows: ${_rows.length}, dates: ${_idxs.length})<br>ลองปรับ filter หรือเลือกช่วงวันที่ใหม่`;
  }

  // Debug context (shows on every query)
  const _dateRange = _idxs.length ? `${DATES[_idxs[0]]} – ${DATES[_idxs[_idxs.length-1]]}` : '—';
  const _ctx = `<span style="font-size:9px;color:#94a3b8;display:block;margin-top:4px">📌 ${_rows.length} rows | ${_idxs.length} วัน | ${_dateRange}</span>`;

  // ── Entity extraction (uses full DATA for name lookup, but queries use _rows/_idxs) ──
  let tc = null; META.c.forEach(c => { if(ql.includes(c.toLowerCase())) tc = c; });
  let tg = null; ['export','retail','consign','total'].forEach(g => { if(ql.includes(g)) tg = g.charAt(0).toUpperCase()+g.slice(1); });
  let ti = null;
  const allItems = new Set(); DATA.forEach(r => { if(r[2]) allItems.add(r[2]); if(r[3]) allItems.add(r[3]); });
  allItems.forEach(it => { if(ql.includes(it.toLowerCase())) ti = it; });
  let tic = null;
  const allCodes = new Set(); DATA.forEach(r => { if(r[4]) allCodes.add(r[4]); });
  allCodes.forEach(c => { if(ql.includes(c.toLowerCase())) tic = c; });

  // ── Top 5 ลูกค้า ────────────────────────────────────────────────────
  if(ql.includes('top') && ql.includes('ลูกค้า')) {
    const cd = {};
    _rows.forEach(r => {
      if(!r[COLS.CUST]) return;
      if(!cd[r[COLS.CUST]]) cd[r[COLS.CUST]] = {o:0, a:0};
      _idxs.forEach(i => { cd[r[COLS.CUST]].o += r[COLS.FG_DEMAND][i]||0; cd[r[COLS.CUST]].a += r[COLS.FG_ACTUAL][i]||0; });
    });
    if(ql.includes('สั่ง')) { const s = Object.entries(cd).sort((a,b)=>b[1].o-a[1].o).slice(0,5); return 'Top 5 ลูกค้ายอดสั่งสูงสุด:<br>'+s.map((e,i)=>`${i+1}. <strong>${e[0]}</strong>: ${fN(e[1].o)} หน่วย`).join('<br>')+_ctx; }
    if(ql.includes('ส่ง')) { const s = Object.entries(cd).sort((a,b)=>b[1].a-a[1].a).slice(0,5); return 'Top 5 ลูกค้ายอดส่งสูงสุด:<br>'+s.map((e,i)=>`${i+1}. <strong>${e[0]}</strong>: ${fN(e[1].a)} หน่วย`).join('<br>')+_ctx; }
    // Default: sort by Fill Rate asc (worst first)
    const s2 = Object.entries(cd).filter(([,v])=>v.o>0).map(([nm,v])=>({nm,fr:v.a/v.o*100,gap:v.o-v.a})).sort((a,b)=>a.fr-b.fr).slice(0,5);
    return 'Top 5 ลูกค้า Fill Rate ต่ำสุด:<br>'+s2.map((e,i)=>`${i+1}. <strong>${e.nm}</strong>: ${e.fr.toFixed(1)}% (ขาด ${fN(e.gap)})`).join('<br>')+_ctx;
  }

  // ── Top 5 สินค้า Diff ──────────────────────────────────────────────
  if(ql.includes('top') && (ql.includes('สินค้า')||ql.includes('item'))) {
    const id = {};
    _rows.forEach(r => {
      const nm = r[COLS.ITEM_TH]||r[COLS.ITEM_EN]||''; if(!nm) return;
      if(!id[nm]) id[nm] = 0;
      _idxs.forEach(i => { id[nm] += diffU(r,i); });
    });
    const s = Object.entries(id).filter(e=>e[1]<0).sort((a,b)=>a[1]-b[1]).slice(0,5);
    if(!s.length) return 'ไม่พบสินค้า Diff ติดลบในช่วงที่เลือก ✅'+_ctx;
    return 'Top 5 สินค้า Diff ติดลบสูงสุด:<br>'+s.map((e,i)=>`${i+1}. <strong>${e[0]}</strong>: ${fN(e[1])} หน่วย`).join('<br>')+_ctx;
  }

  // ── สินค้า/รหัส เฉพาะ ─────────────────────────────────────────────
  if(ti || tic) {
    const fd = _rows.filter(r => { if(ti) return r[COLS.ITEM_TH]===ti||r[COLS.ITEM_EN]===ti; return r[COLS.CODE]===tic; });
    if(!fd.length) return `ไม่พบ "${ti||tic}" ใน filter ปัจจุบัน (ลองปรับ filter)`;
    let tO=0,tA=0,tD=0;
    fd.forEach(r => _idxs.forEach(i => { const o=r[COLS.FG_DEMAND][i]||0,a=r[COLS.FG_ACTUAL][i]||0; tO+=o; tA+=a; tD+=a-o; }));
    const p = tO>0?(tA/tO*100).toFixed(1):'0'; const name = ti||tic;
    if(ql.includes('diff')||ql.includes('ส่วนต่าง')) return `Diff ของ <strong>${name}</strong>: ${fN(tD)} หน่วย ${tD<0?'(ส่งน้อยกว่าสั่ง)':'(ส่งครบ)'}`+_ctx;
    return `ข้อมูล <strong>${name}</strong>: สั่ง ${fN(tO)}, ส่ง ${fN(tA)}, Diff ${fN(tD)}, อัตราส่ง ${p}% (${fd.length} records)`+_ctx;
  }

  // ── ลูกค้า เฉพาะ ──────────────────────────────────────────────────
  if(tc) {
    const cd = _rows.filter(r => r[COLS.CUST]===tc);
    if(!cd.length) return `ไม่พบลูกค้า "${tc}" ใน filter ปัจจุบัน`;
    let tO=0,tA=0,tD=0;
    cd.forEach(r => _idxs.forEach(i => { const o=r[COLS.FG_DEMAND][i]||0,a=r[COLS.FG_ACTUAL][i]||0; tO+=o; tA+=a; tD+=a-o; }));
    const p = tO>0?(tA/tO*100).toFixed(1):'0';
    if(ql.includes('ส่ง')||ql.includes('actual')) return `ยอดส่ง <strong>${tc}</strong>: <strong>${fN(tA)}</strong> หน่วย (จากยอดสั่ง ${fN(tO)}, คิดเป็น ${p}%)`+_ctx;
    if(ql.includes('diff')) return `Diff ของ <strong>${tc}</strong>: ${fN(tD)} หน่วย ${tD<0?'(ส่งน้อยกว่าสั่ง)':''}`+_ctx;
    return `ข้อมูล <strong>${tc}</strong>: สั่ง ${fN(tO)}, ส่ง ${fN(tA)}, Diff ${fN(tD)}, อัตราส่ง ${p}% (${cd.length} รายการ)`+_ctx;
  }

  // ── กลุ่ม เฉพาะ ───────────────────────────────────────────────────
  if(tg) {
    const gd = _rows.filter(r => r[COLS.GROUP]===tg);
    if(!gd.length) return `ไม่พบกลุ่ม "${tg}" ใน filter ปัจจุบัน`;
    let tO=0,tA=0,tD=0;
    gd.forEach(r => _idxs.forEach(i => { const o=r[COLS.FG_DEMAND][i]||0,a=r[COLS.FG_ACTUAL][i]||0; tO+=o; tA+=a; tD+=a-o; }));
    const p = tO>0?(tA/tO*100).toFixed(1):'0';
    return `กลุ่ม <strong>${tg}</strong>: สั่ง ${fN(tO)}, ส่ง ${fN(tA)}, Diff ${fN(tD)}, อัตราส่ง ${p}%`+_ctx;
  }

  // ── สรุปภาพรวม ─────────────────────────────────────────────────────
  if(ql.includes('สรุป') || ql.includes('ภาพรวม')) {
    let tO=0,tA=0,tD=0;
    _rows.forEach(r => _idxs.forEach(i => { const o=r[COLS.FG_DEMAND][i]||0,a=r[COLS.FG_ACTUAL][i]||0; tO+=o; tA+=a; tD+=a-o; }));
    const p = tO>0?(tA/tO*100).toFixed(1):'0';
    const custCount = new Set(_rows.map(r=>r[COLS.CUST]).filter(Boolean)).size;
    return `สรุปภาพรวม (${_dateRange}):<br>• ยอดสั่ง: <strong>${fN(tO)}</strong><br>• ยอดส่ง: <strong>${fN(tA)}</strong><br>• Diff: <strong>${fN(tD)}</strong><br>• Fulfillment: <strong>${p}%</strong><br>• รายการ: ${_rows.length} | ลูกค้า: ${custCount} ราย`+_ctx;
  }

  // ── วันที่ Diff ติดลบมากสุด ────────────────────────────────────────
  if(ql.includes('วันไหน') && (ql.includes('diff')||ql.includes('ติดลบ'))) {
    const dd = {};
    _idxs.forEach(i => {
      dd[DATES[i]] = 0;
      _rows.forEach(r => { dd[DATES[i]] += diffU(r,i); });
    });
    const s = Object.entries(dd).sort((a,b)=>a[1]-b[1]).slice(0,3);
    return 'วันที่ Diff ติดลบมากสุด:<br>'+s.map((d,i)=>`${i+1}. <strong>${d[0]}</strong>: ${fN(d[1])} หน่วย`).join('<br>')+_ctx;
  }

  // ── Route to AI engine modules (all use _rows/_idxs = same source) ──
  if(ql.includes('shortage')||ql.includes('ขาด rm')||(ql.includes('top')&&ql.includes('rm'))) {
    const res = detectTopShortage(_rows, _idxs, 5);
    if(!res.length) return 'ไม่พบ RM Shortage ✅'+_ctx;
    return 'Top RM Shortage:<br>'+res.map((v,i)=>`${i+1}. <strong>${_esc(v.name)}</strong>: ${fN(v.shortage)} KG (${(v.coverage*100).toFixed(1)}%)`).join('<br>')+_ctx;
  }
  if(ql.includes('fill rate')&&(ql.includes('ต่ำ')||ql.includes('low')||ql.includes('coverage'))) {
    const res = detectLowCoverage(_rows, _idxs, 0.50);
    if(!res.length) return 'ทุกลูกค้า Fill Rate ≥ 50% ✅'+_ctx;
    return `พบ ${res.length} ลูกค้า Fill Rate < 50%:<br>`+res.slice(0,6).map((v,i)=>`${i+1}. <strong>${_esc(v.cust)}</strong>: ${v.fillRate.toFixed(1)}% (ขาด ${fN(v.gap)})`).join('<br>')+_ctx;
  }
  if(ql.includes('แนวโน้ม')) {
    const res = trendSummary(_rows, _idxs);
    if(!res) return 'ต้องการข้อมูลอย่างน้อย 4 วัน'+_ctx;
    const th = res.trend==='improving'?'ดีขึ้น':res.trend==='declining'?'แย่ลง':'คงที่';
    return `Fill Rate ${th} ${Math.abs(res.frDelta).toFixed(1)}pp | ต้น: ${res.early.fr.toFixed(1)}% → ปลาย: ${res.late.fr.toFixed(1)}%`+_ctx;
  }
  if(ql.includes('anomaly')||ql.includes('ผิดปกติ')||ql.includes('spike')) {
    const res = detectAnomaly(_rows, _idxs);
    if(!res.length) return 'ไม่พบ Anomaly ✅'+_ctx;
    const z=res.filter(a=>a.type==='zero_delivery').length, s2=res.filter(a=>a.type==='demand_spike').length;
    return `พบ Anomaly ${res.length} รายการ (ส่ง 0: ${z} | Spike: ${s2})<br>`+res.slice(0,5).map(a=>`• ${a.date} ${_esc(a.item)} — ${a.type==='zero_delivery'?'Demand '+fN(a.demand)+' ส่ง 0':'Spike '+fN(a.demand)}`).join('<br>')+_ctx;
  }
  if(ql.includes('กลุ่ม')||(ql.includes('group')&&ql.includes('เปรียบ'))) {
    const res = groupPerformance(_rows, _idxs);
    return 'Fill Rate ตามกลุ่ม:<br>'+res.map((v,i)=>`${i+1}. <strong>${_esc(v.group)}</strong>: ${v.fillRate.toFixed(1)}% (ขาด ${fN(v.gap)})`).join('<br>')+_ctx;
  }
  if(ql.includes('insight')||ql.includes('วิเคราะห์')) {
    const ins = generateInsight(_rows, _idxs);
    return `[Insight] ${ins.keyInsight||'—'}<br>[สาเหตุ] ${ins.rootCause||'—'}<br>[ผลกระทบ] ${ins.businessImpact||'—'}<br>[คำแนะนำ] ${ins.recommendation||'—'}`+_ctx;
  }
  return 'ไม่เข้าใจคำถาม ลองกด Preset หรือถามเรื่อง: Top 5 ลูกค้า/สินค้า, ยอดส่ง/Diff [ลูกค้า], สรุปภาพรวม, วันที่ Diff ติดลบ'+_ctx;
}


// ═══ HELPERS ═══════════════════════════════════════════════════════
const skipTotal = r => r[0]==='Total';
const isLayer1Row = r => !skipTotal(r) && (r[1]===''||r[1]==null||r[1]===undefined);
const isLayer2Row = r => !skipTotal(r) && r[1] && r[1]!=='';
// FG Summary row = customer-level aggregate WITH Unit data (cat≠'-')
// Used as source-of-truth for KPI when no customer filter is applied
const isFGSummary = r => isLayer2Row(r) && (r[4]===''||!r[4]) && r[2]!=='-';
const isRMSummary = r => isLayer2Row(r) && (r[4]===''||!r[4]) && r[2]==='-';

/**
 * getKPIData — returns the correct rows for KPI/Chart calculation.
 * RULE: KPI must use ONLY Layer2 FG data, ONLY Unit (r[5]).
 *
 * When no customer selected → use FG Summary rows from ALL data
 *   (these are customer-level aggregates with FG Unit)
 * When customer selected → use filtered data as-is
 *   (includes that customer's FG summary + detail rows)
 */
function getKPIData() {
  const c = document.getElementById('fCustomer').value;
  const g = document.getElementById('fGroup').value;
  if (!c) {
    // No customer → pull FG Summary rows from full DATA (not filtered)
    return DATA.filter(r => {
      if (g && r[0] !== g) return false;
      return isFGSummary(r);
    });
  }
  // Customer selected → filtered already has L2 rows for that customer
  // Use only FG summary row for KPI (detail rows have r[5]=0)
  return filtered.filter(r => isFGSummary(r));
}
// ── Column index schema (immutable) ─────────────────────────────
// Layer2 (FG rows, r[1]=customer):  r[5]=FG demand/date, r[6]=FG actual/date
// Layer1 (RM rows, r[1]=empty):     r[8]=RM demand KG/date, r[9]=RM actual KG/date
// Both layers:  r[0]=group, r[1]=customer, r[2]=item_th, r[3]=item_en, r[4]=code
const COLS = Object.freeze({ GROUP:0, CUST:1, ITEM_TH:2, ITEM_EN:3, CODE:4,
  FG_DEMAND:5, FG_ACTUAL:6, UNIT:7, RM_DEMAND:8, RM_ACTUAL:9 });
// ── RM Planning column map (verified against data.json row structure) ─────────
// r[2] = item category (AA_ผักสลัด, BB_ผักใบ …) — present on all item-level rows
// r[3] = item Thai name | r[4] = item code | r[8/9] = RM demand/actual ARRAYS
const COL_RMP = Object.freeze({
  channelGroup: 0,  // r[0] = Export/Retail/Consign/Wholesale
  customer:     1,  // r[1] = customer name ('' for Channel-level rows)
  rmGroup:      2,  // r[2] = item category (AA_ผักสลัด, BB_ผักใบ …)
  rmName:       3,  // r[3] = item Thai name
  rmCode:       4,  // r[4] = item code
  fgDemand:     5,  // r[5] = FG demand array [by date index]
  fgActual:     6,  // r[6] = FG actual array [by date index]
  unit:         7,  // r[7] = unit array
  rmDemand:     8,  // r[8] = RM demand KG array [by date index]
  rmActual:     9,  // r[9] = RM actual KG array [by date index]
});
const _ric = window.requestIdleCallback||(fn=>setTimeout(fn,1));
const _oiUnitPrice = 20; // ฿ per unit for revenue estimation
function _esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// ═══ OPERATIONAL INTELLIGENCE ══════════════════════════════════════
// NOW receives kpiData + precomputed agg — SAME source as KPI cards
function updateOperational(data,idxs,agg){
  if(!data||!data.length||!idxs||!idxs.length) return;
  const d=data.filter(r=>!skipTotal(r));
  if(!d.length) return;

  // ★ USE precomputed agg — SAME source as KPI cards (NO re-aggregation)
  const totO=agg.totOrd, totA=agg.totAct;
  const gap=totO-totA;
  const stressPct=totO>0?gap/totO*100:0;
  const stressLevel=stressPct<10?'NORMAL':stressPct<25?'TIGHT':'CRITICAL';
  const stressCls=stressLevel==='NORMAL'?'success':stressLevel==='TIGHT'?'warn':'danger';
  const stressEmoji=stressLevel==='NORMAL'?'✅':stressLevel==='TIGHT'?'⚠️':'🚨';
  const stressTh=stressLevel==='NORMAL'?'ปกติ':stressLevel==='TIGHT'?'ตึงตัว':'วิกฤต';
  const fillRate=totO>0?(totA/totO*100).toFixed(1):0;

  // m0 → stressContainer
  const m0=`<div class="rc-item ${stressCls}" style="grid-column:1/-1"><span class="rc-icon">${stressEmoji}</span>
    <span><strong>Stress Index: ${stressPct.toFixed(1)}%</strong> — สถานะ: <strong>${stressTh}</strong> | Fill Rate: <strong>${fillRate}%</strong> | ยอดขาดส่ง: <strong>${fN(gap)}</strong> หน่วย</span></div>`;
  const stressEl=document.getElementById('stressContainer');
  if(stressEl){stressEl.innerHTML=m0;}

  // Customer service level
  const custMap={};
  d.forEach(r=>{if(!r[1])return;if(!custMap[r[1]])custMap[r[1]]={ord:0,act:0};idxs.forEach(i=>{custMap[r[1]].ord+=r[5][i]||0;custMap[r[1]].act+=r[6][i]||0;});});
  const custList=Object.entries(custMap).map(([nm,v])=>({nm,ord:v.ord,act:v.act,fr:v.ord>0?v.act/v.ord*100:100,gap:v.ord-v.act})).filter(x=>x.ord>0).sort((a,b)=>a.fr-b.fr);

  // Stockout risk items (Diff negative every day)
  const stockoutItems=[];
  d.forEach(r=>{const nm=r[3]||r[2]||'';if(!nm)return;let allNeg=true,has=false;idxs.forEach(i=>{if(r[5][i]>0){has=true;if((r[6][i]||0)>=(r[5][i]||0))allNeg=false;}});if(has&&allNeg){let g2=0;idxs.forEach(i=>{g2+=(r[6][i]||0)-(r[5][i]||0);});stockoutItems.push({nm,gap:g2});}});
  stockoutItems.sort((a,b)=>a.gap-b.gap);

  // m1+m2 → opsGrid
  let opsHtml='<div class="section-card" style="margin:0"><h3>📊 Service Level ตามลูกค้า</h3><div class="table-wrap"><table style="width:100%;font-size:11px;border-collapse:collapse"><thead><tr><th style="padding:6px 8px;background:#f1f5f9">ลูกค้า</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">Fill Rate</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">ยอดขาด</th></tr></thead><tbody>';
  custList.slice(0,15).forEach(c=>{const col=c.fr>=98?'#16a34a':c.fr>=95?'#d97706':'#dc2626';const em=c.fr>=98?'✅':c.fr>=95?'⚠️':'🔴';opsHtml+=`<tr><td style="padding:5px 8px">${em} ${_esc(c.nm)}</td><td style="padding:5px 8px;text-align:right;font-weight:600;color:${col}">${c.fr.toFixed(1)}%</td><td style="padding:5px 8px;text-align:right;color:${c.gap>0?'#dc2626':'#16a34a'}">${fN(c.gap)}</td></tr>`;});
  opsHtml+='</tbody></table></div></div>';
  opsHtml+='<div class="section-card" style="margin:0"><h3>⚡ รายการเสี่ยงขาดส่งทุกวัน</h3>';
  if(!stockoutItems.length){opsHtml+='<div class="rc-item success"><span class="rc-icon">✅</span>ไม่พบรายการ Diff ติดลบทุกวัน</div>';}
  else{opsHtml+=`<div class="rc-item danger" style="margin-bottom:8px"><span class="rc-icon">⚠️</span>พบ <strong>${stockoutItems.length} รายการ</strong> Diff ติดลบตลอดช่วงที่เลือก</div>`;opsHtml+='<div class="table-wrap"><table style="width:100%;font-size:11px;border-collapse:collapse"><thead><tr><th style="padding:6px 8px;background:#f1f5f9">รายการ</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">ยอดขาด</th></tr></thead><tbody>';stockoutItems.slice(0,15).forEach(it=>{opsHtml+=`<tr><td style="padding:5px 8px">🔴 ${_esc(it.nm.length>35?it.nm.slice(0,35)+'…':it.nm)}</td><td style="padding:5px 8px;text-align:right;color:#dc2626;font-weight:600">${fN(it.gap)}</td></tr>`;});opsHtml+='</tbody></table></div>';}
  opsHtml+='</div>';
  const opsEl=document.getElementById('opsGrid');if(opsEl)opsEl.innerHTML=opsHtml;

  // m3 → impactGrid
  const lostRev=gap*_oiUnitPrice;
  let impHtml='<div class="section-card" style="margin:0"><h3>💸 ผลกระทบทางธุรกิจ</h3>';
  impHtml+=`<div class="rc-item ${stressCls}"><span class="rc-icon">💸</span><span>รายได้ที่สูญเสีย (ประมาณ): <strong>฿${lostRev>=1e6?(lostRev/1e6).toFixed(2)+'M':fN(lostRev)}</strong> (ที่ ฿${_oiUnitPrice}/หน่วย)</span></div>`;
  impHtml+=`<div class="rc-item info"><span class="rc-icon">📦</span><span>สินค้าขาดส่ง: <strong>${fN(gap)} หน่วย</strong> — ${stressPct.toFixed(1)}% ของยอดสั่ง</span></div>`;
  if(custList.length){impHtml+='<div style="margin-top:10px"><div class="kpi-label" style="margin-bottom:6px">Top 5 ลูกค้าขาดส่งสูงสุด</div>';custList.slice(0,5).forEach((c,i)=>{impHtml+=`<div class="rank-item"><div><span class="rank-num rank-${i+1}">${i+1}</span>${_esc(c.nm)}</div><span class="${c.gap>0?'neg':'pos'}">${fN(c.gap)} หน่วย</span></div>`;});impHtml+='</div>';}
  impHtml+='</div>';
  const grpMap={};d.forEach(r=>{const g=r[0]||'';if(!grpMap[g])grpMap[g]={ord:0,act:0};idxs.forEach(i=>{grpMap[g].ord+=r[5][i]||0;grpMap[g].act+=r[6][i]||0;});});
  impHtml+='<div class="section-card" style="margin:0"><h3>📈 วิเคราะห์กลุ่ม (Group)</h3><div class="table-wrap"><table style="width:100%;font-size:11px;border-collapse:collapse"><thead><tr><th style="padding:6px 8px;background:#f1f5f9">กลุ่ม</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">ยอดสั่ง</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">ยอดส่ง</th><th style="padding:6px 8px;text-align:right;background:#f1f5f9">Fill Rate</th></tr></thead><tbody>';
  Object.entries(grpMap).filter(([,v])=>v.ord>0).sort((a,b)=>b[1].ord-a[1].ord).forEach(([g,v])=>{const fr2=v.ord>0?v.act/v.ord*100:100;const col2=fr2>=98?'#16a34a':fr2>=95?'#d97706':'#dc2626';impHtml+=`<tr><td style="padding:5px 8px">${_esc(g)}</td><td style="padding:5px 8px;text-align:right">${fN(v.ord)}</td><td style="padding:5px 8px;text-align:right">${fN(v.act)}</td><td style="padding:5px 8px;text-align:right;font-weight:600;color:${col2}">${fr2.toFixed(1)}%</td></tr>`;});
  impHtml+='</tbody></table></div></div>';
  const impactEl=document.getElementById('impactGrid');if(impactEl)impactEl.innerHTML=impHtml;
}

// ═══ RM INTELLIGENCE WRAPPER ═══════════════════════════════════════
function updateRMIntelligence(){
  const di=getDateIdxs(),idxs=di.map(x=>x.i),labels=di.map(x=>x.d);
  const data=filtered.length?filtered:DATA;
  updateRMTab(data,idxs,labels);
}

// ═══ EXECUTIVE SUMMARY ═════════════════════════════════════════════
// NOW receives kpiData (FG Summary rows) + precomputed agg from renderDashboard
// This guarantees Executive Summary uses EXACT SAME numbers as KPI cards.
function updateExecSummary(data,idxs,agg){
  const grid=document.getElementById('execSummaryGrid');
  const ts=document.getElementById('execTimestamp');
  if(!grid) return;
  if(!data.length||!idxs.length){grid.innerHTML='<div style="grid-column:1/-1;text-align:center;color:var(--gray);padding:20px;font-size:12px">ℹ️ ไม่มีข้อมูลในช่วงที่เลือก</div>';return;}
  const fNum=v=>Math.round(v).toLocaleString('th-TH');
  const fBaht=v=>'฿'+(v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toLocaleString('th-TH'));
  // ★ USE precomputed agg — SAME source as KPI cards (NO re-aggregation)
  const totOrd=agg.totOrd, totAct=agg.totAct;
  const totGap=totOrd-totAct;
  const fillRate=totOrd>0?totAct/totOrd*100:0;
  const stressPct=totOrd>0?totGap/totOrd*100:0;
  const siLevel=stressPct<10?'NORMAL':stressPct<25?'TIGHT':'CRITICAL';
  const lostRev=totGap*(_oiUnitPrice||20);
  const custGapMap={};
  data.forEach(r=>{if(skipTotal(r)||!r[1]||!r[1].trim())return;const c=r[1].trim();if(!custGapMap[c])custGapMap[c]={ord:0,act:0};idxs.forEach(i=>{custGapMap[c].ord+=r[5][i]||0;custGapMap[c].act+=r[6][i]||0;});});
  const custGapList=Object.entries(custGapMap).map(([n,v])=>({name:n,gap:v.ord-v.act,ord:v.ord,fr:v.ord>0?v.act/v.ord*100:0})).filter(x=>x.ord>0).sort((a,b)=>b.gap-a.gap);
  const topCust=custGapList[0]||null;
  const grpMap2={};
  data.forEach(r=>{if(skipTotal(r))return;const g=r[0]||'';if(!grpMap2[g])grpMap2[g]={ord:0,act:0};idxs.forEach(i=>{grpMap2[g].ord+=r[5][i]||0;grpMap2[g].act+=r[6][i]||0;});});
  const grpList=Object.entries(grpMap2).map(([n,v])=>({name:n,fr:v.ord>0?v.act/v.ord*100:100,gap:v.ord-v.act,ord:v.ord})).filter(x=>x.ord>0).sort((a,b)=>a.fr-b.fr);
  const worstGrp=grpList[0]||null;
  const dailyGap=idxs.map(i=>data.reduce((s,r)=>skipTotal(r)?s:s+(r[5][i]||0)-(r[6][i]||0),0));
  const dgMean=dailyGap.reduce((s,v)=>s+v,0)/Math.max(dailyGap.length,1);
  const dgSd=Math.sqrt(dailyGap.map(v=>(v-dgMean)**2).reduce((s,v)=>s+v,0)/Math.max(dailyGap.length,1));
  const anomalyCount=dailyGap.filter(v=>v>dgMean+1.2*dgSd).length;
  const anomalyDates=idxs.filter((_,k)=>dailyGap[k]>dgMean+1.2*dgSd).map(i=>{const d2=DATES[i];const[y,m,dd2]=d2.split('-');return dd2+'/'+m+'/'+(+y+543);});
  let highRiskCount=0;
  const prodMap2={};
  data.forEach(r=>{if(skipTotal(r))return;const key=r[1]||(r[0]||'');if(!prodMap2[key])prodMap2[key]={ord:0,act:0};idxs.forEach(i=>{prodMap2[key].ord+=r[5][i]||0;prodMap2[key].act+=r[6][i]||0;});});
  Object.values(prodMap2).forEach(v=>{if(v.ord>0&&v.act/v.ord*100<30)highRiskCount++;});
  const activeCustCount=Object.keys(custGapMap).length;
  let trendDir='stable';
  if(idxs.length>=4){const mid=Math.floor(idxs.length/2);const h1=idxs.slice(0,mid),h2=idxs.slice(mid);const ag=arr=>{let o2=0,a2=0;arr.forEach(i=>data.forEach(r=>{if(!skipTotal(r)){o2+=r[5][i]||0;a2+=r[6][i]||0;}}));return o2>0?(o2-a2)/o2*100:0};const g1=ag(h1),g2=ag(h2);if(g2>g1*1.05)trendDir='up';else if(g1>g2*1.05)trendDir='down';}
  const siTh=siLevel==='NORMAL'?'ปกติ':siLevel==='TIGHT'?'ตึงตัว':'วิกฤต';
  const siChipCls=siLevel==='NORMAL'?'green':siLevel==='TIGHT'?'yellow':'red';
  const siEmoji=siLevel==='NORMAL'?'✅':siLevel==='TIGHT'?'⚠️':'🚨';
  const siIcoCls=siLevel==='NORMAL'?'green':siLevel==='TIGHT'?'amber':'red';
  const trendTh=trendDir==='up'?'แย่ลง ↗':trendDir==='down'?'ดีขึ้น ↘':'ทรงตัว →';
  const trendDot=trendDir==='up'?'red':trendDir==='down'?'green':'gray';
  const rB=arr=>arr.map(b=>`<li class="exec-bullet"><span class="exec-dot ${b.dot}"></span><span>${b.text}</span></li>`).join('');
  const situB=[
    {dot:siChipCls==='green'?'green':siChipCls==='yellow'?'amber':'red',text:`ระบบอยู่ในสถานะ <span class="exec-highlight">${siEmoji} ${siTh}</span> — Stress ${stressPct.toFixed(1)}%`},
    {dot:'blue',text:`Fill Rate รวม: <span class="exec-highlight">${fillRate.toFixed(1)}%</span> — ส่ง ${fNum(totAct)} จาก ${fNum(totOrd)}`},
    {dot:trendDot,text:`แนวโน้ม: <span class="exec-highlight">${trendTh}</span>`},
    {dot:'gray',text:`ครอบคลุม <span class="exec-highlight">${idxs.length} วัน</span> และลูกค้า <span class="exec-highlight">${activeCustCount} ราย</span>`},
  ];
  const issueB=[];
  if(topCust)issueB.push({dot:'red',text:`ลูกค้าขาดส่งสูงสุด: <span class="exec-highlight">${_esc(topCust.name)}</span> — ${fNum(topCust.gap)} หน่วย (FR ${topCust.fr.toFixed(1)}%)`});
  if(worstGrp)issueB.push({dot:'amber',text:`กลุ่ม FR ต่ำสุด: <span class="exec-highlight">${_esc(worstGrp.name)}</span> — ${worstGrp.fr.toFixed(1)}%`});
  if(anomalyCount>0)issueB.push({dot:'amber',text:`วันผิดปกติ: <span class="exec-highlight">${anomalyCount} วัน</span>${anomalyDates.length?' ('+anomalyDates.slice(-2).join(', ')+')'  :''}`});
  if(highRiskCount>0)issueB.push({dot:'red',text:`เสี่ยงขาดสต็อก: <span class="exec-highlight">${highRiskCount} รายการ</span> (FR&lt;30%)`});
  if(!issueB.length)issueB.push({dot:'green',text:'ไม่พบปัญหาเร่งด่วนในช่วงที่เลือก'});
  const impB=[
    {dot:'red',text:`รายได้ที่สูญเสีย: <span class="exec-highlight">${fBaht(lostRev)}</span>`},
    {dot:'amber',text:`สินค้าขาดส่ง: <span class="exec-highlight">${fNum(totGap)} หน่วย</span> (${stressPct.toFixed(1)}%)`},
  ];
  if(topCust)impB.push({dot:'amber',text:`ลูกค้า <span class="exec-highlight">${_esc(topCust.name)}</span>: สูญรายได้ ${fBaht(topCust.gap*(_oiUnitPrice||20))}`});
  const recB=[];
  if(siLevel==='CRITICAL'){recB.push({dot:'red',text:'เร่งตรวจสอบกำลังผลิตและแผนส่งสินค้าด่วน'});recB.push({dot:'red',text:'จัดประชุม war room ทีม Operations + Sales'});}
  else if(siLevel==='TIGHT')recB.push({dot:'amber',text:'เฝ้าระวังกำลังผลิตและปรับแผนสำรองล่วงหน้า'});
  if(topCust)recB.push({dot:'amber',text:`จัดลำดับส่งสินค้าให้ <span class="exec-highlight">${_esc(topCust.name)}</span> ก่อน`});
  if(highRiskCount>0)recB.push({dot:'amber',text:`ตรวจสอบ ${highRiskCount} รายการเสี่ยงขาดสต็อก`});
  if(trendDir==='down')recB.push({dot:'green',text:'แนวโน้มกำลังดีขึ้น — รักษาและติดตามต่อเนื่อง'});
  if(!recB.length)recB.push({dot:'green',text:'ระบบทำงานปกติ — ติดตามตัวชี้วัดตามแผน'});
  const c1=`<div class="exec-card"><div class="exec-card-hdr"><div class="exec-card-ico ${siIcoCls}">📈</div><div class="exec-card-ttl">สถานการณ์ภาพรวม</div></div><div class="exec-status-row"><span class="exec-chip ${siChipCls}">${siEmoji} ${siTh}</span><span style="font-size:10px;color:#6b7280">Stress ${stressPct.toFixed(1)}%</span></div><ul class="exec-bullets">${rB(situB)}</ul></div>`;
  const c2=`<div class="exec-card"><div class="exec-card-hdr"><div class="exec-card-ico red">🔍</div><div class="exec-card-ttl">ปัญหาหลักที่พบ</div></div><ul class="exec-bullets">${rB(issueB)}</ul></div>`;
  const c3=`<div class="exec-card"><div class="exec-card-hdr"><div class="exec-card-ico amber">💸</div><div class="exec-card-ttl">ผลกระทบทางธุรกิจ</div></div><ul class="exec-bullets">${rB(impB)}</ul><div style="margin-top:8px;font-size:9.5px;color:#9ca3af">* ประมาณการที่ ฿${_oiUnitPrice||20}/หน่วย</div></div>`;
  const c4=`<div class="exec-card"><div class="exec-card-hdr"><div class="exec-card-ico blue">✅</div><div class="exec-card-ttl">คำแนะนำ</div></div><ul class="exec-bullets">${rB(recB.slice(0,5))}</ul></div>`;
  grid.innerHTML=c1+c2+c3+c4;
}


// ═══════════════════════════════════════════════════════════════════════
// AI DECISION ENGINE  —  all computations use live DATA + date indices
// ═══════════════════════════════════════════════════════════════════════

// ── Internal helpers ──────────────────────────────────────────────────

/** Single-pass FG aggregation: returns { [customer]: {group,ord,act} } */
function _aggFG(data, idxs) {
  const out = {};
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!r[1] || skipTotal(r)) continue;
    const c = r[1], g = r[0] || '';
    if (!out[c]) out[c] = { group: g, ord: 0, act: 0 };
    for (let j = 0; j < idxs.length; j++) {
      const i = idxs[j];
      out[c].ord += r[5][i] || 0;
      out[c].act += r[6][i] || 0;
    }
  }
  return out;
}

/** Single-pass RM aggregation: returns { [code]: {name,demand,actual,shortage,coverage} } */
function _aggRM(data, idxs) {
  const out = {};
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (skipTotal(r)) continue;
    const arr8 = r[8] || [], arr9 = r[9] || [];
    const hasRM = arr8.some(v => v > 0) || arr9.some(v => v > 0);
    if (!hasRM) continue;
    const code = r[4] || r[3] || '', name = r[3] || r[2] || code;
    if (!code) continue;
    if (!out[code]) out[code] = { name, demand: 0, actual: 0 };
    for (let j = 0; j < idxs.length; j++) {
      const i = idxs[j];
      out[code].demand += arr8[i] || 0;
      out[code].actual += arr9[i] || 0;
    }
  }
  // derive shortage + coverage
  const codes = Object.keys(out);
  for (let k = 0; k < codes.length; k++) {
    const v = out[codes[k]];
    v.shortage = v.demand > v.actual ? v.demand - v.actual : 0;
    v.coverage = v.demand > 0 ? v.actual / v.demand : 1;
  }
  return out;
}

// ── 1. detectTopShortage ─────────────────────────────────────────────
/** Top N RM materials by Shortage KG. Returns array sorted desc. */
function detectTopShortage(data, idxs, n) {
  n = n || 5;
  const rm = _aggRM(data, idxs);
  const arr = [];
  const codes = Object.keys(rm);
  for (let k = 0; k < codes.length; k++) {
    const v = rm[codes[k]];
    if (v.shortage > 0) arr.push({ code: codes[k], name: v.name, demand: v.demand, actual: v.actual, shortage: v.shortage, coverage: v.coverage });
  }
  arr.sort((a, b) => b.shortage - a.shortage);
  return arr.slice(0, n);
}

// ── 2. detectLowCoverage ─────────────────────────────────────────────
/** Customers with FG Fill Rate below threshold (default 50%). */
function detectLowCoverage(data, idxs, threshold) {
  threshold = (threshold !== undefined) ? threshold : 0.50;
  const fg = _aggFG(data, idxs);
  const arr = [];
  const keys = Object.keys(fg);
  for (let k = 0; k < keys.length; k++) {
    const v = fg[keys[k]];
    if (v.ord > 0) {
      const fr = v.act / v.ord;
      if (fr < threshold) arr.push({ cust: keys[k], group: v.group, ord: v.ord, act: v.act, fillRate: fr * 100, gap: v.ord - v.act });
    }
  }
  arr.sort((a, b) => a.fillRate - b.fillRate);
  return arr;
}

// ── 3. groupPerformance ──────────────────────────────────────────────
/** Fill Rate and gap by Supply Group, sorted best→worst. */
function groupPerformance(data, idxs) {
  const grp = {};
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!r[1] || skipTotal(r)) continue;
    const g = r[0] || 'Unknown';
    if (!grp[g]) grp[g] = { ord: 0, act: 0 };
    for (let j = 0; j < idxs.length; j++) {
      const i = idxs[j];
      grp[g].ord += r[5][i] || 0;
      grp[g].act += r[6][i] || 0;
    }
  }
  const arr = [];
  const keys = Object.keys(grp);
  for (let k = 0; k < keys.length; k++) {
    const v = grp[keys[k]];
    if (v.ord > 0) arr.push({ group: keys[k], ord: v.ord, act: v.act, fillRate: v.ord > 0 ? v.act / v.ord * 100 : 100, gap: v.ord - v.act });
  }
  arr.sort((a, b) => a.fillRate - b.fillRate);
  return arr;
}

// ── 4. detectAnomaly ─────────────────────────────────────────────────
/** Detect zero-delivery events and sudden demand spikes from real rows. */
function detectAnomaly(data, idxs) {
  const anomalies = [];
  if (idxs.length < 2) return anomalies;
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!r[1] || skipTotal(r)) continue;
    const nm = r[3] || r[2] || '', cust = r[1];

    // Compute per-row avg demand over period
    let sumD = 0, cntD = 0;
    for (let j = 0; j < idxs.length; j++) { const v = r[5][idxs[j]] || 0; if (v > 0) { sumD += v; cntD++; } }
    const avgD = cntD > 0 ? sumD / cntD : 0;

    for (let j = 0; j < idxs.length; j++) {
      const i = idxs[j];
      const ord = r[5][i] || 0, act = r[6][i] || 0;
      // Zero delivery with meaningful demand
      if (ord > 200 && act === 0) {
        anomalies.push({ type: 'zero_delivery', item: nm, cust, date: DATES[i], demand: ord,
          severity: ord > 1500 ? 'critical' : 'warning' });
      }
      // Demand spike: >3x row average and >500 units
      if (cntD >= 3 && avgD > 0 && ord > avgD * 3 && ord > 500) {
        anomalies.push({ type: 'demand_spike', item: nm, cust, date: DATES[i], demand: ord, avg: avgD,
          severity: ord > avgD * 5 ? 'critical' : 'warning' });
      }
    }
  }
  // Deduplicate by item+date, sort critical first then by demand
  const seen = new Set();
  const uniq = anomalies.filter(a => { const k = a.type + '|' + a.item + '|' + a.date; if (seen.has(k)) return false; seen.add(k); return true; });
  uniq.sort((a, b) => { if (a.severity !== b.severity) return a.severity === 'critical' ? -1 : 1; return b.demand - a.demand; });
  return uniq.slice(0, 25);
}

// ── 5. trendSummary ──────────────────────────────────────────────────
/** Compare early half vs late half of selected period. */
function trendSummary(data, idxs) {
  if (idxs.length < 4) return null;
  const mid = Math.floor(idxs.length / 2);
  const ei = idxs.slice(0, mid), li = idxs.slice(mid);
  let eO = 0, eA = 0, lO = 0, lA = 0;
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!r[1] || skipTotal(r)) continue;
    for (let j = 0; j < ei.length; j++) { eO += r[5][ei[j]] || 0; eA += r[6][ei[j]] || 0; }
    for (let j = 0; j < li.length; j++) { lO += r[5][li[j]] || 0; lA += r[6][li[j]] || 0; }
  }
  const eFR = eO > 0 ? eA / eO * 100 : 0, lFR = lO > 0 ? lA / lO * 100 : 0;
  const delta = lFR - eFR;
  const demDelta = eO > 0 ? (lO - eO) / eO * 100 : 0;
  return {
    early: { start: DATES[idxs[0]], end: DATES[idxs[mid - 1]], ord: eO, act: eA, fr: eFR },
    late:  { start: DATES[idxs[mid]], end: DATES[idxs[idxs.length - 1]], ord: lO, act: lA, fr: lFR },
    frDelta: delta, demandDelta: demDelta,
    trend: delta > 2 ? 'improving' : delta < -2 ? 'declining' : 'stable'
  };
}

// ── generateInsight — master function ───────────────────────────────
function generateInsight(data, idxs) {
  const topRM  = detectTopShortage(data, idxs, 1);
  const lowCov = detectLowCoverage(data, idxs, 0.50);
  const grp    = groupPerformance(data, idxs);
  const trend  = trendSummary(data, idxs);
  const crit   = topRM[0], worstGrp = grp[0], bestGrp = grp[grp.length - 1];
  let keyInsight = '', rootCause = '', businessImpact = '', recommendation = '';
  if (crit) {
    keyInsight = `RM "${crit.name}" Shortage ${fN(crit.shortage)} KG (Coverage ${(crit.coverage*100).toFixed(1)}%)`;
    rootCause  = crit.coverage < 0.3 ? 'Supply ขาดหนัก — Supplier อาจส่งล่าช้าหรือ Lead Time ยาวเกิน'
               : crit.coverage < 0.7 ? 'ปริมาณจัดซื้อต่ำกว่าแผน — ตรวจสอบ PO vs Delivery'
               : 'Demand สูงกว่าคาด — ทบทวน Forecast';
  }
  if (worstGrp && worstGrp.fillRate < 95)
    businessImpact = `กลุ่ม "${worstGrp.group}" Fill Rate ${worstGrp.fillRate.toFixed(1)}% (ขาดส่ง ${fN(worstGrp.gap)} หน่วย)`;
  if (trend)
    recommendation = trend.trend === 'improving' ? `Fill Rate เพิ่ม ${trend.frDelta.toFixed(1)}pp — รักษา momentum นี้ไว้`
                   : trend.trend === 'declining'  ? `เร่งด่วน: Fill Rate ลด ${Math.abs(trend.frDelta).toFixed(1)}pp — ทบทวนแผน Procurement ทันที`
                   : 'สถานการณ์คงที่ — ติดตาม RM Shortage เป็นหลัก';
  return { keyInsight, rootCause, businessImpact, recommendation };
}

// ── runAnalysis — STUBBED (AI disabled) ──────────────────────────────
function runAnalysis(type) {
  addMsg('AI analysis is temporarily disabled', 'bot');
  return;
  let label = '', html_out = '';

  if (type === 'topShortage') {
    label = 'Top 5 RM Shortage สูงสุด';
    const res = detectTopShortage(data, idxs, 5);
    if (!res.length) { html_out = 'ไม่พบ RM Shortage ในช่วงที่เลือก ✅'; }
    else {
      html_out = `<strong>Top ${res.length} RM Shortage สูงสุด</strong><br>`;
      res.forEach((v, i) => {
        const covPct = (v.coverage * 100).toFixed(1);
        const sev = v.coverage < 0.5 ? '🔴' : v.coverage < 0.8 ? '⚠️' : '🟡';
        html_out += `${i+1}. ${sev} <strong>${_esc(v.name)}</strong> (${v.code})<br>`;
        html_out += `&nbsp;&nbsp;&nbsp;Shortage: <strong>${fN(v.shortage)} KG</strong> | Coverage: ${covPct}% | Demand: ${fN(v.demand)} KG<br>`;
      });
      const ins = generateInsight(data, idxs);
      if (ins.rootCause) html_out += `<br>[สาเหตุ] ${ins.rootCause}<br>[คำแนะนำ] ${ins.recommendation}`;
    }
  }
  else if (type === 'lowCoverage') {
    label = 'ลูกค้า Fill Rate ต่ำกว่า 50%';
    const res = detectLowCoverage(data, idxs, 0.50);
    if (!res.length) { html_out = 'ทุกลูกค้ามี Fill Rate ≥ 50% ✅'; }
    else {
      html_out = `<strong>พบ ${res.length} ลูกค้า Fill Rate &lt; 50%</strong><br>`;
      res.slice(0, 8).forEach((v, i) => {
        html_out += `${i+1}. <strong>${_esc(v.cust)}</strong> [${_esc(v.group)}]<br>`;
        html_out += `&nbsp;&nbsp;&nbsp;Fill Rate: <strong style="color:#c53030">${v.fillRate.toFixed(1)}%</strong> | ขาด: ${fN(v.gap)} หน่วย (จาก ${fN(v.ord)})<br>`;
      });
      if (res.length > 8) html_out += `<em>...และอีก ${res.length - 8} ราย</em><br>`;
      html_out += `<br>[ผลกระทบ] ยอดขาดส่งรวม: ${fN(res.reduce((s,v)=>s+v.gap,0))} หน่วย`;
    }
  }
  else if (type === 'groupPerf') {
    label = 'เปรียบเทียบ Fill Rate ตามกลุ่ม';
    const res = groupPerformance(data, idxs);
    if (!res.length) { html_out = 'ไม่มีข้อมูลกลุ่ม'; }
    else {
      html_out = `<strong>Fill Rate ตามกลุ่ม (${res.length} กลุ่ม)</strong><br>`;
      res.forEach((v, i) => {
        const em = v.fillRate >= 98 ? '✅' : v.fillRate >= 90 ? '⚠️' : '🔴';
        const col = v.fillRate >= 98 ? '#276749' : v.fillRate >= 90 ? '#b7791f' : '#c53030';
        html_out += `${i+1}. ${em} <strong>${_esc(v.group)}</strong>: `;
        html_out += `<strong style="color:${col}">${v.fillRate.toFixed(1)}%</strong>`;
        html_out += ` | สั่ง ${fN(v.ord)} | ส่ง ${fN(v.act)} | ขาด ${fN(v.gap)}<br>`;
      });
    }
  }
  else if (type === 'anomaly') {
    label = 'ตรวจสอบ Anomaly';
    const res = detectAnomaly(data, idxs);
    if (!res.length) { html_out = 'ไม่พบ Anomaly ในช่วงที่เลือก ✅'; }
    else {
      const zeros = res.filter(a => a.type === 'zero_delivery');
      const spikes = res.filter(a => a.type === 'demand_spike');
      html_out = `<strong>พบ Anomaly ${res.length} รายการ</strong> (ส่ง 0: ${zeros.length} | Spike: ${spikes.length})<br>`;
      if (zeros.length) {
        html_out += `<br><strong>🔴 ส่ง 0 แต่มี Demand (${zeros.length} รายการ):</strong><br>`;
        zeros.slice(0, 6).forEach(a => {
          html_out += `• ${a.date} — <strong>${_esc(a.item)}</strong> [${_esc(a.cust)}] Demand: ${fN(a.demand)}<br>`;
        });
        if (zeros.length > 6) html_out += `<em>...และอีก ${zeros.length-6} รายการ</em><br>`;
      }
      if (spikes.length) {
        html_out += `<br><strong>⚡ Demand Spike (${spikes.length} รายการ):</strong><br>`;
        spikes.slice(0, 5).forEach(a => {
          html_out += `• ${a.date} — <strong>${_esc(a.item)}</strong>: ${fN(a.demand)} หน่วย (avg ${fN(a.avg|0)})<br>`;
        });
      }
    }
  }
  else if (type === 'trend') {
    label = 'วิเคราะห์แนวโน้ม (ต้น vs ปลายงวด)';
    const res = trendSummary(data, idxs);
    if (!res) { html_out = 'ต้องการข้อมูลอย่างน้อย 4 วัน'; }
    else {
      const em = res.trend === 'improving' ? '📈' : res.trend === 'declining' ? '📉' : '➡️';
      const trendTh = res.trend === 'improving' ? 'ดีขึ้น' : res.trend === 'declining' ? 'แย่ลง' : 'คงที่';
      html_out = `<strong>${em} แนวโน้ม Fill Rate ${trendTh} ${Math.abs(res.frDelta).toFixed(1)}pp</strong><br>`;
      html_out += `ช่วงต้น (${res.early.start} → ${res.early.end}):<br>`;
      html_out += `&nbsp;&nbsp;Fill Rate <strong>${res.early.fr.toFixed(1)}%</strong> | สั่ง ${fN(res.early.ord)} | ส่ง ${fN(res.early.act)}<br>`;
      html_out += `ช่วงหลัง (${res.late.start} → ${res.late.end}):<br>`;
      html_out += `&nbsp;&nbsp;Fill Rate <strong>${res.late.fr.toFixed(1)}%</strong> | สั่ง ${fN(res.late.ord)} | ส่ง ${fN(res.late.act)}<br>`;
      html_out += `<br>Demand ${res.demandDelta >= 0 ? '+' : ''}${res.demandDelta.toFixed(1)}% เทียบช่วงต้น<br>`;
      const ins = generateInsight(data, idxs);
      html_out += `[คำแนะนำ] ${ins.recommendation}`;
    }
  }
  else if (type === 'fullReport') {
    label = 'รายงานสถานการณ์ครบถ้วน';
    const ins = generateInsight(data, idxs);
    const topRM = detectTopShortage(data, idxs, 3);
    const lowC = detectLowCoverage(data, idxs, 0.70);
    const grp = groupPerformance(data, idxs);
    const trend = trendSummary(data, idxs);
    html_out = '<strong>📋 รายงานสถานการณ์ Supply Chain</strong><br><br>';
    if (ins.keyInsight) html_out += `[Insight] ${ins.keyInsight}<br>`;
    if (ins.rootCause)  html_out += `[สาเหตุ] ${ins.rootCause}<br>`;
    if (ins.businessImpact) html_out += `[ผลกระทบ] ${ins.businessImpact}<br>`;
    if (ins.recommendation) html_out += `[คำแนะนำ] ${ins.recommendation}<br>`;
    if (topRM.length) { html_out += `<br><strong>RM เร่งด่วน:</strong> `; html_out += topRM.map(v => `${_esc(v.name)} −${fN(v.shortage)}KG`).join(' | '); html_out += '<br>'; }
    if (grp.length) { html_out += `<br><strong>กลุ่มที่ต้องระวัง:</strong> `; html_out += grp.filter(g => g.fillRate < 95).map(g => `${_esc(g.group)} ${g.fillRate.toFixed(1)}%`).join(' | ') || 'ทุกกลุ่มปกติ'; html_out += '<br>'; }
    if (trend) { const em = trend.trend === 'improving' ? '📈' : trend.trend === 'declining' ? '📉' : '➡️'; html_out += `<br>${em} Fill Rate ${trend.frDelta >= 0 ? '+' : ''}${trend.frDelta.toFixed(1)}pp จากช่วงต้น`; }
  }
  addMsg(label, 'user');
  setTimeout(() => addMsg(html_out, 'bot'), 150);
}

// ── updateAIInsights — STUBBED (AI disabled) ─────────────────────────
function updateAIInsights(data, idxs) {
  const grid = document.getElementById('aiInsightsGrid');
  const ts   = document.getElementById('aiTimestamp');
  if (!grid) return;
  grid.innerHTML = '<div class="rc-item info" style="grid-column:1/-1">'  +
    '<span class="rc-icon">🔧</span>'  +
    '<span>AI analysis temporarily disabled — dashboard data is unaffected</span>'  +
    '</div>';
  // STUB: all analysis removed from core flow

  // Run all 5 modules
  const topRM   = detectTopShortage(d, idxs, 3);
  const lowCov  = detectLowCoverage(d, idxs, 0.50);
  const grp     = groupPerformance(d, idxs);
  const anomaly = detectAnomaly(d, idxs);
  const trend   = trendSummary(d, idxs);

  let cards = '';

  // Card 1: Critical — top RM shortage
  if (topRM.length) {
    const top = topRM[0];
    const covPct = (top.coverage * 100).toFixed(1);
    const sev = top.coverage < 0.5 ? 'danger' : 'warn';
    cards += `<div class="rc-item ${sev}">
      <span class="rc-icon">🚨</span>
      <div style="flex:1">
        <strong>[Critical] RM Shortage สูงสุด</strong><br>
        <strong>${_esc(top.name)}</strong> (${_esc(top.code)}):
        ขาด <strong>${fN(top.shortage)} KG</strong> | Coverage: <strong>${covPct}%</strong><br>
        <span style="font-size:10px;opacity:.75">Demand: ${fN(top.demand)} KG | Delivered: ${fN(top.actual)} KG</span>
        ${topRM.length > 1 ? `<br><span style="font-size:10px;opacity:.75">และ RM อีก ${topRM.length-1} รายการมี Shortage</span>` : ''}
      </div>
    </div>`;
  } else {
    cards += `<div class="rc-item success"><span class="rc-icon">✅</span><strong>[OK] ไม่พบ RM Shortage</strong> ในช่วงที่เลือก</div>`;
  }

  // Card 2: Warning — low coverage cluster
  if (lowCov.length) {
    const w = lowCov[0];
    cards += `<div class="rc-item warn">
      <span class="rc-icon">⚠️</span>
      <div style="flex:1">
        <strong>[Warning] ${lowCov.length} ลูกค้า Fill Rate &lt; 50%</strong><br>
        ต่ำสุด: <strong>${_esc(w.cust)}</strong> [${_esc(w.group)}] — <strong>${w.fillRate.toFixed(1)}%</strong> (ขาด ${fN(w.gap)} หน่วย)<br>
        <span style="font-size:10px;opacity:.75">รวมยอดขาดส่ง: ${fN(lowCov.reduce((s,v)=>s+v.gap,0))} หน่วย</span>
      </div>
    </div>`;
  } else {
    cards += `<div class="rc-item success"><span class="rc-icon">✅</span><strong>[OK] ทุกลูกค้า Fill Rate ≥ 50%</strong></div>`;
  }

  // Card 3: Trend (Opportunity or Alert)
  if (trend) {
    const trendIcon = trend.trend === 'improving' ? '📈' : trend.trend === 'declining' ? '📉' : '➡️';
    const trendCls  = trend.trend === 'improving' ? 'success' : trend.trend === 'declining' ? 'danger' : 'info';
    const trendTh   = trend.trend === 'improving' ? 'ดีขึ้น' : trend.trend === 'declining' ? 'แย่ลง' : 'คงที่';
    cards += `<div class="rc-item ${trendCls}">
      <span class="rc-icon">${trendIcon}</span>
      <div style="flex:1">
        <strong>[Trend] Fill Rate ${trendTh} ${Math.abs(trend.frDelta).toFixed(1)}pp</strong><br>
        ช่วงต้น: <strong>${trend.early.fr.toFixed(1)}%</strong> (${trend.early.start}) →
        ช่วงหลัง: <strong>${trend.late.fr.toFixed(1)}%</strong> (${trend.late.end})<br>
        <span style="font-size:10px;opacity:.75">Demand ${trend.demandDelta >= 0 ? '+' : ''}${trend.demandDelta.toFixed(1)}% | Anomaly: ${anomaly.length} รายการ</span>
      </div>
    </div>`;
  }

  // Card 4: Anomaly summary (if exists)
  if (anomaly.length) {
    const zeros  = anomaly.filter(a => a.type === 'zero_delivery').length;
    const spikes = anomaly.filter(a => a.type === 'demand_spike').length;
    const top    = anomaly[0];
    cards += `<div class="rc-item danger">
      <span class="rc-icon">🔎</span>
      <div style="flex:1">
        <strong>[Anomaly] พบ ${anomaly.length} ความผิดปกติ</strong> (ส่ง 0: ${zeros} | Spike: ${spikes})<br>
        สำคัญสุด: <strong>${_esc(top.item)}</strong> [${top.date}] — ${top.type === 'zero_delivery' ? 'Demand ' + fN(top.demand) + ' แต่ส่ง 0' : 'Spike ' + fN(top.demand) + ' หน่วย'}<br>
        <span style="font-size:10px;opacity:.75">กด "Anomaly" ใน AI Agent เพื่อดูรายละเอียด</span>
      </div>
    </div>`;
  }

  grid.innerHTML = cards;
}


// ═══ RM INTELLIGENCE TAB ══════════════════════════════════════════
let rmDetailPage=1,_rmDetailCache=null; // chart vars removed (performance)

function updateRMTab(data,idxs,labels){
  const rmDemand={},rmShortage={},rmCode={},rmFG={},rmNameMap={};
  const dailyDemand=new Array(idxs.length).fill(0),dailyShortage=new Array(idxs.length).fill(0);
  for(let ri=0;ri<data.length;ri++){
    const r=data[ri];if(skipTotal(r))continue;
    const key=r[4]||r[3]||'';if(!key)continue;
    const nm=r[3]||r[2]||key;
    const ord8=r[8]||[],act9=r[9]||[];
    const hasRMData=ord8.some(v=>v>0)||act9.some(v=>v>0);
    if(!hasRMData)continue;
    if(!rmDemand[key]){rmDemand[key]=0;rmShortage[key]=0;rmCode[key]=r[4]||'';rmFG[key]=new Set();rmNameMap[key]=nm;}
    if(r[1]&&r[1]!=='')rmFG[key].add(r[1]);
    for(let j=0;j<idxs.length;j++){const di2=idxs[j];const o=+(ord8[di2])||0,a=+(act9[di2])||0;rmDemand[key]+=o;const sh=o-a;if(sh>0)rmShortage[key]+=sh;dailyDemand[j]+=o;if(sh>0)dailyShortage[j]+=sh;}
  }
  _applyRMData({rmDemand,rmShortage,rmCode,rmFG,rmNameMap,dailyDemand,dailyShortage},labels);
}

function _applyRMData(d,labels){
  const {rmDemand,rmShortage,rmCode,rmFG,rmNameMap,dailyDemand,dailyShortage}=d;
  let totalDemand=0,totalShortage=0;
  const rmNames=Object.keys(rmDemand);
  for(const nm of rmNames){totalDemand+=rmDemand[nm];totalShortage+=rmShortage[nm];}
  let topShortRM='',topShortVal=0;
  for(const nm of rmNames){if(rmShortage[nm]>topShortVal){topShortVal=rmShortage[nm];topShortRM=nm;}}
  let peakDay='',peakDayVal=0;
  for(let j=0;j<dailyDemand.length;j++){if(dailyDemand[j]>peakDayVal){peakDayVal=dailyDemand[j];peakDay=labels[j]||'';}}
  _setRMKPI('rmKpiDemand',fN2(totalDemand),'KG','neutral');
  _setRMKPI('rmKpiShortage',fN2(totalShortage),'KG',totalShortage>0?'negative':'positive');
  _setRMKPI('rmKpiTopRM',(rmNameMap&&rmNameMap[topShortRM]?rmNameMap[topShortRM]+' ('+topShortRM+')':topShortRM)||'-',fN2(topShortVal)+' KG',topShortVal>0?'negative':'neutral');
  _setRMKPI('rmKpiPeakDay',peakDay||'-',fN2(peakDayVal)+' KG','neutral');
  _renderRMGaugeExec(rmDemand,rmShortage,rmFG);
  _renderRMHeatmap(rmDemand,rmShortage,rmCode,rmFG,rmNameMap);
  _renderRMRanking(rmDemand,rmShortage,rmCode,rmFG,rmNameMap);
  _renderRMRiskTables(rmDemand,rmShortage,rmCode,rmFG,rmNameMap);
  _buildRMDetailCache(rmDemand,rmShortage,rmCode,rmFG,labels,rmNameMap);
  rmDetailPage=1;_renderRMDetailPage();
}

function _setRMKPI(id,v,sub,cls){const el=document.getElementById(id);if(!el)return;el.textContent=v;const subEl=document.getElementById(id+'Sub');if(subEl)subEl.textContent=sub;const card=el.closest('.kpi-card');if(card)card.className='kpi-card '+cls;}

function _renderRMGaugeExec(rmDemand,rmShortage,rmFG){
  const codes=Object.keys(rmDemand);let totDem=0,totSh=0,totImpact=0,critCount=0,covSum=0,covN=0;
  for(const c of codes){const dem=rmDemand[c]||0,sh=rmShortage[c]||0,fc=rmFG[c]?rmFG[c].size:0;const impact=fc*sh;totDem+=dem;totSh+=sh;totImpact+=impact;if(dem>0){covSum+=(dem-sh)/dem;covN++;}if(impact>3000)critCount++;}
  const riskIdx=totDem>0?totSh/totDem:0,avgCov=covN>0?covSum/covN:null;
  const fmt=n=>n.toLocaleString('th-TH',{maximumFractionDigits:0});
  const pctFmt=v=>v===null?'N/A':(v*100).toFixed(1)+'%';
  const ARC_LEN=245,fill=Math.min(1,riskIdx)*ARC_LEN;
  const gaugeColor=riskIdx<0.05?'#22c55e':riskIdx<0.15?'#f97316':'#dc2626';
  const arc=document.getElementById('rmGaugeArc');
  if(arc){arc.setAttribute('stroke-dasharray',fill+' '+ARC_LEN);arc.setAttribute('stroke',gaugeColor);}
  const pctEl=document.getElementById('rmGaugePct');if(pctEl){pctEl.textContent=(riskIdx*100).toFixed(1)+'%';pctEl.setAttribute('fill',gaugeColor);}
  const _set=(id2,v2)=>{const el=document.getElementById(id2);if(el)el.textContent=v2;};
  _set('rmExecShortage',fmt(totSh));_set('rmExecCritical',critCount);_set('rmExecCoverage',pctFmt(avgCov));_set('rmExecImpact',fmt(totImpact));
  const critEl=document.getElementById('rmExecCritical');if(critEl)critEl.style.color=critCount>0?'#dc2626':'#16a34a';
}

function _renderRMHeatmap(rmDemand,rmShortage,rmCode,rmFG,rmNameMap){
  const grid=document.getElementById('rmHeatmapGrid');if(!grid)return;
  const codes=Object.keys(rmDemand);
  if(!codes.length){grid.innerHTML='<div style="color:#94a3b8;font-size:12px;padding:20px;grid-column:1/-1;text-align:center">ไม่มีข้อมูล RM</div>';return;}
  const fmt=n=>n.toLocaleString('th-TH',{maximumFractionDigits:0});
  const items=codes.map(code=>{const dem=rmDemand[code]||0,sh=rmShortage[code]||0,fc=rmFG[code]?rmFG[code].size:0,impact=fc*sh,cov=dem>0?(dem-sh)/dem:null,nm=rmNameMap&&rmNameMap[code]?rmNameMap[code]:rmCode[code]||code;
    let cls,label;
    if(impact>3000){cls='rm-heat-critical';label='Critical';}
    else if(sh>0&&fc>=3){cls='rm-heat-high';label='High';}
    else if(sh>0&&(cov!==null&&cov<0.9||sh>50)){cls='rm-heat-warning';label='Warning';}
    else if(sh===0&&dem>0){cls='rm-heat-normal';label='Normal';}
    else{cls='rm-heat-none';label='No Data';}
    return{code,nm,sh,fc,impact,cov,cls,label};
  }).sort((a,b)=>b.impact-a.impact);
  grid.innerHTML=items.map(it=>{const covStr=it.cov!==null?(it.cov*100).toFixed(1)+'%':'N/A';
    return `<div class="rm-heat-tile ${it.cls}" title="FG:${it.fc}|Cov:${covStr}|${it.label}|Short:${fmt(it.sh)}" onclick="_heatmapClick(${JSON.stringify(it.code)})"><div class="rm-heat-name">${_esc(it.nm)}</div><div><div class="rm-heat-impact">Impact:${fmt(it.impact)}</div><div class="rm-heat-short">Short:${fmt(it.sh)}KG</div></div></div>`;
  }).join('');
}

function _heatmapClick(code){switchTab('rmreport');_ric(()=>{const el=document.getElementById('rmrFCode');if(el){el.value=code;}_rmrPage=1;if(_lastFilteredRows)updateRMReportTab(_lastFilteredRows,_lastIdxs,_lastLabels);});}

function _renderRMRanking(rmDemand,rmShortage,rmCode,rmFG,rmNameMap){
  const tbody=document.getElementById('rmRankingBody');if(!tbody)return;
  const fmt=n=>n.toLocaleString('th-TH',{maximumFractionDigits:0});
  const pct=v=>v===null?'N/A':(v*100).toFixed(1)+'%';
  const rows=Object.keys(rmDemand).map(code=>{const dem=rmDemand[code]||0,sh=rmShortage[code]||0,fc=rmFG[code]?rmFG[code].size:0,impact=fc*sh,cov=dem>0?(dem-sh)/dem:null;return{code,nm:rmNameMap&&rmNameMap[code]?rmNameMap[code]+' ('+code+')':rmCode[code]||code,sh,fc,cov,impact};}).filter(r=>r.sh>0).sort((a,b)=>b.impact-a.impact).slice(0,10);
  if(!rows.length){tbody.innerHTML='<tr><td colspan="6" style="text-align:center;color:#16a34a;padding:16px;font-weight:600">✓ ไม่มี RM Shortage</td></tr>';return;}
  tbody.innerHTML=rows.map((r,i)=>{const rank=i+1,isCrit=r.impact>3000,badgeCls=rank<=3?`rm-rank-${rank}`:'rm-rank-n',rowCls=isCrit?'class="rm-rank-crit"':'',covColor=r.cov===null?'':(r.cov>=1?'color:#16a34a':r.cov>=0.9?'color:#f97316':'color:#dc2626;font-weight:700');
    return `<tr ${rowCls}><td><span class="rm-rank-badge ${badgeCls}">${rank}</span></td><td style="font-weight:${rank<=3?700:400}">${_esc(r.nm)}</td><td class="num" style="color:#dc2626;font-weight:600">${fmt(r.sh)}</td><td class="num">${r.fc}</td><td class="num" style="${covColor}">${pct(r.cov)}</td><td class="num" style="font-weight:700;color:${r.impact>3000?'#b91c1c':r.impact>1000?'#c2410c':'#374151'}">${fmt(r.impact)}</td></tr>`;
  }).join('');
}

// _renderRMDemandChart removed (chart eliminated for performance)

// _renderRMShortageChart removed (chart eliminated for performance)

function _renderRMRiskTables(rmDemand,rmShortage,rmCode,rmFG,rmNameMap){
  const _rn=key=>rmNameMap&&rmNameMap[key]?rmNameMap[key]+' ('+key+')':key;
  const demandArr=Object.entries(rmDemand).sort((a,b)=>b[1]-a[1]).slice(0,20);
  let h1='';demandArr.forEach(([nm,v],i)=>{h1+=`<tr><td>${i+1}</td><td>${_esc(_rn(nm))}</td><td style="text-align:right;font-weight:600">${fN2(v)}</td></tr>`;});
  const tb1=document.getElementById('rmTopDemandBody');if(tb1)tb1.innerHTML=h1;
  const shortArr=Object.entries(rmShortage).filter(e=>e[1]>0).sort((a,b)=>b[1]-a[1]).slice(0,20);
  let h2='';shortArr.forEach(([nm,v],i)=>{h2+=`<tr><td>${i+1}</td><td>${_esc(_rn(nm))}</td><td style="text-align:right;font-weight:600;color:#dc2626">${fN2(v)}</td></tr>`;});
  const tb2=document.getElementById('rmTopShortageBody');if(tb2)tb2.innerHTML=h2;
  const usageArr=Object.entries(rmFG).map(([nm,st])=>[nm,st.size]).filter(e=>e[1]>0).sort((a,b)=>b[1]-a[1]).slice(0,20);
  let h3='';usageArr.forEach(([nm,cnt],i)=>{h3+=`<tr><td>${i+1}</td><td>${_esc(_rn(nm))}</td><td style="text-align:right;font-weight:600;color:#059669">${cnt}</td></tr>`;});
  const tb3=document.getElementById('rmTopUsageBody');if(tb3)tb3.innerHTML=h3;
}

const RM_DETAIL_PAGE_SIZE=50;
function _buildRMDetailCache(rmDemand,rmShortage,rmCode,rmFG,labels,rmNameMap){
  const rows=[];
  for(const nm of Object.keys(rmDemand)){
    const dem=rmDemand[nm],sh=rmShortage[nm],del2=dem-sh,code=rmCode[nm]||'';
    const fgSet=rmFG[nm];
    const fgStr=fgSet?[...fgSet].slice(0,5).join(', ')+(fgSet.size>5?' (+'+(fgSet.size-5)+')':''):'';
    const displayNm=rmNameMap&&rmNameMap[nm]?rmNameMap[nm]:nm;
    rows.push({code,nm:displayNm,dem,del:del2,sh,fgStr,fgCount:fgSet?fgSet.size:0});
  }
  rows.sort((a,b)=>b.dem-a.dem);_rmDetailCache=rows;
}

function _renderRMDetailPage(){
  if(!_rmDetailCache)return;
  const q=((document.getElementById('rmSearch')||{}).value||'').toLowerCase();
  const filt=q?_rmDetailCache.filter(r=>r.nm.toLowerCase().includes(q)||r.code.includes(q)):_rmDetailCache;
  const total=filt.length,maxPage=Math.max(1,Math.ceil(total/RM_DETAIL_PAGE_SIZE));
  if(rmDetailPage<1)rmDetailPage=1;if(rmDetailPage>maxPage)rmDetailPage=maxPage;
  const start=(rmDetailPage-1)*RM_DETAIL_PAGE_SIZE,end=Math.min(start+RM_DETAIL_PAGE_SIZE,total),slice=filt.slice(start,end);
  let h='';
  for(const r of slice){const shCls=r.sh>0?'color:#dc2626;font-weight:600':'color:#059669';h+=`<tr><td style="padding:5px 8px">-</td><td style="padding:5px 8px">${_esc(r.code)}</td><td style="padding:5px 8px">${_esc(r.nm)}</td><td style="padding:5px 8px;text-align:right;font-weight:600">${fN2(r.dem)}</td><td style="padding:5px 8px;text-align:right">${fN2(r.del)}</td><td style="padding:5px 8px;text-align:right;${shCls}">${fN2(r.sh)}</td><td style="padding:5px 8px;font-size:10px;color:var(--gray)">${_esc(r.fgStr||'-')}</td></tr>`;}
  const tb=document.getElementById('rmDetailBody');if(tb)tb.innerHTML=h;
  const info=document.getElementById('rmDetailInfo');if(info)info.textContent=total+' RM items';
  const pInfo=document.getElementById('rmDetailPageInfo');if(pInfo)pInfo.textContent=(start+1)+'-'+end+' / '+total;
}
function rmDetailGo(p){rmDetailPage=p;_renderRMDetailPage();}
function filterRMDetail(){rmDetailPage=1;_renderRMDetailPage();}

// ═══ RM REPORT TAB ════════════════════════════════════════════════
let _rmrRawRows=[],_rmrFiltered=[],_rmrPage=1;
const _RMR_PER_PAGE=50;
let _rmrSortCol='impact',_rmrSortDir='desc',_rmrReady=false;
let _rmrWeekDateMap={},_rmrManualDateFrom='',_rmrManualDateTo=''; // week→{min,max} + saved manual dates

function _rmrWeekKey(dateStr){const d=new Date(dateStr+'T00:00:00');const thu=new Date(d);thu.setDate(d.getDate()-((d.getDay()+6)%7)+3);const jan4=new Date(thu.getFullYear(),0,4);const wk=1+Math.round((thu-jan4)/6048e5);return thu.getFullYear()+'-W'+String(wk).padStart(2,'0');}

function _rmrInitDropdowns(idxs,labels){
  if(_rmrReady)return;
  const groups=new Set(),codeMap=new Map();
  const src=(typeof DATA!=='undefined')?DATA:[];
  for(let ri=0;ri<src.length;ri++){const r=src[ri];if(!isLayer1Row(r))continue;if(r[0])groups.add(r[0]);if(r[4])codeMap.set(r[4],r[3]||r[4]);}
  groups.delete('Total');
  const fGroup=document.getElementById('rmrFGroup');if(fGroup)[...groups].sort().forEach(g=>{const o=document.createElement('option');o.value=g;o.textContent=g;fGroup.appendChild(o);});
  const fCode=document.getElementById('rmrFCode');if(fCode)[...codeMap.entries()].sort((a,b)=>a[0].localeCompare(b[0])).forEach(([code,nm])=>{const o=document.createElement('option');o.value=code;o.textContent=code+(nm&&nm!==code?' — '+nm:'');fCode.appendChild(o);});
  // Build weekMap from full DATES (not filtered labels) so all weeks are always shown
  const _allDates=(typeof DATES!=='undefined'&&DATES.length)?DATES:labels;
  const weekMap=new Map();_allDates.forEach(lbl=>{const wk=_rmrWeekKey(lbl);if(!weekMap.has(wk))weekMap.set(wk,{min:lbl,max:lbl});else{const e=weekMap.get(wk);if(lbl<e.min)e.min=lbl;if(lbl>e.max)e.max=lbl;}});
  weekMap.forEach((v,k)=>{ _rmrWeekDateMap[k]=v; }); // expose globally for rmrWeekChange
  const fWeek=document.getElementById('rmrFWeek');if(fWeek)[...weekMap.entries()].sort((a,b)=>a[0].localeCompare(b[0])).forEach(([wk,{min,max}])=>{const o=document.createElement('option');o.value=wk;const fmt=d2=>d2.slice(5).replace('-','/');o.textContent=wk+'  ('+fmt(min)+' – '+fmt(max)+')';fWeek.appendChild(o);});
  if(labels.length){const df=document.getElementById('rmrFDateFrom');const dt=document.getElementById('rmrFDateTo');if(df)df.value=labels[0];if(dt)dt.value=labels[labels.length-1];}
  _rmrReady=true;
}

function updateRMReportTab(data,idxs,labels){
  _rmrInitDropdowns(idxs,labels);
  const fG=(document.getElementById('rmrFGroup')||{}).value||'';
  const fCode=(document.getElementById('rmrFCode')||{}).value||'';
  const fWk=(document.getElementById('rmrFWeek')||{}).value||'';
  const fDF=(document.getElementById('rmrFDateFrom')||{}).value||'';
  const fDT=(document.getElementById('rmrFDateTo')||{}).value||'';
  const fgMap={};
  for(let ri=0;ri<data.length;ri++){const r=data[ri];if(!isLayer2Row(r))continue;const code=r[4]||'';if(!code)continue;if(!fgMap[code])fgMap[code]=new Set();if(r[1])fgMap[code].add(r[1]);}
  const localIdxs=[],localLabels=[];
  for(let j=0;j<idxs.length;j++){const lbl=labels[j];if(fWk&&_rmrWeekKey(lbl)!==fWk)continue;if(fDF&&lbl<fDF)continue;if(fDT&&lbl>fDT)continue;localIdxs.push(idxs[j]);localLabels.push(lbl);}
  const rows=[];
  const src=(typeof DATA!=='undefined')?DATA:data;
  for(let ri=0;ri<src.length;ri++){
    const r=src[ri];if(!isLayer1Row(r))continue;
    if(fG&&r[0]!==fG)continue;if(fCode&&r[4]!==fCode)continue;
    const code=r[4]||'',nm=r[3]||'',grp=r[0]||'',fgCount=fgMap[code]?fgMap[code].size:0;
    const ord8=r[8]||[],act9=r[9]||[];
    for(let j=0;j<localIdxs.length;j++){const di2=localIdxs[j];const o=+(ord8[di2])||0,a=+(act9[di2])||0,sh=Math.max(0,o-a);if(o===0&&a===0)continue;
      const riskLevel=sh===0?0:fgCount>=3?3:fgCount===2?2:1;
      const risk=sh===0?'No Risk':fgCount>=3?'High':fgCount===2?'Medium':'Low';
      const impact=fgCount*sh;
      const coverage=o>0?a/o:null;
      rows.push({date:localLabels[j],rmCode:code,rmName:nm,demand:o,delivered:a,shortage:sh,group:grp,fgCount,coverage,risk,riskLevel,impact});}
  }
  _rmrRawRows=rows;_rmrPage=1;_applyRMReport();
}

function rmrLocalFilter(){_rmrPage=1;if(_lastFilteredRows&&_lastIdxs&&_lastLabels)updateRMReportTab(_lastFilteredRows,_lastIdxs,_lastLabels);}
function rmrWeekChange(){
  const wk=document.getElementById('rmrFWeek').value;
  const df=document.getElementById('rmrFDateFrom');
  const dt=document.getElementById('rmrFDateTo');
  if(!wk){
    // Restore previously saved manual date range
    if(_rmrManualDateFrom){df.value=_rmrManualDateFrom;}
    if(_rmrManualDateTo){dt.value=_rmrManualDateTo;}
    _rmrManualDateFrom='';_rmrManualDateTo='';
  } else {
    // Save current manual dates (only on first week selection)
    if(!_rmrManualDateFrom&&df.value)_rmrManualDateFrom=df.value;
    if(!_rmrManualDateTo&&dt.value)_rmrManualDateTo=dt.value;
    const entry=_rmrWeekDateMap[wk];
    if(entry){df.value=entry.min;dt.value=entry.max;}
  }
  rmrLocalFilter();
}

function _applyRMReport(){
  const search=((document.getElementById('rmrSearch')||{}).value||'').toLowerCase().trim();
  const fName=((document.getElementById('rmrFName')||{}).value||'').toLowerCase().trim();
  let rows=_rmrRawRows;
  if(fName)rows=rows.filter(r=>r.rmName.toLowerCase().includes(fName));
  if(search)rows=rows.filter(r=>r.rmCode.toLowerCase().includes(search)||r.rmName.toLowerCase().includes(search));
  const col=_rmrSortCol,dir=_rmrSortDir==='asc'?1:-1;
  rows=[...rows].sort((a,b)=>{const av=a[col],bv=b[col];if(typeof av==='number')return(av-bv)*dir;return String(av).localeCompare(String(bv),'th')*dir;});
  _rmrFiltered=rows;_renderRMRPage();_rmrUpdateSortIcons();
}

function _renderRMRPage(){
  const tbody=document.getElementById('rmrTbody'),pager=document.getElementById('rmrPager'),rowCount=document.getElementById('rmrRowCount');if(!tbody)return;
  const total=_rmrFiltered.length,pages=Math.max(1,Math.ceil(total/_RMR_PER_PAGE));
  if(_rmrPage>pages)_rmrPage=pages;
  const start=(_rmrPage-1)*_RMR_PER_PAGE,end=Math.min(start+_RMR_PER_PAGE,total),slice=_rmrFiltered.slice(start,end);
  if(rowCount)rowCount.textContent=total.toLocaleString('th-TH')+' แถว';
  if(!slice.length){tbody.innerHTML=`<tr><td colspan="11" class="rmr-no-data">ไม่มีข้อมูลที่ตรงกับเงื่อนไข<br><small>ปรับเงื่อนไข Filter หรือกด รีเซ็ต</small></td></tr>`;if(pager)pager.innerHTML='';return;}
  tbody.innerHTML=slice.map(r=>{
    const shCls=r.shortage>0?' rmr-shortage':'';
    const covStr=r.coverage!==null?(r.coverage*100).toFixed(1)+'%':'—';
    return '<tr>'
      +'<td>'+r.date+'</td>'
      +'<td><code style="font-size:11px;background:#f1f5f9;padding:1px 5px;border-radius:3px">'+_esc(r.rmCode||'—')+'</code></td>'
      +'<td>'+_esc(r.rmName||'—')+'</td>'
      +'<td class="rmr-num">'+fN2(r.demand)+'</td>'
      +'<td class="rmr-num">'+fN2(r.delivered)+'</td>'
      +'<td class="rmr-num'+shCls+'">'+(r.shortage>0?fN2(r.shortage):'—')+'</td>'
      +'<td>'+_esc(r.group||'—')+'</td>'
      +'<td class="rmr-num">'+r.fgCount+'</td>'
      +'<td class="rmr-num">'+covStr+'</td>'
      +'<td><span class="rmr-risk rmr-risk-'+r.riskLevel+'">'+_esc(r.risk||'—')+'</span></td>'
      +'<td class="rmr-num">'+fN2(r.impact)+'</td>'
      +'</tr>';
  }).join('');
  if(pager){const maxBtn=7,half=Math.floor(maxBtn/2);let lo=Math.max(1,_rmrPage-half);let hi=Math.min(pages,lo+maxBtn-1);if(hi-lo<maxBtn-1)lo=Math.max(1,hi-maxBtn+1);let p='<button class="rmr-pg-btn" onclick="_rmrGo('+(_rmrPage-1)+')" '+(_rmrPage===1?'disabled':'')+'>‹</button>';if(lo>1){p+='<button class="rmr-pg-btn" onclick="_rmrGo(1)">1</button>';if(lo>2)p+='<span style="padding:0 3px;color:var(--gray)">…</span>';}for(let pg=lo;pg<=hi;pg++)p+='<button class="rmr-pg-btn'+(pg===_rmrPage?' active-pg':'')+'" onclick="_rmrGo('+pg+')">'+pg+'</button>';if(hi<pages){if(hi<pages-1)p+='<span style="padding:0 3px;color:var(--gray)">…</span>';p+='<button class="rmr-pg-btn" onclick="_rmrGo('+pages+')">'+pages+'</button>';}p+='<button class="rmr-pg-btn" onclick="_rmrGo('+(_rmrPage+1)+')" '+(_rmrPage===pages?'disabled':'')+'>›</button>';p+='<span style="font-size:11px;color:var(--gray);margin-left:8px">หน้า '+_rmrPage+' / '+pages+'</span>';pager.innerHTML=p;}
}
function _rmrGo(p){const pages=Math.max(1,Math.ceil(_rmrFiltered.length/_RMR_PER_PAGE));_rmrPage=Math.max(1,Math.min(pages,p));_renderRMRPage();}
function rmrSort(col){if(_rmrSortCol===col)_rmrSortDir=_rmrSortDir==='asc'?'desc':'asc';else{_rmrSortCol=col;_rmrSortDir=(col==='demand'||col==='shortage'||col==='delivered'||col==='fgCount'||col==='risk'||col==='impact')?'desc':'asc';}_rmrPage=1;_applyRMReport();}
function _rmrUpdateSortIcons(){['date','rmCode','rmName','demand','delivered','shortage','group','fgCount','coverage','risk','impact'].forEach(c=>{const el=document.getElementById('rmrIcon_'+c);if(!el)return;if(c===_rmrSortCol)el.textContent=_rmrSortDir==='asc'?' ▲':' ▼';else el.textContent=' ↕';el.style.color=c===_rmrSortCol?'var(--primary)':'#94a3b8';});}
function rmrResetFilters(){['rmrFWeek','rmrFGroup','rmrFCode'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});['rmrFName','rmrSearch'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});if(_lastLabels&&_lastLabels.length){const df=document.getElementById('rmrFDateFrom');const dt=document.getElementById('rmrFDateTo');if(df)df.value=_lastLabels[0];if(dt)dt.value=_lastLabels[_lastLabels.length-1];}_rmrManualDateFrom='';_rmrManualDateTo=''; // clear saved manual dates on full reset
  _rmrPage=1;_rmrSortCol='impact';_rmrSortDir='desc';if(_lastFilteredRows&&_lastIdxs&&_lastLabels)updateRMReportTab(_lastFilteredRows,_lastIdxs,_lastLabels);}

function exportRMReportExcel(){
  if(!_rmrFiltered.length){alert('ไม่มีข้อมูลสำหรับ Export');return;}
  if(typeof XLSX==='undefined'){alert('SheetJS library ไม่พร้อมใช้งาน');return;}
  const headers=['วันที่','RM Code','RM Name','Demand (KG)','Delivered (KG)','Shortage (KG)','Group','FG Count','Coverage %','Risk','Impact Score'];
  const aoa=[headers];
  _rmrFiltered.forEach(r=>{aoa.push([r.date,r.rmCode||'',r.rmName||'',r.demand,r.delivered,r.shortage,r.group||'',r.fgCount,r.coverage!==null?r.coverage:'',r.risk||'',r.impact||0]);});
  const wb=XLSX.utils.book_new(),ws=XLSX.utils.aoa_to_sheet(aoa);
  ws['!cols']=[{wch:12},{wch:16},{wch:30},{wch:14},{wch:14},{wch:14},{wch:12},{wch:10},{wch:12},{wch:10},{wch:14}];
  const numFmt='#,##0.00';
  for(let i=1;i<aoa.length;i++){const row=i+1;['D','E','F','K'].forEach(col=>{const cell=ws[col+row];if(cell&&typeof cell.v==='number'){cell.t='n';cell.z=numFmt;}});}
  ws['!freeze']={xSplit:0,ySplit:1};
  XLSX.utils.book_append_sheet(wb,ws,'RM Report');
  const now=new Date(),ymd=now.getFullYear().toString()+String(now.getMonth()+1).padStart(2,'0')+String(now.getDate()).padStart(2,'0');
  XLSX.writeFile(wb,'RM_Report_'+ymd+'.xlsx');
}

// ════════════════════════════════════════════════════════════════════════
// RM PLANNING ENGINE — Flexible Base Week · Day-Based · RM Filter
// ════════════════════════════════════════════════════════════════════════
let _rmpResults = [];       // cache for export
// Stock state — always reflects LATEST upload only (Upload → Parse → Use → Discard)
let stockMap       = {};   // { rmCode: stockBalanceKG }  — replaced wholesale each upload
let stockTimestamp = null; // Date object of last successful upload (null = no file loaded)
let stockLoaded    = false;// true only while a valid stock snapshot is active
const _RMP_DAY_NAMES = ['จ.','อ.','พ.','พฤ.','ศ.','ส.','อา.'];

/* ── ISO 8601 week helpers ── */
function _getISOWeek(dateStr) {
  const d = new Date(dateStr + 'T00:00:00');
  const day = d.getDay() || 7;       // 1=Mon … 7=Sun
  d.setDate(d.getDate() + 4 - day);  // shift to nearest Thursday
  const y1 = new Date(d.getFullYear(), 0, 1);
  return { week: Math.ceil(((d - y1) / 86400000 + 1) / 7), year: d.getFullYear() };
}
function _getDayOfWeek(dateStr) {
  // 0=Mon … 6=Sun  (JS getDay(): 0=Sun → shift)
  return (new Date(dateStr + 'T00:00:00').getDay() + 6) % 7;
}
function _maxISOWeek(year) {
  const d = new Date(year, 11, 31);
  const w = _getISOWeek(d.toISOString().slice(0,10));
  if (w.year === year) return w.week;
  return _getISOWeek(new Date(year, 11, 24).toISOString().slice(0,10)).week;
}

/* ── Compute W-2 offset info — used only for auto-suggesting Base Week
   in buildRMPlanWeekOptions() and rmpWeekChange().
   runRMPlanning() no longer calls this — it reads rmpBaseWeek directly. ── */
function _computeW2WeekInfo(weekVal) {
  const parts = weekVal.split('-');
  const year = parseInt(parts[0]), week = parseInt(parts[1]);
  let baseWeek = week - 2, baseYear = year;
  if (baseWeek <= 0) { baseYear = year - 1; baseWeek = _maxISOWeek(baseYear) + baseWeek; }
  return { targetYear: year, targetWeek: week, baseWeek, baseYear };
}

/* ── Populate Target Week + Base Week dropdowns from DATES ── */
function buildRMPlanWeekOptions() {
  if (typeof DATES === 'undefined' || !DATES.length) return;
  const tSel = document.getElementById('rmpTargetWeek');
  const bSel = document.getElementById('rmpBaseWeek');
  if (!tSel) return;

  // Build week index from all dates in DATES array
  const weekMap = {};
  DATES.forEach(ds => {
    const w = _getISOWeek(ds);
    const key = w.year + '-' + String(w.week).padStart(2,'0');
    if (!weekMap[key]) weekMap[key] = { year: w.year, week: w.week, dates: [] };
    weekMap[key].dates.push(ds);
  });
  const keys = Object.keys(weekMap).sort().reverse(); // newest first

  // Build shared options HTML (same pool for both selects)
  const optHtml = keys.map(k => {
    const wm = weekMap[k];
    const ds = wm.dates.slice().sort();
    const lbl = 'W' + String(wm.week).padStart(2,'0') + ' / ' + wm.year +
                '  (' + ds[0].slice(5) + ' – ' + ds[ds.length-1].slice(5) + ')';
    return '<option value="' + k + '">' + lbl + '</option>';
  }).join('');

  // ── Target Week: default = most recent ──
  const prevT = tSel.value;
  tSel.innerHTML = '<option value="">— เลือก Target Week —</option>' + optHtml;
  if (prevT && tSel.querySelector('option[value="'+prevT+'"]')) tSel.value = prevT;
  else if (keys.length) tSel.value = keys[0];

  // ── Base Week: default = W-2 of current target (user can freely override) ──
  if (bSel) {
    const prevB = bSel.value;
    bSel.innerHTML = '<option value="">— เลือก Base Week —</option>' + optHtml;
    if (prevB && bSel.querySelector('option[value="'+prevB+'"]')) {
      bSel.value = prevB;  // restore user's previous manual selection
    } else if (tSel.value) {
      // Auto-suggest W-2 of target for convenience
      const info   = _computeW2WeekInfo(tSel.value);
      const autoKey = info.baseYear + '-' + String(info.baseWeek).padStart(2,'0');
      if (bSel.querySelector('option[value="'+autoKey+'"]')) bSel.value = autoKey;
      else if (keys.length > 1) bSel.value = keys[1]; // second most recent fallback
    }
  }

  rmpWeekChange();
  buildRMPlanGroupOptions();
}

/* ── Populate RM Group dropdown  (r[COL_RMP.rmGroup] = r[2] = item category) ── */
function buildRMPlanGroupOptions() {
  const sel = document.getElementById('rmpGroupFilter');
  if (!sel) return;
  const data = getFilteredData();
  const groups = new Set();
  // r[2] = item category — present on all item-level rows
  for (let i = 0; i < data.length; i++) {
    if (data[i][COL_RMP.rmGroup]) groups.add(data[i][COL_RMP.rmGroup]);
  }
  const sorted = Array.from(groups).sort();
  sel.innerHTML = '<option value="">ทุก Group</option>' +
    sorted.map(g => '<option value="' + _esc(g) + '">' + _esc(g) + '</option>').join('');
}

/* ── Badge + Base Week handlers ────────────────────────────────────────
   rmpWeekChange()     : Target changed → auto-suggest Base = W-2 of Target
   rmpBaseWeekChange() : Base changed manually → update badge only
   _rmpUpdateBadge()   : Shared badge renderer showing Target / Base / offset
   ── */
function rmpWeekChange() {
  const tSel = document.getElementById('rmpTargetWeek');
  const bSel = document.getElementById('rmpBaseWeek');
  if (!tSel || !tSel.value) { _rmpUpdateBadge(); return; }

  // Auto-suggest Base = W-2 of Target ONLY when base is blank (not yet chosen)
  // Preserves manual base selection when target is changed
  if (bSel && !bSel.value) {
    const info   = _computeW2WeekInfo(tSel.value);
    const autoKey = info.baseYear + '-' + String(info.baseWeek).padStart(2,'0');
    if (bSel.querySelector('option[value="'+autoKey+'"]')) bSel.value = autoKey;
  }
  _rmpUpdateBadge();
}

function rmpBaseWeekChange() {
  // Base week manually changed by user — just refresh badge
  _rmpUpdateBadge();
}

function _rmpUpdateBadge() {
  const tSel  = document.getElementById('rmpTargetWeek');
  const bSel  = document.getElementById('rmpBaseWeek');
  const badge = document.getElementById('rmpW2Badge');
  if (!badge) return;
  if (!tSel || !tSel.value || !bSel || !bSel.value) {
    badge.textContent = 'Base: —'; return;
  }
  const tp = tSel.value.split('-'), bp = bSel.value.split('-');
  const tW = parseInt(tp[1]), tY = parseInt(tp[0]);
  const bW = parseInt(bp[1]), bY = parseInt(bp[0]);
  // Approximate ISO week offset (handles year boundary)
  const offset = (tY - bY) * 52 + (tW - bW);
  const offsetLabel = offset > 0 ? '  → W−' + offset
                    : offset < 0 ? '  ⚠ Base อยู่ในอนาคต'
                    :              '  ⚠ Base = Target';
  badge.textContent = 'Base: W' + String(bW).padStart(2,'0') + '/' + bY + offsetLabel;
  badge.style.background = offset <= 0 ? '#fee2e2' : '#dbeafe';
  badge.style.color      = offset <= 0 ? '#991b1b' : '#1d4ed8';
}

/* ── Day selector quick-buttons ── */
function rmpSelectDays(mode) {
  const cbs = document.querySelectorAll('#rmpPlanDayBar input[type=checkbox]');
  cbs.forEach(cb => {
    const v = parseInt(cb.value);
    cb.checked = (mode === 'all') ? true : (mode === 'weekday') ? v <= 4 : false;
    rmpDayToggle(cb);
  });
}
function rmpDayToggle(cb) {
  const lbl = cb.closest ? cb.closest('.rmp-day-lbl') : cb.parentElement;
  if (lbl) lbl.classList.toggle('rmp-day-on', cb.checked);
}

/* ── Main entry: parse UI → build idxs → call engine ── */
/* ── Stock upload handler — stateless: Upload → Parse → Use → Discard
      File format: Parent-Child structure
      Col B = Parent RM Code  |  Col G = Stock KG
      Aggregation: SUM(col G) per Parent — multiple child rows per parent RM
   ── */
function parseStockFile(file) {
  if (!file) return;

  // 🔥 HARD RESET — wipe previous snapshot immediately, before any async work.
  //    Guarantees: if the new parse fails, stale data is NOT used.
  stockMap       = {};
  stockTimestamp = null;
  stockLoaded    = false;

  const badge = document.getElementById('rmpStockBadge');
  const _setBadge = (txt, bg, fg) => {
    if (!badge) return;
    badge.textContent = txt;
    badge.style.background = bg;
    badge.style.color      = fg;
  };
  _setBadge('กำลังโหลด...', '#fef9c3', '#854d0e');

  const reader = new FileReader();
  reader.onload = function(e) {
    try {
      if (typeof XLSX === 'undefined') throw new Error('SheetJS ไม่พร้อมใช้งาน');

      // Single-sheet file: read first (and only) sheet
      // Row 0 = metadata  (col[1] = file date)
      // Row 1 = headers   ['ลำดับ','Item แม่','รายการแม่','Item ลูก','รายการลูก','ประเภท','Stock']
      // Row 2+ = data     col[0]=seq(number), col[1]=ParentCode, col[6]=StockKG
      const wb  = XLSX.read(e.target.result, { type: 'array', cellDates: true });
      const ws  = wb.Sheets[wb.SheetNames[0]];
      const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });

      // Extract file date from row 0, col B (col[1]) — Excel date via cellDates
      const fileDate = (aoa[0] && aoa[0][1] instanceof Date) ? aoa[0][1] : null;

      // Build fresh map: SUM stock per Parent RM Code
      // Multiple child rows share the same parent → aggregate all into parent
      const freshMap = {};
      for (let i = 2; i < aoa.length; i++) {
        const row = aoa[i];
        // Guard: col[0] must be a positive integer (ลำดับ / sequence number)
        if (typeof row[0] !== 'number' || row[0] <= 0) continue;
        const parentCode = String(row[1] || '').trim();   // col B = Parent RM Code
        if (!parentCode) continue;
        // col G = Stock KG — skip empty, '-', or zero (no stock to plan against)
        const raw = row[6];
        if (raw === '' || raw === '-' || raw === null) continue;
        const bal = typeof raw === 'number' ? raw : parseFloat(String(raw).replace(/,/g, ''));
        if (!isFinite(bal) || bal <= 0) continue;
        // SUM into parent — handles multiple child items per parent RM
        freshMap[parentCode] = (freshMap[parentCode] || 0) + bal;
      }

      // ✅ Atomic swap — replace state only after successful parse
      stockMap       = freshMap;
      stockTimestamp = new Date();
      stockLoaded    = true;

      const cnt        = Object.keys(stockMap).length;
      const uploadTime = stockTimestamp.toLocaleString('th-TH', {
        day:'2-digit', month:'2-digit', year:'numeric',
        hour:'2-digit', minute:'2-digit'
      });
      // Show file date (from Excel header) if available, else fall back to upload time
      const fileDateLabel = fileDate
        ? fileDate.toLocaleDateString('th-TH', { day:'2-digit', month:'2-digit', year:'numeric' })
        : uploadTime;
      _setBadge(
        '✓ จำนวน RM ที่มี stock: ' + cnt + ' รายการ | Stock ล่าสุด: ' + fileDateLabel,
        '#dcfce7', '#166534'
      );

      runRMPlanning();

    } catch (err) {
      // 🔥 On failure: keep state cleared (already reset above)
      stockMap       = {};
      stockTimestamp = null;
      stockLoaded    = false;
      _setBadge('⚠ Error: ' + err.message, '#fee2e2', '#991b1b');
    }
  };
  reader.onerror = function() {
    stockMap = {}; stockTimestamp = null; stockLoaded = false;
    _setBadge('⚠ ไม่สามารถอ่านไฟล์ได้', '#fee2e2', '#991b1b');
  };
  reader.readAsArrayBuffer(file);
}

function runRMPlanning() {
  if (typeof DATES === 'undefined' || !DATES.length) {
    _rmpRenderEmpty('ไม่มีข้อมูลวันที่'); return;
  }
  const weekVal    = (document.getElementById('rmpTargetWeek')  || {}).value || '';
  const _rawBuf    = parseFloat((document.getElementById('rmpBuffer') || {}).value);
  const bufPct     = isNaN(_rawBuf) ? 20 : _rawBuf;   // 0 is valid — do NOT use || 20
  const buffer     = bufPct / 100;
  const groupFilt  = (document.getElementById('rmpGroupFilter') || {}).value || '';
  const rmSearch   = ((document.getElementById('rmpSearch')     || {}).value || '').trim().toLowerCase();

  const dayCbs = document.querySelectorAll('#rmpPlanDayBar input[type=checkbox]');
  const selectedDays = [];
  dayCbs.forEach(cb => { if (cb.checked) selectedDays.push(parseInt(cb.value)); });
  if (!selectedDays.length) { _rmpRenderEmpty('กรุณาเลือกอย่างน้อย 1 วัน'); return; }
  if (!weekVal)              { _rmpRenderEmpty('กรุณาเลือก Target Week'); return; }

  // ── Free Base Week: read user selection directly — NOT auto-computed W-2 ──
  const baseVal = (document.getElementById('rmpBaseWeek') || {}).value || '';
  if (!baseVal) { _rmpRenderEmpty('กรุณาเลือก Base Week'); return; }

  // Parse Target Week
  const tParts     = weekVal.split('-');
  const targetYear = parseInt(tParts[0]);
  const targetWeek = parseInt(tParts[1]);

  // Parse Base Week (any week — no offset arithmetic)
  const bParts   = baseVal.split('-');
  const baseYear = parseInt(bParts[0]);
  const baseWeek = parseInt(bParts[1]);

  // ── Strict scope guard: Base ≠ Target, intermediate weeks irrelevant ──
  // Intermediate weeks (e.g. W12 when Base=W11, Target=W13) are structurally
  // excluded — they are never added to w2DayMap, so they cannot influence orders.
  if (baseWeek === targetWeek && baseYear === targetYear) {
    _rmpRenderEmpty('⚠ Base Week ต้องไม่ใช่สัปดาห์เดียวกับ Target Week'); return;
  }

  // ── Classify date indices: coverageIdxs (target week) + w2DayMap (base week) ──
  // STRICT ISOLATION: only baseWeek and targetWeek are ever touched.
  // Any date that belongs to a different week — including ALL intermediate weeks
  // (e.g., W12 when base=W11, target=W13) — is EXPLICITLY dropped at the first
  // check. No fallthrough, no implicit skip.
  const coverageIdxs = [];   // target week indices (kept for reference)
  const w2DayMap     = {};   // base week indices for ALL 7 DOWs — needed for full-week demand/actual
  for (let d = 0; d < 7; d++) w2DayMap[d] = [];
  const _baseWeekDowSet = new Set();  // tracks distinct DOWs present in base week

  for (let i = 0; i < DATES.length; i++) {
    const w        = _getISOWeek(DATES[i]);
    const isBase   = (w.year === baseYear   && w.week === baseWeek);
    const isTarget = (w.year === targetYear && w.week === targetWeek);

    // EXPLICIT DROP — anything that is neither base nor target is rejected immediately.
    // This is the primary guard against intermediate-week contamination.
    if (!isBase && !isTarget) continue;

    if (isTarget) { coverageIdxs.push(i); continue; }

    // isBase only from here — collect for ALL DOWs (needed for full-week demand sum)
    const dow = _getDayOfWeek(DATES[i]);
    _baseWeekDowSet.add(dow);
    w2DayMap[dow].push(i);
  }

  // N: total distinct days base week operated (used in DayRatio = M / N)
  const baseWeekTotalDays = _baseWeekDowSet.size;

  const data = getFilteredData();
  updateRMPlanning(data, coverageIdxs, w2DayMap, selectedDays, buffer,
                   targetWeek, targetYear, baseWeek, baseYear, groupFilt, rmSearch,
                   baseWeekTotalDays);
}

/* ── Daily Stock Consumption Simulator ────────────────────────────────
   Simulates stock depletion day-by-day in order.
   Each day: if stock covers demand → consume & carry forward
             if stock insufficient  → order shortage×(1+buffer), stock→0

   @param demandByDay  number[]  — W-2 demand per selected day, in sequence
   @param stock        number    — current available stock KG (from stockMap)
   @param buffer       number    — safety buffer as decimal (0.20 = 20%)
   @returns { dayPlan: number[], totalOrder: number, endingStock: number }

   Rules enforced:
     1. Days processed in sequence — stock state carries forward
     2. remainingStock never goes below 0
     3. Buffer applied ONLY to shortage, not to demand already covered by stock
     4. Days with 0 demand are skipped (no order, no stock consumption)
   ── */
function _simulateDailyStock(demandByDay, stock, buffer) {
  let remainingStock = stock;   // carries forward through simulation
  let totalOrder     = 0;
  const dayPlan      = new Array(demandByDay.length);

  for (let i = 0; i < demandByDay.length; i++) {
    const demand = demandByDay[i] || 0;

    if (demand <= 0) {
      dayPlan[i] = 0;  // no demand — skip, preserve stock
      continue;
    }

    if (remainingStock >= demand) {
      // Stock fully covers this day — consume and carry remainder forward
      remainingStock -= demand;
      dayPlan[i]      = 0;
    } else {
      // Shortfall: place order for (shortage × buffer) then deplete stock
      const shortage  = demand - remainingStock;
      const orderQty  = shortage * (1 + buffer);
      totalOrder     += orderQty;
      dayPlan[i]      = orderQty;
      remainingStock  = 0;  // stock fully consumed on shortage day
    }
  }

  return { dayPlan, totalOrder, endingStock: remainingStock };
}

/* ── Engine: single-pass O(n) accumulation ── */
function updateRMPlanning(data, coverageIdxs, w2DayMap, selectedDays, buffer,
                          targetWeek, targetYear, baseWeek, baseYear,
                          rmGroupFilter, rmSearch, baseWeekTotalDays) {
  // DATA SOURCE: Layer1 rows ONLY — identical filter to RM Report (isLayer1Row).
  // Layer1 = rows where r[1] is empty (no customer).
  // Layer2 (customer breakdown rows) are excluded from demand accumulation —
  // including them would double-count demand already captured in Layer1.
  // coverageIdxs: reference only, not used in planning formula.

  // Pre-scan 1: codeGroupMap — item category labels from rows where r[2] != ''.
  // Used to resolve RM group for rows where r[2] may be empty.
  const codeGroupMap = {};
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (r[COL_RMP.rmGroup] && r[COL_RMP.rmCode])
      codeGroupMap[r[COL_RMP.rmCode]] = r[COL_RMP.rmGroup];
  }

  // Pre-scan 2: codeFgMap — distinct customer count per RM code (from Layer2 rows).
  // Separated from demand accumulation so Layer2 never contaminates Layer1 demand values.
  const codeFgMap = {};
  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!isLayer2Row(r)) continue;
    const code = r[COL_RMP.rmCode] || r[COL_RMP.rmName] || '';
    if (!code) continue;
    if (!codeFgMap[code]) codeFgMap[code] = new Set();
    if (r[COL_RMP.customer]) codeFgMap[code].add(r[COL_RMP.customer]);
  }

  // Main accumulation: LAYER1 ROWS ONLY.
  // r[8]=rmDemand, r[9]=rmActual on Layer1 rows — same arrays RM Report reads.
  // No aggregation logic: values are read directly from data, not recomputed.
  const rmMap = {}; // key -> {name, code, group, w2DemByDay, w2ActByDay}

  for (let ri = 0; ri < data.length; ri++) {
    const r = data[ri];
    if (!isLayer1Row(r)) continue;   // LAYER1 ONLY — matches RM Report exactly
    const key = r[COL_RMP.rmCode] || r[COL_RMP.rmName] || ''; if (!key) continue;

    if (rmGroupFilter && (codeGroupMap[key] || '') !== rmGroupFilter) continue;
    if (rmSearch) {
      const nm = (r[COL_RMP.rmName] || '').toLowerCase();
      const cd = (r[COL_RMP.rmCode] || '').toLowerCase();
      if (!nm.includes(rmSearch) && !cd.includes(rmSearch)) continue;
    }

    const ord8 = r[COL_RMP.rmDemand] || [];   // Layer1: r[8] = RM planned demand array
    const act9 = r[COL_RMP.rmActual]  || [];   // Layer1: r[9] = RM actual demand array

    if (!rmMap[key]) {
      const byDay = {}, byAct = {};
      for (let d = 0; d < 7; d++) { byDay[d] = 0; byAct[d] = 0; }
      rmMap[key] = { name: r[COL_RMP.rmName] || key,
                     code: r[COL_RMP.rmCode] || '',
                     group: codeGroupMap[key] || '',
                     w2DemByDay: byDay, w2ActByDay: byAct };
    }
    const dm = rmMap[key];
    // Layer1 rows have r[1]='' — no customer to add here.
    // FG customer counts are in codeFgMap (pre-scan 2).

    // Read base-week demand + actual for all 7 DOWs (O(7) per row, no nesting).
    for (let dow = 0; dow < 7; dow++) {
      const idxArr = w2DayMap[dow];
      if (!idxArr || idxArr.length === 0) continue;
      const baseIdx = idxArr[0];   // 1 date per DOW per ISO week
      dm.w2DemByDay[dow] += +(ord8[baseIdx]) || 0;
      dm.w2ActByDay[dow] += +(act9[baseIdx]) || 0;
    }

  }

  // ── RM Planning — Direct Layer1 Demand (no scaling, no averaging) ─────
  // Formula (per RM, Layer1 rows only — matches RM Report exactly):
  //
  //   RM_Demand     = SUM(Layer1 demand for this RM across all base-week DOWs)
  //   PlannedImport = MAX(RM_Demand - Stock, 0) x (1 + buffer)
  //   DailyRatio[d] = w2DemByDay[d] / RM_Demand  (base-week daily pattern)
  //   DailyPlan[d]  = PlannedImport x DailyRatio[d]  (selected days only)
  //   CoverageRatio = Stock / RM_Demand
  //   EndingStock   = MAX(Stock - RM_Demand, 0)
  // ─────────────────────────────────────────────────────────────────────
  const activeDays = selectedDays;
  const results    = [];

  for (const key of Object.keys(rmMap)) {
    const dm = rmMap[key];

    // Step 1: Sum base-week Layer1 demand across all 7 DOWs — direct total, no scaling
    let baseDemandFull = 0;
    for (let d = 0; d < 7; d++) {
      baseDemandFull += dm.w2DemByDay[d] || 0;
    }

    const stock        = stockLoaded ? (stockMap[key] || 0) : 0;
    const usedFallback = (baseDemandFull === 0);
    if (usedFallback && stock === 0) continue;

    // Step 2: PlannedImport = MAX(RM_Demand - Stock, 0) x (1 + buffer)
    // No scaling factor, no partial stock weight, no minimum floor.
    // RM_Demand = raw Layer1 total — matches Excel Layer1 exactly.
    const shortage    = Math.max(baseDemandFull - stock, 0);
    const finalPlan   = shortage * (1 + buffer);
    const endingStock = Math.max(stock - baseDemandFull, 0);

    // Step 3: Distribute finalPlan using base-week daily pattern (selected days only)
    // DailyRatio[d] = w2DemByDay[d] / baseDemandFull — preserves actual demand shape.
    // Fallback: equal split across selected days when base-week day data is absent.
    const dayPlan = {};
    if (finalPlan > 0 && baseDemandFull > 0) {
      activeDays.forEach(d => {
        dayPlan[d] = finalPlan * ((dm.w2DemByDay[d] || 0) / baseDemandFull);
      });
    } else {
      const perDay = activeDays.length > 0 ? finalPlan / activeDays.length : 0;
      activeDays.forEach(d => { dayPlan[d] = perDay; });
    }
    let totalPlanned = 0;
    activeDays.forEach(d => { totalPlanned += dayPlan[d] || 0; });

    // Step 4: Risk metrics — coverage = stock vs full Layer1 demand
    const forecastDemand = baseDemandFull;
    const coverageRatio  = baseDemandFull > 0 ? stock / baseDemandFull
                         : (stock > 0 ? 1 : 0);
    const riskLevel      = coverageRatio < 0.30 ? 'CRITICAL'
                         : coverageRatio < 0.70 ? 'WARNING' : 'SAFE';
    const priorityScore  = (totalPlanned * 0.6) + ((1 - Math.min(1, coverageRatio)) * 100 * 0.4);

    // Build note
    const baseLabel = 'W' + String(baseWeek).padStart(2,'0') + '/' + baseYear;
    const notes     = [];

    if (usedFallback) {
      notes.push('ไม่มีข้อมูล Layer1 | ' + baseLabel);
    } else {
      notes.push('Layer1 Direct | ' + baseLabel + ' | Buffer ' + Math.round(buffer * 100) + '%');
      if (stockLoaded) {
        const tsNote = stockTimestamp
          ? ' [' + stockTimestamp.toLocaleDateString('th-TH') + ']' : '';
        notes.push('Stock ' + stock.toLocaleString('th-TH',{maximumFractionDigits:0}) +
                   ' KG' + tsNote);
      } else {
        notes.push('ไม่มีข้อมูล Stock');
      }
      if (coverageRatio < 0.30) notes.push('CRITICAL: Coverage ' + (coverageRatio * 100).toFixed(0) + '%');
      else if (coverageRatio < 0.70) notes.push('WARNING: Coverage ' + (coverageRatio * 100).toFixed(0) + '%');
      if (endingStock > 0)
        notes.push('Ending Stock ' +
                   endingStock.toLocaleString('th-TH',{maximumFractionDigits:0}) + ' KG');
    }

    results.push({ key, name: dm.name, code: dm.code, group: dm.group,
                   forecastDemand, stock, endingStock, coverageRatio,
                   totalPlanned, dayPlan, riskLevel, priorityScore,
                   note: notes.join(' | '), usedFallback,
                   fgCount: (codeFgMap[key] ? codeFgMap[key].size : 0),
                   sourceLayerUsed: 'Layer1',
                   demandFromLayer1: baseDemandFull,
                   demandUsedInPlanning: forecastDemand,
                   targetWeek, targetYear, baseWeek, baseYear });
  }

  // Executive sort: CRITICAL → WARNING → SAFE, then Missing (KG) descending
  const _riskRank = { CRITICAL: 0, WARNING: 1, SAFE: 2 };
  results.forEach(r => {
    r.missingKg = Math.max(0, r.forecastDemand - (r.stock + r.totalPlanned));
  });
  results.sort((a, b) => {
    const rA = _riskRank[a.riskLevel] ?? 9;
    const rB = _riskRank[b.riskLevel] ?? 9;
    if (rA !== rB) return rA - rB;
    return b.missingKg - a.missingKg;
  });
  _rmpResults = results;
  _renderRMPlanningTable(results, selectedDays, baseWeek, baseYear, buffer);
}

function _rmpRenderEmpty(msg) {
  const tb = document.getElementById('rmpTbody');
  if (tb) tb.innerHTML = '<tr><td colspan="20" style="text-align:center;padding:20px;color:#94a3b8">' + msg + '</td></tr>';
}

function _renderRMPlanningTable(results, selectedDays, baseWeek, baseYear, buffer) {
  const tbody = document.getElementById('rmpTbody');
  const thead = document.getElementById('rmpThead');
  if (!tbody || !thead) return;

  const fmt    = v => (isFinite(v) && v > 0) ? v.toLocaleString('th-TH',{maximumFractionDigits:0}) : (v===0?'—':'∞');
  const fmtRaw = v => isFinite(v) ? v.toLocaleString('th-TH',{maximumFractionDigits:0}) : '∞';

  // KPI strip
  let critCount = 0, warnCount = 0, totalImport = 0;
  for (const r of results) {
    if (r.riskLevel==='CRITICAL') critCount++;
    else if (r.riskLevel==='WARNING') warnCount++;
    totalImport += r.totalPlanned;
  }
  const _kv = (id,v) => { const el=document.getElementById(id); if(el) el.textContent=v; };
  _kv('rmpKpiCritical', critCount);
  _kv('rmpKpiWarning',  warnCount);
  _kv('rmpKpiImport',   fmtRaw(totalImport));
  _kv('rmpKpiRMs',      results.length);

  // Fixed 6-day columns: Mon(0) to Sat(5)
  const FIXED_DAYS   = [0,1,2,3,4,5];
  const DAY_EN       = ['จ.','อ.','พ.','พฤ.','ศ.','ส.'];
  const wLabel = 'W' + String(baseWeek).padStart(2,'0') + '/' + baseYear;

  const dayHdrs = FIXED_DAYS.map(d =>
    '<th class="rmp-num" style="min-width:72px">' + DAY_EN[d] +
    '<br><span style="font-weight:400;font-size:9px">Plan (KG)</span></th>'
  ).join('');

  thead.innerHTML = '<tr>' +
    '<th style="width:30px;text-align:center">#</th>' +
    '<th style="min-width:160px">RM Name</th>' +
    '<th style="min-width:90px">RM Code</th>' +
    '<th style="min-width:90px">RM Group</th>' +
    '<th class="rmp-num">' + wLabel + ' Demand<br><span style="font-weight:400;font-size:9px">Base (KG)</span></th>' +
    '<th class="rmp-num">Stock (KG)</th>' +
    '<th class="rmp-num">Coverage<br><span style="font-weight:400;font-size:9px">(%)</span></th>' +
    dayHdrs +
    '<th class="rmp-num">Total Import<br><span style="font-weight:400;font-size:9px">(KG)</span></th>' +
    '<th class="rmp-num">Missing<br><span style="font-weight:400;font-size:9px">(KG)</span></th>' +
    '<th class="rmp-num">Ending Stock<br><span style="font-weight:400;font-size:9px">(KG)</span></th>' +
    '<th style="text-align:center">Risk</th>' +
    '<th style="text-align:center">Rank</th>' +
    '<th>Note</th>' +
    '</tr>';

  const TOTAL_COLS = 14;
  if (!results.length) {
    tbody.innerHTML = '<tr><td colspan="' + TOTAL_COLS + '" style="text-align:center;padding:20px;color:#94a3b8">ไม่มีข้อมูล RM ที่ตรงเงื่อนไข</td></tr>';
    return;
  }

  const badgeCls = r => r==='CRITICAL'?'rmp-badge-crit':r==='WARNING'?'rmp-badge-warn':'rmp-badge-safe';
  const rowCls   = r => r==='CRITICAL'?'rmp-row-crit':r==='WARNING'?'rmp-row-warn':'';
  const impColor = v => v>0?'color:#b91c1c;font-weight:700':'color:#15803d';
  const covColor = ratio => ratio<0.30?'color:#b91c1c;font-weight:700':
                            ratio<0.70?'color:#c2410c;font-weight:600':'color:#15803d';
  const misColor = v => v>0?'color:#b91c1c;font-weight:700':'color:#94a3b8';

  tbody.innerHTML = results.map((r,i) => {
    const fbWarn   = r.usedFallback ? ' <span title="ไม่มีข้อมูล Base Week" style="color:#f59e0b;font-size:10px">⚠</span>' : '';
    const missingKg = r.missingKg != null ? r.missingKg : Math.max(0, r.forecastDemand - (r.stock + r.totalPlanned));
    const dayCells = FIXED_DAYS.map(d =>
      '<td class="rmp-num" style="' + impColor(r.dayPlan[d]||0) + '">' + fmt(r.dayPlan[d]||0) + '</td>'
    ).join('');
    return '<tr class="' + rowCls(r.riskLevel) + '">' +
      '<td style="text-align:center;color:#94a3b8;font-weight:700;font-size:11px">' + (i+1) + '</td>' +
      '<td style="font-weight:' + (r.riskLevel!=='SAFE'?600:400) + ';max-width:160px;white-space:normal">' + _esc(r.name) + '</td>' +
      '<td style="font-size:11px;color:#334155">' + _esc(r.code||'—') + '</td>' +
      '<td style="font-size:11px;color:#334155">' + _esc(r.group||'—') + '</td>' +
      '<td class="rmp-num">' + fmtRaw(r.forecastDemand) + fbWarn + '</td>' +
      '<td class="rmp-num">' + fmtRaw(r.stock) + '</td>' +
      '<td class="rmp-num" style="' + covColor(r.coverageRatio) + '">' + (r.coverageRatio*100).toFixed(0) + '%</td>' +
      dayCells +
      '<td class="rmp-num" style="' + impColor(r.totalPlanned) + '">' + fmt(r.totalPlanned) + '</td>' +
      '<td class="rmp-num" style="' + misColor(missingKg) + '">' + (missingKg>0 ? fmtRaw(missingKg) : '—') + '</td>' +
      '<td class="rmp-num" style="color:' +
           (r.endingStock > 0 ? '#15803d' : '#94a3b8') + ';font-weight:' +
           (r.endingStock > 0 ? '600' : '400') + '">' + fmtRaw(r.endingStock || 0) + '</td>' +
      '<td style="text-align:center"><span class="' + badgeCls(r.riskLevel) + '">' + r.riskLevel + '</span></td>' +
      '<td style="text-align:center;font-weight:700;font-size:12px;color:#334155">' + (i+1) + '</td>' +
      '<td class="rmp-note">' + _esc(r.note) + '</td>' +
      '</tr>';
  }).join('');
}

function exportRMPlanningExcel() {
  if (!_rmpResults.length) { alert('ไม่มีข้อมูล — กรุณากด "คำนวณใหม่" ก่อน Export'); return; }
  if (typeof XLSX === 'undefined') { alert('SheetJS library ไม่พร้อมใช้งาน'); return; }

  const r0 = _rmpResults[0];
  const targetWeekPad = String(r0.targetWeek).padStart(2,'0');
  const baseWeek      = r0.baseWeek, baseYear = r0.baseYear;

  const today = new Date();
  const ymd   = today.getFullYear().toString() +
                String(today.getMonth()+1).padStart(2,'0') +
                String(today.getDate()).padStart(2,'0');

  // Fixed 6-day columns: Mon–Sat
  const FIXED_DAYS  = [0,1,2,3,4,5];
  const DAY_EN_FULL = ['Mon Plan (KG)','Tue Plan (KG)','Wed Plan (KG)',
                       'Thu Plan (KG)','Fri Plan (KG)','Sat Plan (KG)'];

  const headers = [
    'Export Date', 'Target Week', 'Base Week',
    'RM Name', 'RM Code', 'RM Group',
    'Base Demand (KG)', 'Stock (KG)', 'Coverage Ratio (%)',
    ...DAY_EN_FULL,
    'Total Planned Import (KG)', 'Missing (KG)', 'Ending Stock (KG)',
    'Risk Level', 'Priority Rank', 'Note'
  ];

  const aoa = [headers];
  _rmpResults.forEach((r, i) => {
    const missingKg = r.missingKg != null ? r.missingKg : Math.max(0, r.forecastDemand - (r.stock + r.totalPlanned));
    aoa.push([
      today.toISOString().slice(0,10),
      'W' + String(r.targetWeek).padStart(2,'0') + '/' + r.targetYear,
      'W' + String(baseWeek).padStart(2,'0') + '/' + baseYear,
      r.name, r.code, r.group,
      +r.forecastDemand.toFixed(2),
      +r.stock.toFixed(2),
      +(r.coverageRatio*100).toFixed(1),
      ...FIXED_DAYS.map(d => +((r.dayPlan[d]||0).toFixed(2))),
      +r.totalPlanned.toFixed(2),
      +missingKg.toFixed(2),
      +r.endingStock.toFixed(2),
      r.riskLevel, i+1, r.note
    ]);
  });

  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.aoa_to_sheet(aoa);

  // Column widths: ExportDate,TargetWk,BaseWk,RMName,RMCode,RMGroup,BaseDemand,Stock,Coverage,
  //               Mon-Sat(x6), TotalImport, Missing, EndingStock, RiskLevel, Rank, Note
  ws['!cols'] = [
    {wch:12},{wch:14},{wch:14},
    {wch:34},{wch:14},{wch:16},
    {wch:18},{wch:16},{wch:16},
    {wch:14},{wch:14},{wch:14},{wch:14},{wch:14},{wch:14}, // Mon-Sat
    {wch:22},{wch:14},{wch:16},
    {wch:12},{wch:12},{wch:50}
  ];

  // Numeric format for KG/% columns
  const numFmt = '#,##0.00';
  // Col indices (0-based): BaseDemand=6, Stock=7, Coverage=8, Mon-Sat=9..14,
  //                        TotalImport=15, Missing=16, EndingStock=17
  const numColIdxs = [6,7,8,9,10,11,12,13,14,15,16,17];
  for (let row = 2; row <= aoa.length; row++) {
    numColIdxs.forEach(ci => {
      const colLetter = ci < 26 ? String.fromCharCode(65+ci)
                                : String.fromCharCode(64+Math.floor(ci/26)) + String.fromCharCode(65+(ci%26));
      const cell = ws[colLetter + row];
      if (cell && typeof cell.v === 'number') { cell.t = 'n'; cell.z = numFmt; }
    });
  }
  ws['!freeze'] = {xSplit: 0, ySplit: 1};
  XLSX.utils.book_append_sheet(wb, ws, 'RM Planning');
  XLSX.writeFile(wb, 'RM_Planning_Week' + targetWeekPad + '_' + ymd + '.xlsx');
}

// ── updateDataTimestamp: ONLY uses generatedAt (pipeline generation time) ──
// ★ FIX 2026-03-23: RAW.last_updated is NEVER used (stale legacy field)
// Single source of truth = data.m.generatedAt
function updateDataTimestamp(data) {
  const el = document.getElementById('lastUpdate');
  if (!el) return;

  // ONLY source: generatedAt from pipeline metadata
  const ts = (data && data.m && data.m.generatedAt)
          || (typeof META !== 'undefined' && META.generatedAt);

  if (!ts) {
    console.warn('[Timestamp] ⚠️ No generatedAt found in data');
    el.textContent = 'Last Update: -';
    return;
  }

  const d = new Date(ts);
  const formatted = d.toLocaleDateString('th-TH') + ' ' + d.toLocaleTimeString('th-TH');
  el.textContent = 'Last Update: ' + formatted;
  console.log('[Timestamp] 🕒 DISPLAY:', formatted, '(source: m.generatedAt =', ts, ')');
}
// ═══════════════════════════════════════════════════════════════════════════
// ★ AUTO-REFRESH WATCHER (Added 2026-03-23)
// ═══════════════════════════════════════════════════════════════════════════
// Polls data.json every 30s. If generatedAt changed → full reload.
// Ensures: Pipeline generates new data.json → Dashboard auto-updates.
// ═══════════════════════════════════════════════════════════════════════════
let _watcherInterval = null;
let _lastGeneratedAt = null;

function startDataWatcher() {
  // Store the current generatedAt as baseline
  _lastGeneratedAt = (META && META.generatedAt) || null;
  console.log('[Watcher] 👁️ Started — polling every 30s, baseline:', _lastGeneratedAt);

  // Clear any existing watcher
  if (_watcherInterval) clearInterval(_watcherInterval);

  _watcherInterval = setInterval(async () => {
    try {
      const res = await fetch(DATA_PATH + '?check=' + Date.now(), { cache: 'no-store' });
      if (!res.ok) return;
      const check = await res.json();
      const newGen = check.m && check.m.generatedAt;

      if (newGen && newGen !== _lastGeneratedAt) {
        console.log('[Watcher] 🔄 NEW DATA DETECTED!', {
          old: _lastGeneratedAt,
          new: newGen
        });
        clearInterval(_watcherInterval);

        // Show notification bar
        const bar = document.createElement('div');
        bar.id = 'refreshBar';
        bar.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:9999;'
          + 'background:linear-gradient(90deg,#2563eb,#7c3aed);color:#fff;'
          + 'padding:10px 20px;text-align:center;font-size:14px;font-weight:600;'
          + 'box-shadow:0 2px 8px rgba(0,0,0,.2);cursor:pointer';
        bar.innerHTML = '🔄 พบข้อมูลใหม่ (generatedAt: ' + newGen + ') — กำลัง Refresh อัตโนมัติ...';
        document.body.prepend(bar);

        // Auto-reload with fresh data
        setTimeout(async () => {
          try {
            const freshRAW = await loadDashboardData();
            initDashboard(freshRAW);
            console.log('[Watcher] ✅ Dashboard auto-refreshed with new data');
            bar.style.background = '#16a34a';
            bar.innerHTML = '✅ Dashboard อัปเดตสำเร็จ — generatedAt: ' + newGen;
            setTimeout(() => bar.remove(), 4000);
          } catch (e) {
            console.error('[Watcher] ❌ Auto-refresh failed:', e);
            bar.style.background = '#dc2626';
            bar.innerHTML = '❌ Auto-refresh ล้มเหลว — กด F5 เพื่อ Refresh';
          }
        }, 500);
      }
    } catch (e) {
      // Silent fail on poll — network hiccup, don't spam console
    }
  }, 30000); // Poll every 30 seconds
}

// ── Manual refresh button handler ──
function refreshDashboardData() {
  console.log('[Refresh] 🔄 Manual refresh triggered');
  if (_watcherInterval) clearInterval(_watcherInterval);
  loadDashboardData()
    .then(RAW => {
      initDashboard(RAW);
      console.log('[Refresh] ✅ Manual refresh complete');
    })
    .catch(err => {
      console.error('[Refresh] ❌ Failed:', err);
      alert('Refresh ล้มเหลว: ' + err.message);
    });
}

// ══════════════════════════════════════════════════════════════════════════
// RM SHORTAGE ANALYSIS — CHECK_LAYER2 Architecture (1 row = 1 customer + 1 RM)
// Data source: DATA (global raw dataset) — NO dependency on getFilteredData(),
//   getCurrentIdxs(), _lastFilteredRows, or any dashboard filter state.
// Own filters: fgGroup, rmCustomer, fgDateFrom, fgDateTo (all data-level)
// NO cross-row aggregation — each Layer2 detail row maps directly to 1 table entry.
// sumArr used ONLY for date-range slicing within a single row.
// r[4] = Item ภายใน (Internal Code, e.g. 01010301200)
// r[3] = รายการภายใน (Internal Name, e.g. ชิ้ง, สลัด กรีนโอ๊ค)
// ══════════════════════════════════════════════════════════════════════════
(function(){
  'use strict';

  const _fN = v => Math.round(v).toLocaleString('th-TH');
  const _esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const _norm = v => String(v||'').trim().toLowerCase();

  // ── Exception RM (FG Shortage scope only — DO NOT reference outside IIFE) ──
  // r[3] = Internal Name in data.json (RM Name field).
  // มะนาว (code 01010402000) appears in 20 rows across all 4 groups:
  //   - 'มะนาว'     : 16 Layer2 detail rows (r[1]=customer) → toggled by this filter
  //   - 'มะนาว(ลูก)': 4  Layer1 summary rows (r[1]='') → excluded by base filter already
  // Toggle controls 16 Layer2 detail rows only.
  const EXCEPTION_RM = ['มะนาว', 'มะนาว(ลูก)'];
  const _excNorm = EXCEPTION_RM.map(_norm);   // pre-normalize once at init
  let _showException = false;                 // default: exclude exception RM from pipeline

  // ── RM-specific date index calculator (reads own date inputs) ──
  function _fgDateIdxs(){
    if(typeof DATES==='undefined'||!DATES||!DATES.length) return [];
    const dMin = new Date(DATES[0]);
    const dMax = new Date(DATES[DATES.length-1]);
    const elFrom = document.getElementById('fgDateFrom');
    const elTo   = document.getElementById('fgDateTo');
    const df = (elFrom && elFrom.value) ? new Date(Math.max(new Date(elFrom.value), dMin)) : dMin;
    const dt = (elTo   && elTo.value)   ? new Date(Math.min(new Date(elTo.value),   dMax)) : dMax;
    return DATES.map((d,i)=>({d,i})).filter(x=>{const xd=new Date(x.d);return xd>=df&&xd<=dt;}).map(x=>x.i);
  }

  // ── RM data filter (Group + Customer — all data-level) ──
  // CHECK_LAYER2 architecture: each row = 1 customer + 1 RM pair.
  // Customer filter is now SAFE — no cross-customer aggregation needed.
  // RULE: "Filter raw → then transform → never reverse"
  function _fgFilteredRows(){
    if(typeof DATA==='undefined'||!DATA||!DATA.length) return [];
    const g = (document.getElementById('fgGroup')||{}).value || '';
    const c = (document.getElementById('rmCustomer')||{}).value || '';
    const cNorm = _norm(c);

    // Step 1: Layer2 detail base (has customer + RM code + valid category)
    let rows = DATA.filter(r => {
      if(r[0]==='Total') return false;
      if(!r[1]||r[1]==='') return false;
      if(!r[4]||r[4]==='') return false;
      if(r[2]==='-') return false;
      return true;
    });
    const afterBase = rows.length;

    // Step 2: Group filter
    if(g){
      rows = rows.filter(r => r[0]===g);
    }
    const afterGroup = rows.length;

    // Step 3: Customer filter (normalized comparison)
    if(cNorm){
      rows = rows.filter(r => _norm(r[1])===cNorm);
    }
    const afterCust = rows.length;

    // Step 4: Exception RM filter — applied to r[3] (Internal Name in data.json)
    // Controlled by _showException toggle (FG Shortage scope only)
    if(!_showException){
      rows = rows.filter(r => !_excNorm.includes(_norm(r[3])));
    }
    const afterExc = rows.length;

    console.log('[RMShortage:filter]',
      'base:', afterBase,
      '→ group(' + (g||'ALL') + '):', afterGroup,
      '→ customer(' + (c||'ALL') + '):', afterCust,
      '→ exc(' + (_showException?'show':'hide') + '):', afterExc);

    return rows;
  }

  // ── Populate ALL RM filter dropdowns from raw DATA ──
  // Group populates from all data; Customer cascades from selected Group.
  function _populateFGDropdowns(){
    if(typeof DATA==='undefined'||!DATA||!DATA.length) return;
    const gEl = document.getElementById('fgGroup');
    const cEl = document.getElementById('rmCustomer');
    if(!gEl) return;

    // Groups — from all Layer2 detail rows
    const curG = gEl.value;
    const groups = new Set();
    DATA.forEach(r=>{
      if(r[0]&&r[0]!=='Total'&&r[1]&&r[1]!==''&&r[4]&&r[4]!=='') groups.add(r[0]);
    });
    gEl.innerHTML = '<option value="">ทั้งหมด</option>';
    [...groups].sort().forEach(g=>{
      const o=document.createElement('option');o.value=g;o.textContent=g;gEl.appendChild(o);
    });
    if(curG && groups.has(curG)) gEl.value = curG;

    // Customers — cascade from selected Group (normalized matching for restore)
    if(cEl){
      const curC = cEl.value;
      const curCNorm = _norm(curC);
      const selG = gEl.value;
      const custs = new Set();
      DATA.forEach(r=>{
        if(r[0]==='Total') return;
        if(!r[1]||r[1]==='') return;
        if(!r[4]||r[4]==='') return;
        if(r[2]==='-') return;
        if(selG && r[0]!==selG) return;
        custs.add(r[1]);
      });
      cEl.innerHTML = '<option value="">ทั้งหมด</option>';
      let restored = false;
      [...custs].sort().forEach(c=>{
        const o=document.createElement('option');o.value=c;o.textContent=c;cEl.appendChild(o);
        if(!restored && curCNorm && _norm(c)===curCNorm){ cEl.value = c; restored = true; }
      });
      // If customer not in new group → reset to ALL
      if(curCNorm && !restored) cEl.value = '';
    }

    // Date defaults
    if(typeof DATES!=='undefined' && DATES && DATES.length){
      const dfEl = document.getElementById('fgDateFrom');
      const dtEl = document.getElementById('fgDateTo');
      if(dfEl && !dfEl.value) dfEl.value = DATES[0];
      if(dtEl && !dtEl.value) dtEl.value = DATES[DATES.length-1];
    }
  }

  // ══════════════════════════════════════════════════════════════════
  // MAIN RENDER — CHECK_LAYER2 direct-row (NO aggregation)
  // ══════════════════════════════════════════════════════════════════
  window.renderFGShortage = function renderFGShortage(){
    const kpiBar  = document.getElementById('fgsKpiBar');
    const tableEl = document.getElementById('fgsTableWrap');
    if(!kpiBar || !tableEl) return;

    // Populate all dropdowns (Group → Customer cascade)
    _populateFGDropdowns();

    // ══ 1. Independent data source — own filters, raw DATA ══
    // All filters are data-level: Group + Customer + Date
    const fgDetailRows = _fgFilteredRows();
    const idxs         = _fgDateIdxs();

    const _selGroup = (document.getElementById('fgGroup')||{}).value || 'ALL';
    const _selCust  = (document.getElementById('rmCustomer')||{}).value || 'ALL';
    console.log('[RMShortage] Group:', _selGroup,
                ', Customer:', _selCust,
                '→ detail rows:', fgDetailRows.length,
                ', dateIdxs:', idxs.length);

    if(!fgDetailRows.length || !idxs.length){
      kpiBar.innerHTML = '';
      tableEl.innerHTML = '<div style="text-align:center;color:#9ca3af;padding:40px;font-size:12px">' +
        'No RM detail data — rows=' + fgDetailRows.length + ', dateIdxs=' + idxs.length + '</div>';
      return;
    }

    // ══ 2. Read shortage-specific controls ══
    const filterType = (document.getElementById('fgsFilterType')||{}).value || 'all';
    const sortBy     = (document.getElementById('fgsSortBy')||{}).value || 'gap';
    const topN       = parseInt((document.getElementById('fgsTopN')||{}).value || '50', 10);

    // ══ 3. Direct per-row calculation — NO aggregation ══
    // Each Layer2 detail row = 1 customer + 1 RM → 1 table item.
    // sumArr slices by date range only (within single row).
    let grandOrd = 0, grandAct = 0;
    let grandOrdKG = 0, grandActKG = 0;

    let allItems = fgDetailRows.map(r => {
      const ord   = sumArr(r[5], idxs);
      const act   = sumArr(r[6], idxs);
      const ordKG = (r[8] && r[8].length) ? sumArr(r[8], idxs) : ord;
      const actKG = (r[9] && r[9].length) ? sumArr(r[9], idxs) : act;
      grandOrd += ord;  grandAct += act;
      grandOrdKG += ordKG;  grandActKG += actKG;
      const gap   = ord - act;
      const gapKG = ordKG - actKG;
      const fr    = ord > 0 ? (act/ord*100) : (act===0 ? 0 : 100);
      return {
        code: r[4], name: r[3]||'', cat: r[2]||'', cust: r[1]||'',
        ord:ord, act:act, gap:gap, fr:fr,
        ordKG:ordKG, actKG:actKG, gapKG:gapKG
      };
    });

    // ══ 4. Cross-check log ══
    const grandGap = grandOrd - grandAct;
    const grandFR  = grandOrd > 0 ? (grandAct/grandOrd*100) : 0;
    const grandGapKG = grandOrdKG - grandActKG;
    console.log('[RMShortage] Direct rows (no aggregation):', {
      totalRows:allItems.length, filteredRows:fgDetailRows.length,
      grandOrd:Math.round(grandOrd).toLocaleString(),
      grandAct:Math.round(grandAct).toLocaleString(),
      grandGap:Math.round(grandGap).toLocaleString(),
      grandFR:grandFR.toFixed(1)+'%',
      grandOrdKG:Math.round(grandOrdKG).toLocaleString()+' KG',
      grandGapKG:Math.round(grandGapKG).toLocaleString()+' KG'
    });

    // ══ 5. Filter by shortage type ══
    const totalCount = allItems.length;
    let items;
    if(filterType === 'zero'){
      items = allItems.filter(e => e.act === 0 && e.ord > 0);
    } else if(filterType === 'partial'){
      items = allItems.filter(e => e.act > 0 && e.act < e.ord);
    } else {
      items = allItems.filter(e => e.gap > 0);
    }

    // ══ 6. Sort ══
    if(sortBy === 'gap')           items.sort((a,b) => b.gap - a.gap);
    else if(sortBy === 'order')    items.sort((a,b) => b.ord - a.ord);
    else if(sortBy === 'fillrate') items.sort((a,b) => a.fr - b.fr);

    // ══ 7. Summary KPIs ══
    const shortageCount = items.length;
    const sumGap   = items.reduce((s,e) => s + e.gap, 0);
    const sumSOrd  = items.reduce((s,e) => s + e.ord, 0);
    const sumSAct  = items.reduce((s,e) => s + e.act, 0);
    const avgFR    = sumSOrd > 0 ? (sumSAct/sumSOrd*100) : 0;
    const zeroCount = allItems.filter(e => e.act===0 && e.ord>0).length;
    const sumGapKG = items.reduce((s,e) => s + e.gapKG, 0);

    // ══ 8. Top N ══
    if(topN > 0) items = items.slice(0, topN);

    // ══ 9. Render KPI bar ══
    const kpiCard = (label, val, sub, color) =>
      `<div style="background:#fff;border-radius:10px;padding:14px 16px;border:1px solid #e5e7eb;box-shadow:0 1px 2px rgba(0,0,0,.04)">
        <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">${label}</div>
        <div style="font-size:22px;font-weight:700;color:${color}">${val}</div>
        <div style="font-size:10px;color:#9ca3af;margin-top:2px">${sub}</div>
      </div>`;

    kpiBar.innerHTML =
      kpiCard('RM ขาดส่ง', _fN(shortageCount),
              `จาก ${_fN(totalCount)} รายการ (row)`, '#1e293b') +
      kpiCard('ขาดส่ง (Unit)', _fN(sumGap), 'หน่วย', '#dc2626') +
      kpiCard('ขาดส่ง (KG)', _fN(sumGapKG), 'กิโลกรัม', '#dc2626') +
      kpiCard('Fill Rate (Shortage)', avgFR.toFixed(1)+'%',
              'เฉพาะ RM ขาดส่ง', avgFR>=80?'#16a34a':avgFR>=50?'#d97706':'#dc2626') +
      kpiCard('Zero Delivery', _fN(zeroCount),
              'ACT=0 แต่มียอดสั่ง', zeroCount>0?'#dc2626':'#16a34a');

    // ══ 10. Render table ══
    if(!items.length){
      tableEl.innerHTML = '<div style="text-align:center;color:#16a34a;padding:40px;font-size:13px">' +
        'ไม่พบรายการ RM ขาดส่งตามเงื่อนไขที่เลือก</div>';
      return;
    }

    const selCustVal = (document.getElementById('rmCustomer')||{}).value || '';
    const showCust = !selCustVal; // Show customer column when viewing ALL customers

    let html = `<div style="font-size:10px;color:#9ca3af;margin-bottom:8px">` +
      `แสดง ${_fN(items.length)} จาก ${_fN(shortageCount)} รายการ | ` +
      `Source: DATA→Layer2 (${fgDetailRows.length} rows, direct — no aggregation)` +
      (selCustVal ? ` | Customer: ${_esc(selCustVal)}` : '') +
      ` | Filter: ${filterType} | Sort: ${sortBy}</div>`;

    html += `<table style="width:100%;font-size:11px;border-collapse:collapse;background:#fff">
      <thead><tr style="background:#f1f5f9;position:sticky;top:0">
        <th style="padding:8px 10px;text-align:left;font-weight:600">#</th>` +
        (showCust ? `<th style="padding:8px 10px;text-align:left;font-weight:600">Customer</th>` : '') +
        `<th style="padding:8px 10px;text-align:left;font-weight:600">RM Code</th>
        <th style="padding:8px 10px;text-align:left;font-weight:600">รายการภายใน</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Order (Unit)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Order (KG)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Actual (Unit)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Actual (KG)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Gap (Unit)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Gap (KG)</th>
        <th style="padding:8px 10px;text-align:right;font-weight:600">Fill Rate</th>
        <th style="padding:8px 10px;text-align:center;font-weight:600;min-width:100px">Status</th>
      </tr></thead><tbody>`;

    items.forEach((e, idx) => {
      const frColor = e.fr >= 80 ? '#16a34a' : e.fr >= 50 ? '#d97706' : '#dc2626';
      const statusTag = e.act === 0
        ? '<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600">Zero Delivery</span>'
        : e.fr < 50
          ? '<span style="background:#fef2f2;color:#dc2626;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600">Critical</span>'
          : e.fr < 80
            ? '<span style="background:#fffbeb;color:#d97706;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600">Warning</span>'
            : '<span style="background:#f0fdf4;color:#16a34a;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:600">Minor</span>';

      const barW = Math.min(e.fr, 100);
      const barColor = e.fr >= 80 ? '#86efac' : e.fr >= 50 ? '#fde68a' : '#fca5a5';

      html += `<tr style="border-bottom:1px solid #f1f5f9;${idx%2?'background:#fafbfc':''}">
        <td style="padding:6px 10px;color:#9ca3af">${idx+1}</td>` +
        (showCust ? `<td style="padding:6px 10px;font-weight:500;color:#475569">${_esc(e.cust)}</td>` : '') +
        `<td style="padding:6px 10px;font-family:monospace;font-weight:600;color:#1e40af">${_esc(e.code)}</td>
        <td style="padding:6px 10px;font-weight:500">${_esc(e.name)}</td>
        <td style="padding:6px 10px;text-align:right">${_fN(e.ord)}</td>
        <td style="padding:6px 10px;text-align:right;color:#6b7280">${_fN(e.ordKG)}</td>
        <td style="padding:6px 10px;text-align:right">${_fN(e.act)}</td>
        <td style="padding:6px 10px;text-align:right;color:#6b7280">${_fN(e.actKG)}</td>
        <td style="padding:6px 10px;text-align:right;font-weight:700;color:#dc2626">${_fN(e.gap)}</td>
        <td style="padding:6px 10px;text-align:right;font-weight:700;color:#dc2626">${_fN(e.gapKG)}</td>
        <td style="padding:6px 10px;text-align:right">
          <div style="display:flex;align-items:center;justify-content:flex-end;gap:6px">
            <div style="width:50px;height:6px;background:#e5e7eb;border-radius:3px;overflow:hidden">
              <div style="width:${barW}%;height:100%;background:${barColor};border-radius:3px"></div>
            </div>
            <span style="font-weight:600;color:${frColor};min-width:42px;text-align:right">${e.fr.toFixed(1)}%</span>
          </div>
        </td>
        <td style="padding:6px 10px;text-align:center">${statusTag}</td>
      </tr>`;
    });

    html += '</tbody></table>';
    tableEl.innerHTML = html;
  };

  // ══ Wire RM filter/sort events ══
  document.addEventListener('DOMContentLoaded', function(){

    // Data-level filters (Group + Customer + Date) — all affect data pipeline
    ['fgGroup','rmCustomer','fgDateFrom','fgDateTo'].forEach(id => {
      const el = document.getElementById(id);
      if(el) el.addEventListener('change', () => { renderFGShortage(); });
    });

    // Shortage-specific controls
    ['fgsFilterType','fgsSortBy','fgsTopN'].forEach(id => {
      const el = document.getElementById(id);
      if(el) el.addEventListener('change', () => { renderFGShortage(); });
    });

    // Exception RM toggle (FG Shortage scope only)
    // Toggles _showException state and updates button visual, then re-renders
    const excBtn  = document.getElementById('fgsExcToggle');
    const excIcon = document.getElementById('fgsExcIcon');
    if(excBtn){
      excBtn.addEventListener('click', function(){
        _showException = !_showException;
        if(_showException){
          excBtn.style.background  = '#fef9c3';
          excBtn.style.borderColor = '#fbbf24';
          excBtn.style.color       = '#92400e';
          excIcon.textContent      = '👁';
          excBtn.childNodes[1].textContent = ' มะนาว: แสดง';
        } else {
          excBtn.style.background  = '#f1f5f9';
          excBtn.style.borderColor = '#d1d5db';
          excBtn.style.color       = '#374151';
          excIcon.textContent      = '🚫';
          excBtn.childNodes[1].textContent = ' มะนาว: ซ่อน';
        }
        console.log('[RMShortage] Exception toggle:', _showException ? 'SHOW' : 'HIDE');
        renderFGShortage();
      });
    }

  });

})();
// ═══ END RM SHORTAGE ANALYSIS ══════════════════════════════════════════

// ── Init Lucide icons ──
if(typeof lucide!=='undefined') lucide.createIcons();
