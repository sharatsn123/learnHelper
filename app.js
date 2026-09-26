
/* =========================================================
   IndexedDB layer
   ========================================================= */
const DB_NAME = 'srTrainerDB';
const DB_VERSION = 1;
const STORE_QUESTIONS = 'questions';
const STORE_LOGS = 'logs';

let dbPromise = null;

function openDb() {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = (e) => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains(STORE_QUESTIONS)) {
        db.createObjectStore(STORE_QUESTIONS, { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains(STORE_LOGS)) {
        db.createObjectStore(STORE_LOGS, { keyPath: 'logId', autoIncrement: true });
      }
    };
    req.onsuccess = (e) => resolve(e.target.result);
    req.onerror = (e) => reject(e.target.error);
  });
  return dbPromise;
}

function tx(storeName, mode) {
  return openDb().then(db => db.transaction(storeName, mode).objectStore(storeName));
}

function reqToPromise(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function getAllQuestions() {
  const store = await tx(STORE_QUESTIONS, 'readonly');
  return reqToPromise(store.getAll());
}

async function getQuestion(id) {
  const store = await tx(STORE_QUESTIONS, 'readonly');
  return reqToPromise(store.get(id));
}

async function putQuestion(q) {
  const store = await tx(STORE_QUESTIONS, 'readwrite');
  return reqToPromise(store.put(q));
}

async function clearQuestions() {
  const store = await tx(STORE_QUESTIONS, 'readwrite');
  return reqToPromise(store.clear());
}

async function addLog(entry) {
  const store = await tx(STORE_LOGS, 'readwrite');
  return reqToPromise(store.add(entry));
}

async function getAllLogs() {
  const store = await tx(STORE_LOGS, 'readonly');
  return reqToPromise(store.getAll());
}

async function clearLogs() {
  const store = await tx(STORE_LOGS, 'readwrite');
  return reqToPromise(store.clear());
}

/* Bulk import: for each csv row, update question/topic/answer fields only,
   preserving priority/lastShown if the id already exists. */
async function bulkImport(rows) {
  const db = await openDb();
  const store = db.transaction(STORE_QUESTIONS, 'readwrite').objectStore(STORE_QUESTIONS);
  let inserted = 0, updated = 0;

  for (const row of rows) {
    const existing = await reqToPromise(store.get(row.id));
    if (existing) {
      existing.question = row.question;
      existing.topic = row.topic;
      existing.answer = row.answer;
      store.put(existing);
      updated++;
    } else {
      store.put({
        id: row.id,
        question: row.question,
        topic: row.topic,
        answer: row.answer,
        priority: 5,
        lastShown: null
      });
      inserted++;
    }
  }
  return { inserted, updated };
}

/* =========================================================
   CSV parsing (handles quoted fields, escaped quotes, commas)
   ========================================================= */
function parseCSV(text) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  // normalize line endings
  text = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else { inQuotes = false; }
      } else {
        field += c;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
      } else if (c === ',') {
        row.push(field); field = '';
      } else if (c === '\n') {
        row.push(field); field = '';
        rows.push(row); row = [];
      } else {
        field += c;
      }
    }
  }
  // last field/row (if file doesn't end with newline)
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  // drop trailing empty rows
  return rows.filter(r => !(r.length === 1 && r[0].trim() === ''));
}

function csvRowsToObjects(rows) {
  if (rows.length === 0) return [];
  const header = rows[0].map(h => h.trim().toLowerCase());
  const idIdx = header.indexOf('id');
  const qIdx = header.indexOf('question');
  const tIdx = header.indexOf('topic');
  const aIdx = header.indexOf('answer');
  if (idIdx === -1 || qIdx === -1 || tIdx === -1 || aIdx === -1) {
    throw new Error('CSV must contain columns: id, question, topic, answer');
  }
  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    if (!r || r.every(c => c.trim() === '')) continue;
    out.push({
      id: (r[idIdx] || '').trim(),
      question: (r[qIdx] || '').trim(),
      topic: (r[tIdx] || '').trim(),
      answer: (r[aIdx] || '').trim()
    });
  }
  return out;
}

function csvEscape(value) {
  const s = String(value ?? '');
  if (/[",\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

/* =========================================================
   Weighted question selection
   score = priority * (1 + log(1 + daysSinceLastShown))
   never-shown => treated as maximally stale
   ========================================================= */
const MS_PER_DAY = 86400000;
const NEVER_SHOWN_DAYS = 3650; // ~10 years: large but finite, so weighting math stays well-defined

function daysSinceLastShown(lastShown) {
  if (lastShown === null || lastShown === undefined) return NEVER_SHOWN_DAYS;
  return Math.max(0, (Date.now() - lastShown) / MS_PER_DAY);
}

function staleFactor(days) {
  return 1 + Math.log(1 + days);
}

function computeScore(q) {
  return q.priority * staleFactor(daysSinceLastShown(q.lastShown));
}

function weightedPick(questions) {
  const scores = questions.map(computeScore);
  const total = scores.reduce((a, b) => a + b, 0);
  if (total <= 0) return questions[Math.floor(Math.random() * questions.length)];
  let r = Math.random() * total;
  for (let i = 0; i < questions.length; i++) {
    r -= scores[i];
    if (r <= 0) return questions[i];
  }
  return questions[questions.length - 1];
}

/* =========================================================
   App state & view switching
   ========================================================= */
let currentTopic = null;
let currentQuestion = null;

function showView(id) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}

async function showTopicsView() {
  showView('view-topics');
  await renderTopics();
}

async function showStatsView() {
  showView('view-stats');
  await renderStats();
}

async function renderTopics() {
  const questions = await getAllQuestions();
  const listEl = document.getElementById('topicsList');
  const emptyEl = document.getElementById('topicsEmpty');
  listEl.innerHTML = '';

  if (questions.length === 0) {
    emptyEl.classList.remove('d-none');
    return;
  }
  emptyEl.classList.add('d-none');

  const counts = {};
  questions.forEach(q => { counts[q.topic] = (counts[q.topic] || 0) + 1; });
  const topics = Object.keys(counts).sort((a, b) => a.localeCompare(b));

  topics.forEach(topic => {
    const col = document.createElement('div');
    col.className = 'col-md-4 col-sm-6';
    col.innerHTML = `
      <div class="card topic-card p-3 h-100" onclick="startTopic(${JSON.stringify(topic).replace(/"/g, '&quot;')})">
        <h5 class="mb-1">${escapeHtml(topic)}</h5>
        <span class="muted small">${counts[topic]} question${counts[topic] === 1 ? '' : 's'}</span>
      </div>`;
    listEl.appendChild(col);
  });
}

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s ?? '';
  return div.innerHTML;
}

async function startTopic(topic) {
  currentTopic = topic;
  showView('view-review');
  await nextQuestion();
}

async function nextQuestion() {
  const all = await getAllQuestions();
  const pool = all.filter(q => q.topic === currentTopic);
  if (pool.length === 0) {
    showTopicsView();
    return;
  }
  currentQuestion = weightedPick(pool);

  // Update lastShown immediately upon display
  currentQuestion.lastShown = Date.now();
  await putQuestion(currentQuestion);
  await addLog({ timestamp: Date.now(), questionId: currentQuestion.id, topic: currentQuestion.topic, event: 'shown' });

  renderQuestion();
}

function renderQuestion() {
  document.getElementById('reviewTopicBadge').textContent = currentQuestion.topic;
  document.getElementById('reviewPriorityBadge').textContent = 'Priority ' + currentQuestion.priority;
  document.getElementById('questionText').textContent = currentQuestion.question;
  renderAnswerMarkdown(currentQuestion.answer);
  document.getElementById('answerSection').classList.add('d-none');
  document.getElementById('showAnswerBtn').classList.remove('d-none');
  document.getElementById('answeredBtn').classList.add('d-none');
  document.getElementById('unansweredBtn').classList.add('d-none');
  document.getElementById('reviewMeta').textContent = 'ID: ' + currentQuestion.id;
}

function renderAnswerMarkdown(markdown) {
  const box = document.getElementById('answerText');
  const html = (typeof marked !== 'undefined') ? marked.parse(markdown ?? '') : escapeHtml(markdown ?? '');
  box.innerHTML = (typeof DOMPurify !== 'undefined') ? DOMPurify.sanitize(html) : html;
  if (typeof Prism !== 'undefined') {
    Prism.highlightAllUnder(box);
  }
}

function onShowAnswer() {
  document.getElementById('answerSection').classList.remove('d-none');
  document.getElementById('showAnswerBtn').classList.add('d-none');
  document.getElementById('answeredBtn').classList.remove('d-none');
  document.getElementById('unansweredBtn').classList.remove('d-none');
}

async function onRecordResponse(result) {
  if (!currentQuestion) return;
  if (result === 'answered') {
    currentQuestion.priority = Math.max(1, currentQuestion.priority - 1);
  } else {
    currentQuestion.priority = Math.min(10, currentQuestion.priority + 1);
  }
  await putQuestion(currentQuestion);
  await addLog({ timestamp: Date.now(), questionId: currentQuestion.id, topic: currentQuestion.topic, event: result });
  await nextQuestion();
}

/* =========================================================
   CSV Import
   ========================================================= */
async function handleImport() {
  const input = document.getElementById('csvFileInput');
  const resultEl = document.getElementById('importResult');
  resultEl.classList.remove('d-none', 'alert-danger', 'alert-info');

  if (!input.files || input.files.length === 0) {
    resultEl.classList.add('alert-danger');
    resultEl.textContent = 'Please choose a CSV file first.';
    return;
  }

  try {
    const text = await input.files[0].text();
    const rows = parseCSV(text);
    const objects = csvRowsToObjects(rows);
    if (objects.length === 0) {
      throw new Error('No data rows found in file.');
    }
    const { inserted, updated } = await bulkImport(objects);
    resultEl.classList.add('alert-info');
    resultEl.textContent = `Imported: ${inserted} new, ${updated} updated.`;
    await renderTopics();
  } catch (err) {
    resultEl.classList.add('alert-danger');
    resultEl.textContent = 'Import failed: ' + err.message;
  }
}

/* =========================================================
   Export CSV
   ========================================================= */
async function exportCsv() {
  const questions = await getAllQuestions();
  const header = ['id', 'question', 'topic', 'answer', 'priority', 'lastShown'];
  const lines = [header.join(',')];
  questions.forEach(q => {
    lines.push([
      csvEscape(q.id),
      csvEscape(q.question),
      csvEscape(q.topic),
      csvEscape(q.answer),
      csvEscape(q.priority),
      csvEscape(q.lastShown ? new Date(q.lastShown).toISOString() : '')
    ].join(','));
  });
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'sr_trainer_export_' + new Date().toISOString().slice(0, 10) + '.csv';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/* =========================================================
   Data viewer / reset / clear
   ========================================================= */
document.getElementById('dataModal').addEventListener('show.bs.modal', renderDataTable);

function computeNextId(questions) {
  let maxNum = 0;
  questions.forEach(q => {
    const n = parseInt(q.id, 10);
    if (!isNaN(n) && n > maxNum) maxNum = n;
  });
  return String(maxNum + 1);
}

function matchesSearch(q, term) {
  if (!term) return true;
  term = term.toLowerCase();
  return String(q.id).toLowerCase().includes(term)
    || String(q.topic).toLowerCase().includes(term)
    || String(q.question).toLowerCase().includes(term)
    || String(q.answer).toLowerCase().includes(term);
}

function makeEditableCell(value, onCommit, tag) {
  const td = document.createElement('td');
  const el = document.createElement(tag || 'input');
  el.className = 'form-control form-control-sm';
  if (tag === 'textarea') {
    el.rows = 2;
    el.value = value ?? '';
  } else {
    el.type = 'text';
    el.value = value ?? '';
  }
  el.addEventListener('change', () => onCommit(el.value));
  td.appendChild(el);
  return td;
}

async function renderDataTable() {
  const questions = await getAllQuestions();
  const term = document.getElementById('dataSearchInput').value.trim();
  const filtered = questions.filter(q => matchesSearch(q, term));
  document.getElementById('dataCountLine').textContent =
    `${filtered.length} of ${questions.length} question(s) shown.`;
  const body = document.getElementById('dataTableBody');
  body.innerHTML = '';
  filtered
    .sort((a, b) => a.topic.localeCompare(b.topic) || String(a.id).localeCompare(String(b.id)))
    .forEach(q => {
      const tr = document.createElement('tr');

      const idTd = document.createElement('td');
      idTd.textContent = q.id;
      tr.appendChild(idTd);

      tr.appendChild(makeEditableCell(q.topic, (v) => updateQuestionField(q.id, 'topic', v), 'input'));
      tr.appendChild(makeEditableCell(q.question, (v) => updateQuestionField(q.id, 'question', v), 'textarea'));
      tr.appendChild(makeEditableCell(q.answer, (v) => updateQuestionField(q.id, 'answer', v), 'textarea'));

      const priorityTd = document.createElement('td');
      const priorityInput = document.createElement('input');
      priorityInput.type = 'number';
      priorityInput.min = 1;
      priorityInput.max = 10;
      priorityInput.style.width = '70px';
      priorityInput.className = 'form-control form-control-sm';
      priorityInput.value = q.priority;
      priorityInput.addEventListener('change', () => updateQuestionField(q.id, 'priority', priorityInput.value));
      priorityTd.appendChild(priorityInput);
      tr.appendChild(priorityTd);

      const lastShownTd = document.createElement('td');
      lastShownTd.className = 'small';
      if (q.lastShown) {
        lastShownTd.textContent = new Date(q.lastShown).toLocaleString();
      } else {
        const span = document.createElement('span');
        span.className = 'muted';
        span.textContent = 'never';
        lastShownTd.appendChild(span);
      }
      tr.appendChild(lastShownTd);

      const actionsTd = document.createElement('td');
      const delBtn = document.createElement('button');
      delBtn.type = 'button';
      delBtn.className = 'btn btn-outline-danger btn-sm';
      delBtn.textContent = 'Delete';
      delBtn.addEventListener('click', () => deleteQuestionRow(q.id));
      actionsTd.appendChild(delBtn);
      tr.appendChild(actionsTd);

      body.appendChild(tr);
    });
}

async function updateQuestionField(id, field, value) {
  const q = await getQuestion(id);
  if (!q) return;
  if (field === 'priority') {
    let n = parseInt(value, 10);
    if (isNaN(n)) n = q.priority;
    q.priority = Math.min(10, Math.max(1, n));
  } else {
    q[field] = value;
  }
  await putQuestion(q);
  if (field === 'topic') await renderTopics();
  await renderDataTable();
}

async function addNewQuestion() {
  const questions = await getAllQuestions();
  const newId = computeNextId(questions);
  const newQuestion = { id: newId, topic: '', question: '', answer: '', priority: 5, lastShown: null };
  await putQuestion(newQuestion);
  document.getElementById('dataSearchInput').value = '';
  await renderDataTable();
  await renderTopics();
}

async function deleteQuestionRow(id) {
  if (!confirm('Delete question ' + id + '?')) return;
  const store = await tx(STORE_QUESTIONS, 'readwrite');
  await reqToPromise(store.delete(id));
  await renderDataTable();
  await renderTopics();
}

async function resetProgress() {
  if (!confirm('Reset priority to 5 and lastShown to never for ALL questions? Question content is kept.')) return;
  const questions = await getAllQuestions();
  for (const q of questions) {
    q.priority = 5;
    q.lastShown = null;
    await putQuestion(q);
  }
  await renderDataTable();
  await renderTopics();
  alert('Progress reset for ' + questions.length + ' question(s).');
}

async function clearAllData() {
  if (!confirm('This will permanently delete ALL questions and history logs. Continue?')) return;
  await clearQuestions();
  await clearLogs();
  await renderDataTable();
  await renderTopics();
  bootstrap.Modal.getInstance(document.getElementById('dataModal'))?.hide();
  showTopicsView();
  alert('All data cleared.');
}

/* =========================================================
   Statistics (last 10 days)
   ========================================================= */
function dayKey(ts) {
  const d = new Date(ts);
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

function dayLabel(key) {
  const [y, m, d] = key.split('-').map(Number);
  const dt = new Date(y, m - 1, d);
  return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

async function renderStats() {
  const logs = await getAllLogs();

  // Build last 10 days (oldest -> newest), including today
  const days = [];
  for (let i = 9; i >= 0; i--) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    days.push(dayKey(d.getTime()));
  }

  const stats = {};
  days.forEach(k => { stats[k] = { attempted: 0, answered: 0, unanswered: 0 }; });

  const cutoff = Date.now() - 10 * MS_PER_DAY;
  logs.filter(l => l.timestamp >= cutoff).forEach(l => {
    const key = dayKey(l.timestamp);
    if (!stats[key]) return;
    if (l.event === 'shown') stats[key].attempted++;
    else if (l.event === 'answered') stats[key].answered++;
    else if (l.event === 'unanswered') stats[key].unanswered++;
  });

  // Chart
  const maxVal = Math.max(1, ...days.map(k => Math.max(stats[k].attempted, stats[k].answered + stats[k].unanswered)));
  const chartEl = document.getElementById('statsChart');
  chartEl.innerHTML = '';
  days.forEach(key => {
    const s = stats[key];
    const row = document.createElement('div');
    row.className = 'chart-row';
    const answeredPct = (s.answered / maxVal) * 100;
    const unansweredPct = (s.unanswered / maxVal) * 100;
    row.innerHTML = `
      <div class="chart-day-label">${dayLabel(key)}</div>
      <div class="chart-bars">
        <div class="bar-track" title="Answered: ${s.answered}">
          <div class="bar-fill answered" style="width:${answeredPct}%"></div>
          <span class="bar-count">${s.answered} answered</span>
        </div>
        <div class="bar-track" title="Didn't Answer: ${s.unanswered}">
          <div class="bar-fill unanswered" style="width:${unansweredPct}%"></div>
          <span class="bar-count">${s.unanswered} didn't answer</span>
        </div>
      </div>
      <div class="ms-2 muted small" style="width:110px; text-align:right;">${s.attempted} attempted</div>`;
    chartEl.appendChild(row);
  });

  // Table
  const body = document.getElementById('statsTableBody');
  body.innerHTML = '';
  days.slice().reverse().forEach(key => {
    const s = stats[key];
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${dayLabel(key)}</td><td>${s.attempted}</td><td>${s.answered}</td><td>${s.unanswered}</td>`;
    body.appendChild(tr);
  });
}

/* =========================================================
   Init
   ========================================================= */
window.addEventListener('DOMContentLoaded', () => {
  showTopicsView();
});