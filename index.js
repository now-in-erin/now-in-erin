require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const Database = require('better-sqlite3');

const app = express();
app.use(cors({
  origin: '*'
}));
app.use(express.json());

const API_KEY = process.env.NEXON_API_KEY;
const SERVER_NAME = process.env.SERVER_NAME || '류트';
const PORT = process.env.PORT || 3000;

// DB 초기화
const db = new Database('erin.db');
db.exec(`
  CREATE TABLE IF NOT EXISTS horn (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    character_name TEXT,
    message TEXT,
    date_send TEXT,
    category TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_date_send ON horn(date_send);
  CREATE INDEX IF NOT EXISTS idx_category ON horn(category);
`);

// 카테고리 분류
function classify(msg) {
  if (/파티|구함|모집|인원|\/\d|[0-9]\/[0-9]/.test(msg)) return 'party';
  if (/팝니다|팝|판매|삽니다|구매|구입|얼마|골드|가격/.test(msg)) return 'trade';
  return 'etc';
}

// 넥슨 API 호출 & DB 저장
async function fetchAndSave() {
  try {
    const url = `https://open.api.nexon.com/mabinogi/v1/horn-bugle-world/history?server_name=${encodeURIComponent(SERVER_NAME)}`;
    const res = await fetch(url, {
      headers: { 'x-nxopen-api-key': API_KEY }
    });

    if (!res.ok) {
      console.error(`[API 오류] HTTP ${res.status}`);
      return;
    }

    const data = await res.json();
    const items = data.horn_bugle_world_history || [];

    const insert = db.prepare(`
      INSERT OR IGNORE INTO horn (character_name, message, date_send, category)
      VALUES (?, ?, ?, ?)
    `);

    // date_send + character_name + message 조합으로 중복 방지
    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_unique ON horn(character_name, message, date_send)`);

    const insertMany = db.transaction((rows) => {
      let count = 0;
      for (const item of rows) {
        try {
          const info = insert.run(
            item.character_name,
            item.message,
            item.date_send,
            classify(item.message)
          );
          if (info.changes > 0) count++;
        } catch (e) {
          // 중복 무시
        }
      }
      return count;
    });

    const newCount = insertMany(items);
    const now = new Date().toLocaleTimeString('ko-KR');
    console.log(`[${now}] ${SERVER_NAME} 서버 — ${items.length}건 조회, ${newCount}건 신규 저장`);

  } catch (e) {
    console.error('[오류]', e.message);
  }
}

// ─── API 엔드포인트 ───────────────────────────────

// 최근 메시지 피드
app.get('/api/feed', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  const category = req.query.category; // 선택적 필터
  const keyword = req.query.keyword;

  let query = 'SELECT * FROM horn';
  const params = [];
  const conditions = [];

  if (category && category !== 'all') {
    conditions.push('category = ?');
    params.push(category);
  }

  if (keyword) {
    conditions.push('(message LIKE ? OR character_name LIKE ?)');
    params.push(`%${keyword}%`, `%${keyword}%`);
  }

  if (conditions.length > 0) {
    query += ' WHERE ' + conditions.join(' AND ');
  }

  query += ' ORDER BY date_send DESC LIMIT ?';
  params.push(limit);

  const rows = db.prepare(query).all(...params);
  res.json({ items: rows, count: rows.length });
});

// 시간대별 활동량 (KST 기준)
app.get('/api/stats/hourly', (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT 
      CAST((CAST(strftime('%H', date_send) AS INTEGER) + 9) % 24 AS INTEGER) as hour,
      COUNT(*) as count
    FROM horn
    WHERE date_send >= ?
    GROUP BY hour
    ORDER BY hour
  `).all(since);

  // 0~23 전체 채우기
  const hourly = new Array(24).fill(0);
  rows.forEach(r => { hourly[r.hour] = r.count; });

  res.json({ hourly, since, days });
});

// 카테고리 분포
app.get('/api/stats/category', (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT category, COUNT(*) as count
    FROM horn
    WHERE date_send >= ?
    GROUP BY category
  `).all(since);

  const result = { party: 0, trade: 0, etc: 0 };
  rows.forEach(r => { result[r.category] = r.count; });

  res.json(result);
});

// 키워드 트렌드 TOP 20
app.get('/api/stats/keywords', (req, res) => {
  const days = parseInt(req.query.days) || 1;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const rows = db.prepare(`
    SELECT message FROM horn WHERE date_send >= ?
  `).all(since);

  // 단어 빈도 분석
  const stopWords = new Set(['이', '가', '은', '는', '을', '를', '에', '의', '와', '과', '도', '로', '으로', '에서', '하고', '이고', '그', '저', '것', '수', '있', '없', '합니다', '합니다.', '해요', '해요.', '임', '임.']);
  const freq = {};

  rows.forEach(({ message }) => {
    // 2글자 이상 단어만
    const words = message.split(/[\s\[\]#:,.!?~ㅋㅎ]+/).filter(w => w.length >= 2);
    words.forEach(w => {
      if (!stopWords.has(w)) {
        freq[w] = (freq[w] || 0) + 1;
      }
    });
  });

  const sorted = Object.entries(freq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([word, count]) => ({ word, count }));

  res.json({ keywords: sorted });
});

// 전체 통계 요약
app.get('/api/stats/summary', (req, res) => {
  const total = db.prepare('SELECT COUNT(*) as count FROM horn').get();
  const today = db.prepare(`
    SELECT COUNT(*) as count FROM horn 
    WHERE date_send >= datetime('now', '-24 hours')
  `).get();
  const oldest = db.prepare('SELECT MIN(date_send) as d FROM horn').get();
  const newest = db.prepare('SELECT MAX(date_send) as d FROM horn').get();

  res.json({
    total: total.count,
    today: today.count,
    oldest: oldest.d,
    newest: newest.d,
    server: SERVER_NAME
  });
});

// ─── 서버 시작 ───────────────────────────────────

app.listen(PORT, () => {
  console.log(`\n🎺 지금 에린에서는 — 백엔드 서버 시작`);
  console.log(`   포트: ${PORT}`);
  console.log(`   서버: ${SERVER_NAME}`);
  console.log(`   http://localhost:${PORT}\n`);

  // 서버 시작 시 즉시 한 번 호출
  fetchAndSave();
});

// 10분마다 자동 수집
cron.schedule('*/10 * * * *', () => {
  fetchAndSave();
});

console.log('⏰ 10분마다 자동 수집 스케줄 등록 완료');
