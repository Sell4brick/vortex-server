// Сигнальный сервер VortexRoulette
// Запуск: npm install && npm start (порт 8080)
//
// Задачи сервера:
//  1. Подбор пар по фильтрам (пол, страна)
//  2. Пересылка SDP/ICE-сообщений между собеседниками
//  3. Пересылка сообщений текстового чата
//  4. Приём жалоб

const { WebSocketServer } = require('ws');

const wss = new WebSocketServer({ port: 8080 });
console.log('✅ Сигнальный сервер запущен на ws://0.0.0.0:8080');

/** очередь ожидающих: [{ ws, profile, filters }] */
let queue = [];
/** активные пары: Map<ws, ws> */
const pairs = new Map();
/** профили: Map<ws, profile> */
const profiles = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function matches(a, b) {
  // a и b — элементы очереди { profile, filters }
  const genderOk = (f, p) =>
    !f.preferredGender || f.preferredGender === 'Любой' || f.preferredGender === p.gender;
  const countryOk = (f, p) =>
    !f.country || f.country === '🌍 Весь мир' || f.country === p.country;
  return (
    genderOk(a.filters, b.profile) && countryOk(a.filters, b.profile) &&
    genderOk(b.filters, a.profile) && countryOk(b.filters, a.profile)
  );
}

function tryMatch(entry) {
  const idx = queue.findIndex((other) => other.ws !== entry.ws && matches(entry, other));
  if (idx === -1) {
    queue.push(entry);
    return;
  }
  const partner = queue.splice(idx, 1)[0];

  pairs.set(entry.ws, partner.ws);
  pairs.set(partner.ws, entry.ws);

  send(entry.ws, { kind: 'matched', peer: partner.profile });
  send(partner.ws, { kind: 'matched', peer: entry.profile });
  console.log(`🤝 Пара: ${entry.profile.nickname} ↔ ${partner.profile.nickname}`);
}

function breakPair(ws, notifyPartner = true) {
  const partner = pairs.get(ws);
  if (partner) {
    pairs.delete(ws);
    pairs.delete(partner);
    if (notifyPartner) send(partner, { kind: 'peerLeft' });
  }
  queue = queue.filter((e) => e.ws !== ws);
}

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.kind) {
      case 'hello':
        if (msg.profile) profiles.set(ws, msg.profile);
        break;

      case 'search': {
        breakPair(ws);
        const profile = msg.profile || profiles.get(ws);
        if (!profile) return;
        profiles.set(ws, profile);
        tryMatch({ ws, profile, filters: msg.filters || {} });
        break;
      }

      // Пересылаем партнёру как есть
      case 'offer':
      case 'answer':
      case 'candidate':
      case 'chat': {
        const partner = pairs.get(ws);
        if (partner) send(partner, msg);
        break;
      }

      case 'next':
      case 'leave':
        breakPair(ws);
        break;

      case 'report': {
        const reporter = profiles.get(ws);
        console.log(`🚩 Жалоба от ${reporter?.nickname || '?'} на ${msg.from}: ${msg.payload}`);
        // Здесь сохраняйте жалобы в БД и применяйте автоблокировки
        break;
      }
    }
  });

  ws.on('close', () => {
    breakPair(ws);
    profiles.delete(ws);
  });
});
