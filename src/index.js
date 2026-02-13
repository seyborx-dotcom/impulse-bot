/**
 * IMPULSE ULTRA BOT (Firestore)
 * - "Одно окно" в личке (одно сообщение редактируется)
 * - Admin menu (RU fixed) + User menu (RU/UA/DE)
 * - Bind topics in group: /bindtopic <key>
 * - Wizard: Create RSVP in private -> publish post (with photos) into chosen group topic -> then RSVP card under it
 * - RSVP votes stored in Firestore, card always edited (no new messages)
 * - Results button opens private chat (deep link) and shows poll-like lists
 * - Monthly TOP-5 post (last day of month 21:00 Berlin)
 * - Year winner post (Dec 31 20:00 Berlin)
 */


const path = require("path");
require("dotenv").config();
// ===== SINGLE INSTANCE LOCK (anti-409) =====
const fs = require("fs");

const LOCK_PATH = path.join(__dirname, "..", ".bot.lock");

function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

function acquireLock() {
  if (fs.existsSync(LOCK_PATH)) {
    const raw = fs.readFileSync(LOCK_PATH, "utf8").trim();
    const oldPid = Number(raw || 0);

    if (oldPid && pidAlive(oldPid)) {
      console.log(`❌ Bot already running (PID ${oldPid}). Stop it first (Ctrl+C) or close extra terminal.`);
      process.exit(1);
    }
  }

  fs.writeFileSync(LOCK_PATH, String(process.pid), "utf8");

  const cleanup = () => {
    try { fs.unlinkSync(LOCK_PATH); } catch (_) {}
  };

  process.on("exit", cleanup);
  process.on("SIGINT", () => { cleanup(); process.exit(0); });
  process.on("SIGTERM", () => { cleanup(); process.exit(0); });
}

acquireLock();
// ===== /SINGLE INSTANCE LOCK =====
console.log("BOT STARTED");

const TelegramBot = require("node-telegram-bot-api");
const admin = require("firebase-admin");
const cron = require("node-cron");

// ====== ENV ======
const BOT_TOKEN = process.env.BOT_TOKEN;
if (!BOT_TOKEN) throw new Error("BOT_TOKEN missing in .env");
const BOT_USERNAME = process.env.BOT_USERNAME || "impulseTop5Bot"; // without @, needed for deep link

const ADMIN_IDS = String(process.env.ADMIN_IDS || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

function isAdmin_(userId) {
  return ADMIN_IDS.includes(String(userId));
}
const TZ = "Europe/Berlin";

// ====== FIREBASE ADMIN INIT ======
/**
 * Вариант A: через GOOGLE_APPLICATION_CREDENTIALS
 * admin.initializeApp({ credential: admin.credential.applicationDefault() })
 *
 * Вариант B: через require serviceAccount.json
 * const serviceAccount = require("./serviceAccount.json");
 * admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
 */

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  projectId: serviceAccount.project_id,
});

const db = admin.firestore();
// ====== DISPLAY NAME CACHE (fast) ======
const nameCache = new Map(); // uid -> { name, exp }
const NAME_TTL_MS = 6 * 60 * 60 * 1000; // 6 hours
// ===== Firestore retry (network protection) =====
function sleep_(ms) { return new Promise(r => setTimeout(r, ms)); }

async function fsRetry_(fn, label, tries = 3) {
  let lastErr = null;
  for (let i = 1; i <= tries; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      const msg = e?.details || e?.message || String(e);
      console.error(`FIRESTORE ${label} try ${i}/${tries}:`, msg);
      // backoff: 400ms, 1200ms, 2400ms
      await sleep_(400 * i * i);
    }
  }
  throw lastErr;
}
// ===== Firestore safe getters (use fsRetry_) =====
async function fsGet_(ref, label) {
  return await fsRetry_(() => ref.get(), label || "ref.get");
}

async function fsQuery_(query, label) {
  return await fsRetry_(() => query.get(), label || "query.get");
}
// ===== /Firestore safe getters =====

// ====== BOT INIT ======
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// ====== SAFETY: do not crash on polling/errors ======
bot.on("polling_error", (err) => {
  console.error("polling_error:", err?.message || err);
});

process.on("unhandledRejection", (reason) => {
  console.error("unhandledRejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("uncaughtException:", err);
  // optional: keep process alive (not recommended if state is broken)
});

// ====== SETTINGS ======

// ====== Firestore refs ======
const CFG = db.collection("config").doc("bot");          // общий конфиг (groupChatId и т.п.)
const TOPICS = db.collection("topics");                  // темы/топики (key -> chatId/threadId)
const USERS = db.collection("users");                    // user settings, language
const RSVP = db.collection("rsvp");                      // rsvp polls
const POINTS = db.collection("points");                  // ledger points: {userId, points, ts, source, meta}



function displayNameFromTg_(from) {
  const first = String(from?.first_name || "").trim();
  const last  = String(from?.last_name  || "").trim();
  const full  = [first, last].filter(Boolean).join(" ").trim();
  const user  = String(from?.username || "").trim();
  return full || (user ? `@${user}` : "") || "Без имени";
}
 async function addPoints_(userId, name, points, source, meta = {}) {
  await POINTS.add({
    userId: Number(userId),
    name: String(name || ""),
    points: Number(points || 0),
    ts: admin.firestore.Timestamp.now(),
    source: String(source || "checkin"),
    meta: meta || {},
  });
}

async function addPointsOnce_(docId, userId, name, points, source, meta = {}) {
  const ref = POINTS.doc(String(docId));
  await db.runTransaction(async (tx) => {
    const s = await tx.get(ref);
    if (s.exists) return; // уже начисляли — выходим
    tx.set(ref, {
      userId: Number(userId),
      name: String(name || ""),
      points: Number(points || 0),
      ts: admin.firestore.Timestamp.now(),
      source: String(source || "checkin"),
      meta: meta || {},
    });
  });
}

// ====== UI: ONE SCREEN (private) ======
/**
 * Храним для каждого пользователя messageId "экрана" (одно окно) в users/{uid}.ui.mainMessageId
 * Все меню/результаты редактируют именно это сообщение.
 */
async function getUserDoc_(userId) {
  const ref = USERS.doc(String(userId));
  const snap = await fsRetry_(() => ref.get(), "users.get");
  if (!snap.exists) {
    await fsRetry_(() => ref.set({
  lang: "RU",
  createdAt: admin.firestore.FieldValue.serverTimestamp(),
  ui: { mainMessageId: null }
}, { merge: true }), "users.set");
  }
  return ref;
}

async function getUserLang_(userId) {
  const ref = await getUserDoc_(userId);
  const s = await fsRetry_(() => ref.get(), "users.get.lang");
  const lang = (s.data()?.lang || "RU").toUpperCase();
  if (!["RU","UA","DE"].includes(lang)) return "RU";
  return lang;
}

async function getDisplayName_(userId, tgFrom) {

  const uid = String(userId);

// cache hit
const hit = nameCache.get(uid);
if (hit && hit.exp > Date.now() && hit.name) return hit.name;

  const ref = await getUserDoc_(userId);
  const snap = await ref.get();

  const fixedName = snap.data()?.profile?.displayName;

  if (fixedName && fixedName.trim()) {
    const out = fixedName.trim();
nameCache.set(uid, { name: out, exp: Date.now() + NAME_TTL_MS });
return out;
  }

  // fallback -> telegram
  const first = String(tgFrom?.first_name || "").trim();
  const last = String(tgFrom?.last_name || "").trim();
  const full = [first, last].filter(Boolean).join(" ").trim();

  const user = String(tgFrom?.username || "").trim();

  const out = full || (user ? `@${user}` : "") || "Без имени";
nameCache.set(uid, { name: out, exp: Date.now() + NAME_TTL_MS });
return out;
}

function t_(lang, key) {
  const dict = {
    RU: {
      MENU_TITLE_ADMIN: "Меню (админ)",
      MENU_TITLE_USER: "Меню",
      BTN_PROFILE: "Мой профиль",
      BTN_POS: "Моя позиция",
      BTN_RATING: "Общий рейтинг",
      BTN_RULES: "Система баллов",
      BTN_RSVP: "Все события",
      BTN_CREATE_RSVP: "Создать событие",
      BTN_CHECKIN: "Отметить пришедших",
      BTN_BACK: "Назад",
      BTN_CANCEL: "Отмена",
      BTN_LANG_RU: "Русский",
      BTN_LANG_UA: "Українська",
      BTN_LANG_DE: "Deutsch",
      TXT_LANG: "Язык",
      TXT_CHOOSE: "Выбери действие:",
      TXT_NO_ACCESS: "⛔️ Нет доступа.",
      TXT_OPEN_PM: "Открываю личку бота…",
      TXT_OLD_MSG: "Сообщение слишком старое, Telegram не даёт редактировать.",
      TXT_MY_POSITION_TITLE: "Твоя позиция",
      TXT_NAME: "Имя",
      TXT_PLACE: "Место",
      TXT_POINTS: "Баллы",
      BTN_EVENTS_PRO: "Events PRO",
      TXT_EVENTS_PRO_TITLE: "Events PRO",
      TXT_EVENTS_PRO_EMPTY: "Событий пока нет.",
      TXT_EVENTS_PRO_PAGE: "Страница",
      BTN_PREV: "Назад",
      BTN_NEXT: "Вперёд",
      BTN_OPEN_RESULTS: "Результаты",
      BTN_OPEN_POST: "Открыть пост",
      BTN_REFRESH: "Обновить",
      TXT_EVENTS_TITLE: "События",
      BTN_OPEN: "Открыть",
      BTN_RESULTS: "Результаты",
      WIZ_STEP1_TITLE: "Создать RSVP — шаг 1/4",
      WIZ_STEP1_TEXT: "Выбери тему (куда публиковать в группе):",
      WIZ_STEP2_TEXT: "Теперь пришли ПОСТ (текст) и/или фото.",
      WIZ_STEP3_TEXT: "Напиши вопрос RU/DE в 2 строки.",
      WIZ_STEP4_TEXT: "Введи дату/время.",
      CHECKIN_TITLE: "ЧЕК-ИН",
      CHECKIN_PICK: "Выбери событие:",
      CHECKIN_NO_EVENTS: "Нет активных событий.",
      CHECKIN_CLOSING: "Закрываю и начисляю баллы…",
      ERR_GENERIC: "Ошибка. Попробуй ещё раз.",
      BTN_PUBLISH: "Опубликовать",
      BTN_MENU: "Меню",
      ETC: "…",
    },
    UA: {
      MENU_TITLE_ADMIN: "Меню (адмін)",
      MENU_TITLE_USER: "Меню",
      BTN_PROFILE: "Мій профіль",
      BTN_POS: "Моя позиція",
      BTN_RATING: "Загальний рейтинг",
      BTN_RULES: "Система балів",
      BTN_RSVP: "Усі події",
      BTN_CREATE_RSVP: "Створити подію",
      BTN_CHECKIN: "Відмітити присутніх",
      BTN_BACK: "Назад",
      BTN_CANCEL: "Скасувати",
      BTN_LANG_RU: "Русский",
      BTN_LANG_UA: "Українська",
      BTN_LANG_DE: "Deutsch",
      TXT_LANG: "Мова",
      TXT_CHOOSE: "Обери дію:",
      TXT_NO_ACCESS: "⛔️ Немає доступу.",
      TXT_OPEN_PM: "Відкриваю приватний чат…",
      TXT_OLD_MSG: "Повідомлення занадто старе — Telegram не дає редагувати.",
      TXT_MY_POSITION_TITLE: "Твоя позиція",
      TXT_NAME: "Ім'я",
      TXT_PLACE: "Місце",
      TXT_POINTS: "Бали",
      BTN_EVENTS_PRO: "Events PRO",
      TXT_EVENTS_PRO_TITLE: "Events PRO",
      TXT_EVENTS_PRO_EMPTY: "Подій поки немає.",
      TXT_EVENTS_PRO_PAGE: "Сторінка",
      BTN_PREV: "Назад",
      BTN_NEXT: "Вперед",
      BTN_OPEN_RESULTS: "Результати",
      BTN_OPEN_POST: "Відкрити пост",
      BTN_REFRESH: "Оновити",
      TXT_EVENTS_TITLE: "Події",
      BTN_OPEN: "Відкрити",
      BTN_RESULTS: "Результати",
    },
    DE: {
      MENU_TITLE_ADMIN: "Menü (Admin)",
      MENU_TITLE_USER: "Menü",
      BTN_PROFILE: "Mein Profil",
      BTN_POS: "Meine Position",
      BTN_RATING: "Gesamtranking",
      BTN_RULES: "Punktesystem",
      BTN_RSVP: "Alle Events",
      BTN_CREATE_RSVP: "Event erstellen",
      BTN_CHECKIN: "Anwesenheit",
      BTN_BACK: "Zurück",
      BTN_CANCEL: "Abbrechen",
      BTN_LANG_RU: "Русский",
      BTN_LANG_UA: "Українська",
      BTN_LANG_DE: "Deutsch",
      TXT_LANG: "Sprache",
      TXT_CHOOSE: "Wähle eine Aktion:",
      TXT_NO_ACCESS: "⛔️ Kein Zugriff.",
      TXT_OPEN_PM: "Ich öffne den Bot-Chat…",
      TXT_OLD_MSG: "Die Nachricht ist zu alt — Telegram lässt kein Edit mehr zu.",
      TXT_MY_POSITION_TITLE: "Deine Position",
      TXT_NAME: "Name",
      TXT_PLACE: "Platz",
      TXT_POINTS: "Punkte",
      BTN_EVENTS_PRO: "Events PRO",
      TXT_EVENTS_PRO_TITLE: "Events PRO",
      TXT_EVENTS_PRO_EMPTY: "Noch keine Events.",
      TXT_EVENTS_PRO_PAGE: "Seite",
      BTN_PREV: "Zurück",
      BTN_NEXT: "Weiter",
      BTN_OPEN_RESULTS: "Ergebnisse",
      BTN_OPEN_POST: "Post öffnen",
      BTN_REFRESH: "Aktualisieren",
      TXT_EVENTS_TITLE: "Events",
      BTN_OPEN: "Öffnen",
      BTN_RESULTS: "Ergebnisse",
    },
  };
  return (dict[lang] && dict[lang][key]) ? dict[lang][key] : (dict.RU[key] || key);
}

async function ensureMainScreen_(chatId, userId, initialText, replyMarkup) {
  const ref = await getUserDoc_(userId);
  const snap = await ref.get();
  const mainMessageId = snap.data()?.ui?.mainMessageId || null;

  if (!mainMessageId) {
    const sent = await bot.sendMessage(chatId, initialText, {
  reply_markup: replyMarkup,
  parse_mode: "HTML",
  disable_web_page_preview: true,
});
    await ref.set({ ui: { mainMessageId: sent.message_id } }, { merge: true });
    return sent.message_id;
  }

  // try edit existing
  try {
   await bot.editMessageText(initialText, {
  chat_id: chatId,
  message_id: mainMessageId,
  reply_markup: replyMarkup,
  parse_mode: "HTML",
  disable_web_page_preview: true,
});
    return mainMessageId;
  } catch (e) {
    // if edit failed (deleted etc.) send new, then set new id
    const sent = await bot.sendMessage(chatId, initialText, {
  reply_markup: replyMarkup,
  parse_mode: "HTML",
  disable_web_page_preview: true,
});
    await ref.set({ ui: { mainMessageId: sent.message_id } }, { merge: true });
    return sent.message_id;
  }
}

async function editMainScreen_(chatId, userId, text, replyMarkup) {
  const ref = await getUserDoc_(userId);
  const snap = await ref.get();
  const mid = snap.data()?.ui?.mainMessageId || null;
  if (!mid) return ensureMainScreen_(chatId, userId, text, replyMarkup);

  try {
  await bot.editMessageText(text, {
    chat_id: chatId,
    message_id: mid,
    reply_markup: replyMarkup,
    parse_mode: "HTML",
    disable_web_page_preview: true,
  });

  } catch (e) {
    // если не получилось — создаём новый "экран"
    const sent = await bot.sendMessage(chatId, text, {
  reply_markup: replyMarkup,
  parse_mode: "HTML",
  disable_web_page_preview: true,
});
    await ref.set({ ui: { mainMessageId: sent.message_id } }, { merge: true });
  }
}

// ====== TOPIC BIND (group forum topics) ======
async function saveTopic_(key, chatId, threadId) {
  await TOPICS.doc(String(key)).set({
    key: String(key),
    chatId: Number(chatId),
    threadId: Number(threadId),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  // сохраним groupChatId (для публикаций)
  await CFG.set({
    groupChatId: Number(chatId),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

async function getTopic_(key) {
  const s = await fsGet_(TOPICS.doc(String(key)), "topics.get");
  if (!s.exists) return null;
  return s.data();
}

async function listTopics_() {
  const snap = await fsQuery_(TOPICS.orderBy("key", "asc"), "topics.list");
  const out = [];
  snap.forEach(d => out.push(d.data()));
  return out;
}

// ====== TEXT HELPERS (Telegram limits) ======
const TG_MSG_LIMIT = 3900;     // safe < 4096
const TG_CAPTION_LIMIT = 900;  // safe < 1024 (album caption)

function escHtml_(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function splitText_(text, maxLen) {
  const s = String(text || "");
  if (!s) return [];
  const chunks = [];
  let i = 0;
  while (i < s.length) {
    chunks.push(s.slice(i, i + maxLen));
    i += maxLen;
  }
  return chunks;
}

async function sendTextChunks_(chatId, threadId, text) {
  const parts = splitText_(text, TG_MSG_LIMIT);
  for (const part of parts) {
    if (part && part.trim()) {
      await bot.sendMessage(chatId, part, { message_thread_id: threadId });
    }
  }
}
// ====== RSVP: rendering ======
function topicEmoji_(topicKey) {
  const k = String(topicKey || "").toLowerCase();
  if (k.includes("бег") || k.includes("run")) return "🏃";
  if (k.includes("волейбол") || k.includes("volley")) return "🤾";
  if (k.includes("вело") || k.includes("bike") || k.includes("rad")) return "🚴";
  if (k.includes("поход") || k.includes("hike")) return "🚶";
  if (k.includes("плав") || k.includes("swim")) return "🏊";
  if (k.includes("event") || k.includes("меропр")) return "🎪";
  return "⚡️";
}

function clamp(n, a, b) { return Math.max(a, Math.min(b, n)); }

function barSegments(pct, totalSeg = 10) {
  const filled = Math.round((pct / 100) * totalSeg);
  const f = "▰".repeat(clamp(filled, 0, totalSeg));
  const e = "▱".repeat(clamp(totalSeg - filled, 0, totalSeg));
  return f + e;
}

function ratingBar10(pct) {
  const total = 10;
  const filled = clamp(Math.round((pct / 100) * total), 0, total);

  const bar = "▰".repeat(filled) + "▱".repeat(total - filled);
  return `<code>${bar}</code>`;
}

// ====== RSVP LOCK (no changes on event day / after) ======
function parseEventYMD_(dtStr) {
  // expects "DD.MM.YYYY, HH:MM" or "DD.MM.YYYY"
  const s = String(dtStr || "").trim();
  const m = s.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (!m) return null;
  const dd = String(m[1]).padStart(2, "0");
  const mm = String(m[2]).padStart(2, "0");
  const yyyy = String(m[3]);
  return `${yyyy}-${mm}-${dd}`; // Y-M-D
}

function todayYMD_(tz) {
  // "en-CA" gives YYYY-MM-DD
  return new Date().toLocaleDateString("en-CA", { timeZone: tz || "Europe/Berlin" });
}

function isRsvpLocked_(dtStr) {
  const eventYMD = parseEventYMD_(dtStr);
  if (!eventYMD) return false; // если дату не распознали — не блокируем
  const nowYMD = todayYMD_(TZ);
  // блокируем в день события и позже
  return eventYMD <= nowYMD;
}

function formatRsvpCardText({ qRu, qDe, dt, topicKey, chatMembers }, yes, no) {
  yes = Number(yes || 0);
  no  = Number(no || 0);

const votes = yes + no;
const members = Number(chatMembers || 0);

// % YES и % NO от всех участников
// fallback: если members=0, считаем от votes
const pctYes = (members > 0)
  ? Math.round((yes / members) * 100)
  : (votes > 0 ? Math.round((yes / votes) * 100) : 0);

const pctNo = (members > 0)
  ? Math.round((no / members) * 100)
  : (votes > 0 ? Math.round((no / votes) * 100) : 0);

// показываем бары, если кто-то голосовал
const lineYes = (votes > 0)
  ? `${topicEmoji_(topicKey)} ${ratingBar10(pctYes)} ${pctYes}%`
  : "";

const lineNo = (votes > 0)
  ? `✖️ ${ratingBar10(pctNo)} ${pctNo}%`
  : "";

return [
  `${qRu || ""}`.trim(),
  `${qDe || ""}`.trim(),
  `${dt || ""}`.trim(),
  lineYes,
  lineNo
].filter(Boolean).join("\n");
}

function rsvpKeyboard(pollId, yes, no) {
  return {
    inline_keyboard: [
      [
        { text: `Да / Ja (${yes})`, callback_data: `rsvp_yes_${pollId}`, style: "success" },
{ text: `Нет / Nein (${no})`, callback_data: `rsvp_no_${pollId}`, style: "danger" },
      ],
      [
        { text: `Результаты / Ergebnisse`, callback_data: `rsvp_results_${pollId}`, style: "primary" }
      ]
    ]
  };
}

function rsvpKeyboardResultsOnly_(pollId) {
  return {
    inline_keyboard: [
      [
      { text: `Результаты / Ergebnisse`, callback_data: `rsvp_results_${pollId}`, style: "primary" }
      ]
    ]
  };
}

// ====== RSVP: counts transaction (no duplicates) ======

async function rsvpVote_(pollId, user, choice) {
  const pollRef = RSVP.doc(String(pollId));
  const voteRef = pollRef.collection("votes").doc(String(user.id));
  const fixedName = await getDisplayName_(user.id, user);

  await db.runTransaction(async (tx) => {
    const pollSnap = await tx.get(pollRef);
    if (!pollSnap.exists) throw new Error("Poll not found");

    const poll = pollSnap.data() || {};
    let yes = Number(poll.yes || 0);
    let no  = Number(poll.no || 0);

    const oldSnap = await tx.get(voteRef);
    const old = oldSnap.exists ? (oldSnap.data()?.choice || null) : null;

    // adjust old
    if (old === "YES") yes = Math.max(0, yes - 1);
    if (old === "NO")  no  = Math.max(0, no - 1);

    // apply new
    if (choice === "YES") yes += 1;
    if (choice === "NO")  no  += 1;

    tx.set(voteRef, {
      choice,
      name: fixedName,
      username: "",
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    tx.set(pollRef, {
      yes,
      no,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  });
}

async function getRsvpCounts_(pollId) {
  const s = await fsGet_(RSVP.doc(String(pollId)), "rsvp.get.counts");
  if (!s.exists) return { yes: 0, no: 0 };
  return { yes: Number(s.data()?.yes || 0), no: Number(s.data()?.no || 0) };
}

// ====== RSVP: edit card message only (no new messages) ======
async function editRsvpCardNoNew_(groupChatId, cardMessageId, text, markup, cbqIdForAlert, langForAlert = "RU") {
  try {
    await bot.editMessageText(text, {
  chat_id: groupChatId,
  message_id: cardMessageId,
  reply_markup: markup,
  parse_mode: "HTML",
  disable_web_page_preview: true,
});
    return true;
  } catch (e) {
    // no new message, only alert
    if (cbqIdForAlert) {
      try {
        await bot.answerCallbackQuery(cbqIdForAlert, { show_alert: true, text: t_(langForAlert, "TXT_OLD_MSG") });
      } catch (_) {}
    }
    return false;
  }
}

// ====== RESULTS SCREEN (private) ======
async function buildResultsText_(pollId) {
  const pollSnap = await fsGet_(RSVP.doc(String(pollId)), "rsvp.get.results");
  if (!pollSnap.exists) return "RSVP не найден.";

  const poll = pollSnap.data() || {};
  const qRu = poll.qRu || "";
  const qDe = poll.qDe || "";
  const dt  = poll.dt || "";

  const votesSnap = await fsGet_(RSVP.doc(String(pollId)).collection("votes"), "rsvp.votes.get.results");

  const yes = [];
  const no = [];

   const voteDocs = [];
  votesSnap.forEach(d => voteDocs.push(d));

 for (const d of voteDocs) {
  const v = d.data() || {};
  const fixed = String(v.name || "").trim() || "Без имени"; // только имя из базы
  if (v.choice === "YES") yes.push(fixed);
  if (v.choice === "NO")  no.push(fixed);
}

  const total = yes.length + no.length;

  return [
    `РЕЗУЛЬТАТЫ / ERGEBNISSE`,
    ``,
    `${qRu}`,
    `${qDe}`,
    `${dt}`,
    ``,
    `Да / Ja — ${yes.length}`,
    yes.length ? yes.map((x,i)=>`${i+1}. ${x}`).join("\n") : "—",
    ``,
    `Нет / Nein — ${no.length}`,
    no.length ? no.map((x,i)=>`${i+1}. ${x}`).join("\n") : "—",
    ``,
    `Всего / Total: ${total}`
  ].join("\n");
}

// ====== RESULTS: PAGINATION (private) ======
function resultsKb_(pollId, tab, page, hasPrev, hasNext) {
  const p = Number(page || 0);

  const tabsRow = [
  {
    text: "Да / Ja",
    callback_data: `res_${pollId}_YES_0`,
    style: "success" // 🟢 зеленая
  },
  {
    text: "Нет / Nein",
    callback_data: `res_${pollId}_NO_0`,
    style: "danger" // 🔴 красная
  },
];

const navRow = [];

if (hasPrev)
  navRow.push({
    text: "⬅️",
    callback_data: `res_${pollId}_${tab}_${p - 1}`,
  });

navRow.push({
  text: `${p + 1}`,
  callback_data: "noop"
});

if (hasNext)
  navRow.push({
    text: "➡️",
    callback_data: `res_${pollId}_${tab}_${p + 1}`,
  });

return {
  inline_keyboard: [
    tabsRow,
    navRow.length ? navRow : [{ text: `${p + 1}`, callback_data: "noop" }],
    [
      {
        text: "Меню / Menü",
        callback_data: "res_menu",
        style: "primary" // 🔵 синяя
      }
    ],
  ],
};
}

async function buildResultsPage_(pollId, tab, page, pageSize = 20) {
  const pollSnap = await fsGet_(RSVP.doc(String(pollId)), "rsvp.get.page");
  if (!pollSnap.exists) return { text: "RSVP не найден.", kb: null };

  const poll = pollSnap.data() || {};
  const qRu = poll.qRu || "";
  const qDe = poll.qDe || "";
  const dt  = poll.dt || "";

  const votesSnap = await fsGet_(RSVP.doc(String(pollId)).collection("votes"), "rsvp.votes.get.page");

  const yes = [];
  const no = [];

const voteDocs = [];
votesSnap.forEach(d => voteDocs.push(d));

for (const d of voteDocs) {

  const v = d.data() || {};

  const uid = String(d.id);

  const fixed = await getDisplayName_(uid, {
    first_name: v.name || "",
    username: v.username || ""
  });

const line = String(fixed || "").trim() || "Без имени";

if (v.choice === "YES") yes.push(line);
if (v.choice === "NO")  no.push(line);
}

  const list = (tab === "NO") ? no : yes;
  const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
  const p = Math.max(0, Math.min(Number(page || 0), totalPages - 1));

  const start = p * pageSize;
  const slice = list.slice(start, start + pageSize);

  const hasPrev = p > 0;
  const hasNext = p < totalPages - 1;

  const title = (tab === "NO") ? `Нет / Nein — ${no.length}` : `Да / Ja — ${yes.length}`;
  const body = slice.length ? slice.map((x, i) => `${start + i + 1}. ${x}`).join("\n") : "—";

  const text = [
    `РЕЗУЛЬТАТЫ / ERGEBNISSE`,
    ``,
    `${qRu}`,
    `${qDe}`,
    `${dt}`,
    ``,
    `Всего / Total: ${yes.length + no.length}`,
    ``,
    `${title}`,
    body,
    ``,
    `Страница / Seite: ${p + 1}/${totalPages}`,
  ].join("\n").slice(0, TG_MSG_LIMIT);

  const kb = resultsKb_(pollId, tab === "NO" ? "NO" : "YES", p, hasPrev, hasNext);
  return { text, kb };
}

async function showResults_(chatId, userId, pollId, tab, page) {
  const t = (tab === "NO") ? "NO" : "YES";
  const { text, kb } = await buildResultsPage_(pollId, t, page, 20);
  await editMainScreen_(chatId, userId, text, kb);
}

  function mainMenuKeyboard(lang, adminMode) {
  const langRow = [
    { text: t_(lang, "BTN_LANG_RU"), callback_data: "lang_RU" },
    { text: t_(lang, "BTN_LANG_UA"), callback_data: "lang_UA" },
    { text: t_(lang, "BTN_LANG_DE"), callback_data: "lang_DE" },
  ];

  const userRows = [
    [{ text: t_(lang,"BTN_PROFILE"), callback_data: "m_profile", style: "primary" }],
    [{ text: t_(lang,"BTN_POS"),     callback_data: "m_pos",     style: "primary" }],
    [{ text: t_(lang,"BTN_RATING"),  callback_data: "m_rating",  style: "primary" }],
    [{ text: t_(lang,"BTN_RULES"),   callback_data: "m_rules",   style: "primary" }],
  ];

  const adminRows = [
    [{ text: t_(lang,"BTN_PROFILE"),     callback_data: "m_profile" }],
    [{ text: t_(lang,"BTN_POS"),         callback_data: "m_pos" }],
    [{ text: t_(lang,"BTN_RATING"),      callback_data: "m_rating" }],
    [{ text: t_(lang,"BTN_RULES"),       callback_data: "m_rules" }],
    [{ text: t_(lang,"BTN_RSVP"),        callback_data: "events_pro",    style: "primary" }],
    [{ text: t_(lang,"BTN_CREATE_RSVP"), callback_data: "a_create_rsvp", style: "success" }],
    [{ text: t_(lang,"BTN_CHECKIN"),     callback_data: "a_checkin",     style: "success" }],
  ];

  return {
    inline_keyboard: adminMode ? [langRow, ...adminRows] : [langRow, ...userRows]
  };
}

async function showMenu_(chatId, userId) {
  const adminMode = isAdmin_(userId);
const lang = await getUserLang_(userId);

  const title = adminMode ? t_(lang, "MENU_TITLE_ADMIN") : t_(lang, "MENU_TITLE_USER");
  const text = `${title}\n\n${t_(lang, "TXT_CHOOSE")}`;
  const kb = mainMenuKeyboard(lang, adminMode);

  await ensureMainScreen_(chatId, userId, text, kb);
}

function tgMsgLink_(chatId, messageId) {
  // Работает для супер-групп: chatId вида -1001234567890
  const cid = Number(chatId);
  const mid = Number(messageId);
  if (!cid || !mid) return null;

  const s = String(cid);
  if (!s.startsWith("-100")) return null;

  const internalId = s.replace("-100", ""); // t.me/c/<id>/<msg>
  return `https://t.me/c/${internalId}/${mid}`;
}

async function buildEventsScreen_(lang, limit = 12) {
  // Берём последние активные RSVP
  const snap = await fsQuery_(
  RSVP.where("active", "==", true).orderBy("createdAt", "desc").limit(limit),
  "rsvp.list.active"
);

  const items = [];
  snap.forEach(d => {
    const p = d.data() || {};
    items.push({
      id: d.id,
      qRu: p.qRu || "",
      qDe: p.qDe || "",
      dt: p.dt || "",
      yes: Number(p.yes || 0),
      no: Number(p.no || 0),
      groupChatId: p.groupChatId,
      postMessageId: p.postMessageId,
    });
  });

  const title = t_(lang, "TXT_EVENTS_TITLE");
  if (!items.length) {
    return { text: `${title}\n\n(пока нет активных событий)`, kb: null };
  }

  // Текст (краткий список)
  const lines = items.map((x, i) => {
    const q = (lang === "DE" ? (x.qDe || x.qRu) : x.qRu) || x.qRu || x.qDe || "(без названия)";
    const dt = x.dt ? ` • ${x.dt}` : "";
    return `${i + 1}. ${q}${dt}\nJa: ${x.yes} • Nein: ${x.no}\nID: ${x.id}`;
  }).join("\n\n");

  // Кнопки: на каждое событие 2 кнопки
  const inline_keyboard = [];
  for (const x of items) {
    inline_keyboard.push([
      { text: `${t_(lang, "BTN_OPEN")} #${x.id.slice(-4)}`, callback_data: `ev_open_${x.id}` },
      { text: `${t_(lang, "BTN_RESULTS")} #${x.id.slice(-4)}`, callback_data: `ev_results_${x.id}` },
    ]);
  }

  // кнопка назад (в меню)
  inline_keyboard.push([{ text: t_(lang, "BTN_BACK"), callback_data: "ev_back" }]);

  return { text: `${title}\n\n${lines}`, kb: { inline_keyboard } };
}

// ====== WIZARD: CREATE RSVP (admin, private, one screen) ======
/**
 * createWizard[userId] = {
 *   step, data, tempMedia, mainMessageId
 * }
 */
const createWizard = {};

// ====== CHECK-IN SESSIONS (in-memory) ======
const checkinSession = {}; 
const userBusy = {}; // userId -> true/false

async function closeCheckinAndApply_(adminUserId, pollId) {
  // 1) берём сессию
  const sess = checkinSession[adminUserId];
  if (!sess || String(sess.pollId) !== String(pollId)) {
    return { ok: false, reason: "no_session" };
  }

  // 2) читаем событие
  const pollRef = RSVP.doc(String(pollId));
  const pollSnap = await pollRef.get();
  if (!pollSnap.exists) return { ok: false, reason: "no_poll" };

  const poll = pollSnap.data() || {};
  const topicKey = String(poll.topicKey || "");
  const ptsAward = Number(getPointsForTopic_(topicKey) || 0);
  const penalty = -5;

  // 3) список YES
  const yesList = await getYesUsers_(pollId);
  const presentSet = sess.present || new Set();

  const present = [];
  const noshow = [];

  yesList.forEach(u => {
    if (presentSet.has(String(u.userId))) present.push(u);
    else noshow.push(u);
  });

  // 4) защита от повторного закрытия (флаг в RSVP)
  //    транзакционно ставим checkinClosedAt, если уже стоит — выходим
  const lockRes = await db.runTransaction(async (tx) => {
    const s = await tx.get(pollRef);
    if (!s.exists) return { ok: false, reason: "no_poll" };
    const cur = s.data() || {};
    if (cur.checkinClosed === true) return { ok: false, reason: "already_closed" };

    tx.set(pollRef, {
      checkinClosed: true,
      checkinClosedAt: admin.firestore.FieldValue.serverTimestamp(),
      checkinClosedBy: Number(adminUserId),
      checkinSummary: {
        yes: yesList.length,
        present: present.length,
        noshow: noshow.length,
        ptsAward,
        penaltyAbs: Math.abs(penalty),
      }
    }, { merge: true });

    return { ok: true };
  });

  if (!lockRes.ok) return lockRes;

  // 5) начисляем без дублей: фиксированные docId
  //    docId: chk_<pollId>_<uid> and pen_<pollId>_<uid>
  for (const u of present) {
    const docId = `chk_${pollId}_${u.userId}`;
    await addPointsOnce_(docId, u.userId, u.name, ptsAward, "checkin", {
      pollId,
      kind: "present",
      topicKey,
      dt: String(poll.dt || ""),
    });
  }

  for (const u of noshow) {
    const docId = `pen_${pollId}_${u.userId}`;
    await addPointsOnce_(docId, u.userId, u.name, penalty, "penalty", {
      pollId,
      kind: "noshow",
      topicKey,
      dt: String(poll.dt || ""),
    });
  }

  return { ok: true, present: present.length, noshow: noshow.length, yes: yesList.length, ptsAward, penalty };
}

// ====== CHECK-IN: CLOSE (transactional, no duplicates) ======
async function closeCheckinTx_(adminUserId, pollId) {
  const sess = checkinSession[adminUserId];
  if (!sess || String(sess.pollId) !== String(pollId)) {
    return { ok: false, msg: "Сессия чек-ина не найдена. Открой чек-ин заново." };
  }

  const presentSet = sess.present || new Set();

  const pollRef = RSVP.doc(String(pollId));
  const votesYesQuery = pollRef.collection("votes").where("choice", "==", "YES");

  const result = await db.runTransaction(async (tx) => {
    const pollSnap = await tx.get(pollRef);
    if (!pollSnap.exists) return { ok: false, msg: "Событие не найдено." };

    const poll = pollSnap.data() || {};
    if (poll.checkinClosed === true) {
      return {
        ok: true,
        already: true,
        arrived: Number(poll.checkinArrived || 0),
        noshow: Number(poll.checkinNoShow || 0),
        msg: "Чек-ин уже был закрыт ранее (дублей не будет)."
      };
    }

    // читаем YES голоса внутри транзакции
    const yesSnap = await tx.get(votesYesQuery);
    const yesUsers = [];
    yesSnap.forEach((d) => {
      const v = d.data() || {};
      yesUsers.push({
        userId: String(d.id),
        name: (v.name || "").trim() || "Без имени",
        username: v.username ? String(v.username) : ""
      });
    });

// --- OVERRIDE NAMES from USERS/{id}.profile.displayName (inside TX) ---
const userRefs = yesUsers.map(u => USERS.doc(String(u.userId)));
const userSnaps = await Promise.all(userRefs.map(r => tx.get(r)));

const nameMap = new Map(); // uid -> displayName
userSnaps.forEach(s => {
  if (!s.exists) return;
  const d = s.data() || {};
  const dn = d.profile?.displayName;
  const full = `${d.firstName || ""}${d.lastName ? " " + d.lastName : ""}`.trim();
  const best = (dn && String(dn).trim()) ? String(dn).trim() : full;
  if (best) nameMap.set(String(s.id), best);
});

yesUsers.forEach(u => {
  const fixed = nameMap.get(String(u.userId));
  if (fixed) u.name = fixed;
});

    const topicKey = String(poll.topicKey || "");
    const pts = getPointsForTopic_(topicKey);
    const penalty = -5;

    const arrived = [];
    const noshow = [];

    for (const u of yesUsers) {
      if (presentSet.has(String(u.userId))) arrived.push(u);
      else noshow.push(u);
    }

    // Пишем points-леджер детерминированными docId
    // => даже если кто-то попробует повторить транзакцию, poll.checkinClosed не даст
    for (const u of arrived) {
      const docId = `ci_${pollId}_${u.userId}`;
      tx.set(POINTS.doc(docId), {
        userId: Number(u.userId),
        name: String(u.name || ""),
        points: Number(pts || 0),
        ts: admin.firestore.Timestamp.now(),
        source: "checkin",
        meta: {
          pollId: String(pollId),
          topicKey,
          kind: "arrived",
          username: u.username || ""
        }
      }, { merge: false });
    }

    for (const u of noshow) {
      const docId = `ns_${pollId}_${u.userId}`;
      tx.set(POINTS.doc(docId), {
        userId: Number(u.userId),
        name: String(u.name || ""),
        points: Number(penalty),
        ts: admin.firestore.Timestamp.now(),
        source: "penalty",
        meta: {
          pollId: String(pollId),
          topicKey,
          kind: "noshow",
          username: u.username || ""
        }
      }, { merge: false });
    }

    // Закрываем чек-ин (главный анти-дубль флаг)
    tx.set(pollRef, {
      checkinClosed: true,
      checkinClosedAt: admin.firestore.FieldValue.serverTimestamp(),
      checkinClosedBy: Number(adminUserId),
      checkinArrived: arrived.length,
      checkinNoShow: noshow.length
    }, { merge: true });

    return {
      ok: true,
      already: false,
      arrived: arrived.length,
      noshow: noshow.length,
      pts,
      penalty
    };
  });

  return result;
}

// checkinSession[adminUserId] = { pollId: "...", present: Set(["123","456"]), page: 0 }

function resetCreateWizard_(userId) {
  delete createWizard[userId];
}

function wizardCancelKb_() {
  return { inline_keyboard: [[{ text: "Отмена", callback_data: "w_cancel" }]] };
}

async function wizardEditScreen_(chatId, userId, text, replyMarkup) {
  // редактируем "экран" пользователя (одно окно)
  await editMainScreen_(chatId, userId, text, replyMarkup);
}


// step prompts
async function startCreateRsvpWizard_(chatId, userId) {
  console.log("WIZARD START");
  createWizard[userId] = { step: 1, data: {} };

  const topics = await listTopics_();
  if (!topics.length) {
    await wizardEditScreen_(chatId, userId,
      "Нет сохранённых тем.\n\nСначала в группе зайди в нужную тему и напиши:\n/bindtopic бег\n/bindtopic top5\nи т.д.",
      mainMenuKeyboard("RU", true)
    );
    resetCreateWizard_(userId);
    return;
  }

  const buttons = topics.slice(0, 20).map(t => [{ text: t.key, callback_data: `w_topic_${t.key}` }]);
  buttons.push([{ text: "Отмена", callback_data: "w_cancel" }]);

  await wizardEditScreen_(chatId, userId,
    "Создать RSVP — шаг 1/4\n\nВыбери тему (куда публиковать в группе):",
    { inline_keyboard: buttons }
  );
}

async function wizardAskContent_(chatId, userId) {
  createWizard[userId].step = 2;
  await wizardEditScreen_(chatId, userId,
    "Создать RSVP — шаг 2/4\n\nТеперь пришли ПОСТ (текст) и/или фото.\n\nМожно:\n• одно фото + подпись\n• альбом до 10 фото (отправь сразу как альбом)\n• только текст\n\nПосле этого я спрошу вопрос RU/DE и дату.",
    wizardCancelKb_()
  );
}

async function wizardAskQuestion_(chatId, userId) {
  createWizard[userId].step = 3;
  await wizardEditScreen_(chatId, userId,
    "Создать RSVP — шаг 3/4\n\nНапиши вопрос в 2 строки:\n1) RU\n2) DE\n\nПример:\nЕдёшь с нами в музей + кофе?\nFährst du mit uns ins Museum + Kaffee?",
    wizardCancelKb_()
  );
}

async function wizardAskDate_(chatId, userId) {
  createWizard[userId].step = 4;
  await wizardEditScreen_(chatId, userId,
    "Создать RSVP — шаг 4/4\n\nВведи дату/время (как ты хочешь видеть в посте), например:\n21.02.2026, 14:00",
    wizardCancelKb_()
  );
}

function wizardConfirmKb_() {
  return {
    inline_keyboard: [
      [{ text: "Опубликовать", callback_data: "w_publish" }],
      [{ text: "Отмена", callback_data: "w_cancel" }]
    ]
  };
}

async function wizardConfirm_(chatId, userId) {
  const w = createWizard[userId];
  const { topicKey, postText, media, qRu, qDe, dt } = w.data;

  const preview = [
    "Проверь перед публикацией:",
    "",
    `ТЕМА: ${topicKey}`,
    "",
    "ПОСТ:",
    postText ? postText : "(без текста)",
    media?.length ? `\nФото: ${media.length} шт.` : "\nФото: нет",
    "",
    "RSVP:",
    qRu,
    qDe,
    dt
  ].join("\n");

  await wizardEditScreen_(chatId, userId, preview, wizardConfirmKb_());
}

async function publishWizard_(chatId, userId) {
  const w = createWizard[userId];
  const { topicKey, postText, media, qRu, qDe, dt } = w.data;

  const topic = await getTopic_(topicKey);
  if (!topic) {
    await wizardEditScreen_(chatId, userId, `Тема "${topicKey}" не найдена.`, mainMenuKeyboard("RU", true));
    resetCreateWizard_(userId);
    return;
  }

  const groupChatId = Number(topic.chatId);
  const threadId = Number(topic.threadId);

  // 1) publish post (media group or text)
  let postMessageId = null;

  if (media && media.length) {
    // send album (max 10). caption only on first
    const mg = media.slice(0, 10).map((fileId, idx) => ({
      type: "photo",
      media: fileId,
      caption: idx === 0 ? (postText || "").slice(0, TG_CAPTION_LIMIT) : undefined
    }));

    const sent = await bot.sendMediaGroup(groupChatId, mg, { message_thread_id: threadId });
    // sent is array
   postMessageId = sent && sent[0] ? sent[0].message_id : null;
// await sendTextChunks_(groupChatId, threadId, postText);  // <- убрать, чтобы не было дубля
  } else if (postText && postText.trim()) {
    const sent = await bot.sendMessage(groupChatId, postText, { message_thread_id: threadId });
    postMessageId = sent.message_id;
  } else {
    // no content -> still allow, but send minimal text
    const sent = await bot.sendMessage(groupChatId, " ", { message_thread_id: threadId });
    postMessageId = sent.message_id;
  }

  // 1.5) snapshot: total members in chat (for progress bar)
let chatMembers = 0;
try {
  chatMembers = await bot.getChatMemberCount(groupChatId);
} catch (e) {
  console.log("getChatMemberCount failed:", e.message);
  chatMembers = 0;
}

 // 2) create poll doc
const pollRef = await RSVP.add({
  createdAt: admin.firestore.FieldValue.serverTimestamp(),
  createdBy: Number(userId),

  groupChatId: Number(groupChatId),
  threadId: Number(threadId),
  topicKey: String(topicKey || ""),  // ✅ ДОБАВИТЬ
  // snapshot total members (for progress bar)
  chatMembers: Number(chatMembers || 0),

  postMessageId: Number(postMessageId || 0),
  cardMessageId: 0,

  qRu: String(qRu || ""),
  qDe: String(qDe || ""),
  dt: String(dt || ""),

  yes: 0,
  no: 0,

  active: true,
  uiLocked: false
});

  const pollId = pollRef.id;

  // 3) send RSVP card below post
  const cardText = formatRsvpCardText({ qRu, qDe, dt, topicKey, chatMembers }, 0, 0);
  const sentCard = await bot.sendMessage(groupChatId, cardText, {
    message_thread_id: threadId,
    reply_markup: rsvpKeyboard(pollId, 0, 0),
  });

  await pollRef.set({ cardMessageId: Number(sentCard.message_id) }, { merge: true });

  // done
  await wizardEditScreen_(chatId, userId, `✅ Опубликовано.\n\nТЕМА: ${topicKey}\nRSVP ID: ${pollId}`, mainMenuKeyboard("RU", true));
  resetCreateWizard_(userId);
}

// ====== MEDIA GROUP COLLECTOR (private) ======
/**
 * Чтобы корректно поймать альбом фото, нужно собрать file_id из media_group_id
 * и только потом перейти к следующему шагу.
 */
const mediaGroupBuffer = {}; // media_group_id -> { userId, chatId, files[], timer }

function pushMediaGroup_(mediaGroupId, userId, chatId, fileId, captionText) {
  if (!mediaGroupBuffer[mediaGroupId]) {
    mediaGroupBuffer[mediaGroupId] = { userId, chatId, files: [], captionText: captionText || "", timer: null };
  }
  const buf = mediaGroupBuffer[mediaGroupId];
  buf.files.push(fileId);
  if (captionText) buf.captionText = captionText;

  // reset timer
  if (buf.timer) clearTimeout(buf.timer);
  buf.timer = setTimeout(async () => {
    try {
      const w = createWizard[userId];
      if (!w || w.step !== 2) return;

      w.data.media = buf.files.slice(0, 10);
      if (buf.captionText && !w.data.postText) w.data.postText = buf.captionText;

      delete mediaGroupBuffer[mediaGroupId];

      await wizardAskQuestion_(chatId, userId);
    } catch (e) {
      console.error(e);
    }
  }, 800);
}

// ====== /start and /menu ======
  bot.onText(/^\/start(.*)?$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from?.id;
  if (!userId) return;

  // deep link payload
  const payload = (match && match[1]) ? String(match[1]).trim() : "";
  // payload like " results_<pollId>"
  const p = payload.replace(/^\s+/, "");

  await showMenu_(chatId, userId);

  if (p.startsWith("results_")) {
  const pollId = p.replace("results_", "").trim();
  // по умолчанию открываем вкладку YES, страница 0
  await showResults_(chatId, userId, pollId, "YES", 0);
}
});

bot.onText(/^\/menu$/i, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from?.id;
  if (!userId) return;
  await showMenu_(chatId, userId);
});

// ===== ADMIN: set custom display name =====
// usage: /setname 123456789 Vitalii K.
bot.onText(/^\/setname\s+(\d+)\s+(.+)$/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const adminId = msg.from?.id;
  if (!isAdmin_(adminId)) return;

  // только в личке
  if (chatId < 0) return;

  const uid = String(match?.[1] || "").trim();
  const name = String(match?.[2] || "").trim();

  if (!uid || !name) {
    await bot.sendMessage(chatId, "Формат: /setname 123456789 Новое Имя");
    return;
  }

  await (await getUserDoc_(uid)).set(
    { profile: { displayName: name } },
    { merge: true }
  );

  // сброс кэша имени для этого пользователя
  nameCache.delete(String(uid));

  await bot.sendMessage(chatId, `✅ Имя обновлено:\n${uid} → ${name}`);
});

// ====== /bindtopic KEY (in group topic) ======
bot.onText(/^\/bindtopic(?:\s+(.+))?$/i, async (msg, match) => {
  try {
    const chatId = msg.chat.id;
    const threadId = msg.message_thread_id;
    const userId = msg.from?.id;

    if (!isAdmin_(userId)) {
      return bot.sendMessage(chatId, "⛔️ Нет доступа.", { message_thread_id: threadId });
    }

    const key = (match && match[1]) ? String(match[1]).trim() : "";
    if (!key) {
      return bot.sendMessage(chatId, "Формат: /bindtopic бег", { message_thread_id: threadId });
    }
    if (!threadId) {
      return bot.sendMessage(chatId, "⚠️ Команду нужно писать внутри темы (TOPIC).");
    }

    await saveTopic_(key, chatId, threadId);

    return bot.sendMessage(chatId,
      "✅ Тема сохранена:\n" +
      `KEY: ${key}\n` +
      `CHAT: ${chatId}\n` +
      `THREAD: ${threadId}`,
      { message_thread_id: threadId }
    );
  } catch (e) {
    console.error(e);
  }
});

// ===== EVENTS PRO ULTRA: list screen (with pagination) =====
async function showEventsPro_(chatId, userId, page) {
  const pageSize = 5;
  const p = Math.max(0, Number(page || 0));

  // берем чуть больше, чтобы понять есть ли следующая страница
  const need = (p + 1) * pageSize + 1;

  const snap = await fsQuery_(
  RSVP.where("active", "==", true).orderBy("createdAt", "desc").limit(need),
  "rsvp.list.eventspro"
);

  const docs = snap.docs || [];
  const hasNext = docs.length > (p + 1) * pageSize;
  const slice = docs.slice(p * pageSize, p * pageSize + pageSize);

  const lang2 = await getUserLang_(userId);

  // --- UI: compact list like "choose event" screen (buttons) ---
const titleLabel = (lang2 === "DE") ? "Wähle ein Event:" :
                   (lang2 === "UA") ? "Оберіть подію:" :
                   "Выберите событие:";

// короткий текст экрана (без длинных карточек)
const txt = [
  (lang2 === "DE") ? "EVENTS / Ereignisse" :
  (lang2 === "UA") ? "ПОДІЇ / Events" :
  "EVENTS / События",
  "",
  titleLabel
].join("\n");

// кнопки событий (по 1 строке на событие)
const rows = [];

slice.forEach((d) => {
  const e = d.data() || {};
  const id = d.id;

  const dt = String(e.dt || "");
  const parts = dt.split(",");
  const dtDate = (parts[0] || "").trim();   // "22.02.2026"
  const dtTime = (parts[1] || "").trim();   // "15:00"

  const yes = Number(e.yes || 0);
  const no  = Number(e.no  || 0);
  const total = yes + no;

  const title = (lang2 === "DE") ? String(e.qDe || e.qRu || "") : String(e.qRu || e.qDe || "");

  // ряд: [дата/время] [название] [участники] [результаты]

// кнопка только дата + время
const btnText = `${dtDate} ${dtTime}`.trim();

rows.push([
  { text: btnText.slice(0, 32), callback_data: `evp_open_${id}` },
]);

});

// пагинация
const navRow = [];
if (p > 0) navRow.push({ text: "‹", callback_data: `evp_page_${p - 1}` });
navRow.push({ text: `${p + 1}`, callback_data: "noop" });
if (hasNext) navRow.push({ text: "›", callback_data: `evp_page_${p + 1}` });
if (navRow.length) rows.push(navRow);

// назад в меню
rows.push([{
  text: (lang2 === "DE") ? "Menü" : (lang2 === "UA") ? "Меню" : "Меню",
  callback_data: "evp_back_menu"
}]);

await editMainScreen_(chatId, userId, txt, { inline_keyboard: rows });
return;

}
// ---------- helpers: 3 languages ----------
function t3(lang, de, ua, ru) {
  if (lang === "DE") return de;
  if (lang === "UA") return ua;
  return ru;
}

function categoryLabel_(lang, topicKey) {
  const k = normTopicKey_(topicKey);

  // показываем красиво на выбранном языке
  const map = {
    "бег":        { RU: "Бег",        UA: "Біг",          DE: "Laufen" },
    "волейбол":   { RU: "Волейбол",   UA: "Волейбол",     DE: "Volleyball" },
    "вело":       { RU: "Велозаезд",  UA: "Велозаїзд",    DE: "Radfahren" },
    "поход":      { RU: "Поход",      UA: "Похід",        DE: "Wandern" },
    "плавание":   { RU: "Плавание",   UA: "Плавання",     DE: "Schwimmen" },
    "мероприятия":{ RU: "Мероприятия",UA: "Заходи",       DE: "Events" },
  };

  const rec = map[k];
  if (!rec) return topicKey || "—";
  return rec[lang] || rec.RU;
}

function buildRulesText_(lang) {
  const items = [
    { key: "бег",         ru: "Бег",         ua: "Біг",         de: "Laufen" },
    { key: "волейбол",    ru: "Волейбол",    ua: "Волейбол",    de: "Volleyball" },
    { key: "вело",        ru: "Велозаезд",   ua: "Велозаїзд",   de: "Radtour" },
    { key: "поход",       ru: "Поход",       ua: "Похід",       de: "Wanderung" },
    { key: "плавание",    ru: "Плавание",    ua: "Плавання",    de: "Schwimmen" },
    { key: "мероприятия", ru: "Мероприятия", ua: "Події",       de: "Events" },
  ];

  const title =
    lang === "DE"
      ? "Punktesystem (Saison 2026)"
      : lang === "UA"
        ? "Система балів (сезон 2026)"
        : "Правила начисления баллов (сезон 2026)";

  const lines = items.map(x => {
    const pts = Number(POINTS_BY_TOPIC[x.key] || 0);
    const label = lang === "DE"
      ? x.de
      : lang === "UA"
        ? x.ua
        : x.ru;

    return `• ${label} — ${pts}`;
  });

 const penaltyAbs = Math.abs(-5); // штраф за NO-SHOW (сейчас -5 в коде)

const footer1 =
  lang === "DE"
    ? "Punkte werden für den Check-in (Anwesenheit) vergeben."
    : lang === "UA"
      ? "Бали нараховуються за відмітку присутності."
      : "Баллы начисляются за отметку присутствия.";

const footer2 =
  lang === "DE"
    ? `Wer „Ja“ klickt und am Event-Tag nicht erscheint, bekommt −${penaltyAbs} Punkte.`
    : lang === "UA"
      ? `Якщо натиснув «Так» і не прийшов у день події −${penaltyAbs} балів.`
      : `Если нажал «Да» и не пришёл в день события −${penaltyAbs} баллов.`;

return [title, "", ...lines, "", footer1, footer2].join("\n");
}

// ---------- show one event card ----------
async function showEventCard_(chatId, userId, pollId) {
  const pollSnap = await RSVP.doc(String(pollId)).get();
  if (!pollSnap.exists) return;

  const e = pollSnap.data() || {};
  const l = await getUserLang_(userId);

  const title =
  (l === "DE") ? (e.qDe || e.qRu || "") :
  (l === "UA") ? (e.qRu || e.qDe || "") :   // у тебя UA текста нет → показываем RU как базовый
               (e.qRu || e.qDe || "");
  const dt = String(e.dt || "");
const catKey = String(e.topicKey || e.category || e.cat || "");
const catShown = categoryLabel_(l, catKey);

  const yes = Number(e.yes || 0);
  const no = Number(e.no || 0);
  const total = yes + no;

  const peopleLabel = t3(l, "Teilnehmer", "Учасники", "Участники");
  const catLabelText = t3(l, "Kategorie",   "Категорія", "Категория");

  const text = [
    `🗓️ ${dt}`,
    `<b>${escHtml_(title)}</b>`,
    `${catLabelText}: ${catShown || "—"}`,
    `${peopleLabel}: ${total}`
  ].join("\n");

  // buttons: Back + Next (2 columns), Menu bottom
  const kb = {
    inline_keyboard: [
      [
        { text: t3(l, "Zurück", "Назад", "Назад"), callback_data: "events_pro" },
        { text: t3(l, "Nächstes", "Наступне", "Следующее"), callback_data: `evp_next_${pollId}` },
      ],
      [
        { text: t3(l, "Menü", "Меню", "Меню"), callback_data: "evp_back_menu" }
      ]
    ]
  };

  await editMainScreen_(chatId, userId, text, kb);
}

// ---------- get next event id (by createdAt desc list order) ----------
async function getNextEventId_(currentId) {
  const snap = await RSVP
    .where("active", "==", true)
    .orderBy("createdAt", "desc")
    .get();

  const docs = snap.docs || [];
  const i = docs.findIndex(d => String(d.id) === String(currentId));
  if (i < 0) return null;

  // "next" = next item in this list (older one)
  const nextDoc = docs[i + 1];
  return nextDoc ? nextDoc.id : null;
}

function ymdToDate_(ymd) {
  // ymd: "YYYY-MM-DD"
  const [y,m,d] = String(ymd || "").split("-").map(Number);
  if (!y || !m || !d) return null;
  // ставим 12:00 чтобы не ловить DST-ошибки
  return new Date(y, m - 1, d, 12, 0, 0);
}

function diffDaysBerlin_(dtStr) {
  const eventYMD = parseEventYMD_(dtStr);
  if (!eventYMD) return 9999;
  const nowYMD = todayYMD_(TZ);
  const a = ymdToDate_(eventYMD);
  const b = ymdToDate_(nowYMD);
  if (!a || !b) return 9999;
  const ms = a.getTime() - b.getTime();
  return Math.round(ms / (24 * 3600 * 1000)); // -1 вчера, 0 сегодня, +1 завтра
}

async function showCheckinPickEvent_(chatId, userId, page) {
  const pageSize = 6;
  const p = Math.max(0, Number(page || 0));
  const need = (p + 1) * pageSize + 1;

  const snap = await fsQuery_(
  RSVP.where("active", "==", true).orderBy("createdAt", "desc").limit(60),
  "rsvp.list.checkin"
);

 const docs = snap.docs || [];

// 1) ближайшие: вчера/сегодня/завтра
const near = docs.filter(d => {
  const e = d.data() || {};
  const dd = diffDaysBerlin_(e.dt);
  return dd >= -1 && dd <= 1;
});

// 2) если рядом событий мало — добавим ещё “самые свежие”, чтобы список не был пустым
const pool = near.length ? near : docs;

// 3) пагинация по pool
const hasNext = pool.length > (p + 1) * pageSize;
const slice = pool.slice(p * pageSize, p * pageSize + pageSize);

  if (!slice.length) {
    await editMainScreen_(chatId, userId, "ЧЕК-ИН\n\nНет активных событий.", mainMenuKeyboard("RU", true));
    return;
  }

  const rows = [];

let lastDate = "";
let green = true; // первая дата будет зелёной

slice.forEach(d => {
  const e = d.data() || {};
  const id = d.id;
  const dt = String(e.dt || "");
  const q = String(e.qRu || e.qDe || "(без названия)");
  const yes = Number(e.yes || 0);
  const dtDate = (dt.split(",")[0] || "").trim();

if (dtDate && dtDate !== lastDate) {
  if (lastDate) green = !green;
  lastDate = dtDate;
}
const style = green ? "success" : undefined;

rows.push([{
  text: ` ${q}`.slice(0, 58),
  callback_data: `ci_pick_${id}`,
  ...(style ? { style } : {})
}]);

 rows.push([{
  text: `${dt} • YES: ${yes}`.slice(0, 58),
  callback_data: `ci_pick_${id}`,
  ...(style ? { style } : {})
}]);

});

  const nav = [];
  if (p > 0) nav.push({ text: "‹", callback_data: `ci_page_${p - 1}` });
  nav.push({ text: `${p + 1}`, callback_data: "noop" });
  if (hasNext) nav.push({ text: "›", callback_data: `ci_page_${p + 1}` });
  if (nav.length) rows.push(nav);

  rows.push([{ text: "Меню", callback_data: "ci_back_menu" }]);

  await editMainScreen_(chatId, userId, "ЧЕК-ИН\n\nВыбери событие:", { inline_keyboard: rows });
}

async function applyNameOverrides_(usersArr) {
  const ids = (usersArr || []).map(u => String(u.userId)).filter(Boolean);
  if (!ids.length) return usersArr;

  // batch get
  const refs = ids.map(id => USERS.doc(String(id)));
  const snaps = await db.getAll(...refs);

  const map = new Map(); // uid -> displayName
  snaps.forEach(s => {
  if (!s.exists) return;
  const d = s.data() || {};

  const dn = d.profile?.displayName;
  const full = `${d.firstName || ""}${d.lastName ? " " + d.lastName : ""}`.trim();

  if (dn && String(dn).trim()) map.set(String(s.id), String(dn).trim());
  else if (full) map.set(String(s.id), full);
});

  return usersArr.map(u => {
    const dn = map.get(String(u.userId));
    return dn ? { ...u, name: dn } : u;
  });
}


// ====== NAME OVERRIDES for TOP lists ======
async function applyNameOverridesTop_(topArr) {
  const ids = (topArr || []).map(x => String(x.userId)).filter(Boolean);
  if (!ids.length) return topArr || [];

  // batch get users docs
  const refs = ids.map(id => USERS.doc(String(id)));
  const snaps = await db.getAll(...refs);

  const map = new Map(); // uid -> displayName
  snaps.forEach(s => {
  if (!s.exists) return;
  const d = s.data() || {}; 
  const dn = d.profile?.displayName;
  const full = `${d.firstName || ""}${d.lastName ? " " + d.lastName : ""}`.trim();
if (dn && String(dn).trim()) map.set(String(s.id), String(dn).trim());
else if (full) map.set(String(s.id), full);
  });

  return (topArr || []).map(x => {
    const dn = map.get(String(x.userId));
    return dn ? { ...x, name: dn } : x;
  });
}

async function getYesUsers_(pollId) {
  const votesSnap = await fsGet_(RSVP.doc(String(pollId)).collection("votes"), "rsvp.votes.get.yesUsers");
  const yes = [];
  votesSnap.forEach(d => {
    const v = d.data() || {};
    if (v.choice === "YES") {
      yes.push({
        userId: String(d.id),
        name: (v.name || "").trim() || "Без имени",
        username: v.username ? String(v.username) : ""
      });
    }
  });
  return yes;
}

function checkinPeopleKb_(adminUserId, pollId, yesList, page, pageSize = 8) {
  const sess = checkinSession[adminUserId] || { present: new Set(), page: 0 };
  const present = sess.present || new Set();

  const total = yesList.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const p = Math.max(0, Math.min(Number(page || 0), totalPages - 1));

  const start = p * pageSize;
  const slice = yesList.slice(start, start + pageSize);

  const rows = [];

  slice.forEach(u => {
    const isHere = present.has(String(u.userId));
    rows.push([
     {
  text: `${(u.name || "Без имени").slice(0, 32)}`,
  callback_data: "noop"
},
   { 
  text: isHere ? "Пришёл ✓" : "Пришёл",
  callback_data: `ci_t_${pollId}_${u.userId}_${p}`,
  style: isHere ? "success" : undefined
}
    ]);
  });

  rows.push([
   { text: "Отметить всех", callback_data: `ci_all_${pollId}_${p}`, style: "success" },
{ text: "Снять всем",    callback_data: `ci_none_${pollId}_${p}`, style: "danger" },
  ]);

  const nav = [];
  if (p > 0) nav.push({ text: "‹", callback_data: `ci_people_${pollId}_${p - 1}` });
  nav.push({ text: `${p + 1}/${totalPages}`, callback_data: "noop" });
  if (p < totalPages - 1) nav.push({ text: "›", callback_data: `ci_people_${pollId}_${p + 1}` });
  rows.push(nav);

  rows.push([{ text: "Готово / Закрыть чек-ин", callback_data: `ci_done_${pollId}`, style: "primary" }]);
  rows.push([{ text: "Назад", callback_data: "a_checkin" }]);

  return { inline_keyboard: rows };
}

async function showCheckinPeople_(chatId, adminUserId, pollId, page) {
  const pollSnap = await fsGet_(RSVP.doc(String(pollId)), "rsvp.get.checkinPeople");
  if (!pollSnap.exists) {
    await editMainScreen_(chatId, adminUserId, "ЧЕК-ИН\n\nСобытие не найдено.", mainMenuKeyboard("RU", true));
    return;
  }

  const poll = pollSnap.data() || {};
  let yesList = await getYesUsers_(pollId);
  yesList = await applyNameOverrides_(yesList);

  if (!checkinSession[adminUserId]) checkinSession[adminUserId] = { pollId, present: new Set(), page: 0 };
  checkinSession[adminUserId].pollId = pollId;
  checkinSession[adminUserId].page = Number(page || 0);

  const presentCount = (checkinSession[adminUserId].present || new Set()).size;

  const title = String(poll.qRu || poll.qDe || "").trim() || "—";
  const dt = String(poll.dt || "");
  const topicKey = String(poll.topicKey || "");
  const pts = getPointsForTopic_(topicKey);

  const text = [
    "ЧЕК-ИН",
    "",
    `Событие: ${title}`,
    `Дата: ${dt}`,
    `Категория: ${topicKey || "—"}`,
    `Баллы: ${pts}`,
    "",
    `YES: ${yesList.length}`,
    `Пришли: ${presentCount} из ${yesList.length}`,
    "",
    "Отметь кто пришёл:"
  ].join("\n");

  const kb = checkinPeopleKb_(adminUserId, pollId, yesList, page, 8);
  await editMainScreen_(chatId, adminUserId, text, kb);
}

// ====== CALLBACKS (menu + wizard + rsvp) ======
bot.on("callback_query", async (q) => {
  console.log("CB:", q.data, "from:", q.from?.id);
 
  const data = q.data || "";
  const from = q.from;
  const userId = from?.id;
  await (await getUserDoc_(userId)).set({
  firstName: from.first_name || "",
  lastName: from.last_name || "",
  username: from.username || "",
}, { merge: true });
const chatId = q.message?.chat?.id;
  const messageId = q.message?.message_id;
// 1) remove loading instantly (НО НЕ для results-ссылки)
if (!String(data).startsWith("rsvp_results_")) {
  try { await bot.answerCallbackQuery(q.id); } catch (_) {}
}

  if (!userId || !chatId || !messageId) return;
  // anti-spam: one callback at a time per user
if (userBusy[userId]) return;
userBusy[userId] = true;
try {
 const adminMode = isAdmin_(userId);
  let lang = await getUserLang_(userId);

  // ===== RESULTS (private) pagination buttons =====
if (data === "res_menu") {
  await showMenu_(chatId, userId);
  return;
}

const rr = String(data).match(/^res_([A-Za-z0-9]+)_(YES|NO)_(\d+)$/);
if (rr) {
  const pollId = rr[1];
  const tab = rr[2];
  const page = Number(rr[3] || 0);
  await showResults_(chatId, userId, pollId, tab, page);
  return;
}

if (data === "ci_back_menu") {
  await showMenu_(chatId, userId);
  return;
}

// ===== CHECK-IN: pick event =====
const cip = String(data).match(/^ci_page_(\d+)$/);
if (cip) {
  await showCheckinPickEvent_(chatId, userId, Number(cip[1] || 0));
  return;
}

const pick = String(data).match(/^ci_pick_(.+)$/);
if (pick) {
  const pollId = pick[1];
  checkinSession[userId] = { pollId, present: new Set(), page: 0 };

  await showCheckinPeople_(chatId, userId, pollId, 0);
  return;
}

// ===== CHECK-IN: people list actions =====
const cipp = String(data).match(/^ci_people_(.+)_(\d+)$/);
if (cipp) {
  const pollId = cipp[1];
  const page = Number(cipp[2] || 0);
  await showCheckinPeople_(chatId, userId, pollId, page);
  return;
}

const cit = String(data).match(/^ci_t_(.+)_(.+)_(\d+)$/);
if (cit) {
  const pollId = cit[1];
  const uid = cit[2];
  const page = Number(cit[3] || 0);

  if (!checkinSession[userId]) checkinSession[userId] = { pollId, present: new Set(), page: 0 };
  const set = checkinSession[userId].present || new Set();

  if (set.has(String(uid))) set.delete(String(uid));
  else set.add(String(uid));

  checkinSession[userId].present = set;

  await showCheckinPeople_(chatId, userId, pollId, page);
  return;
}

const ciall = String(data).match(/^ci_all_(.+)_(\d+)$/);
if (ciall) {
  const pollId = ciall[1];
  const page = Number(ciall[2] || 0);
  const yesList = await getYesUsers_(pollId);

  if (!checkinSession[userId]) checkinSession[userId] = { pollId, present: new Set(), page: 0 };
  checkinSession[userId].present = new Set(yesList.map(x => String(x.userId)));

  await showCheckinPeople_(chatId, userId, pollId, page);
  return;
}

const cinone = String(data).match(/^ci_none_(.+)_(\d+)$/);
if (cinone) {
  const pollId = cinone[1];
  const page = Number(cinone[2] || 0);

  if (!checkinSession[userId]) checkinSession[userId] = { pollId, present: new Set(), page: 0 };
  checkinSession[userId].present = new Set();

  await showCheckinPeople_(chatId, userId, pollId, page);
  return;
}

// закрыть чек-ин (начислить + штраф, транзакционно, без дублей)
const cidone = String(data).match(/^ci_done_(.+)$/);
if (cidone) {
  const pollId = cidone[1];

  // покажем "обработка"
  await editMainScreen_(chatId, userId, "ЧЕК-ИН\n\nЗакрываю и начисляю баллы…", { inline_keyboard: [] });

  const r = await closeCheckinTx_(userId, pollId);

  if (!r || !r.ok) {
    await editMainScreen_(chatId, userId, `ЧЕК-ИН\n\nОшибка: ${r?.msg || "неизвестно"}`, mainMenuKeyboard("RU", true));
    return;
  }

  const text = [
    "ЧЕК-ИН закрыт ✅",
    "",
    `Пришли: ${r.arrived}`,
    `Не пришли (YES): ${r.noshow}`,
    "",
    (r.already ? "Дублей нет: чек-ин был закрыт ранее." : `Начисление: +${r.pts} / Штраф: ${r.penalty}`),
  ].join("\n");

  await editMainScreen_(chatId, userId, text, mainMenuKeyboard("RU", true));

  // чистим сессию в памяти
  delete checkinSession[userId];
  return;
}

  // ===== WIZARD callbacks (topic/cancel/publish) =====
if (data === "w_cancel") {
  resetCreateWizard_(userId);   // или resetCreateWizard_ если так у тебя называется
  await showMenu_(chatId, userId);
  return;
}

if (data.startsWith("w_topic_")) {
  if (!adminMode) {
    await editMainScreen_(chatId, userId, t_(lang, "TXT_NO_ACCESS"), mainMenuKeyboard(lang, adminMode));
    return;
  }

  const key = data.slice("w_topic_".length).trim();
  // ВАЖНО: у тебя wizard-объект называется createWizard (судя по скрину ниже) — проверь имя:
  if (!createWizard[userId]) createWizard[userId] = { step: 1, data: {} };
  createWizard[userId].data.topicKey = key;

  await wizardAskContent_(chatId, userId); // шаг 2/4
  return;
}

if (data === "w_publish") {
  if (!adminMode) return;
  if (!createWizard[userId]) return;
  await publishWizard_(chatId, userId);
  return;
}

// ===== Admin: start wizard (Create RSVP) =====
if (data === "a_create_rsvp") {
  if (!adminMode) {
    await editMainScreen_(chatId, userId, t_(lang, "TXT_NO_ACCESS"), mainMenuKeyboard(lang, adminMode));
    return;
  }

  try {
    await startCreateRsvpWizard_(chatId, userId);
  } catch (e) {
    console.error("startCreateRsvpWizard_ error:", e);
    await editMainScreen_(chatId, userId, "Ошибка при запуске мастера создания события.", mainMenuKeyboard("RU", true));
  }
  return;
}

// ===== Admin: Check-in (stub) =====
if (data === "a_checkin") {
  if (!adminMode) {
    await editMainScreen_(chatId, userId, t_(lang, "TXT_NO_ACCESS"), mainMenuKeyboard(lang, adminMode));
    return;
  }

  await showCheckinPickEvent_(chatId, userId, 0);
  return;
}

  // ===== EVENTS PRO ULTRA (entry) =====
if (data === "events_pro") {
 if (!adminMode) {
    await editMainScreen_(chatId, userId, t_(lang, "TXT_NO_ACCESS"), mainMenuKeyboard(lang, adminMode));
    return;
  }

  await showEventsPro_(chatId, userId, 0);
  return;
}

// pagination in Events PRO list
const epg = String(data).match(/^evp_page_(\d+)$/);
if (epg) {
  const page = Number(epg[1] || 0);
  await showEventsPro_(chatId, userId, page);
  return;
}

// меню из Events PRO
if (data === "evp_back_menu") {
  await showMenu_(chatId, userId);
  return;
}

// открыть событие из списка (кнопка во всю ширину)
const op = data.match(/^evp_open_(.+)$/);
if (op) {
  const pollId = op[1];
  await showEventCard_(chatId, userId, pollId);
  return;
}

// следующее событие (из карточки)
const nx = data.match(/^evp_next_(.+)$/);
if (nx) {
  const currentId = nx[1];
  const nextId = await getNextEventId_(currentId);

  if (!nextId) {
    const l = await getUserLang_(userId);
    const text = t3(l, "Keine weiteren Events.", "Наступних подій немає.", "Следующих событий нет.");
    const kb = {
      inline_keyboard: [
        [
          { text: t3(l, "Zurück", "Назад", "Назад"), callback_data: "events_pro" },
          { text: t3(l, "Menü", "Меню", "Меню"), callback_data: "evp_back_menu" },
        ]
      ]
    };
    await editMainScreen_(chatId, userId, text, kb);
    return;
  }

  await showEventCard_(chatId, userId, nextId);
  return;
}

  // ===== Language change (users only) =====
  if (data.startsWith("lang_")) {
    const newLang = data.replace("lang_", "").trim().toUpperCase();
    if (["RU","UA","DE"].includes(newLang)) {
      await (await getUserDoc_(userId)).set({ lang: newLang }, { merge: true });
      console.log("LANG SET TO:", newLang, "userId:", userId);
    }
    await showMenu_(chatId, userId);
    return;
  }

  // ===== Main menu actions =====
  if (data === "m_profile") {
    // minimal profile now (extend as needed)
    const uref = await getUserDoc_(userId);
    const us = await uref.get();
    const l = await getUserLang_(userId);
   const { n, level, days } = await getUserActivity30d_(userId);

const labelActivity =
  (l === "DE") ? "Aktivität" :
  (l === "UA") ? "Активність" :
  "Активность";

const labelActivities =
  (l === "DE") ? `Aktivitäten (${days} Tage)` :
  (l === "UA") ? `Активності (${days} днів)` :
  `Активности (${days} дней)`;

  const labelName =
  (l === "DE") ? "Name" :
  (l === "UA") ? "Ім’я" :
  "Имя";


  const dn = us.data()?.profile?.displayName;
const fullFrom = `${from.first_name || ""}${from.last_name ? " " + from.last_name : ""}`.trim();
const shownName = (dn && String(dn).trim()) ? String(dn).trim() : (fullFrom || "—");
const txt = [
  t_(l,"BTN_PROFILE"),
  "",
  `${labelName}: ${shownName}`,
  `Username: ${from.username ? "@"+from.username : "—"}`,
  `ID: ${userId}`,
  `${t_(l,"TXT_LANG")}: ${(us.data()?.lang || "RU")}`,
  `${labelActivity}: ${activityLabel_(l, level)}`,
  `${labelActivities}: ${n}`,
].join("\n");

    await editMainScreen_(chatId, userId, txt, mainMenuKeyboard(l, adminMode));
    return;
  }

  if (data === "m_pos") {
    // Заглушка: позиция считается по points ledger (год текущий)
    const now = new Date();
    const year = now.getFullYear();
    const { place, points } = await computeUserPlaceYear_(userId, year);
    const l = await getUserLang_(userId);
    const uref = await getUserDoc_(userId);
    const us = await uref.get();
    const dn = us.data()?.profile?.displayName;
    const fullFrom = `${from.first_name || ""}${from.last_name ? " " + from.last_name : ""}`.trim();
    const shownName = (dn && String(dn).trim()) ? String(dn).trim() : (fullFrom || "—");
    const txt = [
     `${t_(l,"TXT_MY_POSITION_TITLE")} — ${year}`,
     `${t_(l,"TXT_NAME")}: ${shownName}`,
     `${t_(l,"TXT_PLACE")}: ${place || "—"}`,
     `${t_(l,"TXT_POINTS")}: ${points || 0}`,
  ].join("\n");

    await editMainScreen_(chatId, userId, txt, mainMenuKeyboard(l, adminMode));
    return;
  }

  if (data === "m_rating") {
    const now = new Date();
    const year = now.getFullYear();
    const top = await computeTopYear_(year, 20);
    const top2 = await applyNameOverridesTop_(top);

    const lines = top2.length
  ? top2.map((x,i)=>`${i+1}. ${x.name} — ${x.points}`).join("\n")
  : (lang === "DE")
    ? "Noch keine Daten."
    : (lang === "UA")
      ? "Поки немає даних."
      : "Пока нет данных.";

const title =
  (lang === "DE")
    ? `Gesamtranking — ${year}`
    : (lang === "UA")
      ? `Загальний рейтинг — ${year}`
      : `Общий рейтинг — ${year}`;

const txt = [title, "", lines].join("\n");
    await editMainScreen_(chatId, userId, txt, mainMenuKeyboard(lang, adminMode));
    return;
  }

  if (data === "m_rules") {
    // Rules can be stored in Firestore config/rules
   const txt = buildRulesText_(lang);
await editMainScreen_(chatId, userId, txt, mainMenuKeyboard(lang, adminMode));
    return;
  }
  

  // ===== RSVP callbacks in group =====
// rsvp_yes_<id> | rsvp_no_<id> | rsvp_results_<id>
const m = data.match(/^rsvp_(yes|no|results)_(.+)$/);
if (m) {
  const action = m[1];
  const pollId = m[2];

  const pollSnap = await RSVP.doc(String(pollId)).get();
  if (!pollSnap.exists) return;

  const poll = pollSnap.data() || {};
  const groupChatId = Number(poll.groupChatId);
  const cardMessageId = Number(poll.cardMessageId);

  // 1) RESULTS — всегда разрешено
  if (action === "results") {
    const url = `https://t.me/${BOT_USERNAME}?start=results_${pollId}`;
    try {
      await bot.answerCallbackQuery(q.id, { url });
    } catch (_) {
      try {
        await bot.answerCallbackQuery(q.id, { show_alert: true, text: t_(lang, "TXT_OPEN_PM") });
      } catch (_) {}
    }
    return;
  }

  // 2) YES/NO — может быть заблокировано
  const locked = isRsvpLocked_(poll.dt);

  if (locked) {
    // 2.1) alert
    try {
      await bot.answerCallbackQuery(q.id, {
        show_alert: true,
        text: (lang === "DE")
          ? "Ab heute ist die Abstimmung geschlossen."
          : (lang === "UA")
            ? "Від сьогодні голосування закрите."
            : "С сегодняшнего дня голосование закрыто."
      });
    } catch (_) {}

    // 2.2) привести карточку к виду "только Результаты"
    const text = formatRsvpCardText({
      qRu: poll.qRu,
      qDe: poll.qDe,
      dt: poll.dt,
      topicKey: poll.topicKey,
      chatMembers: poll.chatMembers
    }, poll.yes, poll.no);

    const kb = rsvpKeyboardResultsOnly_(pollId);
    await editRsvpCardNoNew_(groupChatId, cardMessageId, text, kb, null, lang);
    return;
  }

  // 3) голосуем и обновляем карточку
  const choice = (action === "yes") ? "YES" : "NO";
  await rsvpVote_(pollId, from, choice);

  const { yes, no } = await getRsvpCounts_(pollId);

  const text = formatRsvpCardText({
    qRu: poll.qRu,
    qDe: poll.qDe,
    dt: poll.dt,
    topicKey: poll.topicKey,
    chatMembers: poll.chatMembers
  }, yes, no);

  // если вдруг после голосования день уже наступил — оставим только Results
  const lockedNow = isRsvpLocked_(poll.dt);
  const kb = lockedNow ? rsvpKeyboardResultsOnly_(pollId) : rsvpKeyboard(pollId, yes, no);

  await editRsvpCardNoNew_(groupChatId, cardMessageId, text, kb, null, lang);
  return;
}
}
  catch (e) {
  console.error("callback_query error:", e);
} finally {
  userBusy[userId] = false;
}
});
// ====== Wizard content capture (private messages) ======
bot.on("message", async (msg) => {
  try {
    const chatId = msg.chat.id;
    const userId = msg.from?.id;
    if (!userId) return;

    // only private wizard
    if (chatId < 0) return;

    const w = createWizard[userId];
    if (!w) return;

    // ignore commands
    const text = (msg.text || "").trim();
    if (text.startsWith("/")) return;

    // cancel via text
    if (text.toLowerCase() === "отмена") {
      resetCreateWizard_(userId);
      await showMenu_(chatId, userId);
      return;
    }

    // Step 2: capture post content (text/photo/album)
    if (w.step === 2) {
      // album
      if (msg.media_group_id && msg.photo && msg.photo.length) {
        const fileId = msg.photo[msg.photo.length - 1].file_id;
        pushMediaGroup_(msg.media_group_id, userId, chatId, fileId, msg.caption || "");
        return;
      }

      // single photo
      if (msg.photo && msg.photo.length) {
        const fileId = msg.photo[msg.photo.length - 1].file_id;
        w.data.media = [fileId];
        if (msg.caption) w.data.postText = msg.caption;
        await wizardAskQuestion_(chatId, userId);
        return;
      }

      // text only
      if (text) {
        w.data.postText = text;
        w.data.media = [];
        await wizardAskQuestion_(chatId, userId);
        return;
      }

      return;
    }

    // Step 3: question RU/DE (2 lines)
    if (w.step === 3) {
      const lines = String(msg.text || "").split("\n").map(s => s.trim()).filter(Boolean);
      if (lines.length < 2) {
        await wizardEditScreen_(chatId, userId,
          "Нужно 2 строки:\n1) RU\n2) DE\n\nПример:\nЕдёшь с нами...\nFährst du mit uns...",
          wizardCancelKb_()
        );
        return;
      }
      w.data.qRu = lines[0];
      w.data.qDe = lines[1];
      await wizardAskDate_(chatId, userId);
      return;
    }

    // Step 4: date string
    if (w.step === 4) {
      w.data.dt = text || (msg.text || "");
      // confirm
      await wizardConfirm_(chatId, userId);
      return;
    }
  } catch (e) {
    console.error(e);
  }
});

// ====== POINTS RULES (hard mapping) ======
const POINTS_BY_TOPIC = {
  "бег": 30,
  "волейбол": 20,
  "вело": 15,
  "поход": 10,
  "плавание": 7,
  "мероприятия": 5,
};

function normTopicKey_(s) {
  let k = String(s || "")
    .trim()
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/\s+/g, " ");

  // алиасы → базовые ключи
  if (k === "велозаезд") k = "вело";
  if (k === "велозаїзд") k = "вело";

  return k;
}

function getPointsForTopic_(topicKey) {
  const k = normTopicKey_(topicKey);
  console.log("TOPIC RAW:", topicKey, "=> NORM:", k, "=> PTS:", POINTS_BY_TOPIC[k] || 0);    // После проверки можешь удалить console.log, чтобы не шумело.
  return Number(POINTS_BY_TOPIC[k] || 0);
}

// ====== ACTIVITY (last 30 days) ======
async function getUserActivity30d_(userId) {
  const days = 30;
  const start = new Date();
  start.setDate(start.getDate() - days);

  // Считаем "активности" как кол-во записей в POINTS за 30 дней
  // (checkin/penalty/bonus — всё, что пишется в ledger)
  const snap = await POINTS
    .where("userId", "==", Number(userId))
    .where("ts", ">=", admin.firestore.Timestamp.fromDate(start))
    .get();

  const n = snap.size || 0;

  // Пороговые уровни (можно потом легко поменять)
  let level = "LOW";
  if (n >= 5) level = "HIGH";
  else if (n >= 2) level = "MED";

  return { n, level, days };
}

function activityLabel_(lang, level) {
  if (lang === "DE") {
    if (level === "HIGH") return "Hoch";
    if (level === "MED")  return "Mittel";
    return "Niedrig";
  }

  if (lang === "UA") {
    if (level === "HIGH") return "Висока";
    if (level === "MED")  return "Середня";
    return "Низька";
  }

  // RU
  if (level === "HIGH") return "Высокая";
  if (level === "MED")  return "Средняя";
  return "Низкая";
}
// ====== TOP-5 COMPUTATION (points ledger) ======
/**
 * points doc example:
 * points/{auto} = {
 *   userId: 123,
 *   name: "Vitalii",
 *   points: 30,
 *   ts: Timestamp,
 *   source: "checkin" | "bonus" | "penalty",
 *   meta: { ... }
 * }
 */

async function computeTopMonth_(year, month1to12, limit = 5) {
  const start = new Date(year, month1to12 - 1, 1, 0, 0, 0);
  const end = new Date(year, month1to12, 1, 0, 0, 0);

  const snap = await POINTS
    .where("ts", ">=", admin.firestore.Timestamp.fromDate(start))
    .where("ts", "<", admin.firestore.Timestamp.fromDate(end))
    .get();

  const map = new Map(); // userId -> {points, name}
  snap.forEach(d => {
    const p = d.data() || {};
    const uid = String(p.userId || "");
    if (!uid) return;
    const cur = map.get(uid) || { points: 0, name: p.name || "Без имени" };
    cur.points += Number(p.points || 0);
    if (p.name) cur.name = p.name;
    map.set(uid, cur);
  });

  const arr = Array.from(map.entries()).map(([userId, v]) => ({ userId, name: v.name, points: v.points }));
  arr.sort((a,b)=>b.points - a.points);

  return arr.slice(0, limit);
}

async function computeTopYear_(year, limit = 5) {
  const start = new Date(year, 0, 1, 0, 0, 0);
  const end = new Date(year + 1, 0, 1, 0, 0, 0);

  const snap = await POINTS
    .where("ts", ">=", admin.firestore.Timestamp.fromDate(start))
    .where("ts", "<", admin.firestore.Timestamp.fromDate(end))
    .get();

  const map = new Map();
  snap.forEach(d => {
    const p = d.data() || {};
    const uid = String(p.userId || "");
    if (!uid) return;
    const cur = map.get(uid) || { points: 0, name: p.name || "Без имени" };
    cur.points += Number(p.points || 0);
    if (p.name) cur.name = p.name;
    map.set(uid, cur);
  });

  const arr = Array.from(map.entries()).map(([userId, v]) => ({ userId, name: v.name, points: v.points }));
  arr.sort((a,b)=>b.points - a.points);

  return arr.slice(0, limit);
}

async function computeUserPlaceYear_(userId, year) {
  const top = await computeTopYear_(year, 200); // enough
  const idx = top.findIndex(x => String(x.userId) === String(userId));
  if (idx < 0) return { place: null, points: 0 };
  return { place: idx + 1, points: top[idx].points };
}

// ====== TOP-5 POSTS (monthly + yearly) ======
function monthNameRu_(m) {
  const a = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"];
  return a[m-1] || String(m);
}
function monthNameDe_(m) {
  const a = ["Januar","Februar","März","April","Mai","Juni","Juli","August","September","Oktober","November","Dezember"];
  return a[m-1] || String(m);
}

async function getTop5Topic_() {
  const t = await getTopic_("top5");
  return t ? { chatId: Number(t.chatId), threadId: Number(t.threadId) } : null;
}

async function postMonthlyTop5IfNeeded_(force = false) {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;

  // last day of month?
  const tomorrow = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
  const isLastDay = tomorrow.getMonth() !== now.getMonth();
  if (!isLastDay && !force) return;

  const cfgSnap = await CFG.get();
  const lastMonthKey = cfgSnap.data()?.lastMonthlyTop5 || "";
  const key = `${year}-${String(month).padStart(2,"0")}`;
 if (lastMonthKey === key && !force) return;

  const topic = await getTop5Topic_();
  if (!topic) return;

  const topRaw = await computeTopMonth_(year, month, 5);
  const top = await applyNameOverridesTop_(topRaw); // ✅ полные имена из users

  let text = "";
  const monthDe = `${monthNameDe_(month)} ${year}`;

  if (!top.length) {
   text = [
    `<b>🏆 TOP-5</b>`,
    `<b>${escHtml_(monthDe)}</b>`,
    ``,
    `🇺🇦 В этом месяце не было событий `,
    `но это не пауза, это разгон.`,
    ``,
    `Следующий месяц наш.💪 `,
    ``,
    `🇩🇪 In diesem Monat gab es keine Events `,
    `aber das ist keine Pause, sondern Anlauf.`,
    ``,
    `Der nächste Monat gehört uns.💪 `,
    ``,
    `IMPULSE TEAM 🔥`
  ].join("\n");
  } else {
    const lines = top.map((x, i) => `${i + 1}. ${escHtml_(x.name)} - ${x.points}`).join("\n");
    text = [
      `<b>🏆 TOP-5</b>`,
      `<b>${escHtml_(monthDe)}</b>`,
      ``,
      lines,
      ``,
      `IMPULSE TEAM 🔥`
    ].join("\n");
  }
  const cfg = cfgSnap.data() || {};
  const monthPhotoId = cfg.monthlyTop5PhotoFileId || null;
  const emptyPhotoId = cfg.monthlyTop5EmptyPhotoFileId || null;
  const photoIdToSend = (!top.length ? (emptyPhotoId || monthPhotoId) : monthPhotoId);
  if (photoIdToSend) {
    await bot.sendPhoto(topic.chatId, photoIdToSend, {
      message_thread_id: topic.threadId,
      caption: text,
      parse_mode: "HTML",
    });
  } else {
    await bot.sendMessage(topic.chatId, text, {
      message_thread_id: topic.threadId,
      parse_mode: "HTML",
    });
  }
  await CFG.set({ lastMonthlyTop5: key }, { merge: true });
}

async function postYearWinnerIfNeeded_(force = false) {
  const now = new Date();
  const year = now.getFullYear();

  // Dec 31 only
  if (!(now.getMonth() === 11 && now.getDate() === 31) && !force) return;

  const cfgSnap = await CFG.get();
  const lastYearKey = cfgSnap.data()?.lastYearWinner || "";
  if (String(lastYearKey) === String(year) && !force) return;

  const topic = await getTop5Topic_();
  if (!topic) return;

  const topRaw = await computeTopYear_(year, 1);
  const top = await applyNameOverridesTop_(topRaw); // ✅ полные имена
  if (!top.length) return;

  const winner = top[0];

  const text = [
  `<b>🏆 CHAMPION ${year}</b>`,
  ``,
  `Лучший участник года`,
  `Teilnehmer*in des Jahres`,
  ``,
  `<b>${winner.name} 🥇</b>`,
  `Баллы за год: ${winner.points}`,
  `Jahrespunkte: ${winner.points}`,
  ``,
  `🇺🇦 🎉 Поздравляем!`,
  `Ты становишься главным чемпионом года и получаешь приз!`,
  ``,
  `🇩🇪 🎉 Glückwunsch!`,
  `Du bist unser Champion des Jahres und bekommst einen Preis!`,
  ``,
  `🔥 IMPULSE TEAM`
].join("\n");

  const cfg2 = await CFG.get();
const photoId = cfg2.data()?.yearWinnerPhotoFileId;

if (photoId) {
  await bot.sendPhoto(topic.chatId, photoId, {
    message_thread_id: topic.threadId,
    caption: text,
    parse_mode: "HTML",
  });
} else {
  await bot.sendMessage(topic.chatId, text, { message_thread_id: topic.threadId, parse_mode: "HTML" });
}
  await CFG.set({ lastYearWinner: String(year) }, { merge: true });
}

async function autoLockRsvpCardsIfNeeded_() {
  const snap = await RSVP.where("active", "==", true).where("uiLocked", "==", false).get();
  const docs = snap.docs || [];
  if (!docs.length) return;

  for (const d of docs) {
    const pollId = d.id;
    const poll = d.data() || {};
    const dt = String(poll.dt || "");

    if (!isRsvpLocked_(dt)) continue;

    const groupChatId = Number(poll.groupChatId);
    const cardMessageId = Number(poll.cardMessageId);
    if (!groupChatId || !cardMessageId) {
      await d.ref.set({ uiLocked: true }, { merge: true });
      continue;
    }

    const text = formatRsvpCardText({
      qRu: poll.qRu,
      qDe: poll.qDe,
      dt: poll.dt,
      topicKey: poll.topicKey,
      chatMembers: poll.chatMembers
    }, poll.yes, poll.no);

    const kb = rsvpKeyboardResultsOnly_(pollId);

    // без callback (cron), поэтому cbqId = null
    await editRsvpCardNoNew_(groupChatId, cardMessageId, text, kb, null, "RU");

    await d.ref.set({ uiLocked: true }, { merge: true });
  }
}

// ====== CRON SCHEDULES (Berlin) ======
// ===== TEST CRON (TEMP) =====
bot.onText(/^\/test_monthly$/i, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from?.id;
  if (!isAdmin_(userId)) return bot.sendMessage(chatId, "⛔️ Нет доступа.");
  await bot.sendMessage(chatId, "OK: запускаю postMonthlyTop5IfNeeded_()");
  await postMonthlyTop5IfNeeded_(true);
});

bot.onText(/^\/test_year$/i, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from?.id;
  if (!isAdmin_(userId)) return bot.sendMessage(chatId, "⛔️ Нет доступа.");
  await bot.sendMessage(chatId, "OK: запускаю postYearWinnerIfNeeded_()");
  await postYearWinnerIfNeeded_(true);
});
// ===== /TEST CRON (TEMP) =====

// ===== SET TOP-5 PHOTOS (file_id) =====
bot.on("message", async (msg) => {
  try {
    if (!msg.photo || !msg.photo.length) return;

    const cap = String(msg.caption || "").trim();
   if (cap !== "/set_month_photo" && cap !== "/set_month_empty_photo" && cap !== "/set_year_photo") return;

    const chatId = msg.chat.id;
    const userId = msg.from?.id;
    if (!isAdmin_(userId)) {
      await bot.sendMessage(chatId, "⛔️ Нет доступа.");
      return;
    }

    const p = msg.photo[msg.photo.length - 1];
    const fileId = p?.file_id;
    if (!fileId) {
      await bot.sendMessage(chatId, "Не вижу file_id у фото. Пришли фото ещё раз.");
      return;
    }

    if (cap === "/set_month_photo") {
      await CFG.set({ monthlyTop5PhotoFileId: fileId }, { merge: true });
      await bot.sendMessage(chatId, "✅ Сохранено monthlyTop5PhotoFileId");
      return;
    }

   if (cap === "/set_month_empty_photo") {
  await CFG.set({ monthlyTop5EmptyPhotoFileId: fileId }, { merge: true });
  await bot.sendMessage(chatId, "✅ Сохранено monthlyTop5EmptyPhotoFileId");
  return;
}

    if (cap === "/set_year_photo") {
      await CFG.set({ yearWinnerPhotoFileId: fileId }, { merge: true });
      await bot.sendMessage(chatId, "✅ Сохранено yearWinnerPhotoFileId");
      return;
    }
  } catch (e) {
    console.error(e);
  }
});
// ===== /SET TOP-5 PHOTOS =====


// Каждый день 21:00 — проверяем, последний ли день месяца → постим TOP-5
cron.schedule("0 21 * * *", async () => {
  try { await postMonthlyTop5IfNeeded_(); } catch (e) { console.error(e); }
}, { timezone: TZ });

// Каждый день 20:00 — 31 декабря → постим победителя года
cron.schedule("0 20 * * *", async () => {
  try { await postYearWinnerIfNeeded_(); } catch (e) { console.error(e); }
}, { timezone: TZ });

// Каждый день 00:10 — закрываем RSVP карточки на "только Результаты" (если наступил день события)
cron.schedule("10 0 * * *", async () => {
  try { await autoLockRsvpCardsIfNeeded_(); } catch (e) { console.error(e); }
}, { timezone: TZ });