const idMtNotesModal = "mt_notes_modal";
import "dotenv/config";
import fs from "fs";
import path from "path";
import mysql from "mysql2/promise";
import { DateTime } from "luxon";
import {
  Client,
  GatewayIntentBits,
  Partials,
  Events,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  MessageFlags,
  EmbedBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} from "discord.js";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

/* =========================
   LOAD FILES (ONLY THESE)
========================= */
function loadJson(relPath) {
  return JSON.parse(fs.readFileSync(path.resolve(relPath), "utf8"));
}

const config = loadJson("config/core_bot_config.json");
const uiJson = loadJson("config/core_bot_ui_components_structured.json");
loadJson("config/core_bot_functional_flow_machine.json"); // authority only

console.log("Config loaded:", config?.meta?.name ?? "unknown");
console.log("UI components loaded");
console.log("Functional flow loaded");

const CORE_ENV = process.env.CORE_ENV; // testing | production
if (!["testing", "production"].includes(CORE_ENV)) {
  throw new Error('CORE_ENV must be "testing" or "production"');
}

/* =========================
   CONSTANTS
========================= */
const ZWSP = "\u200b";
const UPLOAD_TIMEOUT_MS = 2 * 60 * 1000;

/* =========================
   UI HELPERS
========================= */
function getUi(ref) {
  const item = uiJson?.components?.[ref];
  if (!item) throw new Error(`Missing UI ref in ui file: ${ref}`);
  return item;
}
function getRender(ref) {
  return getUi(ref).render ?? {};
}
function resolveChannelId(ref) {
  const r = getRender(ref);
  const direct = CORE_ENV === "production" ? r.production_channel_id : r.testing_channel_id;
  if (direct) return direct;

  // Fallback: some discord_channel UI components only include IDs in description text
  try {
    const item = getUi(ref);
    const desc = String(item?.description ?? "");
    const prod = desc.match(/Production Channel ID:\s*(\d{6,})/i)?.[1] ?? null;
    const test = desc.match(/Testing Channel ID:\s*(\d{6,})/i)?.[1] ?? null;
    return CORE_ENV === "production" ? prod : test;
  } catch (_) {
    return null;
  }
}
function resolveRoleId(ref) {
  const r = getRender(ref);
  return CORE_ENV === "production" ? r.production_role_id : r.testing_role_id;
}

function checkInRoleKeyToEventChannelKey(checkInRoleKey) {
  const m = String(checkInRoleKey ?? "").match(/^EVENT_(\d+)_CHECKED_IN$/i);
  if (!m) return null;
  return `EVENT_CHANNEL_${m[1]}`;
}

function resolveEventChannelIdFromKey(channelKey) {
  const envCfg = getEnvConfig();
  const uiRef = envCfg?.event_channels?.[channelKey] ?? null;
  if (!uiRef) return null;
  return resolveChannelId(uiRef);
}


function resolveChannelKeyFromResolvedChannelId(resolvedChannelId) {
  const envCfg = getEnvConfig();
  const mapping = envCfg?.event_channels ?? {};
  for (const [key, uiRef] of Object.entries(mapping)) {
    const id = resolveChannelId(uiRef);
    if (id && String(id) === String(resolvedChannelId)) return key;
  }
  return null;
}


function makeUiButton(ref, { disabled = false } = {}) {
  const r = getRender(ref);
  if (!r.custom_id) throw new Error(`Missing custom_id for button: ${ref}`);

  const styleStr = (r.style ?? "primary").toLowerCase();
  const style =
    styleStr === "success"
      ? ButtonStyle.Success
      : styleStr === "danger"
      ? ButtonStyle.Danger
      : styleStr === "secondary"
      ? ButtonStyle.Secondary
      : ButtonStyle.Primary;

  return new ButtonBuilder()
    .setCustomId(r.custom_id)
    .setLabel(r.label ?? "Button")
    .setStyle(style)
    .setDisabled(disabled);
}

/* =========================
   GIF TIMING
========================= */
function gifDurationSeconds(ref) {
  const v = config?.gif_durations_seconds?.[ref];
  if (typeof v !== "number") throw new Error(`Missing gif duration in config.gif_durations_seconds for: ${ref}`);
  return v;
}

function sleepForUploadedEventGif(uiRefOrUnknown, loops = 1) {
  const ms = Math.round(gifDurationSeconds(uiRefOrUnknown) * loops * 1000);
  // Uploaded per-event gifs don't have a duration entry; use a safe default.
  const safeMs = ms && ms >= 750 ? ms : 2000;
  return sleep(safeMs);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function defaultLoops(key, fallback) {
  const v = config?.timers?.transitional_gif_default_loops?.[key];
  return typeof v === "number" ? v : fallback;
}
const LOOPS = {
  initiation_boot: defaultLoops("initiation_intro", 3),
  terminal_boot: defaultLoops("terminal_boot", 3),
};

const LEADERBOARD_CACHE_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours
const LEADERBOARD_MAX_ROWS = 25;
const LEADERBOARD_TZ = "America/Los_Angeles"; // PT

function envNameFromGuildId(guildId) {
  const gid = String(guildId);
  const prodId = String(config?.discord?.guilds?.production?.guild_id ?? "");
  const testId = String(config?.discord?.guilds?.testing?.guild_id ?? "");
  if (prodId && gid === prodId) return "production";
  if (testId && gid === testId) return "testing";
  return "production";
}

function getCycle90ConfigForEnv(guildId) {
  const base = config?.cycles?.cycle90 ?? {};
  const tz = base.timezone || LEADERBOARD_TZ;
  const durationDaysBase = Number(base.duration_days || 90);

  const envName = envNameFromGuildId(guildId);
  const envCfg = (base.environments && base.environments[envName]) ? base.environments[envName] : {};
  const startLocal = envCfg.start_at_local || base.start_at_local || "2026-02-02T00:00:00";
  const durationDays = Number(envCfg.duration_days_override || durationDaysBase);

  // Optional reset_time_local exists in config; we keep it for future if needed.
  const resetTimeLocal = base.reset_time_local || "23:59:00";

  return { tz, startLocal, durationDays, resetTimeLocal, envName };
}


const leaderboardCache = new Map(); // guildId -> { refreshedAtMs, allTimeRows, cycleRows, cycleWindow }

function invalidateLeaderboardCache(guildId) {
  try {
    leaderboardCache.delete(String(guildId));
  } catch (_) {}
}


function getCycle90WindowPt(now = null, guildId = null) {
  const gid = guildId ? String(guildId) : envGuildId();
  const cfg = getCycle90ConfigForEnv(gid);
  const zone = cfg.tz;

  const n = (now ? now : DateTime.now().setZone(zone)).setZone(zone);

  // Anchor start is environment-specific and in the configured timezone.
  const anchor = DateTime.fromISO(cfg.startLocal, { zone });

  const days = Math.floor(n.startOf("day").diff(anchor.startOf("day"), "days").days);
  const k = days >= 0 ? Math.floor(days / cfg.durationDays) : -Math.ceil(Math.abs(days) / cfg.durationDays);

  const start = anchor.plus({ days: cfg.durationDays * k });
  const end = start.plus({ days: cfg.durationDays });

  return { start, end, tz: zone, durationDays: cfg.durationDays, envName: cfg.envName };
}

function getPreviousCycle90WindowPt(now = null, guildId = null) {
  const gid = guildId ? String(guildId) : envGuildId();
  const cur = getCycle90WindowPt(now, gid);
  const end = cur.start;
  const start = end.minus({ days: cur.durationDays });
  return { start, end, tz: cur.tz, durationDays: cur.durationDays, envName: cur.envName };
}

async function fetchCycleLeaderboardRows(guildId, startUtcSql, endUtcSql) {
  const [rows] = await pool.query(
    `
    SELECT user_id, SUM(delta) AS merits
    FROM (
      SELECT ec.user_id AS user_id, e.merits_awarded AS delta
      FROM event_checkins ec
      JOIN events e ON e.id = ec.event_id AND e.guild_id = ec.guild_id
      WHERE ec.guild_id = ? AND ec.checked_in_at >= ? AND ec.checked_in_at < ?

      UNION ALL
      SELECT target_user_id AS user_id, amount AS delta
      FROM merit_transfer_requests
      WHERE guild_id = ? AND status = 'approved' AND decided_at IS NOT NULL AND decided_at >= ? AND decided_at < ?

      UNION ALL
      SELECT target_user_id AS user_id,
             CASE WHEN mode = 'add' THEN amount ELSE -amount END AS delta
      FROM merit_adjustment_requests
      WHERE guild_id = ? AND decided_at IS NOT NULL AND decided_at >= ? AND decided_at < ?
    ) t
    GROUP BY user_id
    ORDER BY merits DESC
    `,
    [guildId, startUtcSql, endUtcSql, guildId, startUtcSql, endUtcSql, guildId, startUtcSql, endUtcSql]
  );
  return Array.isArray(rows) ? rows : [];
}

async function runLeaderboardTop3AnnouncementTick() {
  const guildId = envGuildId();
    const cfg = getCycle90ConfigForEnv(guildId);
  const nowPt = DateTime.now().setZone(cfg.tz);

  // We announce the PREVIOUS cycle within 24 hours AFTER it ends.
  const cur = getCycle90WindowPt(nowPt, guildId);
  const prev = getPreviousCycle90WindowPt(nowPt, guildId);

  // Only announce during the first 24h of the new cycle
  if (nowPt < cur.start) return;
  if (nowPt >= cur.start.plus({ hours: 24 })) return;

  const cycleStartUtc = prev.start.toUTC().toSQL({ includeOffset: false });
  const cycleEndUtc = prev.end.toUTC().toSQL({ includeOffset: false });

  // Idempotency: only once per cycle
  const [existing] = await pool.query(
    `SELECT id FROM leaderboard_cycle_announcements
     WHERE guild_id = ? AND cycle_start = ? AND cycle_end = ?
     LIMIT 1`,
    [guildId, cycleStartUtc, cycleEndUtc]
  );
  if (existing && existing.length) return;

  const topRows = await fetchCycleLeaderboardRows(guildId, cycleStartUtc, cycleEndUtc);
  const top3 = topRows.slice(0, 3);

  const lines = [];
  for (let i = 0; i < top3.length; i++) {
    const r = top3[i];
    lines.push(`${i + 1}. <@${String(r.user_id)}> — **${Number(r.merits ?? 0)}**`);
  }

  const header = "🏆 Top 3 — Current Cycle (Final)";
  const windowLabel = `${prev.start.toFormat("dd LLL yyyy")} – ${prev.end.minus({ days: 1 }).toFormat("dd LLL yyyy")} (PT)`;
  const content = [header, windowLabel, "", (lines.length ? lines.join("\n") : "_No data yet._")].join("\n");

  const guild = await fetchEnvGuild().catch(() => null);
  if (!guild) return;

  const channelId = resolveChannelId("[UI: Discord channel-leaderboard]");
  const channel = await guild.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) return;

  const msg = await channel.send({ content });

  await pool.query(
    `INSERT INTO leaderboard_cycle_announcements
     (guild_id, cycle_start, cycle_end, posted_at, channel_id, message_id)
     VALUES (?, ?, ?, NOW(), ?, ?)`,
    [guildId, cycleStartUtc, cycleEndUtc, String(channelId), String(msg.id)]
  );

  console.log("[LEADERBOARD_TOP3_ANNOUNCED]", {
    guild_id: guildId,
    cycle_start: cycleStartUtc,
    cycle_end: cycleEndUtc,
    message_id: String(msg.id),
  });
}

function startLeaderboardTop3AnnouncementScheduler() {
  // Run once on boot, then hourly
  runLeaderboardTop3AnnouncementTick().catch((e) => console.error("leaderboard top3 tick error:", e));
  setInterval(() => {
    runLeaderboardTop3AnnouncementTick().catch((e) => console.error("leaderboard top3 tick error:", e));
  }, 60 * 60 * 1000);
}



async function refreshLeaderboardCache(guildId, force = false) {
  const nowMs = Date.now();
  const existing = leaderboardCache.get(guildId);
  if (!force && existing && nowMs - existing.refreshedAtMs < LEADERBOARD_CACHE_TTL_MS) return existing;

  // All-time
  const [allRows] = await pool.query(
    `SELECT user_id, merits
     FROM user_merits
     WHERE guild_id = ?
     ORDER BY merits DESC`,
    [guildId]
  );

  // Current 90-day fixed cycle (PT)
    const cfg = getCycle90ConfigForEnv(guildId);
  const nowPt = DateTime.now().setZone(cfg.tz);
  const win = getCycle90WindowPt(nowPt, guildId);
  const startSql = win.start.toUTC().toSQL({ includeOffset: false });
  const endSql = win.end.toUTC().toSQL({ includeOffset: false });

  const [cycleRows] = await pool.query(
    `
    SELECT user_id, SUM(delta) AS merits
    FROM (
      SELECT ec.user_id AS user_id, e.merits_awarded AS delta
      FROM event_checkins ec
      JOIN events e ON e.id = ec.event_id AND e.guild_id = ec.guild_id
      WHERE ec.guild_id = ? AND ec.checked_in_at >= ? AND ec.checked_in_at < ?

      UNION ALL
      SELECT target_user_id AS user_id, amount AS delta
      FROM merit_transfer_requests
      WHERE guild_id = ? AND status = 'approved' AND decided_at IS NOT NULL AND decided_at >= ? AND decided_at < ?

      UNION ALL
      SELECT target_user_id AS user_id,
             CASE WHEN mode = 'add' THEN amount ELSE -amount END AS delta
      FROM merit_adjustment_requests
      WHERE guild_id = ? AND decided_at IS NOT NULL AND decided_at >= ? AND decided_at < ?
    ) t
    GROUP BY user_id
    ORDER BY merits DESC
    `,
    [guildId, startSql, endSql, guildId, startSql, endSql, guildId, startSql, endSql]
  );

  const packed = {
    refreshedAtMs: nowMs,
    allTimeRows: Array.isArray(allRows) ? allRows : [],
    cycleRows: Array.isArray(cycleRows) ? cycleRows : [],
    cycleWindow: win,
  };
  leaderboardCache.set(guildId, packed);
  return packed;


async function runLeaderboardCycleTop3AnnouncementCheck() {
  const guildId = envGuildId();
  const nowPt = DateTime.now().setZone(LEADERBOARD_TZ);
  const win = getCycle90WindowPt(nowPt);

  // Only announce within 24h AFTER cycle end
  if (nowPt < win.end) return;
  if (nowPt >= win.end.plus({ hours: 24 })) return;

  const cycleStartUtc = win.start.toUTC().toSQL({ includeOffset: false });
  const cycleEndUtc = win.end.toUTC().toSQL({ includeOffset: false });

  const [existing] = await pool.query(
    `SELECT id FROM leaderboard_cycle_announcements
     WHERE guild_id = ? AND cycle_start = ? AND cycle_end = ?
     LIMIT 1`,
    [guildId, cycleStartUtc, cycleEndUtc]
  );
  if (existing && existing.length) return;

  const cache = await refreshLeaderboardCache(guildId, true);
  const top = cache.cycleRows.slice(0, 3);

  const lines = [];
  for (let i = 0; i < top.length; i++) {
    const r = top[i];
    lines.push(`${i + 1}. <@${String(r.user_id)}> — **${Number(r.merits ?? 0)}**`);
  }
  const announceText = getRender("[UI: Leaderboard-top 3-announcement-text]").text ?? "Top 3 (Current Cycle)";
  const content = [announceText, "", lines.length ? lines.join("\n") : "_No data yet._"].join("\n");

  const guild = await fetchEnvGuild().catch(() => null);
  if (!guild) return;

  const channelId = resolveChannelId("[UI: Discord channel-leaderboard]");
  const channel = await guild.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) return;

  const msg = await channel.send({ content });

  await pool.query(
    `INSERT INTO leaderboard_cycle_announcements
     (guild_id, cycle_start, cycle_end, posted_at, channel_id, message_id)
     VALUES (?, ?, ?, NOW(), ?, ?)`,
    [guildId, cycleStartUtc, cycleEndUtc, String(channelId), String(msg.id)]
  );

  console.log("[LEADERBOARD_CYCLE_TOP3_ANNOUNCED]", {
    guild_id: guildId,
    cycle_start: cycleStartUtc,
    cycle_end: cycleEndUtc,
    message_id: String(msg.id),
  });
}

function startLeaderboardCycleAnnouncementScheduler() {
  // check hourly
  runLeaderboardCycleTop3AnnouncementCheck().catch((e) => console.error("leaderboard top3 check error:", e));
  setInterval(() => {
  }, 60 * 60 * 1000);
}

function startLeaderboardCacheRefresher() {
  // Refresh in background every 2 hours
  setInterval(() => {
    const gid = envGuildId();
    refreshLeaderboardCache(gid, true).catch((e) => console.error("leaderboard cache refresh error:", e));
  }, LEADERBOARD_CACHE_TTL_MS);
}


}

function leaderboardRankFromRows(rows, userId) {
  const uid = String(userId);
  for (let i = 0; i < rows.length; i++) {
    if (String(rows[i].user_id) === uid) return i + 1;
  }
  return null;
}

function leaderboardMeritsFromRows(rows, userId) {
  const uid = String(userId);
  const r = rows.find((x) => String(x.user_id) === uid);
  if (!r) return 0;
  const v = Number(r.merits ?? 0);
  return Number.isFinite(v) ? v : 0;
}

/* =========================
   TIMEZONE / SCHEDULE PARSING (PT)
========================= */
const PT_ZONE = config?.cycles?.cycle90?.timezone ?? "America/Los_Angeles";

// UPDATED: supports date-only "YYYY-MM-DD" and date-time "YYYY-MM-DD HH:mm"
// dateOnlyMode:
//   - "start" => 00:00 PT
//   - "end"   => 23:59 PT
function parsePtToUtcJsDate(input, { dateOnlyMode = "start" } = {}) {
  const raw = String(input ?? "").trim();
  if (!raw) return null;

  // Date-only: YYYY-MM-DD
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const time = dateOnlyMode === "end" ? "23:59" : "00:00";
    const dtDateOnly = DateTime.fromFormat(`${raw} ${time}`, "yyyy-MM-dd HH:mm", { zone: PT_ZONE });
    if (!dtDateOnly.isValid) return null;
    return dtDateOnly.toUTC().toJSDate();
  }

  // With time: YYYY-MM-DD HH:mm
  let dt = DateTime.fromFormat(raw, "yyyy-MM-dd HH:mm", { zone: PT_ZONE });

  // ISO fallback
  if (!dt.isValid) dt = DateTime.fromISO(raw, { zone: PT_ZONE });

  if (!dt.isValid) return null;
  return dt.toUTC().toJSDate();
}

function nowUtcJsDate() {
  return DateTime.utc().toJSDate();
}
function computeObjectiveStatus(beginUtc, endUtc) {
  const n = nowUtcJsDate().getTime();
  const b = beginUtc.getTime();
  const e = endUtc.getTime();
  if (n < b) return "scheduled";
  if (n >= b && n < e) return "active";
  return "expired";
}

function applyTemplate(text, vars = {}) {
  let out = String(text ?? "");
  for (const [k, v] of Object.entries(vars)) {
    out = out.split(k).join(String(v));
  }
  return out;
}

function dmUiRefForProofType(proofType) {
  if (proofType === "image") return UI.T_OBJ_SUBMIT_DM_IMAGE;
  if (proofType === "audio") return UI.T_OBJ_SUBMIT_DM_AUDIO;
  if (proofType === "text_file") return UI.T_OBJ_SUBMIT_DM_TEXT_FILE;
  return UI.T_OBJ_SUBMIT_DM_TEXT;
}

function proofAttachmentMatchesType(proofType, attachment) {
  if (!attachment) return false;
  const name = String(attachment.name ?? "").toLowerCase();
  const ct = String(attachment.contentType ?? "").toLowerCase();

  if (proofType === "image") {
    const okExt = name.endsWith(".png") || name.endsWith(".jpg") || name.endsWith(".jpeg") || name.endsWith(".gif") || name.endsWith(".tif") || name.endsWith(".tiff");
    const okMime = ct.startsWith("image/");
    return okExt || okMime;
  }

  if (proofType === "audio") {
    const okExt = name.endsWith(".mp3") || name.endsWith(".wav") || name.endsWith(".m4a") || name.endsWith(".aiff") || name.endsWith(".aif") || name.endsWith(".aac");
    const okMime = ct.startsWith("audio/");
    return okExt || okMime;
  }

  if (proofType === "text_file") {
    const okExt = name.endsWith(".txt") || name.endsWith(".docx") || name.endsWith(".csv") || name.endsWith(".pdf");
    const okMime =
      ct.startsWith("text/") ||
      ct.includes("application/pdf") ||
      ct.includes("application/vnd.openxmlformats-officedocument.wordprocessingml.document") ||
      ct.includes("application/octet-stream");
    return okExt || okMime;
  }

  return false;
}

function buildStoredProofKey({ guildId, objectiveId, userId, filename }) {
  const safe = String(filename ?? "proof").replace(/[^a-zA-Z0-9._-]/g, "_");
  return `core-bot/objective_submissions/guild_${guildId}/objective_${objectiveId}/user_${userId}/${Date.now()}_${safe}`;
}

async function uploadProofFromDiscordToS3(discordUrl, key, contentType) {
  const res = await fetch(discordUrl);
  if (!res.ok) {
    throw new Error(`Failed to download Discord attachment (${res.status})`);
  }
  const buf = Buffer.from(await res.arrayBuffer());

  const maxBytes = Number(process.env.OBJECTIVE_PROOF_MAX_BYTES || 0);
  if (maxBytes > 0 && buf.length > maxBytes) {
    throw new Error(`PROOF exceeds size limit (${buf.length} bytes)`);
  }

  await S3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: buf,
      ContentType: contentType || "application/octet-stream",
      ACL: "public-read",
      CacheControl: "public, max-age=31536000",
      ContentDisposition: "inline",
    })
  );

  return `${S3_PUBLIC_BASE_URL}/${key}`;
}


/* =========================
   S3 / SPACES HELPERS
========================= */
function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const S3 = new S3Client({
  region: requireEnv("S3_REGION"),
  endpoint: requireEnv("S3_ENDPOINT"),
  credentials: {
    accessKeyId: requireEnv("S3_ACCESS_KEY_ID"),
    secretAccessKey: requireEnv("S3_SECRET_ACCESS_KEY"),
  },
});

const S3_BUCKET = requireEnv("S3_BUCKET");
const S3_PUBLIC_BASE_URL = requireEnv("S3_PUBLIC_BASE_URL").replace(/\/$/, "");

function buildStoredGifKey({ guildId, userId, slot }) {
  return `core-bot/objectives/guild_${guildId}/user_${userId}/${slot}/${Date.now()}.gif`;
}


function buildAnnouncementAudioS3Key({ guildId, operatorUserId, filename }) {
  const safeName = String(filename ?? "announcement").replace(/[^a-zA-Z0-9._-]+/g, "_");
  const ts = Date.now();
  return `core-bot/announcements/guild_${guildId}/operator_${operatorUserId}/${ts}_${safeName}`;
}

async function fetchUrlToBuffer(url) {
  const res = await fetch(String(url));
  if (!res.ok) throw new Error(`fetch failed ${res.status} for ${url}`);
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

async function uploadGifFromDiscordToS3(discordUrl, key) {
  const res = await fetch(discordUrl);
  if (!res.ok) {
    throw new Error(`Failed to download Discord attachment (${res.status})`);
  }
  const buf = Buffer.from(await res.arrayBuffer());

  const maxBytes = Number(process.env.OBJECTIVE_PROOF_MAX_BYTES || 0);
  if (maxBytes > 0 && buf.length > maxBytes) {
    throw new Error(`GIF exceeds size limit (${buf.length} bytes)`);
  }

  await S3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: buf,
      ContentType: "image/gif",
      ACL: "public-read", // IMPORTANT so Discord can embed it
      CacheControl: "public, max-age=31536000",
      ContentDisposition: "inline",
    })
  );

  return `${S3_PUBLIC_BASE_URL}/${key}`;
}

/* =========================
   MYSQL (AUTHORITATIVE)
========================= */
function envNumber(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

const MYSQL_HOST = requireEnv("MYSQL_HOST");
const MYSQL_PORT = envNumber("MYSQL_PORT", 3306);
const MYSQL_DATABASE = requireEnv("MYSQL_DATABASE");
const MYSQL_USER = requireEnv("MYSQL_USER");
const MYSQL_PASSWORD = requireEnv("MYSQL_PASSWORD");

// OPTION A: DigitalOcean Managed MySQL CA-based TLS
function buildMysqlSslOptionA() {
  const enabled = String(process.env.MYSQL_SSL || "").trim().toLowerCase();
  const on = ["true", "1", "yes"].includes(enabled);
  if (!on) return undefined;

  const caPathRaw = requireEnv("MYSQL_SSL_CA");
  const caPath = path.resolve(caPathRaw);
  const ca = fs.readFileSync(caPath, "utf8");

  return {
    ca,
    rejectUnauthorized: true,
  };
}

const pool = mysql.createPool({
  host: MYSQL_HOST,
  port: MYSQL_PORT,
  database: MYSQL_DATABASE,
  user: MYSQL_USER,
  password: MYSQL_PASSWORD,
  waitForConnections: true,
  connectionLimit: envNumber("MYSQL_POOL_MAX", 10),
  queueLimit: 0,
  ssl: buildMysqlSslOptionA(),
});

async function initDb() {
  // objectives
  await pool.query(`
    CREATE TABLE IF NOT EXISTS objectives (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      guild_id VARCHAR(32) NOT NULL,
      created_by_user_id VARCHAR(32) NOT NULL,
      name VARCHAR(128) NOT NULL,
      merits_awarded INT NOT NULL,
      proof_type VARCHAR(32) NOT NULL,
      schedule_begin_at DATETIME NOT NULL,
      schedule_end_at DATETIME NOT NULL,
      status VARCHAR(16) NOT NULL DEFAULT 'scheduled',
      created_at DATETIME NOT NULL,
      updated_at DATETIME NOT NULL,
      PRIMARY KEY (id),
      INDEX idx_objectives_guild_status (guild_id, status),
      INDEX idx_objectives_guild_begin (guild_id, schedule_begin_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  // objective gifs (1:1)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS objective_gifs (
      objective_id BIGINT UNSIGNED NOT NULL,
      main_url TEXT NOT NULL,
      submit_url TEXT NOT NULL,
      back_url TEXT NOT NULL,
      PRIMARY KEY (objective_id),
      CONSTRAINT fk_objective_gifs_objective
        FOREIGN KEY (objective_id) REFERENCES objectives(id)
        ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);



// objective submissions (user proof intake)
await pool.query(`
  CREATE TABLE IF NOT EXISTS objective_submissions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    objective_id BIGINT UNSIGNED NOT NULL,
    submitted_by_user_id VARCHAR(32) NOT NULL,
    proof_type VARCHAR(32) NOT NULL,
    proof_url TEXT NULL,
    proof_text TEXT NULL,
    review_channel_id VARCHAR(32) NULL,
    review_message_id VARCHAR(32) NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'pending_review',
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_obj_submissions_guild_obj (guild_id, objective_id),
    INDEX idx_obj_submissions_user (guild_id, submitted_by_user_id),
    CONSTRAINT fk_obj_submissions_objective
      FOREIGN KEY (objective_id) REFERENCES objectives(id)
      ON DELETE CASCADE
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

// Objective submissions review-post linkage (safe on existing DBs)
try {
  await pool.query(`ALTER TABLE objective_submissions ADD COLUMN review_channel_id VARCHAR(32) NULL;`);
} catch (e) {
  // ignore duplicate column
  // events
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      guild_id VARCHAR(32) NOT NULL,
      created_by_user_id VARCHAR(32) NOT NULL,
      name VARCHAR(128) NOT NULL,
      merits_awarded INT NOT NULL,
      check_in_role_key VARCHAR(32) NOT NULL,
      resolved_role_id VARCHAR(32) NOT NULL,
      schedule_begin_at DATETIME NOT NULL,
      schedule_end_at DATETIME NOT NULL,
      status VARCHAR(16) NOT NULL DEFAULT 'scheduled',
      created_at DATETIME NOT NULL,
      updated_at DATETIME NOT NULL,
      PRIMARY KEY (id),
      INDEX idx_events_guild_status (guild_id, status),
      INDEX idx_events_guild_begin (guild_id, schedule_begin_at),
      INDEX idx_events_guild_role (guild_id, check_in_role_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS event_gifs (
      event_id BIGINT UNSIGNED NOT NULL,
      main_url TEXT NOT NULL,
      check_in_url TEXT NOT NULL,
      back_url TEXT NOT NULL,
      PRIMARY KEY (event_id),
      CONSTRAINT fk_event_gifs_event
        FOREIGN KEY (event_id) REFERENCES events(id)
        ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

}
try {
  await pool.query(`ALTER TABLE objective_submissions ADD COLUMN review_message_id VARCHAR(32) NULL;`);
} catch (e) {
  // ignore duplicate column
}


// objective submissions review metadata (safe on existing DBs)
try { 
  await pool.query(`
    CREATE TABLE IF NOT EXISTS event_checkins (
      event_id BIGINT UNSIGNED NOT NULL,
      guild_id VARCHAR(32) NOT NULL,
      user_id VARCHAR(32) NOT NULL,
      checked_in_at DATETIME NOT NULL,
      PRIMARY KEY (event_id, user_id),
      INDEX idx_event_checkins_guild_user (guild_id, user_id),
      CONSTRAINT fk_event_checkins_event
        FOREIGN KEY (event_id) REFERENCES events(id)
        ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);

  // event_checkins schema extensions (safe on existing DBs)
  try {
    await pool.query(`ALTER TABLE event_checkins ADD COLUMN IF NOT EXISTS resolved_role_id VARCHAR(32) NULL;`);
  } catch (e) {}
  try {
    await pool.query(`ALTER TABLE event_checkins ADD COLUMN IF NOT EXISTS check_in_role_key VARCHAR(32) NULL;`);
  } catch (e) {}

  // event role lifecycle cleanup marker
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS event_role_cleanup (
        event_id BIGINT UNSIGNED NOT NULL,
        guild_id VARCHAR(32) NOT NULL,
        cleaned_at DATETIME NOT NULL,
        PRIMARY KEY (event_id, guild_id),
        CONSTRAINT fk_event_role_cleanup_event
          FOREIGN KEY (event_id) REFERENCES events(id)
          ON DELETE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    `);
  } catch (e) {}


await pool.query(`ALTER TABLE objective_submissions ADD COLUMN reviewed_by_user_id VARCHAR(32) NULL;`); } catch (_) {}
try { await pool.query(`ALTER TABLE objective_submissions ADD COLUMN reviewed_at DATETIME NULL;`); } catch (_) {}
try { await pool.query(`ALTER TABLE objective_submissions ADD COLUMN undone_by_user_id VARCHAR(32) NULL;`); } catch (_) {}
try { await pool.query(`ALTER TABLE objective_submissions ADD COLUMN undone_at DATETIME NULL;`); } catch (_) {}

// user merits (simple authoritative ledger for now)
await pool.query(`
  CREATE TABLE IF NOT EXISTS user_merits (
    guild_id VARCHAR(32) NOT NULL,
    user_id VARCHAR(32) NOT NULL,
    merits INT NOT NULL DEFAULT 0,
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (guild_id, user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS roll_calls (
    id BIGINT NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    operator_user_id VARCHAR(32) NOT NULL,
    channel_key VARCHAR(64) NOT NULL,
    resolved_channel_id VARCHAR(32) NOT NULL,
    message_id VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL,
    closed_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_roll_calls_guild_channel (guild_id, resolved_channel_id),
    UNIQUE KEY uniq_roll_calls_message (guild_id, message_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS roll_call_awards (
    roll_call_id BIGINT NOT NULL,
    guild_id VARCHAR(32) NOT NULL,
    user_id VARCHAR(32) NOT NULL,
    awarded_at DATETIME NOT NULL,
    PRIMARY KEY (roll_call_id, user_id),
    KEY idx_roll_call_awards_guild_user (guild_id, user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS announcement_requests (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    operator_user_id VARCHAR(32) NOT NULL,
    audio_url TEXT NOT NULL,
    audio_name VARCHAR(255) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    review_channel_id VARCHAR(32) NULL,
    review_message_id VARCHAR(32) NULL,
    created_at DATETIME NOT NULL,
    decided_at DATETIME NULL,
    decided_by_user_id VARCHAR(32) NULL,
    PRIMARY KEY (id),
    KEY idx_ann_guild_status (guild_id, status),
    UNIQUE KEY uniq_ann_review_msg (guild_id, review_message_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS merit_transfer_requests (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    operator_user_id VARCHAR(32) NOT NULL,
    target_user_id VARCHAR(32) NOT NULL,
    amount INT NOT NULL,
    notes TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'pending',
    review_channel_id VARCHAR(32) NULL,
    review_message_id VARCHAR(32) NULL,
    created_at DATETIME NOT NULL,
    decided_at DATETIME NULL,
    decided_by_user_id VARCHAR(32) NULL,
    PRIMARY KEY (id),
    KEY idx_mt_guild_status (guild_id, status),
    UNIQUE KEY uniq_mt_review_msg (guild_id, review_message_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);


await pool.query(`
  CREATE TABLE IF NOT EXISTS merit_adjustment_requests (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    lt_user_id VARCHAR(32) NOT NULL,
    target_user_id VARCHAR(32) NOT NULL,
    mode ENUM('add','deduct') NOT NULL,
    amount INT NOT NULL,
    notes TEXT NULL,
    created_at DATETIME NOT NULL,
    decided_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_ma_guild_target (guild_id, target_user_id),
    KEY idx_ma_guild_lt (guild_id, lt_user_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS user_suspensions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    lt_user_id VARCHAR(32) NOT NULL,
    target_user_id VARCHAR(32) NOT NULL,
    duration_key VARCHAR(8) NOT NULL,
    suspended_role_id_snapshot VARCHAR(32) NOT NULL,
    starts_at DATETIME NOT NULL,
    ends_at DATETIME NOT NULL,
    lifted_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_us_guild_target (guild_id, target_user_id),
    KEY idx_us_ends_at (ends_at)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);



  

await pool.query(`
  CREATE TABLE IF NOT EXISTS leaderboard_cycle_announcements (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    guild_id VARCHAR(32) NOT NULL,
    cycle_start DATETIME NOT NULL,
    cycle_end DATETIME NOT NULL,
    posted_at DATETIME NOT NULL,
    channel_id VARCHAR(32) NOT NULL,
    message_id VARCHAR(32) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_lb_cycle (guild_id, cycle_start, cycle_end)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`);

console.log("✅ MySQL tables ready");
}

async function dbInsertObjectiveWithGifs({
  guildId,
  createdByUserId,
  name,
  meritsAwarded,
  proofType,
  beginUtc,
  endUtc,
  gifMainUrl,
  gifSubmitUrl,
  gifBackUrl,
}) {
  const createdAt = nowUtcJsDate();
  const updatedAt = createdAt;
  const status = computeObjectiveStatus(beginUtc, endUtc);

  const [res] = await pool.query(
    `
    INSERT INTO objectives
      (guild_id, created_by_user_id, name, merits_awarded, proof_type,
       schedule_begin_at, schedule_end_at, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `,
    [
      guildId,
      createdByUserId,
      name,
      meritsAwarded,
      proofType,
      beginUtc,
      endUtc,
      status,
      createdAt,
      updatedAt,
    ]
  );

  const objectiveId = res.insertId;

  await pool.query(
    `
    INSERT INTO objective_gifs (objective_id, main_url, submit_url, back_url)
    VALUES (?, ?, ?, ?)
  `,
    [objectiveId, gifMainUrl, gifSubmitUrl, gifBackUrl]
  );

  return objectiveId;
}

async function dbInsertEventWithGifs({
  guildId,
  createdByUserId,
  name,
  meritsAwarded,
  checkInRoleKey,
  resolvedRoleId,
  scheduleBeginRaw,
  scheduleEndRaw,
  gifMainUrl,
  gifCheckInUrl,
  gifBackUrl,
}) {
  const beginJs = parsePtToUtcJsDate(scheduleBeginRaw, { dateOnlyMode: "start" });
  const endJs = parsePtToUtcJsDate(scheduleEndRaw, { dateOnlyMode: "end" });
  if (!beginJs || !endJs) throw new Error("Invalid schedule");
  if (endJs.getTime() < beginJs.getTime()) throw new Error("End before begin");

  const now = new Date();
  const [res] = await pool.query(
    `
    INSERT INTO events
      (guild_id, created_by_user_id, name, merits_awarded, check_in_role_key, resolved_role_id, schedule_begin_at, schedule_end_at, status, created_at, updated_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?)
    `,
    [
      String(guildId),
      String(createdByUserId),
      String(name),
      Number(meritsAwarded),
      String(checkInRoleKey),
      String(resolvedRoleId),
      beginJs,
      endJs,
      now,
      now,
    ]
  );

  const eventId = Number(res.insertId);

  await pool.query(
    `
    INSERT INTO event_gifs (event_id, main_url, check_in_url, back_url)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      main_url = VALUES(main_url),
      check_in_url = VALUES(check_in_url),
      back_url = VALUES(back_url)
    `,
    [eventId, String(gifMainUrl), String(gifCheckInUrl), String(gifBackUrl)]
  );

  return eventId;
}

async function dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey }) {
  const [rows] = await pool.query(
    `
    SELECT id
    FROM events
    WHERE guild_id = ? AND check_in_role_key = ? AND status IN ('scheduled','active')
      AND schedule_end_at > UTC_TIMESTAMP()
    LIMIT 1
    `,
    [String(guildId), String(checkInRoleKey)]
  );
  return !!rows?.[0]?.id;
}

async function dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey, excludeEventId }) {
  const [rows] = await pool.query(
    `
    SELECT id
    FROM events
    WHERE guild_id = ? AND check_in_role_key = ? AND status IN ('scheduled','active')
      AND schedule_end_at > UTC_TIMESTAMP()
      AND id <> ?
    LIMIT 1
    `,
    [String(guildId), String(checkInRoleKey), Number(excludeEventId)]
  );
  return !!rows?.[0]?.id;
}



async function dbListActiveObjectivesWithGifs(guildId) {
  // Keep it simple for now: "active" = now between begin/end
  const [rows] = await pool.query(
    `
    SELECT
      o.id,
      o.name,
      o.merits_awarded,
      o.proof_type,
      o.schedule_begin_at,
      o.schedule_end_at,
      o.status,
      g.main_url,
      g.submit_url,
      g.back_url
    FROM objectives o
    JOIN objective_gifs g ON g.objective_id = o.id
    WHERE o.guild_id = ?
      AND o.status <> 'deleted'
      AND UTC_TIMESTAMP() >= o.schedule_begin_at
      AND UTC_TIMESTAMP() < o.schedule_end_at
    ORDER BY o.schedule_begin_at ASC, o.id ASC
  `,
    [guildId]
  );

  return rows ?? [];
}

async function dbListActiveEventsWithGifs(guildId) {
  const [rows] = await pool.query(
    `
    SELECT
      e.id,
      e.name,
      e.merits_awarded,
      e.check_in_role_key,
      e.resolved_role_id,
      e.schedule_begin_at,
      e.schedule_end_at,
      e.status,
      g.main_url,
      g.check_in_url,
      g.back_url
    FROM events e
    JOIN event_gifs g ON g.event_id = e.id
    WHERE e.guild_id = ?
      AND e.status <> 'deleted'
      AND UTC_TIMESTAMP() >= e.schedule_begin_at
      AND UTC_TIMESTAMP() < e.schedule_end_at
    ORDER BY e.schedule_begin_at ASC, e.id ASC
  `,
    [guildId]
  );

  return rows ?? [];
}

async function dbTryInsertEventCheckin({ guildId, eventId, userId, resolvedRoleId = null, checkInRoleKey = null }) {
  const checkedInAt = nowUtcJsDate();

  // Prefer inserting snapshot columns when available; fall back for older schemas.
  try {
    const [res] = await pool.query(
      `
      INSERT IGNORE INTO event_checkins (event_id, guild_id, user_id, checked_in_at, resolved_role_id, check_in_role_key)
      VALUES (?, ?, ?, ?, ?, ?)
    `,
      [eventId, guildId, String(userId), checkedInAt, resolvedRoleId ? String(resolvedRoleId) : null, checkInRoleKey ? String(checkInRoleKey) : null]
    );
    return (res?.affectedRows ?? 0) > 0;
  } catch (e) {
    const [res] = await pool.query(
      `
      INSERT IGNORE INTO event_checkins (event_id, guild_id, user_id, checked_in_at)
      VALUES (?, ?, ?, ?)
    `,
      [eventId, guildId, String(userId), checkedInAt]
    );
    return (res?.affectedRows ?? 0) > 0;
  }
}

async function dbHasEventCheckin({ eventId, userId }) {
  const [rows] = await pool.query(
    `SELECT 1 AS ok FROM event_checkins WHERE event_id = ? AND user_id = ? LIMIT 1`,
    [eventId, String(userId)]
  );
  return !!rows?.length;
}

async function dbListEndedEventsNeedingRoleCleanup(guildId, limit = 5) {
  const [rows] = await pool.query(
    `
    SELECT e.*
    FROM events e
    WHERE e.guild_id = ?
      AND e.status <> 'deleted'
      AND e.resolved_role_id IS NOT NULL
      AND e.schedule_end_at <= ?
      AND NOT EXISTS (
        SELECT 1 FROM event_role_cleanup c WHERE c.event_id = e.id AND c.guild_id = e.guild_id
      )
    ORDER BY e.schedule_end_at ASC, e.id ASC
    LIMIT ?
  `,
    [guildId, nowUtcJsDate(), Number(limit)]
  );
  return rows ?? [];
}

async function dbListEventCheckinsForCleanup(guildId, eventId) {
  const [rows] = await pool.query(
    `
    SELECT user_id, resolved_role_id
    FROM event_checkins
    WHERE guild_id = ? AND event_id = ?
  `,
    [guildId, eventId]
  );
  return rows ?? [];
}

async function dbMarkEventRoleCleanupDone(guildId, eventId) {
  await pool.query(
    `
    INSERT IGNORE INTO event_role_cleanup (event_id, guild_id, cleaned_at)
    VALUES (?, ?, ?)
  `,
    [eventId, guildId, nowUtcJsDate()]
  );
}

async function runEventRoleLifecycleTick() {
  // Runs periodically: remove event roles after an event ends (based on check-ins) and mark cleanup done.
  // Must be idempotent and best-effort.
  for (const guild of client.guilds.cache.values()) {
    try {
      const guildId = String(guild.id);
      const ended = await dbListEndedEventsNeedingRoleCleanup(guildId, 3);
      if (!ended.length) continue;

      for (const ev of ended) {
        const eventId = ev.id;
        const eventResolvedRoleId = ev.resolved_role_id ? String(ev.resolved_role_id) : null;

        const checkins = await dbListEventCheckinsForCleanup(guildId, eventId);
        for (const row of checkins) {
          const userId = String(row.user_id);
          const snapRoleId = row.resolved_role_id ? String(row.resolved_role_id) : null;

          const roleIds = Array.from(new Set([snapRoleId, eventResolvedRoleId].filter(Boolean)));
          if (!roleIds.length) continue;

          try {
            const member = await guild.members.fetch(userId).catch(() => null);
            if (!member) continue;
            for (const rid of roleIds) {
              if (member.roles.cache.has(rid)) {
                await member.roles.remove(rid).catch(() => null);
              }
            }
            // small spacing to be gentle with rate limits
            await sleep(250);
          } catch (e) {
            // ignore per-user errors
          }
        }

        await dbMarkEventRoleCleanupDone(guildId, eventId);
      }
    } catch (e) {
      // ignore per-guild errors
    }
  }
}




async function dbHasPendingObjectiveSubmission({ guildId, objectiveId, userId }) {
  const [rows] = await pool.query(
    `
    SELECT id
    FROM objective_submissions
    WHERE guild_id = ? AND objective_id = ? AND submitted_by_user_id = ? AND status = 'pending_review'
    LIMIT 1
  `,
    [guildId, objectiveId, userId]
  );
  return !!(rows && rows.length);
}

async function dbDeletePendingObjectiveSubmission({ guildId, objectiveId, userId }) {
  await pool.query(
    `
    DELETE FROM objective_submissions
    WHERE guild_id = ? AND objective_id = ? AND submitted_by_user_id = ? AND status = 'pending_review'
  `,
    [guildId, objectiveId, userId]
  );
}


async function dbInsertObjectiveSubmission({ guildId, objectiveId, userId, proofType, proofUrl, proofText }) {
  const createdAt = nowUtcJsDate();
  const [res] = await pool.query(
    `
    INSERT INTO objective_submissions
      (guild_id, objective_id, submitted_by_user_id, proof_type, proof_url, proof_text, status, created_at)
    VALUES (?, ?, ?, ?, ?, ?, 'pending_review', ?)
  `,
    [guildId, objectiveId, userId, proofType, proofUrl ?? null, proofText ?? null, createdAt]
  );
  return res.insertId;
}

async function dbSetObjectiveSubmissionReviewPost({ guildId, submissionId, reviewChannelId, reviewMessageId }) {
  await pool.query(
    `
    UPDATE objective_submissions
    SET review_channel_id = ?, review_message_id = ?
    WHERE guild_id = ? AND id = ?
    LIMIT 1
  `,
    [reviewChannelId ?? null, reviewMessageId ?? null, guildId, submissionId]
  );
}

async function dbGetPendingObjectiveSubmissionMeta({ guildId, objectiveId, userId }) {
  const [rows] = await pool.query(
    `
    SELECT id, review_channel_id, review_message_id
    FROM objective_submissions
    WHERE guild_id = ? AND objective_id = ? AND submitted_by_user_id = ? AND status = 'pending_review'
    ORDER BY id DESC
    LIMIT 1
  `,
    [guildId, objectiveId, userId]
  );
  return rows?.[0] ?? null;
}



async function dbFindObjectiveSubmissionByReviewMessage({ guildId, reviewMessageId }) {
  const [rows] = await pool.query(
    `
    SELECT
      s.id,
      s.guild_id,
      s.objective_id,
      s.submitted_by_user_id,
      s.proof_type,
      s.proof_url,
      s.proof_text,
      s.review_channel_id,
      s.review_message_id,
      s.status,
      s.reviewed_by_user_id,
      s.reviewed_at,
      s.undone_by_user_id,
      s.undone_at,
      o.name AS objective_name,
      o.merits_awarded AS objective_merits_awarded
    FROM objective_submissions s
    JOIN objectives o ON o.id = s.objective_id
    WHERE s.guild_id = ? AND s.review_message_id = ?
    LIMIT 1
  `,
    [guildId, String(reviewMessageId)]
  );
  return rows?.[0] ?? null;
}

async function dbGetLatestObjectiveSubmissionStatus({ guildId, objectiveId, userId }) {
  const [rows] = await pool.query(
    `
    SELECT status
    FROM objective_submissions
    WHERE guild_id = ? AND objective_id = ? AND submitted_by_user_id = ?
    ORDER BY id DESC
    LIMIT 1
  `,
    [String(guildId), String(objectiveId), String(userId)]
  );
  return String(rows?.[0]?.status ?? "");
}


async function dbSetObjectiveSubmissionDecision({ guildId, submissionId, status, reviewerUserId }) {
  await pool.query(
    `
    UPDATE objective_submissions
    SET status = ?, reviewed_by_user_id = ?, reviewed_at = UTC_TIMESTAMP(),
        undone_by_user_id = NULL, undone_at = NULL
    WHERE guild_id = ? AND id = ?
    LIMIT 1
  `,
    [status, reviewerUserId ?? null, guildId, submissionId]
  );
}

async function dbUndoObjectiveSubmissionDecision({ guildId, submissionId, undoUserId }) {
  await pool.query(
    `
    UPDATE objective_submissions
    SET status = 'undone',
        reviewed_by_user_id = NULL, reviewed_at = NULL,
        undone_by_user_id = ?, undone_at = UTC_TIMESTAMP()
    WHERE guild_id = ? AND id = ?
    LIMIT 1
  `,
    [undoUserId ?? null, guildId, submissionId]
  );
}

async function dbAddUserMerits({ guildId, userId, delta }) {
  const d = Number(delta) || 0;
  const updatedAt = nowUtcJsDate();
  await pool.query(
    `
    INSERT INTO user_merits (guild_id, user_id, merits, updated_at)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      merits = GREATEST(0, merits + VALUES(merits)),
      updated_at = VALUES(updated_at)
  `,
    [guildId, String(userId), d, updatedAt]
  );

  const [rows] = await pool.query(
    `SELECT merits FROM user_merits WHERE guild_id = ? AND user_id = ? LIMIT 1`,
    [guildId, String(userId)]
  );
  return rows?.[0]?.merits ?? 0;
}

async function dbInsertRollCall({ guildId, operatorUserId, channelKey, resolvedChannelId, messageId }) {
  const createdAt = nowUtcJsDate();
  const [res] = await pool.query(
    `
    INSERT INTO roll_calls (guild_id, operator_user_id, channel_key, resolved_channel_id, message_id, created_at, closed_at)
    VALUES (?, ?, ?, ?, ?, ?, NULL)
  `,
    [guildId, operatorUserId, channelKey, String(resolvedChannelId), String(messageId), createdAt]
  );
  return Number(res.insertId);
}

async function dbGetOpenRollCallByMessage(guildId, messageId) {
  const [rows] = await pool.query(
    `
    SELECT *
    FROM roll_calls
    WHERE guild_id = ? AND message_id = ? AND closed_at IS NULL
    LIMIT 1
  `,
    [guildId, String(messageId)]
  );
  return rows?.[0] ?? null;
}

async function dbCloseExpiredRollCalls() {
  const now = nowUtcJsDate();
  await pool.query(
    `
    UPDATE roll_calls
    SET closed_at = ?
    WHERE closed_at IS NULL AND created_at <= (DATE_SUB(?, INTERVAL 1 HOUR))
  `,
    [now, now]
  );
}

async function dbHasRollCallAward(rollCallId, userId) {
  const [rows] = await pool.query(
    `
    SELECT 1
    FROM roll_call_awards
    WHERE roll_call_id = ? AND user_id = ?
    LIMIT 1
  `,
    [Number(rollCallId), String(userId)]
  );
  return !!rows?.length;
}

async function dbInsertRollCallAward(rollCallId, guildId, userId) {
  const awardedAt = nowUtcJsDate();
  await pool.query(
    `
    INSERT IGNORE INTO roll_call_awards (roll_call_id, guild_id, user_id, awarded_at)
    VALUES (?, ?, ?, ?)
  `,
    [Number(rollCallId), String(guildId), String(userId), awardedAt]
  );
}

async function dbGetActiveEventByCheckInRoleKey(guildId, checkInRoleKey) {
  const now = nowUtcJsDate();
  const [rows] = await pool.query(
    `
    SELECT *
    FROM events
    WHERE guild_id = ?
      AND status <> 'deleted'
      AND check_in_role_key = ?
      AND schedule_begin_at <= ?
      AND schedule_end_at >= ?
    ORDER BY schedule_begin_at DESC
    LIMIT 1
  `,
    [String(guildId), String(checkInRoleKey), now, now]
  );
  return rows?.[0] ?? null;
}


async function dbInsertMeritTransferRequest({
  guildId,
  operatorUserId,
  targetUserId,
  amount,
  notes,
  reviewChannelId,
}) {
  const createdAt = nowUtcJsDate();
  const status = "pending";
  const [res] = await pool.query(
    `
    INSERT INTO merit_transfer_requests
      (guild_id, operator_user_id, target_user_id, amount, notes, status, review_channel_id, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `,
    [String(guildId), String(operatorUserId), String(targetUserId), Number(amount), String(notes), status, String(reviewChannelId), createdAt]
  );
  return res.insertId;
}

async function dbSetMeritTransferReviewMessage({ guildId, id, reviewMessageId }) {
  await pool.query(
    `
    UPDATE merit_transfer_requests
    SET review_message_id = ?
    WHERE guild_id = ? AND id = ?
    LIMIT 1
  `,
    [String(reviewMessageId), String(guildId), Number(id)]
  );
}

async function dbGetMeritTransferByReviewMessage(guildId, reviewMessageId) {
  const [rows] = await pool.query(
    `
    SELECT *
    FROM merit_transfer_requests
    WHERE guild_id = ? AND review_message_id = ?
    LIMIT 1
  `,
    [String(guildId), String(reviewMessageId)]
  );
  return rows?.[0] ?? null;
}

async function dbUpdateMeritTransferDecision({ guildId, id, status, decidedByUserId }) {
  await pool.query(
    `
    UPDATE merit_transfer_requests
    SET status = ?,
        decided_at = ?,
        decided_by_user_id = ?
    WHERE guild_id = ? AND id = ? AND status = 'pending'
    LIMIT 1
  `,
    [String(status), nowUtcJsDate(), String(decidedByUserId), String(guildId), Number(id)]
  );
}
async function dbInsertAnnouncementRequest({ guildId, operatorUserId, audioUrl, audioName, reviewChannelId, reviewMessageId }) {
  const createdAt = nowUtcJsDate();
  const [res] = await pool.query(
    `
    INSERT INTO announcement_requests
      (guild_id, operator_user_id, audio_url, audio_name, status, review_channel_id, review_message_id, created_at, decided_at, decided_by_user_id)
    VALUES
      (?, ?, ?, ?, 'pending', ?, ?, ?, NULL, NULL)
  `,
    [guildId, operatorUserId, audioUrl, audioName, String(reviewChannelId), String(reviewMessageId), createdAt]
  );
  return Number(res.insertId);
}

async function dbGetAnnouncementByReviewMessage(guildId, reviewMessageId) {
  const [rows] = await pool.query(
    `
    SELECT *
    FROM announcement_requests
    WHERE guild_id = ? AND review_message_id = ?
    LIMIT 1
  `,
    [guildId, String(reviewMessageId)]
  );
  return rows?.[0] ?? null;
}

async function dbUpdateAnnouncementDecision({ id, guildId, status, decidedByUserId }) {
  const decidedAt = nowUtcJsDate();
  await pool.query(
    `
    UPDATE announcement_requests
    SET status = ?, decided_at = ?, decided_by_user_id = ?
    WHERE id = ? AND guild_id = ?
  `,
    [status, decidedAt, String(decidedByUserId), Number(id), String(guildId)]
  );
}


/* =========================
   EMBED/COMPONENT BUILDERS
========================= */
function extractScreenFromUiRefs(uiRefs) {
  let gifUrl = null;
  const textParts = [];

  for (const ref of uiRefs) {
    const item = getUi(ref);
    const r = item.render ?? {};
    if (item.kind === "gif" && r.url) gifUrl = r.url;
    if (item.kind === "text" && r.text) textParts.push(r.text);
  }

  return { gifUrl, text: textParts.join("\n\n") };
}

function embedWithImageAndText(imageUrl, text) {
  const embed = new EmbedBuilder();
  if (imageUrl) embed.setImage(imageUrl);
  embed.setDescription(text?.trim() ? text : ZWSP);
  return embed;
}

function buildScreenEmbed(uiRefs, extraText = "") {
  const { gifUrl, text } = extractScreenFromUiRefs(uiRefs);
  const merged = [text, extraText].filter(Boolean).join("\n\n").trim();
  return embedWithImageAndText(gifUrl, merged || ZWSP);
}

function buildRowsFromButtons(buttons) {
  const rows = [];
  let row = new ActionRowBuilder();

  for (const btn of buttons) {
    if (row.components.length >= 5) {
      rows.push(row);
      row = new ActionRowBuilder();
    }
    row.addComponents(btn);
  }
  if (row.components.length) rows.push(row);
  return rows;
}

function disabledRow(label = "Please wait") {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId("disabled_wait")
        .setLabel(label)
        .setStyle(ButtonStyle.Secondary)
        .setDisabled(true)
    ),
  ];
}

/* =========================
   INTERACTION ACK HELPERS
========================= */
async function ackButton(interaction) {
  try {
    await interaction.deferUpdate();
  } catch (_) {}
}
async function safeDeferEphemeral(interaction) {
  try {
    await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  } catch (_) {}
}

/* =========================
   EPHEMERAL UPDATE HELPERS
========================= */
async function editEphemeral(interaction, embed, components = []) {
  await interaction.editReply({
    content: ZWSP,
    embeds: [embed],
    components,
  });
}

async function showTransitionThen(interaction, transitionUiRef, loops, nextEmbed, nextComponents) {
  const transitionEmbed = buildScreenEmbed([transitionUiRef]);
  await editEphemeral(interaction, transitionEmbed, disabledRow());

  const waitMs = Math.round(gifDurationSeconds(transitionUiRef) * loops * 1000);
  await sleep(waitMs);

  await editEphemeral(interaction, nextEmbed, nextComponents);
}

/* =========================
   UI REFS
========================= */
const UI = {
  CH_WELCOME: "[UI: Discord channel-welcome]",
  CH_TERMINAL: "[UI: Discord channel-terminal]",
  // LT COMMAND REVIEWS
  CH_LT_COMMAND_REVIEWS: "[UI: Discord channel-LT Command Reviews]",

  // INITIATE ANNOUNCEMENT (Operator Terminal)
  OP_CMD_ANN_MENU_TEXT: "[UI: Operator-Terminal-commands-initiate-announcement-menu]",
  OP_CMD_ANN_BTN_UPLOAD_AUDIO: "[UI: Operator-Terminal-commands-initiate-announcement-upload-audio-button]",
  OP_CMD_ANN_BTN_INITIATE: "[UI: Operator-Terminal-commands-initiate-announcement-initiate-button]",
  OP_CMD_ANN_BTN_BACK: "[UI: Operator-Terminal-commands-initiate-announcement-back-button]",
  OP_CMD_ANN_UPLOAD_MENU_TEXT: "[UI: Operator-Terminal-commands-initiate-announcement-upload-audio-menu]",
  OP_CMD_ANN_UPLOAD_SUBMIT: "[UI: Operator-Terminal-commands-initiate-announcement-upload-audio-submit-button]",
  OP_CMD_ANN_UPLOAD_BACK: "[UI: Operator-Terminal-commands-initiate-announcement-upload-audio-back-button]",

  // ANNOUNCEMENT REVIEW (LT)
  ANN_REVIEW_BTN_APPROVE: "[UI: Announcement-review-operator submission button-a]",
  ANN_REVIEW_BTN_DENY: "[UI: Announcement-review-operator submission button-b]",
  ANN_REVIEW_TEXT: "[UI: Announcement Initiation Request text]",
  ANN_DM_TEXT: "[UI: Announcement-text DM]",
  CH_TERMINAL: "[UI: Discord channel-terminal]",
  CH_OPERATOR_TERMINAL: "[UI: Discord channel-operator-terminal]",

  WELCOME_TEXT: "[UI: Initiation-text-1]",
  BEGIN_BTN: "[UI: Initiation-button-1]",

  CHARTER: "[UI: Initiation-screen-charter GIF]",
  INTRO: "[UI: Initiation-screen-introduction GIF]",
  COMMS: "[UI: Initiation-screen-comms opt in GIF]",
  CONGRATS: "[UI: Initiation-screen-congratulations GIF]",

  I_T_BOOT: "[UI: Initiation-transitional GIF-1]",
  I_T_ACCEPT: "[UI: Initiation-transitional GIF-2]",
  I_T_ACCEPT_BACK: "[UI: Initiation-transitional GIF-2-back]",
  I_T_UNDERSTAND: "[UI: Initiation-transitional GIF-3]",
  I_T_UNDERSTAND_BACK: "[UI: Initiation-transitional GIF-3-back]",
  I_T_YES: "[UI: Initiation-transitional GIF-4]",
  I_T_NO: "[UI: Initiation-transitional GIF-4-no]",

  ACCEPT: "[UI: Initiation-button-2A]",
  UNDERSTAND: "[UI: Initiation-button-3A]",
  BACK_INTRO: "[UI: Initiation-button-3B]",
  YES: "[UI: Initiation-button-4A]",
  NO: "[UI: Initiation-button-4B]",
  BACK_COMMS: "[UI: Initiation-button-4C]",
  TRY_AGAIN: "[UI: Initiation-button-4D]",

  COMMS_ERROR_TEXT: "[UI: Initiation-screen-comms opt in text-error]",

  DM_OPT_IN_SUCCESS: "[UI: Comms Opt In Success DM text]",
  DM_OPT_OUT_SUCCESS: "[UI: Comms Opt Out Success DM text]",
  DM_BTN_A: "[UI: Comms Opt In Success DM-button-A]",
  DM_BTN_B: "[UI: Comms Opt In Success DM-button-B]",

  ROLE_CORE: "[UI: Discord Role The Core]",
  ROLE_SIGNAL: "[UI: Discord Role The Core Signal]",

  TERMINAL_ACCESS_TEXT: "[UI: Terminal-access text]",
  TERMINAL_ACCESS_BTN: "[UI: Terminal-access-button]",

  T_BOOT: "[UI: Terminal-transitional GIF-1]",
  T_MENU: "[UI: Terminal-main menu-GIF]",
  T_OBJECTIVES_TRANS: "[UI: Terminal-transitional GIF-objectives]",
  T_EVENTS_TRANS: "[UI: Terminal-transitional GIF-events]",
  T_BTN_OBJECTIVES: "[UI: Terminal-objectives-button]",
  T_BTN_EVENTS: "[UI: Terminal-events-button]",

  T_OBJ_BTN_SUBMIT: "[UI: Terminal-objectives-button-submit]",
  T_OBJ_BTN_BACK: "[UI: Terminal-objectives-button-back]",
  T_OBJ_BTN_NEXT: "[UI: Terminal-objectives-button-next]",
  T_OBJ_BTN_PREV: "[UI: Terminal-objectives-button-previous]",

  OP_ACCESS_TEXT: "[UI: Operator-Terminal-access text]",
  OP_ACCESS_BTN: "[UI: Operator-Terminal-access-button]",
  OP_MENU_TEXT: "[UI: Operator-Terminal-menu]",
  OP_BTN_OBJECTIVES: "[UI: Operator-Terminal-objectives-button]",
  OP_BTN_EVENTS: "[UI: Operator-Terminal-events-button]",
  OP_BTN_COMMANDS: "[UI: Operator-Terminal-commands-button]",

// COMMANDS (Operator Terminal)
OP_CMD_MENU_TEXT: "[UI: Operator-Terminal-commands-menu]",
OP_CMD_BTN_ROLL_CALL: "[UI: Operator-Terminal-commands-button-roll-call]",
OP_CMD_BTN_ANNOUNCEMENT: "[UI: Operator-Terminal-commands-button-announcement]",
OP_CMD_BTN_MERIT_TRANSFER: "[UI: Operator-Terminal-commands-button-merit-transfer]",
OP_CMD_BTN_BACK: "[UI: Operator-Terminal-commands-button-back]",

// INITIATE ROLL CALL (Operator Terminal)
OP_CMD_ROLL_MENU_TEXT: "[UI: Operator-Terminal-commands-initiate-roll-call-menu]",
OP_CMD_ROLL_BTN_TARGET_CHANNEL: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-button]",
OP_CMD_ROLL_BTN_INITIATE: "[UI: Operator-Terminal-commands-initiate-roll-call-initiate-button]",
OP_CMD_ROLL_BTN_BACK: "[UI: Operator-Terminal-commands-initiate-roll-call-back-button]",
OP_CMD_ROLL_TARGET_MENU_TEXT: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-menu]",
OP_CMD_ROLL_TARGET_CH1: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-channel-1-button]",
OP_CMD_ROLL_TARGET_CH2: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-channel-2-button]",
OP_CMD_ROLL_TARGET_CH3: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-channel-3-button]",
OP_CMD_ROLL_TARGET_CH4: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-channel-4-button]",
OP_CMD_ROLL_TARGET_CH5: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-channel-5-button]",
OP_CMD_ROLL_TARGET_SUBMIT: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-submit-button]",
OP_CMD_ROLL_TARGET_BACK: "[UI: Operator-Terminal-commands-initiate-roll-call-target-channel-back-button]",

  // EVENTS (Operator Terminal)
  OP_EVT_MENU_TEXT: "[UI: Operator-Terminal-events-menu]",
  OP_EVT_BTN_UPLOAD: "[UI: Operator-Terminal-events-button-upload]",
  OP_EVT_BTN_SCHEDULING: "[UI: Operator-Terminal-events-button-scheduling]",
  OP_EVT_BTN_BACK: "[UI: Operator-Terminal-events-button-back]",

  OP_EVT_UPLOAD_MENU_TEXT: "[UI: Operator-Terminal-events-upload-menu]",
  OP_EVT_UPLOAD_NAME_BTN: "[UI: Operator-Terminal-events-upload-name-button]",
  OP_EVT_UPLOAD_MERITS_BTN: "[UI: Operator-Terminal-events-upload-merits-awarded-button]",
  OP_EVT_UPLOAD_SCHEDULE_BTN: "[UI: Operator-Terminal-events-upload-schedule-button]",
  OP_EVT_UPLOAD_ROLE_BTN: "[UI: Operator-Terminal-events-upload-role-allocated-button]",
  OP_EVT_UPLOAD_GIF_BTN: "[UI: Operator-Terminal-events-upload-GIF-button]",
  OP_EVT_UPLOAD_SUBMIT_BTN: "[UI: Operator-Terminal-events-upload-submit-event-button]",
  OP_EVT_UPLOAD_BACK_BTN: "[UI: Operator-Terminal-events-upload-back-button]",

  OP_EVT_ROLE_MENU_TEXT: "[UI: Operator-Terminal-events-upload-role-allocated-menu]",
  OP_EVT_ROLE_BTN_1: "[UI: Operator-Terminal-events-upload-event-1-role-button]",
  OP_EVT_ROLE_BTN_2: "[UI: Operator-Terminal-events-upload-event-2-role-button]",
  OP_EVT_ROLE_BTN_3: "[UI: Operator-Terminal-events-upload-event-3-role-button]",
  OP_EVT_ROLE_BTN_4: "[UI: Operator-Terminal-events-upload-event-4-role-button]",
  OP_EVT_ROLE_BTN_5: "[UI: Operator-Terminal-events-upload-event-5-role-button]",
  OP_EVT_ROLE_SUBMIT_BTN: "[UI: Operator-Terminal-events-upload-role-allocated-submit-button]",
  OP_EVT_ROLE_BACK_BTN: "[UI: Operator-Terminal-events-upload-role-allocated-back-button]",

  OP_EVT_GIF_MENU_TEXT: "[UI: Operator-Terminal-events-upload-GIF-menu]",
  OP_EVT_GIF_MAIN_BTN: "[UI: Operator-Terminal-events-upload-GIF-main-file-button]",
  OP_EVT_GIF_CHECKIN_BTN: "[UI: Operator-Terminal-events-upload-GIF-check-in-file-button]",
  OP_EVT_GIF_BACK_BTN: "[UI: Operator-Terminal-events-upload-GIF-back-file-button]",
  OP_EVT_GIF_SUBMIT_BTN: "[UI: Operator-Terminal-events-upload-GIF-submit-button]",
  OP_EVT_GIF_BACK_MENU_BTN: "[UI: Operator-Terminal-events-upload-GIF-back-button]",
  OP_EVT_GIF_ANY_BACK_BTN: "[UI: Operator-Terminal-events-upload-GIF-any-file-back-button]",

  ROLE_EVT1_CHECKEDIN: "[UI: Discord Role Event 1 Checked In]",
  ROLE_EVT2_CHECKEDIN: "[UI: Discord Role Event 2 Checked In]",
  ROLE_EVT3_CHECKEDIN: "[UI: Discord Role Event 3 Checked In]",
  ROLE_EVT4_CHECKEDIN: "[UI: Discord Role Event 4 Checked In]",
  ROLE_EVT5_CHECKEDIN: "[UI: Discord Role Event 5 Checked In]",


  // EVENTS — SCHEDULING
  OP_EVT_SCHED_MENU_TEXT: "[UI: Operator-Terminal-events-scheduling-menu]",
  OP_EVT_SCHED_BTN_EDIT: "[UI: Operator-Terminal-events-scheduling-edit-button]",
  OP_EVT_SCHED_BTN_DELETE: "[UI: Operator-Terminal-events-scheduling-delete-button]",
  OP_EVT_SCHED_BTN_NEXT: "[UI: Operator-Terminal-events-scheduling-next-button]",
  OP_EVT_SCHED_BTN_PREV: "[UI: Operator-Terminal-events-scheduling-previous-button]",
  OP_EVT_SCHED_BTN_BACK: "[UI: Operator-Terminal-events-scheduling-back-button]",

  OP_EVT_SCHED_DELETE_MENU_TEXT: "[UI: Operator-Terminal-events-scheduling-delete-menu]",
  OP_EVT_SCHED_DELETE_BTN_CONFIRM: "[UI: Operator-Terminal-events-scheduling-delete-confirm-button]",
  OP_EVT_SCHED_DELETE_BTN_BACK: "[UI: Operator-Terminal-events-scheduling-delete-back-button]",

  OP_EVT_SCHED_EDIT_MENU_TEXT: "[UI: Operator-Terminal-events-scheduling-edit-menu]",
  OP_EVT_SCHED_EDIT_BTN_NAME: "[UI: Operator-Terminal-events-scheduling-edit-name-button]",
  OP_EVT_SCHED_EDIT_BTN_MERITS: "[UI: Operator-Terminal-events-scheduling-edit-merits-awarded-button]",
  OP_EVT_SCHED_EDIT_BTN_SCHEDULE: "[UI: Operator-Terminal-events-scheduling-edit-schedule-button]",
  OP_EVT_SCHED_EDIT_BTN_ROLE: "[UI: Operator-Terminal-events-scheduling-edit-role-allocated-button]",
  OP_EVT_SCHED_EDIT_BTN_GIF: "[UI: Operator-Terminal-events-scheduling-edit-GIF-button]",
  OP_EVT_SCHED_EDIT_BTN_SUBMIT: "[UI: Operator-Terminal-events-scheduling-edit-submit-event-button]",
  OP_EVT_SCHED_EDIT_BTN_BACK: "[UI: Operator-Terminal-events-scheduling-edit-back-button]",

  OP_EVT_SCHED_EDIT_ROLE_MENU_TEXT: "[UI: Operator-Terminal-events-scheduling-edit-role-allocated-menu]",
  OP_EVT_SCHED_EDIT_ROLE_BTN_1: "[UI: Operator-Terminal-events-scheduling-edit-event-1-role-button]",
  OP_EVT_SCHED_EDIT_ROLE_BTN_2: "[UI: Operator-Terminal-events-scheduling-edit-event-2-role-button]",
  OP_EVT_SCHED_EDIT_ROLE_BTN_3: "[UI: Operator-Terminal-events-scheduling-edit-event-3-role-button]",
  OP_EVT_SCHED_EDIT_ROLE_BTN_4: "[UI: Operator-Terminal-events-scheduling-edit-event-4-role-button]",
  OP_EVT_SCHED_EDIT_ROLE_BTN_5: "[UI: Operator-Terminal-events-scheduling-edit-event-5-role-button]",
  OP_EVT_SCHED_EDIT_ROLE_SUBMIT_BTN: "[UI: Operator-Terminal-events-scheduling-edit-role-allocated-submit-button]",
  OP_EVT_SCHED_EDIT_ROLE_BACK_BTN: "[UI: Operator-Terminal-events-scheduling-edit-role-allocated-back-button]",

  OP_EVT_SCHED_EDIT_GIF_MENU_TEXT: "[UI: Operator-Terminal-events-scheduling-edit-GIF-menu]",
  OP_EVT_SCHED_EDIT_GIF_MAIN_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-main-file-button]",
  OP_EVT_SCHED_EDIT_GIF_CHECKIN_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-check-in-file-button]",
  OP_EVT_SCHED_EDIT_GIF_BACK_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-back-file-button]",
  OP_EVT_SCHED_EDIT_GIF_SUBMIT_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-submit-button]",
  OP_EVT_SCHED_EDIT_GIF_BACK_MENU_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-back-button]",
  OP_EVT_SCHED_EDIT_GIF_ANY_BACK_BTN: "[UI: Operator-Terminal-events-scheduling-edit-GIF-any-file-back-button]",

  OP_OBJ_MENU_TEXT: "[UI: Operator-Terminal-objectives-menu]",
  OP_OBJ_BTN_UPLOAD: "[UI: Operator-Terminal-objectives-button-upload]",
  OP_OBJ_BTN_SCHED: "[UI: Operator-Terminal-objectives-button-scheduling]",
  OP_OBJ_BTN_BACK: "[UI: Operator-Terminal-objectives-button-back]",

  // OBJECTIVES — SCHEDULING
  OP_OBJ_SCHED_MENU_TEXT: "[UI: Operator-Terminal-objectives-scheduling-menu]",
  OP_OBJ_SCHED_BTN_EDIT: "[UI: Operator-Terminal-objectives-scheduling-edit-button]",
  OP_OBJ_SCHED_BTN_DELETE: "[UI: Operator-Terminal-objectives-scheduling-delete-button]",
  OP_OBJ_SCHED_BTN_NEXT: "[UI: Operator-Terminal-objectives-scheduling-next-button]",
  OP_OBJ_SCHED_BTN_PREV: "[UI: Operator-Terminal-objectives-scheduling-previous-button]",
  OP_OBJ_SCHED_BTN_BACK: "[UI: Operator-Terminal-objectives-scheduling-back-button]",

  OP_OBJ_SCHED_DELETE_MENU_TEXT: "[UI: Operator-Terminal-objectives-scheduling-delete-menu]",
  OP_OBJ_SCHED_DELETE_BTN_CONFIRM: "[UI: Operator-Terminal-objectives-scheduling-delete-confirm-button]",
  OP_OBJ_SCHED_DELETE_BTN_BACK: "[UI: Operator-Terminal-objectives-scheduling-delete-back-button]",

  OP_OBJ_SCHED_EDIT_MENU_TEXT: "[UI: Operator-Terminal-objectives-scheduling-edit-menu]",
  OP_OBJ_SCHED_EDIT_BTN_NAME: "[UI: Operator-Terminal-objectives-scheduling-edit-name-button]",
  OP_OBJ_SCHED_EDIT_BTN_MERITS: "[UI: Operator-Terminal-objectives-scheduling-edit-merits-awarded-button]",
  OP_OBJ_SCHED_EDIT_BTN_SCHEDULE: "[UI: Operator-Terminal-objectives-scheduling-edit-schedule-button]",
  OP_OBJ_SCHED_EDIT_BTN_FILETYPE: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-button]",
  OP_OBJ_SCHED_EDIT_BTN_GIF: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-button]",
  OP_OBJ_SCHED_EDIT_BTN_SUBMIT: "[UI: Operator-Terminal-objectives-scheduling-edit-submit-objective-button]",
  OP_OBJ_SCHED_EDIT_BTN_BACK: "[UI: Operator-Terminal-objectives-scheduling-edit-back-button]",

  OP_OBJ_SCHED_EDIT_FILETYPE_MENU: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-menu]",
  OP_OBJ_SCHED_EDIT_FILETYPE_IMAGE: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-image-button]",
  OP_OBJ_SCHED_EDIT_FILETYPE_AUDIO: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-audio-button]",
  OP_OBJ_SCHED_EDIT_FILETYPE_TEXT: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-text-button]",
  OP_OBJ_SCHED_EDIT_FILETYPE_TEXT_FILE: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-text-file-button]",
  OP_OBJ_SCHED_EDIT_FILETYPE_SUBMIT: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-submit-button]",
  OP_OBJ_SCHED_EDIT_FILETYPE_BACK: "[UI: Operator-Terminal-objectives-scheduling-edit-file-type-back-button]",

  OP_OBJ_SCHED_EDIT_GIF_MENU: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-menu]",
  OP_OBJ_SCHED_EDIT_GIF_MAIN: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-main-file-button]",
  OP_OBJ_SCHED_EDIT_GIF_SUBMIT: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-submit-file-button]",
  OP_OBJ_SCHED_EDIT_GIF_BACK: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-back-file-button]",
  OP_OBJ_SCHED_EDIT_GIF_SUBMIT_BTN: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-submit-button]",
  OP_OBJ_SCHED_EDIT_GIF_BACK_BTN: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-back-button]",
  OP_OBJ_SCHED_EDIT_GIF_ANYFILE_BACK: "[UI: Operator-Terminal-objectives-scheduling-edit-GIF-any-file-back-button]",


  OP_OBJ_UPLOAD_MENU_TEXT: "[UI: Operator-Terminal-objectives-upload-menu]",
  OP_OBJ_UPLOAD_BTN_NAME: "[UI: Operator-Terminal-objectives-upload-name-button]",
  OP_OBJ_UPLOAD_BTN_MERITS: "[UI: Operator-Terminal-objectives-upload-merits-awarded-button]",
  OP_OBJ_UPLOAD_BTN_SCHEDULE: "[UI: Operator-Terminal-objectives-upload-schedule-button]",
  OP_OBJ_UPLOAD_BTN_FILETYPE: "[UI: Operator-Terminal-objectives-upload-file-type-button]",
  OP_OBJ_UPLOAD_BTN_GIF: "[UI: Operator-Terminal-objectives-upload-GIF-button]",
  OP_OBJ_UPLOAD_BTN_SUBMIT: "[UI: Operator-Terminal-objectives-upload-submit-objective-button]",
  OP_OBJ_UPLOAD_BTN_BACK: "[UI: Operator-Terminal-objectives-upload-back-button]",

  OP_OBJ_FILETYPE_MENU: "[UI: Operator-Terminal-objectives-upload-file-type-menu]",
  OP_OBJ_FILETYPE_IMAGE: "[UI: Operator-Terminal-objectives-upload-file-type-image-button]",
  OP_OBJ_FILETYPE_AUDIO: "[UI: Operator-Terminal-objectives-upload-file-type-audio-button]",
  OP_OBJ_FILETYPE_TEXT: "[UI: Operator-Terminal-objectives-upload-file-type-text-button]",
  OP_OBJ_FILETYPE_TEXT_FILE: "[UI: Operator-Terminal-objectives-upload-file-type-text-file-button]",
  OP_OBJ_FILETYPE_SUBMIT: "[UI: Operator-Terminal-objectives-upload-file-type-submit-button]",
  OP_OBJ_FILETYPE_BACK: "[UI: Operator-Terminal-objectives-upload-file-type-back-button]",

  OP_OBJ_GIF_MENU: "[UI: Operator-Terminal-objectives-upload-GIF-menu]",
  OP_OBJ_GIF_MAIN: "[UI: Operator-Terminal-objectives-upload-GIF-main-file-button]",
  OP_OBJ_GIF_SUBMIT: "[UI: Operator-Terminal-objectives-upload-GIF-submit-file-button]",
  OP_OBJ_GIF_BACK: "[UI: Operator-Terminal-objectives-upload-GIF-back-file-button]",
  OP_OBJ_GIF_SUBMIT_BTN: "[UI: Operator-Terminal-objectives-upload-GIF-submit-button]",
  OP_OBJ_GIF_BACK_BTN: "[UI: Operator-Terminal-objectives-upload-GIF-back-button]",
  OP_OBJ_GIF_ANYFILE_BACK: "[UI: Operator-Terminal-objectives-upload-GIF-any-file-back-button]",

// User Objective Submit (Terminal)
T_OBJ_SUBMIT_TRANS: "[UI: Terminal-transitional GIF-objectives-*objective name*-submit]",
T_OBJ_SUBMIT_GIF: "[UI: Terminal-objectives-GIF-submit]",
T_OBJ_SUBMIT_TRY_AGAIN: "[UI: Terminal-objectives-submit-button-try-again]",
T_OBJ_SUBMIT_BACK: "[UI: Terminal-objectives-submit-button-back]",
T_OBJ_SUBMIT_TRANS_TRY_AGAIN: "[UI: Terminal-transitional GIF-objectives-submit-try-again]",
T_OBJ_SUBMIT_TRANS_BACK: "[UI: Terminal-transitional GIF-objectives-submit-back]",

// Objective Submit (DM)
T_OBJ_SUBMIT_DM_TEXT: "[UI: Terminal-objectives-submit DM text-1-file-type-text]",
T_OBJ_SUBMIT_DM_IMAGE: "[UI: Terminal-objectives-submit DM text-1-file-type-image]",
T_OBJ_SUBMIT_DM_TEXT_FILE: "[UI: Terminal-objectives-submit DM text-1-file-type-text-file]",
T_OBJ_SUBMIT_DM_AUDIO: "[UI: Terminal-objectives-submit DM text-1-file-type-audio-file]",
T_OBJ_SUBMIT_DM_SUCCESS: "[UI: Terminal-objectives-submit DM text-2-success]",
T_OBJ_SUBMIT_DM_ERR_TYPE: "[UI: Terminal-objectives-submit DM text-2-error]",
T_OBJ_SUBMIT_DM_ERR_SIZE: "[UI: Terminal-objectives-submit DM text-2-error-file-size]",
T_OBJ_SUBMIT_DM_ERR_CHAR: "[UI: Terminal-objectives-submit DM text-2-error-character-limit]",
T_OBJ_SUBMIT_DM_ERR_TIMEOUT: "[UI: Terminal-objectives-submit DM text-2-error-timeout]",
T_OBJ_SUBMIT_DM_ERR_ALREADY: "[UI: Terminal-objectives-submit DM text-2-error-already submitted]",
T_OBJ_SUBMIT_DM_ERR_ALREADY_A: "[UI: Terminal-objectives-submit DM button-2-error-already submitted-a]",
T_OBJ_SUBMIT_DM_ERR_ALREADY_B: "[UI: Terminal-objectives-submit DM button-2-error-already submitted-b]",

// Objective Submissions review channel
CH_OBJECTIVE_SUBMISSIONS: "[UI: Discord channel-Objective Submissions]",
ROLE_OPERATOR: "[UI: Discord role Operator]",
OBJ_REVIEW_TEXT: "[UI: Objectives-review-user submission text]",
OBJ_REVIEW_BTN_A: "[UI: Objectives-review-user submission button-a]",
OBJ_REVIEW_BTN_B: "[UI: Objectives-review-user submission button-b]",
OBJ_REVIEW_BTN_C: "[UI: Objectives-review-user submission button-c]",
};

/* =========================
   CUSTOM IDS
========================= */
const CUSTOM = {
  TERMINAL_BACK: "core_terminal_back",
  TERMINAL_CLOSE: "core_terminal_close",
  OP_CLOSE: "core_operator_close",
};

function btnClose(customId, label = "Close") {
  return new ButtonBuilder().setCustomId(customId).setLabel(label).setStyle(ButtonStyle.Secondary);
}

/* =========================
   SESSIONS (IN-MEMORY UI STATE)
========================= */
const operatorSession = new Map(); // key -> session
const dmCooldowns = new Map(); // key = `${userId}:${kind}` -> lastSentAtMs

function canSendDm(userId, kind, cooldownMs = 5000) {
  const key = `${userId}:${kind}`;
  const now = Date.now();
  const last = dmCooldowns.get(key) || 0;
  if (now - last < cooldownMs) return false;
  dmCooldowns.set(key, now);
  return true;
}

async function safeSendDm(user, content, components = null) {
  try {
    const dm = await user.createDM();
    if (Array.isArray(components) && components.length) {
      await dm.send({ content, components });
    } else {
      await dm.send(content);
    }
    return true;
  } catch {
    return false;
  }
}

const pendingGifUploads = new Map(); // key -> { slot, expiresAt, channelId }

const leaderboardSession = new Map(); // key -> session

function ensureLeaderboardSession(guildId, userId) {
  const key = sessionKey(guildId, userId);
  if (!leaderboardSession.has(key)) {
    leaderboardSession.set(key, {
      view: "all_time", // "cycle" | "all_time"
      uiInteraction: null,
    });
  }
  return leaderboardSession.get(key);
}

const terminalSession = new Map(); // key -> { objectiveIndex, cachedObjectives }
const objectiveSubmitSession = new Map(); // key -> { objectiveId, objectiveName, proofType, expiresAt, submitted }

/* =========================
   SESSION HELPERS
========================= */
function sessionKey(guildId, userId) {
  return `${guildId}:${userId}`;
}
function ensureOperatorSession(guildId, userId) {
  const key = sessionKey(guildId, userId);
  if (!operatorSession.has(key)) {
    operatorSession.set(key, {
      screen: "op_menu",
      draft: {
        name: null,
        merits: null,
        schedule_begin: null,
        schedule_end: null,
        file_type: null,
        gifs: { main: null, submit: null, back: null },
      eventDraft: {
        name: null,
        merits: null,
        schedule_begin: null,
        schedule_end: null,
        check_in_role_key: null,
        resolved_role_id: null,
        gifs: { main: null, check_in: null, back: null },
      },
      },
      // objective scheduling browsing state (single current selection)
      objectiveScheduling: { index: 0, cached: [], lastLoadedAt: 0 },
      // current objective being edited (scheduling edit)
      objectiveEdit: { objectiveId: null },
      // event scheduling browsing state (single current selection)
      eventScheduling: { index: 0, cached: [], lastLoadedAt: 0 },
      // current event being edited (scheduling edit)
      eventEdit: { eventId: null },
      commandsDraft: {
        roll_call: { target_channel_key: null, resolved_channel_id: null, pending_channel_key: null },
        announcement: { audio_url: null, audio_name: null },
        merit_transfer: { target_user_id: null, amount: null, notes: null },
      },
      audioWait: null,
      audioWaitUntilMs: 0,
      tempFileType: null,
      uiInteraction: null,
    });
  }
  return operatorSession.get(key);
}
function clearDraft(s) {
  s.draft = {
    name: null,
    merits: null,
    schedule_begin: null,
    schedule_end: null,
    file_type: null,
    gifs: { main: null, submit: null, back: null },
  };
  s.tempFileType = null;
}
function ensureTerminalSession(guildId, userId) {
  const key = sessionKey(guildId, userId);
  if (!terminalSession.has(key)) terminalSession.set(key, { objectiveIndex: 0,
    eventIndex: 0, cachedObjectives: [] });
  return terminalSession.get(key);
}

/* =========================
   DISCORD CLIENT
========================= */

const EVENT_ROLE_LIFECYCLE_TICK_MS = 60_000;
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.DirectMessages,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel, Partials.Message, Partials.Reaction, Partials.User],
});

/* =========================
   GUILD FETCH
========================= */

function getEnvConfig() {
  return CORE_ENV === "production" ? config.discord.guilds.production : config.discord.guilds.testing;
}

function envGuildId() {
  return CORE_ENV === "production"
    ? config.discord.guilds.production.guild_id
    : config.discord.guilds.testing.guild_id;
}
async function fetchEnvGuild() {
  return await client.guilds.fetch(envGuildId());
}

/* =========================
   ACCESS POSTS
========================= */
async function postWelcomeAccessPost() {
  const guild = await fetchEnvGuild();
  const channel = await guild.channels.fetch(resolveChannelId(UI.CH_WELCOME));
  if (!channel?.isTextBased()) return;

  const welcomeText = getRender(UI.WELCOME_TEXT).text ?? "";
  await channel.send({
    content: welcomeText,
    components: [new ActionRowBuilder().addComponents(makeUiButton(UI.BEGIN_BTN))],
  });
  console.log("✅ Welcome post sent");
}

async function postTerminalAccessPost() {
  const guild = await fetchEnvGuild();
  const channel = await guild.channels.fetch(resolveChannelId(UI.CH_TERMINAL));
  if (!channel?.isTextBased()) return;

  const accessText = getRender(UI.TERMINAL_ACCESS_TEXT).text ?? "";
  await channel.send({
    content: accessText,
    components: [new ActionRowBuilder().addComponents(makeUiButton(UI.TERMINAL_ACCESS_BTN))],
  });
  console.log("✅ Terminal access post sent");
}

async function postOperatorAccessPost() {
  const guild = await fetchEnvGuild();
  const channel = await guild.channels.fetch(resolveChannelId(UI.CH_OPERATOR_TERMINAL));
  if (!channel?.isTextBased()) return;

  const accessText = getRender(UI.OP_ACCESS_TEXT).text ?? "";
  await channel.send({
    content: accessText,
    components: [new ActionRowBuilder().addComponents(makeUiButton(UI.OP_ACCESS_BTN))],
  });
  console.log("✅ Operator access post sent");
}


async function postLeaderboardAccessPost() {
  const guild = await fetchEnvGuild();
  const channel = await guild.channels.fetch(resolveChannelId("[UI: Discord channel-leaderboard]"));
  if (!channel?.isTextBased()) return;

  const accessText = "The official rankings of The Core – see where your service resides. Click ‘Access Leaderboard’ below to boot.";
  await channel.send({
    content: accessText,
    components: [new ActionRowBuilder().addComponents(makeUiButton("[UI: Leaderboard-access-button]"))],
  });
  console.log("✅ Leaderboard access post sent");
}

async function postLtTerminalAccessPost() {
  const guild = await fetchEnvGuild();
  const channel = await guild.channels.fetch(resolveChannelId("[UI: Discord channel-lt-terminal]"));
  if (!channel?.isTextBased()) return;

  const accessText = getRender("[UI: LT-Terminal-access text]").text ?? "";
  await channel.send({
    content: accessText,
    components: [new ActionRowBuilder().addComponents(makeUiButton("[UI: LT-Terminal-access-button]"))],
  });
  console.log("✅ LT access post sent");
}


/* =========================
   DM PANEL
========================= */
function buildDmControls() {
  return [new ActionRowBuilder().addComponents(makeUiButton(UI.DM_BTN_A), makeUiButton(UI.DM_BTN_B))];
}
function dmText(ref) {
  try {
    return getRender(ref).text ?? "";
  } catch (_) {
    return "";
  }
}
async function sendOrEditDmPanel(user, existingMessage, textUiRef) {
  const content = dmText(textUiRef)?.trim() ? dmText(textUiRef) : ZWSP;
  const components = buildDmControls();

  if (!existingMessage) return await user.send({ content, components });
  await existingMessage.edit({ content, components });
  return existingMessage;
}

/* =========================
   INITIATION FLOW
========================= */
async function showCongrats(interaction) {
  const embed = buildScreenEmbed([UI.CONGRATS]);
  await editEphemeral(interaction, embed, []);
}

async function handleBegin(interaction) {
  await safeDeferEphemeral(interaction);

  const member = interaction.member;
  if (!member) {
    await interaction.editReply({ content: "This can only be used in a server.", embeds: [], components: [] });
    return;
  }

  const coreRoleId = resolveRoleId(UI.ROLE_CORE);
  if (member.roles.cache.has(coreRoleId)) {
    await interaction.editReply({ content: "You have already been initiated.", embeds: [], components: [] });
    return;
  }

  const nextEmbed = buildScreenEmbed([UI.CHARTER]);
  const nextComponents = buildRowsFromButtons([makeUiButton(UI.ACCEPT)]);
  await showTransitionThen(interaction, UI.I_T_BOOT, LOOPS.initiation_boot, nextEmbed, nextComponents);
}

async function handleAccept(interaction) {
  await ackButton(interaction);
  const nextEmbed = buildScreenEmbed([UI.INTRO]);
  const nextComponents = buildRowsFromButtons([makeUiButton(UI.UNDERSTAND), makeUiButton(UI.BACK_INTRO)]);
  await showTransitionThen(interaction, UI.I_T_ACCEPT, 1, nextEmbed, nextComponents);
}

async function handleBackToCharter(interaction) {
  await ackButton(interaction);
  const nextEmbed = buildScreenEmbed([UI.CHARTER]);
  const nextComponents = buildRowsFromButtons([makeUiButton(UI.ACCEPT)]);
  await showTransitionThen(interaction, UI.I_T_ACCEPT_BACK, 1, nextEmbed, nextComponents);
}

async function handleUnderstand(interaction) {
  await ackButton(interaction);
  const nextEmbed = buildScreenEmbed([UI.COMMS]);
  const nextComponents = buildRowsFromButtons([makeUiButton(UI.YES), makeUiButton(UI.NO), makeUiButton(UI.BACK_COMMS)]);
  await showTransitionThen(interaction, UI.I_T_UNDERSTAND, 1, nextEmbed, nextComponents);
}

async function handleBackToIntro(interaction) {
  await ackButton(interaction);
  const nextEmbed = buildScreenEmbed([UI.INTRO]);
  const nextComponents = buildRowsFromButtons([makeUiButton(UI.UNDERSTAND), makeUiButton(UI.BACK_INTRO)]);
  await showTransitionThen(interaction, UI.I_T_UNDERSTAND_BACK, 1, nextEmbed, nextComponents);
}

async function finalize(interaction, wantSignal) {
  await ackButton(interaction);

  const member = interaction.member;
  const coreRoleId = resolveRoleId(UI.ROLE_CORE);
  const signalRoleId = resolveRoleId(UI.ROLE_SIGNAL);

  const transitionRef = wantSignal ? UI.I_T_YES : UI.I_T_NO;
  const transitionEmbed = buildScreenEmbed([transitionRef]);
  await editEphemeral(interaction, transitionEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(transitionRef) * 1000));

  await member.roles.add(coreRoleId);

  if (wantSignal) {
    await member.roles.add(signalRoleId);
    try {
      await sendOrEditDmPanel(interaction.user, null, UI.DM_OPT_IN_SUCCESS);
    } catch (_) {
      const errEmbed = buildScreenEmbed([UI.COMMS], dmText(UI.COMMS_ERROR_TEXT));
      const comps = buildRowsFromButtons([makeUiButton(UI.TRY_AGAIN), makeUiButton(UI.BACK_COMMS)]);
      await editEphemeral(interaction, errEmbed, comps);
      return;
    }
  }

  await showCongrats(interaction);
}

async function handleYes(interaction) {
  await finalize(interaction, true);
}
async function handleNo(interaction) {
  await finalize(interaction, false);
}

async function handleTryAgain(interaction) {
  await ackButton(interaction);

  const commsEmbed = buildScreenEmbed([UI.COMMS]);
  await editEphemeral(interaction, commsEmbed, disabledRow());

  try {
    await sendOrEditDmPanel(interaction.user, null, UI.DM_OPT_IN_SUCCESS);
    await showCongrats(interaction);
  } catch (_) {
    const errEmbed = buildScreenEmbed([UI.COMMS], dmText(UI.COMMS_ERROR_TEXT));
    const comps = buildRowsFromButtons([makeUiButton(UI.TRY_AGAIN), makeUiButton(UI.BACK_COMMS)]);
    await editEphemeral(interaction, errEmbed, comps);
  }
}

/* =========================
   DM BUTTON HANDLER
========================= */
async function handleDmButton(interaction) {
  await ackButton(interaction);

  const clickedId = interaction.customId;
  const dmA = getRender(UI.DM_BTN_A);
  const dmB = getRender(UI.DM_BTN_B);

  const labelA = String(dmA.label ?? "").toLowerCase();
  const labelB = String(dmB.label ?? "").toLowerCase();

  const isOptOut =
    (clickedId === dmA.custom_id && labelA.includes("opt out")) ||
    (clickedId === dmB.custom_id && labelB.includes("opt out"));

  const isOptIn =
    (clickedId === dmA.custom_id && labelA.includes("opt in")) ||
    (clickedId === dmB.custom_id && labelB.includes("opt in"));

  if (!isOptOut && !isOptIn) return;

  const guild = await fetchEnvGuild().catch(() => null);
  if (!guild) return;

  const member = await guild.members.fetch(interaction.user.id).catch(() => null);
  if (!member) return;

  const signalRoleId = resolveRoleId(UI.ROLE_SIGNAL);

  try {
    if (isOptOut) {
      await member.roles.remove(signalRoleId);
      await sendOrEditDmPanel(interaction.user, interaction.message, UI.DM_OPT_OUT_SUCCESS);
    } else {
      await member.roles.add(signalRoleId);
      await sendOrEditDmPanel(interaction.user, interaction.message, UI.DM_OPT_IN_SUCCESS);
    }
  } catch (e) {
    console.error("DM role change failed:", e);
  }
}

/* =========================
   TERMINAL FLOW
========================= */
function terminalMainMenuEmbed(extraText = "") {
  const menuGifUrl = getRender(UI.T_MENU).url;
  return embedWithImageAndText(menuGifUrl, extraText || ZWSP);
}

function leaderboardEmbed({ view, cache, viewerUserId }) {
  const isCycle = view === "cycle";
  const title = isCycle ? "Leaderboard — 🔄 Current Cycle" : "Leaderboard — ♾️ All Time";

  const rows = isCycle ? cache.cycleRows : cache.allTimeRows;
  const top = Array.isArray(rows) ? rows.slice(0, LEADERBOARD_MAX_ROWS) : [];

  const lines = [];
  for (let i = 0; i < top.length; i++) {
    const r = top[i];
    const rank = i + 1;
    const uid = String(r.user_id);
    const merits = Number(r.merits ?? 0);
    lines.push(`${rank}. <@${uid}> — ${Number.isFinite(merits) ? merits : 0} ¤`);
  }

  const yourRank = leaderboardRankFromRows(rows, viewerUserId);
  const yourMerits = leaderboardMeritsFromRows(rows, viewerUserId);

  const otherRows = isCycle ? cache.allTimeRows : cache.cycleRows;
  const otherRank = leaderboardRankFromRows(otherRows, viewerUserId);
  const otherMerits = leaderboardMeritsFromRows(otherRows, viewerUserId);

  const cycleWin = cache.cycleWindow;
  const cycleLabel = cycleWin
    ? `${cycleWin.start.toFormat("dd LLL yyyy")} – ${cycleWin.end.minus({ days: 1 }).toFormat("dd LLL yyyy")} (PT)`
    : "—";

  const dossier = isCycle
    ? `Current Cycle: ${yourMerits} ¤ (Rank ${yourRank ?? "—"})\nAll Time: ${otherMerits} ¤ (Rank ${otherRank ?? "—"})\nCycle window: ${cycleLabel}`
    : `All Time: ${yourMerits} ¤ (Rank ${yourRank ?? "—"})\nCurrent Cycle: ${otherMerits} ¤ (Rank ${otherRank ?? "—"})\nCycle window: ${cycleLabel}`;

  const listText = lines.length ? lines.join("\n") : "No data yet.";

  return new EmbedBuilder()
    .setTitle(title)
    .addFields(
      { name: "Top 25", value: listText.slice(0, 1024) || "No data yet.", inline: false },
      { name: "Your Dossier", value: dossier.slice(0, 1024) || "—", inline: false }
    );
}

function leaderboardComponents({ view }) {
  const btnAllTime = makeUiButton("[UI: Leaderboard-all-time-button]", { disabled: view === "all_time" });
  const btnCycle = makeUiButton("[UI: Leaderboard-current-cycle-button]", { disabled: view === "cycle" });
  const closeButton = btnClose(CUSTOM.TERMINAL_CLOSE, "Close");
  return buildRowsFromButtons([btnAllTime, btnCycle, closeButton]);
}

async function handleLeaderboardAccess(interaction) {
  await safeDeferEphemeral(interaction);
  const sess = ensureLeaderboardSession(interaction.guildId, interaction.user.id);
  sess.uiInteraction = interaction;
  if (!sess.view) sess.view = "all_time";

  const cache = await refreshLeaderboardCache(interaction.guildId, true);
  await editEphemeral(interaction, leaderboardEmbed({ view: sess.view, cache, viewerUserId: interaction.user.id }), leaderboardComponents({ view: sess.view }));
}

async function handleLeaderboardSetView(interaction, view) {
  await ackButton(interaction);
  const sess = ensureLeaderboardSession(interaction.guildId, interaction.user.id);
  sess.uiInteraction = interaction;
  sess.view = view;

  const cache = await refreshLeaderboardCache(interaction.guildId, true);
  await editEphemeral(interaction, leaderboardEmbed({ view: sess.view, cache, viewerUserId: interaction.user.id }), leaderboardComponents({ view: sess.view }));
}

function terminalMainMenuComponents() {
  const btnObjectives = makeUiButton(UI.T_BTN_OBJECTIVES);
  const btnEvents = makeUiButton(UI.T_BTN_EVENTS);
  const closeBtn = btnClose(CUSTOM.TERMINAL_CLOSE, "Close");
  return buildRowsFromButtons([btnObjectives, btnEvents, closeBtn]);
}
function terminalBackOnlyComponents() {
  return buildRowsFromButtons([
    new ButtonBuilder().setCustomId(CUSTOM.TERMINAL_BACK).setLabel("Back").setStyle(ButtonStyle.Secondary),
    btnClose(CUSTOM.TERMINAL_CLOSE, "Close"),
  ]);
}
function terminalObjectiveComponents({ hasPrev, hasNext, disableSubmit }) {
  const bSubmit = makeUiButton(UI.T_OBJ_BTN_SUBMIT, { disabled: disableSubmit });
  const bBack = makeUiButton(UI.T_OBJ_BTN_BACK);
  const bPrev = makeUiButton(UI.T_OBJ_BTN_PREV, { disabled: !hasPrev });
  const bNext = makeUiButton(UI.T_OBJ_BTN_NEXT, { disabled: !hasNext });
  return buildRowsFromButtons([bSubmit, bBack, bPrev, bNext]);
}

function terminalEventComponents({ hasPrev, hasNext }) {
  const bCheckIn = makeUiButton("[UI: Terminal-events-button-check-in]");
  const bBack = makeUiButton("[UI: Terminal-events-button-back]");
  const bPrev = makeUiButton("[UI: Terminal-events-button-previous]", { disabled: !hasPrev });
  const bNext = makeUiButton("[UI: Terminal-events-button-next]", { disabled: !hasNext });
  return buildRowsFromButtons([bCheckIn, bBack, bPrev, bNext]);
}



async function handleTerminalAccess(interaction) {
  await safeDeferEphemeral(interaction);
  const nextEmbed = terminalMainMenuEmbed();
  const nextComponents = terminalMainMenuComponents();
  await showTransitionThen(interaction, UI.T_BOOT, LOOPS.terminal_boot, nextEmbed, nextComponents);
}

async function refreshTerminalObjectiveCache(interaction) {
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.cachedObjectives = await dbListActiveObjectivesWithGifs(interaction.guildId);
  if (state.objectiveIndex < 0) state.objectiveIndex = 0;
  if (state.objectiveIndex > state.cachedObjectives.length - 1) state.objectiveIndex = state.cachedObjectives.length - 1;
}

async function refreshTerminalEventCache(interaction) {
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.cachedEvents = await dbListActiveEventsWithGifs(interaction.guildId);
  if (state.eventIndex < 0) state.eventIndex = 0;
  if (state.eventIndex > state.cachedEvents.length - 1) state.eventIndex = state.cachedEvents.length - 1;
}

async function showTerminalEvent(interaction) {
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalEventCache(interaction);

  const list = state.cachedEvents ?? [];
  if (!list.length) {
    const embed = terminalMainMenuEmbed("No Events are active right now.");
    await editEphemeral(interaction, embed, terminalBackOnlyComponents());
    return;
  }

  const evt = list[state.eventIndex];
  const embed = embedWithImageAndText(evt.main_url ?? null, ZWSP);

  const hasPrev = state.eventIndex > 0;
  const hasNext = state.eventIndex < list.length - 1;

  const checkedIn = await dbHasEventCheckin({ eventId: evt.id, userId: interaction.user.id }).catch(() => false);
  const canCheckIn = !checkedIn;

  await editEphemeral(interaction, embed, terminalEventComponents({ hasPrev, hasNext, canCheckIn, checkedIn }));
}

async function handleTerminalEvtBack(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalEventCache(interaction);
  const evt = (state.cachedEvents ?? [])[state.eventIndex];

  // Play per-event back GIF if available, then return to terminal main menu
  if (evt?.back_url) {
    await editEphemeral(interaction, embedWithImageAndText(evt.back_url, ZWSP), disabledRow());
    await sleepForUploadedEventGif("[UI: Terminal-transitional GIF-events-*event name*-back]");
  }
  await editEphemeral(interaction, terminalMainMenuEmbed(), terminalMainMenuComponents());
}


async function handleTerminalEvtPrev(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.eventIndex -= 1;
  await showTerminalEvent(interaction);
}
async function handleTerminalEvtNext(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.eventIndex += 1;
  await showTerminalEvent(interaction);
}

async function handleTerminalEvtCheckIn(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalEventCache(interaction);
  const evt = (state.cachedEvents ?? [])[state.eventIndex];
  if (!evt) return;

  // Prevent double check-in (idempotent)
  const inserted = await dbTryInsertEventCheckin({
    guildId: interaction.guildId,
    eventId: evt.id,
    userId: interaction.user.id,
  }).catch(() => false);

  // Transition: play event's check-in GIF (uploaded per-event) for 1 loop, disable buttons
  const transEmbed = embedWithImageAndText(evt.check_in_url ?? null, ZWSP);
  await editEphemeral(interaction, transEmbed, disabledRow());

  await sleepForUploadedEventGif("[UI: Terminal-transitional GIF-events-*event name*-check-in]");

  // Award merits only on first check-in
  if (inserted) {
    await dbAddUserMerits({
      guildId: interaction.guildId,
      userId: interaction.user.id,
      delta: Number(evt.merits_awarded) || 0,
    }).catch(() => null);
  }

  // Assign role snapshot (best-effort)
  let roleAssignErr = null;
  try {
    const guild = interaction.guild ?? (await client.guilds.fetch(interaction.guildId));
    const member = await guild.members.fetch(interaction.user.id);
    if (evt.resolved_role_id) {
      const roleId = String(evt.resolved_role_id);
      if (!member.roles.cache.has(roleId)) {
        await member.roles.add(roleId).catch((e) => {
          roleAssignErr = e;
        });
      }
    }
  } catch (e) {
    roleAssignErr = e;
  }

  // DM text (rate-limited)
  if (canSendDm(interaction.user.id, "evt_checkin", 5000)) {
    const tmpl = getRender("[UI: Terminal-events-check-in DM text]")?.text ?? "You have checked-in to *Event name + ID*.";
    const label = `${evt.name} ID: ${evt.id}`;
    const msg = tmpl.replace("*Event name + ID*", label);
    await safeSendDm(interaction.user, msg);
  }

  const extra = [];
  if (!inserted) extra.push("You have already checked-in to this event.");
  if (roleAssignErr) extra.push("⚠️ I could not assign the event role (check bot permissions / role hierarchy).");

  // Show check-in screen + Try Again/Back
  const nextEmbed = buildScreenEmbed(["[UI: Terminal-events-check-in GIF]"], extra.join("\n"));
  const bTry = makeUiButton("[UI: Terminal-events-check-in-button-try-again]");
  const bBack = makeUiButton("[UI: Terminal-events-check-in-button-back]");
  await editEphemeral(interaction, nextEmbed, buildRowsFromButtons([bTry, bBack]));
}


async function handleTerminalEvtCheckInTryAgain(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalEventCache(interaction);
  const evt = (state.cachedEvents ?? [])[state.eventIndex];
  if (!evt) return;

  const transEmbed = buildScreenEmbed(["[UI: Terminal-transitional GIF-events-check-in-try-again]"]);
  await editEphemeral(interaction, transEmbed, disabledRow());

  const waitMs = Math.round(gifDurationSeconds("[UI: Terminal-transitional GIF-events-check-in-try-again]") * 1000);
  await sleep(waitMs);

  // Resend DM (rate-limited) + ensure role
  try {
    const guild = await client.guilds.fetch(interaction.guildId);
    const member = await guild.members.fetch(interaction.user.id);
    if (evt.resolved_role_id && !member.roles.cache.has(String(evt.resolved_role_id))) {
      await member.roles.add(String(evt.resolved_role_id)).catch(() => null);
    }
  } catch {}

  if (canSendDm(interaction.user.id, "evt_checkin", 5000)) {
    const tmpl = getRender("[UI: Terminal-events-check-in DM text]")?.text ?? "You have checked-in to *Event name + ID*.";
    const label = `${evt.name} ID: ${evt.id}`;
    const msg = tmpl.replace("*Event name + ID*", label);
    await safeSendDm(interaction.user, msg);
  }

  const nextEmbed = buildScreenEmbed(["[UI: Terminal-events-check-in GIF]"]);
  const bTry = makeUiButton("[UI: Terminal-events-check-in-button-try-again]");
  const bBack = makeUiButton("[UI: Terminal-events-check-in-button-back]");
  await editEphemeral(interaction, nextEmbed, buildRowsFromButtons([bTry, bBack]));
}

async function handleTerminalEvtCheckInBack(interaction) {
  await ackButton(interaction);
  const s = ensureTerminalSession(interaction.guildId, interaction.user.id);

  // Transitional GIF: static check-in back (not the per-event back GIF)
  await editEphemeral(
    interaction,
    embedWithImageAndText(getRender("[UI: Terminal-transitional GIF-events-check-in-back]")?.url ?? null, ""),
    []
  );
  await sleep(Math.round(gifDurationSeconds("[UI: Terminal-transitional GIF-events-check-in-back]") * 1000));

  // Always return to Events carousel (not terminal boot)
  return await showTerminalEvent(interaction);
}







async function showTerminalObjective(interaction) {
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalObjectiveCache(interaction);

  const list = state.cachedObjectives ?? [];
  if (!list.length) {
    const embed = terminalMainMenuEmbed("No Objectives are active right now.");
    await editEphemeral(interaction, embed, terminalBackOnlyComponents());
    return;
  }

  const obj = list[state.objectiveIndex];

  const latestStatus = await dbGetLatestObjectiveSubmissionStatus({ guildId: interaction.guildId, objectiveId: obj.id, userId: interaction.user.id }).catch(() => "");
  const disableSubmit = (latestStatus === "approved" || latestStatus === "accepted") || latestStatus === "accepted" || latestStatus === "pending_review";

  const embed = embedWithImageAndText(obj.main_url ?? null, ZWSP);

  const hasPrev = state.objectiveIndex > 0;
  const hasNext = state.objectiveIndex < list.length - 1;
  await editEphemeral(interaction, embed, terminalObjectiveComponents({ hasPrev, hasNext, disableSubmit }));
}

async function handleTerminalObjectives(interaction) {
  await ackButton(interaction);

  // OPTION 2: entering Objectives always starts at the first objective
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.objectiveIndex = 0;

  const transEmbed = buildScreenEmbed([UI.T_OBJECTIVES_TRANS]);
  await editEphemeral(interaction, transEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(UI.T_OBJECTIVES_TRANS) * 1000));

  await showTerminalObjective(interaction);
}

async function handleTerminalEvents(interaction) {
  await ackButton(interaction);

  // entering Events always starts at first event
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.eventIndex = 0;

  const transEmbed = buildScreenEmbed([UI.T_EVENTS_TRANS]);
  await editEphemeral(interaction, transEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(UI.T_EVENTS_TRANS) * 1000));

  await showTerminalEvent(interaction);
}


async function handleTerminalBack(interaction) {
  await ackButton(interaction);
  await editEphemeral(interaction, terminalMainMenuEmbed(), terminalMainMenuComponents());
}
async function handleTerminalClose(interaction) {
  await ackButton(interaction);

  // OPTIONAL (still option 2 friendly): closing clears your browsing state
  terminalSession.delete(sessionKey(interaction.guildId, interaction.user.id));

  await editEphemeral(interaction, embedWithImageAndText(null, "Closed."), []);
}

async function handleTerminalObjPrev(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.objectiveIndex -= 1;
  await showTerminalObjective(interaction);
}
async function handleTerminalObjNext(interaction) {
  await ackButton(interaction);
  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  state.objectiveIndex += 1;
  await showTerminalObjective(interaction);
}
async function handleTerminalObjBack(interaction) {
  await ackButton(interaction);
  await handleTerminalBack(interaction);
}
async function handleTerminalObjSubmit(interaction) {
  await ackButton(interaction);

  const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
  await refreshTerminalObjectiveCache(interaction);

  const obj = state.cachedObjectives[state.objectiveIndex];
  if (!obj) {
    const embed = embedWithImageAndText(null, "Objective not found.");
    await editEphemeral(interaction, embed, terminalBackOnlyComponents());
    return;
  }

  // Transitional GIF (system asset)
  const transEmbed = embedWithImageAndText(obj.submit_url ?? null, ZWSP);
  await editEphemeral(interaction, transEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(UI.T_OBJ_SUBMIT_TRANS) * 1000));

  // Submit screen GIF loops (system asset)
  const submitGifUrl = getRender(UI.T_OBJ_SUBMIT_GIF).url ?? null;
  const submitEmbed = embedWithImageAndText(submitGifUrl, ZWSP);
  await editEphemeral(
    interaction,
    submitEmbed,
    buildRowsFromButtons([makeUiButton(UI.T_OBJ_SUBMIT_TRY_AGAIN), makeUiButton(UI.T_OBJ_SUBMIT_BACK)])
  );

  // Create/refresh DM submission session
  const key = sessionKey(interaction.guildId, interaction.user.id);
  const alreadyPending = await dbHasPendingObjectiveSubmission({
    guildId: interaction.guildId,
    objectiveId: obj.id,
    userId: interaction.user.id,
  }).catch(() => false);

  objectiveSubmitSession.set(key, {
    objectiveId: obj.id,
    objectiveName: obj.name,
    proofType: obj.proof_type,
    expiresAt: Date.now() + UPLOAD_TIMEOUT_MS,
    submitted: alreadyPending,
  });

  const vars = {
    "*Objective name + ID*": `${obj.name} ID: ${obj.id}`,
    "*Discord user ID*": interaction.user.id,
  };

  try {
    if (alreadyPending) {
      const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY).text ?? "", vars) || ZWSP;
      const row = new ActionRowBuilder().addComponents(
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_A),
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_B)
      );
      await interaction.user.send({ content: txt, components: [row] });
    } else {
      const dmRef = dmUiRefForProofType(obj.proof_type);
      let txt = applyTemplate(getRender(dmRef).text ?? "", vars) || ZWSP;

      // Append accepted proof types
      let acceptedTypes = "";
      if (obj.proof_type === "image") acceptedTypes = "Accepted file types: PNG, JPG, JPEG, WEBP.";
      else if (obj.proof_type === "video") acceptedTypes = "Accepted file types: MP4, MOV.";
      else if (obj.proof_type === "audio") acceptedTypes = "Accepted file types: WAV, MP3.";
      else if (obj.proof_type === "text_file") acceptedTypes = "Accepted file types: PDF, DOCX, TXT.";
      else acceptedTypes = "Accepted proof: send a text message (no file required).";

      if (acceptedTypes) txt = txt + "\n\n" + acceptedTypes;

      await interaction.user.send({ content: txt });
    }
  } catch (_) {
    // DM failures are non-fatal
  }
}


async function startObjectiveDmSubmission({ guildId, user, objectiveId }) {
  const obj = await dbGetObjectiveWithGifs(guildId, objectiveId).catch(() => null);
  if (!obj) return false;

  // Guard: if latest status is approved/accepted, do not allow resubmission
  const latestStatus = await dbGetLatestObjectiveSubmissionStatus({
    guildId,
    objectiveId: obj.id,
    userId: user.id,
  }).catch(() => "");

  if (latestStatus === "accepted" || latestStatus === "approved") {
    try {
      await user.send({ content: `This Objective has already been approved. You cannot submit it again.` });
    } catch (_) {}
    return true;
  }

  const key = sessionKey(guildId, user.id);
  const alreadyPending = await dbHasPendingObjectiveSubmission({
    guildId,
    objectiveId: obj.id,
    userId: user.id,
  }).catch(() => false);

  objectiveSubmitSession.set(key, {
    objectiveId: obj.id,
    objectiveName: obj.name,
    proofType: obj.proof_type,
    expiresAt: Date.now() + UPLOAD_TIMEOUT_MS,
    submitted: alreadyPending,
  });

  const vars = {
    "*Objective name + ID*": `${obj.name} ID: ${obj.id}`,
    "*Discord user ID*": String(user.id),
  };

  try {
    if (alreadyPending) {
      const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY).text ?? "", vars) || ZWSP;
      const row = new ActionRowBuilder().addComponents(
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_A),
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_B)
      );
      await user.send({ content: txt, components: [row] });
    } else {
      const dmRef = dmUiRefForProofType(obj.proof_type);
      let txt = applyTemplate(getRender(dmRef).text ?? "", vars) || ZWSP;

      // Append accepted proof / file types
      let acceptedTypes = "";
      if (obj.proof_type === "image") acceptedTypes = "Accepted file types: PNG, JPG, JPEG, WEBP.";
      else if (obj.proof_type === "video") acceptedTypes = "Accepted file types: MP4, MOV.";
      else if (obj.proof_type === "audio") acceptedTypes = "Accepted file types: WAV, MP3.";
      else if (obj.proof_type === "text_file") acceptedTypes = "Accepted file types: PDF, DOCX, TXT.";
      else acceptedTypes = "Accepted proof: send a text message (no file required).";

      if (acceptedTypes) txt = txt + "\n\n" + acceptedTypes;

      await user.send({ content: txt });
    }
  } catch (_) {
    return false;
  }

  return true;
}


/* =========================
   OPERATOR TERMINAL FLOW
========================= */
function opMenuEmbed() {
  const txt = getRender(UI.OP_MENU_TEXT).text ?? "";
  return embedWithImageAndText(null, txt || ZWSP);
}
function opMenuComponents() {
  const bObj = makeUiButton(UI.OP_BTN_OBJECTIVES);
  const bEvents = makeUiButton(UI.OP_BTN_EVENTS);
  const bCmd = makeUiButton(UI.OP_BTN_COMMANDS);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");
  return buildRowsFromButtons([bObj, bEvents, bCmd, bClose]);
}

function opObjectivesMenuEmbed() {
  const txt = getRender(UI.OP_OBJ_MENU_TEXT).text ?? "";
  return embedWithImageAndText(null, txt || ZWSP);
}
function opObjectivesMenuComponents() {
  const bUpload = makeUiButton(UI.OP_OBJ_BTN_UPLOAD);
  const bSched = makeUiButton(UI.OP_OBJ_BTN_SCHED);
  const bBack = makeUiButton(UI.OP_OBJ_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");
  return buildRowsFromButtons([bUpload, bSched, bBack, bClose]);
}


/* =========================
   OBJECTIVE SCHEDULING (LIST / EDIT / DELETE)
========================= */
const OBJECTIVE_SCHED_EXPIRED_VISIBLE_DAYS = 7;
const EDIT_DRAFT_TTL_MS = 24 * 60 * 60 * 1000;

const objectiveEditDrafts = new Map(); // key = guild:user:objectiveId -> { base, draft, updatedAt }


/* =========================
   EVENT SCHEDULING (LIST / EDIT / DELETE)
========================= */
const EVENT_SCHED_EXPIRED_VISIBLE_DAYS = 7;
const eventEditDrafts = new Map(); // key = guild:user:eventId -> { base, draft, updatedAt }


/* ---------- DB: scheduling list ---------- */
async function dbListObjectivesForSchedulingWithGifs(guildId) {
  const [rows] = await pool.query(
    `
    SELECT
      o.id,
      o.name,
      o.merits_awarded,
      o.proof_type,
      o.schedule_begin_at,
      o.schedule_end_at,
      o.status,
      o.created_at,
      o.updated_at,
      g.main_url,
      g.submit_url,
      g.back_url
    FROM objectives o
    JOIN objective_gifs g ON g.objective_id = o.id
    WHERE o.guild_id = ?
      AND o.status <> 'deleted'
      AND (
        UTC_TIMESTAMP() < o.schedule_end_at
        OR o.schedule_end_at >= (UTC_TIMESTAMP() - INTERVAL ? DAY)
      )
    ORDER BY o.schedule_begin_at ASC, o.id ASC
  `,
    [guildId, OBJECTIVE_SCHED_EXPIRED_VISIBLE_DAYS]
  );

  return rows ?? [];
}

async function dbGetObjectiveWithGifs(guildId, objectiveId) {
  const [rows] = await pool.query(
    `
    SELECT
      o.id,
      o.name,
      o.merits_awarded,
      o.proof_type,
      o.schedule_begin_at,
      o.schedule_end_at,
      o.status,
      o.created_at,
      o.updated_at,
      g.main_url,
      g.submit_url,
      g.back_url
    FROM objectives o
    JOIN objective_gifs g ON g.objective_id = o.id
    WHERE o.guild_id = ?
      AND o.id = ?
      AND o.status <> 'deleted'
    LIMIT 1
  `,
    [guildId, objectiveId]
  );

  return rows?.[0] ?? null;
}

async function dbSoftDeleteObjective(guildId, objectiveId) {
  const updatedAt = nowUtcJsDate();
  await pool.query(
    `
    UPDATE objectives
    SET status = 'deleted', updated_at = ?
    WHERE guild_id = ? AND id = ? AND status <> 'deleted'
  `,
    [updatedAt, guildId, objectiveId]
  );
}

async function dbUpdateObjectiveAndMaybeGifs({
  guildId,
  objectiveId,
  name,
  meritsAwarded,
  proofType,
  beginUtc,
  endUtc,
  gifMainUrl,
  gifSubmitUrl,
  gifBackUrl,
}) {
  const updatedAt = nowUtcJsDate();
  const status = computeObjectiveStatus(beginUtc, endUtc);

  await pool.query(
    `
    UPDATE objectives
    SET name = ?, merits_awarded = ?, proof_type = ?,
        schedule_begin_at = ?, schedule_end_at = ?,
        status = ?, updated_at = ?
    WHERE guild_id = ? AND id = ? AND status <> 'deleted'
  `,
    [name, meritsAwarded, proofType, beginUtc, endUtc, status, updatedAt, guildId, objectiveId]
  );

  if (gifMainUrl || gifSubmitUrl || gifBackUrl) {
    const current = await dbGetObjectiveWithGifs(guildId, objectiveId);
    if (!current) return;

    await pool.query(
      `
      UPDATE objective_gifs
      SET main_url = ?, submit_url = ?, back_url = ?
      WHERE objective_id = ?
    `,
      [
        gifMainUrl || current.main_url,
        gifSubmitUrl || current.submit_url,
        gifBackUrl || current.back_url,
        objectiveId,
      ]
    );
  }
}


// ---------- DB: event scheduling list ----------
async function dbListEventsForSchedulingWithGifs(guildId) {
  const [rows] = await pool.query(
    `
    SELECT
      e.id,
      e.name,
      e.merits_awarded,
      e.check_in_role_key,
      e.resolved_role_id,
      e.schedule_begin_at,
      e.schedule_end_at,
      e.status,
      e.created_at,
      e.updated_at,
      g.main_url,
      g.check_in_url,
      g.back_url
    FROM events e
    JOIN event_gifs g ON g.event_id = e.id
    WHERE e.guild_id = ?
      AND e.status <> 'deleted'
      AND (
        UTC_TIMESTAMP() < e.schedule_end_at
        OR e.schedule_end_at >= (UTC_TIMESTAMP() - INTERVAL ? DAY)
      )
    ORDER BY e.schedule_begin_at ASC, e.id ASC
  `,
    [guildId, EVENT_SCHED_EXPIRED_VISIBLE_DAYS]
  );
  const out = (rows ?? []).map((r) => ({
    ...r,
    schedule_begin_at: coerceToDate(r.schedule_begin_at) ?? r.schedule_begin_at,
    schedule_end_at: coerceToDate(r.schedule_end_at) ?? r.schedule_end_at,
  }));
  return out;
}

async function dbGetEventWithGifs(guildId, eventId) {
  const [rows] = await pool.query(
    `
    SELECT
      e.id,
      e.name,
      e.merits_awarded,
      e.check_in_role_key,
      e.resolved_role_id,
      e.schedule_begin_at,
      e.schedule_end_at,
      e.status,
      e.created_at,
      e.updated_at,
      g.main_url,
      g.check_in_url,
      g.back_url
    FROM events e
    JOIN event_gifs g ON g.event_id = e.id
    WHERE e.guild_id = ?
      AND e.id = ?
      AND e.status <> 'deleted'
    LIMIT 1
  `,
    [guildId, eventId]
  );
  return rows?.[0] ?? null;
}

async function dbSoftDeleteEvent(guildId, eventId) {
  const updatedAt = nowUtcJsDate();
  await pool.query(
    `
    UPDATE events
    SET status = 'deleted', updated_at = ?
    WHERE guild_id = ? AND id = ? AND status <> 'deleted'
  `,
    [updatedAt, guildId, eventId]
  );
}

async function dbUpdateEventAndMaybeGifs({
  guildId,
  eventId,
  name,
  meritsAwarded,
  checkInRoleKey,
  resolvedRoleId,
  beginUtc,
  endUtc,
  gifMainUrl,
  gifCheckInUrl,
  gifBackUrl,
}) {
  const updatedAt = nowUtcJsDate();
  const status = computeObjectiveStatus(beginUtc, endUtc); // same logic (scheduled/active/expired)

  await pool.query(
    `
    UPDATE events
    SET name = ?, merits_awarded = ?, check_in_role_key = ?, resolved_role_id = ?,
        schedule_begin_at = ?, schedule_end_at = ?,
        status = ?, updated_at = ?
    WHERE guild_id = ? AND id = ? AND status <> 'deleted'
  `,
    [name, meritsAwarded, checkInRoleKey, resolvedRoleId, beginUtc, endUtc, status, updatedAt, guildId, eventId]
  );

  if (gifMainUrl || gifCheckInUrl || gifBackUrl) {
    const current = await dbGetEventWithGifs(guildId, eventId);
    if (!current) return;

    await pool.query(
      `
      UPDATE event_gifs
      SET main_url = ?, check_in_url = ?, back_url = ?
      WHERE event_id = ?
    `,
      [
        gifMainUrl || current.main_url,
        gifCheckInUrl || current.check_in_url,
        gifBackUrl || current.back_url,
        eventId,
      ]
    );
  }
}

/* ---------- scheduling state helpers ---------- */
function ensureObjectiveSchedulingState(guildId, userId) {
  const s = ensureOperatorSession(guildId, userId);
  if (!s.objectiveScheduling) s.objectiveScheduling = { index: 0, cached: [], lastLoadedAt: 0 };
  return s.objectiveScheduling;
}

async function refreshObjectiveSchedulingCache(interaction) {
  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  st.cached = await dbListObjectivesForSchedulingWithGifs(interaction.guildId);
  st.lastLoadedAt = Date.now();

  if (st.index < 0) st.index = 0;
  if (st.index > st.cached.length - 1) st.index = st.cached.length - 1;
}

function objectiveSchedulingCurrent(st) {
  const list = st.cached ?? [];
  if (!list.length) return null;
  const idx = Math.max(0, Math.min(st.index, list.length - 1));
  return list[idx] ?? null;
}

function fmtPtFromMysqlUtc(mysqlDate) {
  // mysql2 returns JS Date for DATETIME; interpret as UTC (we stored UTC)
  const dt = DateTime.fromJSDate(mysqlDate, { zone: "utc" }).setZone(PT_ZONE);
  return dt.toFormat("yyyy-MM-dd HH:mm");
}

function coerceToDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

function computeObjectiveStatusFromRow(row) {
  const beginUtc = DateTime.fromJSDate(row.schedule_begin_at, { zone: "utc" }).toJSDate();
  const endUtc = DateTime.fromJSDate(row.schedule_end_at, { zone: "utc" }).toJSDate();
  return computeObjectiveStatus(beginUtc, endUtc);
}

function computeEventStatusFromRow(row) {
  if (!row) return "unknown";
  if (row.status === "deleted") return "deleted";
  const now = Date.now();
  const begin = new Date(row.schedule_begin_at).getTime();
  const end = new Date(row.schedule_end_at).getTime();
  if (!Number.isFinite(begin) || !Number.isFinite(end)) return "unknown";
  if (now >= begin && now <= end) return "active";
  if (now < begin) return "scheduled";
  return "expired";
}


function schedulingMenuEmbed(current, index, total) {
  const base = getRender(UI.OP_OBJ_SCHED_MENU_TEXT).text ?? "";
  if (!current) return embedWithImageAndText(null, (base + "\n\nNo Objectives found.").trim());

  const status = computeObjectiveStatusFromRow(current);
  const beginPt = fmtPtFromMysqlUtc(current.schedule_begin_at);
  const endPt = fmtPtFromMysqlUtc(current.schedule_end_at);

  const header = `Objective ${index + 1} of ${total}`;
  const lines = [
    header,
    "",
    `**Status:** ${status}`,
    `**Objective Name:** ${current.name}`,
    `**Merits Awarded:** ${current.merits_awarded}`,
    `**Schedule (PT):** ${beginPt} → ${endPt}`,
    `**Proof File Type:** ${prettyProofType(current.proof_type)}`,
  ];

  return embedWithImageAndText(null, [base, "", lines.join("\n")].filter(Boolean).join("\n"));
}

function schedulingMenuComponents(current, index, total) {
  const hasPrev = total > 0 && index > 0;
  const hasNext = total > 0 && index < total - 1;

  const status = current ? computeObjectiveStatusFromRow(current) : "none";
  const deleteDisabled = !current || status === "active";

  const bEdit = makeUiButton(UI.OP_OBJ_SCHED_BTN_EDIT, { disabled: !current });
  const bDelete = makeUiButton(UI.OP_OBJ_SCHED_BTN_DELETE, { disabled: deleteDisabled });
  const bPrev = makeUiButton(UI.OP_OBJ_SCHED_BTN_PREV, { disabled: !hasPrev });
  const bNext = makeUiButton(UI.OP_OBJ_SCHED_BTN_NEXT, { disabled: !hasNext });
  const bBack = makeUiButton(UI.OP_OBJ_SCHED_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");

  return buildRowsFromButtons([bEdit, bDelete, bPrev, bNext, bBack, bClose]);
}

/* ---------- delete confirm ---------- */
function deleteConfirmEmbed(current) {
  const base = getRender(UI.OP_OBJ_SCHED_DELETE_MENU_TEXT).text ?? "";
  if (!current) return embedWithImageAndText(null, (base + "\n\nNo Objective selected.").trim());
  return embedWithImageAndText(null, [base, "", `Delete **${current.name}**?`].filter(Boolean).join("\n"));
}
function deleteConfirmComponents(current) {
  const status = current ? computeObjectiveStatusFromRow(current) : "none";
  const confirmDisabled = !current || status === "active";
  const bConfirm = makeUiButton(UI.OP_OBJ_SCHED_DELETE_BTN_CONFIRM, { disabled: confirmDisabled });
  const bBack = makeUiButton(UI.OP_OBJ_SCHED_DELETE_BTN_BACK);
  return buildRowsFromButtons([bConfirm, bBack, btnClose(CUSTOM.OP_CLOSE, "Close")]);
}

/* ---------- edit drafts ---------- */
function editDraftKey(guildId, userId, objectiveId) {
  return `${guildId}:${userId}:${objectiveId}`;
}

function getOrCreateEditDraft(guildId, userId, objectiveRow) {
  const key = editDraftKey(guildId, userId, objectiveRow.id);
  const existing = objectiveEditDrafts.get(key);

  if (existing && Date.now() - existing.updatedAt <= EDIT_DRAFT_TTL_MS) return existing;

  const base = {
    name: objectiveRow.name,
    merits_awarded: Number(objectiveRow.merits_awarded),
    schedule_begin: fmtPtFromMysqlUtc(objectiveRow.schedule_begin_at),
    schedule_end: fmtPtFromMysqlUtc(objectiveRow.schedule_end_at),
    proof_type: objectiveRow.proof_type,
    gifs: { main: objectiveRow.main_url, submit: objectiveRow.submit_url, back: objectiveRow.back_url },
  };

  const entry = {
    base,
    draft: JSON.parse(JSON.stringify(base)),
    updatedAt: Date.now(),
  };
  objectiveEditDrafts.set(key, entry);
  return entry;
}

function touchEditDraft(guildId, userId, objectiveId) {
  const key = editDraftKey(guildId, userId, objectiveId);
  const d = objectiveEditDrafts.get(key);
  if (d) d.updatedAt = Date.now();
}

function clearEditDraft(guildId, userId, objectiveId) {
  objectiveEditDrafts.delete(editDraftKey(guildId, userId, objectiveId));
}

function diffChanged(entry) {
  const b = entry.base;
  const d = entry.draft;

  const gifUpdates = [];
  if (d.gifs.main !== b.gifs.main) gifUpdates.push("main");
  if (d.gifs.submit !== b.gifs.submit) gifUpdates.push("submit");
  if (d.gifs.back !== b.gifs.back) gifUpdates.push("back");

  return {
    any:
      d.name !== b.name ||
      d.merits_awarded !== b.merits_awarded ||
      d.schedule_begin !== b.schedule_begin ||
      d.schedule_end !== b.schedule_end ||
      d.proof_type !== b.proof_type ||
      gifUpdates.length > 0,
    name: d.name !== b.name,
    merits: d.merits_awarded !== b.merits_awarded,
    schedule: d.schedule_begin !== b.schedule_begin || d.schedule_end !== b.schedule_end,
    proof: d.proof_type !== b.proof_type,
    gifUpdates,
  };
}

function editMenuLine(label, value, changed) {
  const box = changed ? "🟦" : "⬜";
  return `${box} ${label}: ${value}`;
}

function editMenuEmbed(objectiveRow, entry) {
  const base = getRender(UI.OP_OBJ_SCHED_EDIT_MENU_TEXT).text ?? "";
  if (!objectiveRow || !entry) return embedWithImageAndText(null, (base + "\n\nNo Objective selected.").trim());

  const d = entry.draft;
  const dif = diffChanged(entry);

  const gifText = dif.gifUpdates.length ? dif.gifUpdates.join(", ") : "none";

  const lines = [
    editMenuLine("Objective Name", d.name || "(empty)", dif.name),
    editMenuLine("Merits Awarded", String(d.merits_awarded ?? "(empty)"), dif.merits),
    editMenuLine("Schedule (PT)", `${d.schedule_begin} → ${d.schedule_end}`, dif.schedule),
    editMenuLine("Proof File Type", prettyProofType(d.proof_type), dif.proof),
    editMenuLine("GIF Updates", gifText, dif.gifUpdates.length > 0),
  ];

  return embedWithImageAndText(null, [base, "", lines.join("\n")].filter(Boolean).join("\n"));
}

function editMenuComponents(entry) {
  const dif = entry ? diffChanged(entry) : { any: false };
  const bName = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_NAME);
  const bMerits = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_MERITS);
  const bSchedule = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_SCHEDULE);
  const bFileType = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_FILETYPE);
  const bGif = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_GIF);

  const bSubmit = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_SUBMIT, { disabled: !dif.any });
  const bBack = makeUiButton(UI.OP_OBJ_SCHED_EDIT_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");

  // MUST match upload menu order:
  // Objective Name → Merits Awarded → Schedule → Proof File Type → GIF → Submit → Back
  return buildRowsFromButtons([bName, bMerits, bSchedule, bFileType, bGif, bSubmit, bBack, bClose]);
}

/* ---------- edit: file type menu ---------- */
function opEditFileTypeEmbed(session) {
  const base = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_MENU).text ?? "Select file type:";
  const pick = session.tempFileType ? `\n\nSelected: **${prettyProofType(session.tempFileType)}**` : "";
  return embedWithImageAndText(null, (base + pick) || ZWSP);
}
function opEditFileTypeComponents(session) {
  const pick = session.tempFileType;

  const row1 = new ActionRowBuilder().addComponents(
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_IMAGE),
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_AUDIO),
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_TEXT),
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_TEXT_FILE)
  );
  const row2 = new ActionRowBuilder().addComponents(
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_SUBMIT, { disabled: !pick }),
    makeUiButton(UI.OP_OBJ_SCHED_EDIT_FILETYPE_BACK)
  );

  return [row1, row2];
}

/* ---------- edit: gif menu (partial updates allowed) ---------- */
function opEditGifMenuEmbed(entry) {
  const url = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_MENU).url;
  const d = entry.draft;
  const b = entry.base;

  const updates = [];
  if (d.gifs.main !== b.gifs.main) updates.push("main");
  if (d.gifs.submit !== b.gifs.submit) updates.push("submit");
  if (d.gifs.back !== b.gifs.back) updates.push("back");

  const txt = "GIF updates (optional):\n\n" + (updates.length ? `Updated: **${updates.join(", ")}**` : "Updated: **none**");
  return embedWithImageAndText(url ?? null, txt);
}
function opEditGifMenuComponents(entry) {
  const bMain = makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_MAIN);
  const bSubmitFile = makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_SUBMIT);
  const bBackFile = makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_BACK);

  const bSubmit = makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_SUBMIT_BTN);
  const bBackBtn = makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_BACK_BTN);

  return buildRowsFromButtons([bMain, bSubmitFile, bBackFile, bSubmit, bBackBtn]);
}

/* ---------- modals (scheduling edit) ---------- */
const EDIT_MODALS = {
  NAME: "modal_obj_edit_name",
  MERITS: "modal_obj_edit_merits",
  SCHEDULE: "modal_obj_edit_schedule",
};
const EDIT_MODAL_INPUTS = {
  NAME: "input_obj_edit_name",
  MERITS: "input_obj_edit_merits",
  BEGIN: "input_obj_edit_begin",
  END: "input_obj_edit_end",
};


const EVENT_EDIT_MODALS = {
  NAME: "event_sched_edit_name_modal",
  MERITS: "modal_evt_sched_edit_merits",
  SCHEDULE: "modal_evt_sched_edit_schedule",
};
const EVENT_EDIT_MODAL_INPUTS = {
  NAME: "event_sched_edit_name",
  MERITS: "input_evt_sched_edit_merits",
  BEGIN: "input_evt_sched_edit_begin",
  END: "input_evt_sched_edit_end",
};

async function showEventEditNameModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EVENT_EDIT_MODALS.NAME).setTitle("Event Name");
  const input = new TextInputBuilder()
    .setCustomId(EVENT_EDIT_MODAL_INPUTS.NAME)
    .setLabel("Event Name")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(64)
    .setRequired(true)
    .setValue(String(entry?.draft?.name ?? ""));

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEventEditMeritsModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EVENT_EDIT_MODALS.MERITS).setTitle("Merits Awarded");
  const input = new TextInputBuilder()
    .setCustomId(EVENT_EDIT_MODAL_INPUTS.MERITS)
    .setLabel("Merits (1-9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true);

  // Pre-fill with current draft value
  if (entry?.draft?.merits_awarded != null) input.setValue(String(entry.draft.merits_awarded));

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEventEditScheduleModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EVENT_EDIT_MODALS.SCHEDULE).setTitle("Event Schedule (PT)");
  const begin = new TextInputBuilder()
    .setCustomId(EVENT_EDIT_MODAL_INPUTS.BEGIN)
    .setLabel("Begin (YYYY-MM-DD HH:mm)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(16)
    .setMaxLength(16)
    .setRequired(true);
  const end = new TextInputBuilder()
    .setCustomId(EVENT_EDIT_MODAL_INPUTS.END)
    .setLabel("End (YYYY-MM-DD HH:mm)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(16)
    .setMaxLength(16)
    .setRequired(true);

  const b = entry?.draft?.schedule_begin_at ? DateTime.fromJSDate(coerceToDate(entry.draft.schedule_begin_at)).setZone(PT_ZONE) : null;
  const e = entry?.draft?.schedule_end_at ? DateTime.fromJSDate(coerceToDate(entry.draft.schedule_end_at)).setZone(PT_ZONE) : null;
  if (b && b.isValid) begin.setValue(b.toFormat("yyyy-MM-dd HH:mm"));
  if (e && e.isValid) end.setValue(e.toFormat("yyyy-MM-dd HH:mm"));

  modal.addComponents(new ActionRowBuilder().addComponents(begin));
  modal.addComponents(new ActionRowBuilder().addComponents(end));
  await interaction.showModal(modal);
}


async function showEditNameModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EDIT_MODALS.NAME).setTitle("Objective Name");
  const input = new TextInputBuilder()
    .setCustomId(EDIT_MODAL_INPUTS.NAME)
    .setLabel("Objective Name")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(64)
    .setRequired(true)
    .setValue(String(entry?.draft?.name ?? ""));

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEditMeritsModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EDIT_MODALS.MERITS).setTitle("Merits Awarded");
  const input = new TextInputBuilder()
    .setCustomId(EDIT_MODAL_INPUTS.MERITS)
    .setLabel("Merits Awarded (1–9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true)
    .setValue(entry?.draft?.merits_awarded != null ? String(entry.draft.merits_awarded) : "");

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEditScheduleModal(interaction, entry) {
  const modal = new ModalBuilder().setCustomId(EDIT_MODALS.SCHEDULE).setTitle("Objective Schedule (PT)");

  const begin = new TextInputBuilder()
    .setCustomId(EDIT_MODAL_INPUTS.BEGIN)
    .setLabel("Begin (PT) — YYYY-MM-DD or YYYY-MM-DD HH:mm")
    .setStyle(TextInputStyle.Short)
    .setMinLength(5)
    .setMaxLength(32)
    .setRequired(true)
    .setValue(String(entry?.draft?.schedule_begin ?? ""));

  const end = new TextInputBuilder()
    .setCustomId(EDIT_MODAL_INPUTS.END)
    .setLabel("End (PT) — YYYY-MM-DD or YYYY-MM-DD HH:mm")
    .setStyle(TextInputStyle.Short)
    .setMinLength(5)
    .setMaxLength(32)
    .setRequired(true)
    .setValue(String(entry?.draft?.schedule_end ?? ""));

  modal.addComponents(new ActionRowBuilder().addComponents(begin), new ActionRowBuilder().addComponents(end));
  await interaction.showModal(modal);
}

/* ---------- scheduling handlers ---------- */
async function handleObjectiveSchedulingOpen(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_sched_menu";
  s.uiInteraction = interaction;

  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  await refreshObjectiveSchedulingCache(interaction);

  const cur = objectiveSchedulingCurrent(st);
  const total = st.cached.length;
  await editEphemeral(interaction, schedulingMenuEmbed(cur, st.index, total), schedulingMenuComponents(cur, st.index, total));
}

async function handleObjectiveSchedulingBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_objectives_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opObjectivesMenuEmbed(), opObjectivesMenuComponents());
}

async function handleObjectiveSchedulingPrev(interaction) {
  await ackButton(interaction);
  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  st.index -= 1;
  await handleObjectiveSchedulingOpen(interaction);
}
async function handleObjectiveSchedulingNext(interaction) {
  await ackButton(interaction);
  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  st.index += 1;
  await handleObjectiveSchedulingOpen(interaction);
}

async function handleObjectiveSchedulingDelete(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_sched_delete";
  s.uiInteraction = interaction;

  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  await refreshObjectiveSchedulingCache(interaction);
  const cur = objectiveSchedulingCurrent(st);

  await editEphemeral(interaction, deleteConfirmEmbed(cur), deleteConfirmComponents(cur));
}

async function handleObjectiveSchedulingDeleteBack(interaction) {
  await ackButton(interaction);
  await handleObjectiveSchedulingOpen(interaction);
}

async function handleObjectiveSchedulingDeleteConfirm(interaction) {
  await ackButton(interaction);

  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  await refreshObjectiveSchedulingCache(interaction);
  const cur = objectiveSchedulingCurrent(st);
  if (!cur) return await handleObjectiveSchedulingOpen(interaction);

  const status = computeObjectiveStatusFromRow(cur);
  if (status === "active") return await handleObjectiveSchedulingOpen(interaction);

  try {
    await dbSoftDeleteObjective(interaction.guildId, cur.id);
  } catch (e) {
    console.error("Delete objective failed:", e);
  }

  await refreshObjectiveSchedulingCache(interaction);
  const cur2 = objectiveSchedulingCurrent(st);
  const total = st.cached.length;

  await editEphemeral(
    interaction,
    schedulingMenuEmbed(cur2, st.index, total),
    schedulingMenuComponents(cur2, st.index, total)
  );
}

function eventDeleteConfirmEmbed(current) {
  const base = getRender(UI.OP_EVT_SCHED_DELETE_MENU_TEXT).text ?? "";
  if (!current) return embedWithImageAndText(null, (base + "\n\nNo Event selected.").trim());
  return embedWithImageAndText(null, [base, "", `Delete **${current.name}**?`].filter(Boolean).join("\n"));
}

function eventDeleteConfirmComponents(current) {
  const confirm = makeUiButton(UI.OP_EVT_SCHED_DELETE_BTN_CONFIRM, ButtonStyle.Danger, false);
  const back = makeUiButton(UI.OP_EVT_SCHED_DELETE_BTN_BACK, ButtonStyle.Secondary, false);
  return [new ActionRowBuilder().addComponents(confirm, back)];
}

async function handleEventSchedulingDelete(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "evt_sched_delete";
  s.uiInteraction = interaction;

  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  await refreshEventSchedulingCache(interaction);
  const cur = currentEventSchedulingRow(st);

  await editEphemeral(interaction, eventDeleteConfirmEmbed(cur), eventDeleteConfirmComponents(cur));
}

async function handleEventSchedulingDeleteBack(interaction) {
  await ackButton(interaction);
  return await handleEventSchedulingOpen(interaction);
}

async function handleEventSchedulingDeleteConfirm(interaction) {
  await ackButton(interaction);

  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  await refreshEventSchedulingCache(interaction);
  const cur = currentEventSchedulingRow(st);
  if (!cur) return await handleEventSchedulingOpen(interaction);

  const status = computeEventStatusFromRow(cur);
  if (status === "active") return await handleEventSchedulingOpen(interaction);

  try {
    await dbSoftDeleteEvent(interaction.guildId, cur.id);
  } catch (e) {
    console.error("Delete event failed:", e);
  }

  await refreshEventSchedulingCache(interaction);
  return await handleEventSchedulingOpen(interaction);
}


async function handleObjectiveSchedulingEdit(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_sched_edit";
  s.uiInteraction = interaction;

  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  await refreshObjectiveSchedulingCache(interaction);
  const cur = objectiveSchedulingCurrent(st);
  if (!cur) return await handleObjectiveSchedulingOpen(interaction);

  s.objectiveEdit.objectiveId = cur.id;

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  touchEditDraft(interaction.guildId, interaction.user.id, cur.id);

  await editEphemeral(interaction, editMenuEmbed(cur, entry), editMenuComponents(entry));
}

async function handleObjectiveSchedulingEditBack(interaction) {
  await ackButton(interaction);
  // preserve edit draft (TTL enforced)
  await handleObjectiveSchedulingOpen(interaction);
}


/* =========================
   EVENT SCHEDULING (LIST / EDIT / DELETE)
========================= */

function ensureEventSchedulingState(guildId, userId) {
  const s = ensureOperatorSession(guildId, userId);
  if (!s.eventScheduling) s.eventScheduling = { index: 0, cached: [], lastLoadedAt: 0 };
  return s.eventScheduling;
}

async function refreshEventSchedulingCache(interaction) {
  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  st.cached = await dbListEventsForSchedulingWithGifs(interaction.guildId);
  st.lastLoadedAt = Date.now();
  if (st.index < 0) st.index = 0;
  if (st.index > st.cached.length - 1) st.index = st.cached.length - 1;
}

function currentEventSchedulingRow(st) {
  const list = st.cached ?? [];
  if (!list.length) return null;
  const idx = Math.max(0, Math.min(st.index, list.length - 1));
  return list[idx] ?? null;
}

function prettyRoleKey(key) {
  return String(key ?? "").replace(/_/g, " ");
}

function eventSchedulingMenuEmbed(row, index, total) {
  const base = getRender(UI.OP_EVT_SCHED_MENU_TEXT).text ?? "";
  if (!row) return embedWithImageAndText(null, (base + "\n\nNo Events found.").trim());

  const status = computeObjectiveStatusFromRow({ schedule_begin_at: row.schedule_begin_at, schedule_end_at: row.schedule_end_at });
  const beginPt = fmtPtFromMysqlUtc(row.schedule_begin_at);
  const endPt = fmtPtFromMysqlUtc(row.schedule_end_at);

  const header = `Event ${index + 1} of ${total}`;
  const lines = [
    header,
    "",
    `**Status:** ${status}`,
    `**Event Name:** ${row.name}`,
    `**Merits Awarded:** ${row.merits_awarded}`,
    `**Schedule (PT):** ${beginPt} → ${endPt}`,
    `**Role Allocated:** ${prettyRoleKey(row.check_in_role_key)}`,
  ];

  return embedWithImageAndText(null, [base, "", lines.join("\n")].filter(Boolean).join("\n"));
}

function eventSchedulingMenuComponents(row, index, total) {
  const hasPrev = total > 0 && index > 0;
  const hasNext = total > 0 && index < total - 1;

  const status = row
    ? computeObjectiveStatusFromRow({ schedule_begin_at: row.schedule_begin_at, schedule_end_at: row.schedule_end_at })
    : "none";
  const deleteDisabled = !row || status === "active";

  const bEdit = makeUiButton(UI.OP_EVT_SCHED_BTN_EDIT, { disabled: !row });
  const bDelete = makeUiButton(UI.OP_EVT_SCHED_BTN_DELETE, { disabled: deleteDisabled });
  const bPrev = makeUiButton(UI.OP_EVT_SCHED_BTN_PREV, { disabled: !hasPrev });
  const bNext = makeUiButton(UI.OP_EVT_SCHED_BTN_NEXT, { disabled: !hasNext });
  const bBack = makeUiButton(UI.OP_EVT_SCHED_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");

  return buildRowsFromButtons([bEdit, bDelete, bPrev, bNext, bBack, bClose]);
}

function eventEditDraftKey(guildId, userId, eventId) {
  return `${guildId}:${userId}:${eventId}`;
}

function getOrCreateEventEditDraft(guildId, userId, row) {
  const key = eventEditDraftKey(guildId, userId, row.id);
  const existing = eventEditDrafts.get(key);
  if (existing && Date.now() - existing.updatedAt <= EDIT_DRAFT_TTL_MS) return existing;

  const base = {
    name: row.name,
    merits_awarded: Number(row.merits_awarded),
    schedule_begin_at: coerceToDate(row.schedule_begin_at),
    schedule_end_at: coerceToDate(row.schedule_end_at),
    check_in_role_key: row.check_in_role_key,
    resolved_role_id: row.resolved_role_id,
    gifs: { main: row.main_url, check_in: row.check_in_url, back: row.back_url },
  };

  // Snapshot original GIFs for diff highlighting in edit UI
  const originalGifs = { ...base.gifs };

  const draft = {
    ...base,
    gifs: { ...base.gifs },
    originalGifs,
  };

  const entry = { base, draft, updatedAt: Date.now() };
  eventEditDrafts.set(key, entry);
  return entry;
}


function getEventEditDraftEntry(guildId, userId, eventId) {
  const key = eventEditDraftKey(guildId, userId, eventId);
  return eventEditDrafts.get(key) || null;
}

function eventEditHasChanges(entry) {
  if (!entry) return false;
  const b = entry.base || {};
  const d = entry.draft || {};
  const same = (x, y) => (x ?? null) === (y ?? null);

  if (!same(b.name, d.name)) return true;
  if (!same(Number(b.merits_awarded), Number(d.merits_awarded))) return true;
  if (!same(b.check_in_role_key, d.check_in_role_key)) return true;
  if (!same(b.resolved_role_id, d.resolved_role_id)) return true;

  const bt = (v) => (v instanceof Date ? v.getTime() : (v ? new Date(v).getTime() : null));
  if (!same(bt(b.schedule_begin_at), bt(d.schedule_begin_at))) return true;
  if (!same(bt(b.schedule_end_at), bt(d.schedule_end_at))) return true;

  const bg = b.gifs || {};
  const dg = d.gifs || {};
  if (!same(bg.main, dg.main)) return true;
  if (!same(bg.check_in, dg.check_in)) return true;
  if (!same(bg.back, dg.back)) return true;

  return false;
}



function eventEditDiff(entry) {
  const b = entry.base;
  const d = entry.draft;

  const gifUpdates = [];
  if (d.gifs.main !== b.gifs.main) gifUpdates.push("main");
  if (d.gifs.check_in !== b.gifs.check_in) gifUpdates.push("check-in");
  if (d.gifs.back !== b.gifs.back) gifUpdates.push("back");

  return {
    any:
      d.name !== b.name ||
      d.merits_awarded !== b.merits_awarded ||
      d.schedule_begin_at !== b.schedule_begin_at ||
      d.schedule_end_at !== b.schedule_end_at ||
      d.check_in_role_key !== b.check_in_role_key ||
      gifUpdates.length > 0,
    name: d.name !== b.name,
    merits: d.merits_awarded !== b.merits_awarded,
    schedule: d.schedule_begin_at !== b.schedule_begin_at || d.schedule_end_at !== b.schedule_end_at,
    role: d.check_in_role_key !== b.check_in_role_key,
    gifUpdates,
  };
}

function eventEditMenuEmbed(row, entry) {
  const base = getRender(UI.OP_EVT_SCHED_EDIT_MENU_TEXT).text ?? "";
  if (!row || !entry) return embedWithImageAndText(null, (base + "\n\nNo Event selected.").trim());

  const d = entry.draft;
  const dif = eventEditDiff(entry);
  const gifText = dif.gifUpdates.length ? dif.gifUpdates.join(", ") : "none";

  const beginPt = fmtPtFromMysqlUtc(d.schedule_begin_at);
  const endPt = fmtPtFromMysqlUtc(d.schedule_end_at);

  const lines = [
    editMenuLine("Event Name", d.name || "(empty)", dif.name),
    editMenuLine("Merits Awarded", String(d.merits_awarded ?? "(empty)"), dif.merits),
    editMenuLine("Schedule (PT)", `${beginPt ?? "(empty)"} → ${endPt ?? "(empty)"}`, dif.schedule),
    editMenuLine("Role Allocated", prettyRoleKey(d.check_in_role_key || "(empty)"), dif.role),
    editMenuLine("GIF Updates", gifText, dif.gifUpdates.length > 0),
  ];

  return embedWithImageAndText(null, [base, "", lines.join("\n")].filter(Boolean).join("\n"));
}

function eventEditMenuComponents(entry) {
  const dif = eventEditDiff(entry);
  const bName = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_NAME);
  const bMerits = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_MERITS);
  const bSchedule = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_SCHEDULE);
  const bRole = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_ROLE);
  const bGif = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_GIF);
  const bSubmit = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_SUBMIT, { disabled: !dif.any });
  const bBack = makeUiButton(UI.OP_EVT_SCHED_EDIT_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");
  return buildRowsFromButtons([bName, bMerits, bSchedule, bRole, bGif, bSubmit, bBack, bClose]);
}

async function handleEventSchedulingOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_evt_scheduling";
  s.uiInteraction = interaction;

  await refreshEventSchedulingCache(interaction);
  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  const list = st.cached ?? [];
  const row = currentEventSchedulingRow(st);

  await editEphemeral(interaction, eventSchedulingMenuEmbed(row, st.index, list.length), eventSchedulingMenuComponents(row, st.index, list.length));
}

async function handleEventSchedulingPrev(interaction) {
  await ackButton(interaction);
  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  st.index -= 1;
  return await handleEventSchedulingOpen(interaction);
}

async function handleEventSchedulingNext(interaction) {
  await ackButton(interaction);
  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  st.index += 1;
  return await handleEventSchedulingOpen(interaction);
}

async function handleEventSchedulingBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_menu";
  return await editEphemeral(interaction, opEventsMenuEmbed(), opEventsMenuComponents());
}

async function handleEventSchedulingEdit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  await refreshEventSchedulingCache(interaction);

  const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
  const row = currentEventSchedulingRow(st);
  if (!row) return;

  s.eventEdit = { eventId: row.id };
  s.uiInteraction = interaction;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, row);
  return await editEphemeral(interaction, eventEditMenuEmbed(row, entry), eventEditMenuComponents(entry));
}

async function handleEventSchedulingEditBack(interaction) {
  await ackButton(interaction);
  // return to scheduling list
  return await handleEventSchedulingOpen(interaction);
}

async function handleEventSchedulingEditName(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const row = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!row) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, row);
  return await showEventEditNameModal(interaction, entry);
}

async function handleEventSchedulingEditMerits(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const row = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!row) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, row);
  return await showEventEditMeritsModal(interaction, entry);
}

async function handleEventSchedulingEditSchedule(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const row = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!row) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, row);
  return await showEventEditScheduleModal(interaction, entry);
}

async function handleEventSchedulingEditRole(interaction) {
  return await handleEventSchedulingEditRoleOpen(interaction);
}


async function handleEventSchedulingEditGif(interaction) {
  return await handleEventSchedulingEditGifOpen(interaction);
}



async function handleObjectiveSchedulingEditSubmit(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return await handleObjectiveSchedulingOpen(interaction);

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return await handleObjectiveSchedulingOpen(interaction);

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  const dif = diffChanged(entry);
  if (!dif.any) return await handleObjectiveSchedulingEdit(interaction);

  // parse schedule in PT
  const beginUtc = parsePtToUtcJsDate(entry.draft.schedule_begin, { dateOnlyMode: "start" });
  const endUtc = parsePtToUtcJsDate(entry.draft.schedule_end, { dateOnlyMode: "end" });

  if (!beginUtc || !endUtc || endUtc.getTime() <= beginUtc.getTime()) {
    const embed = embedWithImageAndText(null, `❌ Invalid schedule.\nEnd must be after Begin.`);
    await editEphemeral(interaction, embed, []);
    return;
  }

  // gif partial updates: only send changed urls
  const gifMainUrl = entry.draft.gifs.main !== entry.base.gifs.main ? entry.draft.gifs.main : null;
  const gifSubmitUrl = entry.draft.gifs.submit !== entry.base.gifs.submit ? entry.draft.gifs.submit : null;
  const gifBackUrl = entry.draft.gifs.back !== entry.base.gifs.back ? entry.draft.gifs.back : null;

  try {
    await dbUpdateObjectiveAndMaybeGifs({
      guildId: interaction.guildId,
      objectiveId,
      name: entry.draft.name,
      meritsAwarded: Number(entry.draft.merits_awarded),
      proofType: entry.draft.proof_type,
      beginUtc,
      endUtc,
      gifMainUrl,
      gifSubmitUrl,
      gifBackUrl,
    });
  } catch (e) {
    console.error("Edit objective update failed:", e);
    await editEphemeral(interaction, embedWithImageAndText(null, "❌ Failed to update objective."), []);
    return;
  }

  clearEditDraft(interaction.guildId, interaction.user.id, objectiveId);

  // REQUIRED: return to same objective in scheduling list
  const st = ensureObjectiveSchedulingState(interaction.guildId, interaction.user.id);
  await refreshObjectiveSchedulingCache(interaction);
  // keep index as-is; list order unchanged by id, but schedule might change; try to re-locate objective id
  const idx = st.cached.findIndex((r) => Number(r.id) === Number(objectiveId));
  if (idx >= 0) st.index = idx;

  const cur2 = objectiveSchedulingCurrent(st);
  const total = st.cached.length;
  await editEphemeral(
    interaction,
    schedulingMenuEmbed(cur2, st.index, total),
    schedulingMenuComponents(cur2, st.index, total)
  );
}

/* ---------- edit: file type handlers ---------- */
async function handleObjectiveSchedulingEditFileTypeOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_sched_edit_filetype";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opEditFileTypeEmbed(s), opEditFileTypeComponents(s));
}

async function handleObjectiveSchedulingEditFileTypePick(interaction, type) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.tempFileType = type;
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opEditFileTypeEmbed(s), opEditFileTypeComponents(s));
}

async function handleObjectiveSchedulingEditFileTypeSubmit(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return await handleObjectiveSchedulingOpen(interaction);

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return await handleObjectiveSchedulingOpen(interaction);

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);

  if (!s.tempFileType) {
    await editEphemeral(interaction, opEditFileTypeEmbed(s), opEditFileTypeComponents(s));
    return;
  }

  entry.draft.proof_type = s.tempFileType;
  entry.updatedAt = Date.now();
  s.tempFileType = null;

  await editEphemeral(interaction, editMenuEmbed(cur, entry), editMenuComponents(entry));
}

async function handleObjectiveSchedulingEditFileTypeBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.tempFileType = null;
  await handleObjectiveSchedulingEdit(interaction);
}

/* ---------- edit: gif upload handlers (reuse pendingGifUploads) ---------- */
async function handleObjectiveSchedulingEditGifOpen(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return await handleObjectiveSchedulingOpen(interaction);

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return await handleObjectiveSchedulingOpen(interaction);

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  s.screen = "obj_sched_edit_gif_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opEditGifMenuEmbed(entry), opEditGifMenuComponents(entry));
}

async function handleObjectiveSchedulingEditGifSlot(interaction, slot) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return;

  s.screen = "obj_sched_edit_gif_wait";
  startPendingGif(interaction.guildId, interaction.user.id, interaction.channelId, `edit_${slot}`);

  await editEphemeral(interaction, gifWaitEmbed(slot), buildRowsFromButtons([makeUiButton(UI.OP_OBJ_SCHED_EDIT_GIF_ANYFILE_BACK)]));
}

async function handleObjectiveSchedulingEditGifWaitBack(interaction) {
  await ackButton(interaction);
  clearPendingGif(interaction.guildId, interaction.user.id);
  await handleObjectiveSchedulingEditGifOpen(interaction);
}

async function handleObjectiveSchedulingEditGifMenuBack(interaction) {
  await ackButton(interaction);
  await handleObjectiveSchedulingEdit(interaction);
}

async function handleObjectiveSchedulingEditGifMenuSubmit(interaction) {
  await ackButton(interaction);
  await handleObjectiveSchedulingEdit(interaction);
}

/* ---------- scheduling edit: button handlers (modals) ---------- */
async function handleObjectiveSchedulingEditName(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return;

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return;

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  touchEditDraft(interaction.guildId, interaction.user.id, objectiveId);

  await showEditNameModal(interaction, entry);
}

async function handleObjectiveSchedulingEditMerits(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return;

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return;

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  touchEditDraft(interaction.guildId, interaction.user.id, objectiveId);

  await showEditMeritsModal(interaction, entry);
}

async function handleObjectiveSchedulingEditSchedule(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const objectiveId = s.objectiveEdit?.objectiveId;
  if (!objectiveId) return;

  const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
  if (!cur) return;

  const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
  touchEditDraft(interaction.guildId, interaction.user.id, objectiveId);

  await showEditScheduleModal(interaction, entry);
}

/* ---------- scheduling edit: file type pick handlers ---------- */
async function handleObjectiveSchedulingEditFileTypeSubmitButton(interaction) {
  return await handleObjectiveSchedulingEditFileTypeSubmit(interaction);
}

/* ---------- scheduling edit: gif menu handlers ---------- */
async function handleObjectiveSchedulingEditGifMain(interaction) {
  return await handleObjectiveSchedulingEditGifSlot(interaction, "main");
}
async function handleObjectiveSchedulingEditGifSubmitFile(interaction) {
  return await handleObjectiveSchedulingEditGifSlot(interaction, "submit");
}
async function handleObjectiveSchedulingEditGifBackFile(interaction) {
  return await handleObjectiveSchedulingEditGifSlot(interaction, "back");
}


/* =========================
   OBJECTIVE UPLOAD UI
========================= */
function prettyProofType(type) {
  if (!type) return "";
  return type === "text_file" ? "text file" : type;
}

function objectiveUploadProgressText(d) {
  const yes = "✅";
  const no = "⬜";

  const nameOk = !!d.name;
  const meritsOk = typeof d.merits === "number";
  const scheduleOk = !!d.schedule_begin && !!d.schedule_end;
  const fileOk = !!d.file_type;
  const gifsOk = !!d.gifs.main && !!d.gifs.submit && !!d.gifs.back;

  return [
    `${nameOk ? yes : no} Objective Name`,
    `${meritsOk ? yes : no} Merits Awarded`,
    `${scheduleOk ? yes : no} Schedule`,
    `${fileOk ? yes : no} Proof File Type`,
    `${gifsOk ? yes : no} GIF`,
    "",
    d.name ? `**Objective Name:** ${d.name}` : "",
    meritsOk ? `**Merits Awarded:** ${d.merits}` : "",
    scheduleOk ? `**Schedule (PT):** ${d.schedule_begin} → ${d.schedule_end}` : "",
    fileOk ? `**Proof File Type:** ${prettyProofType(d.file_type)}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}


function eventUploadProgressText(d) {
  const yes = "✅";
  const no = "⬜";

  const nameOk = !!d.name;
  const meritsOk = typeof d.merits === "number";
  const scheduleOk = !!d.schedule_begin && !!d.schedule_end;
  const roleOk = !!d.check_in_role_key && !!d.resolved_role_id;
  const gifsOk = !!d.gifs?.main && !!d.gifs?.check_in && !!d.gifs?.back;

  return [
    `${nameOk ? yes : no} Event Name`,
    `${meritsOk ? yes : no} Merits Awarded`,
    `${scheduleOk ? yes : no} Schedule`,
    `${roleOk ? yes : no} Role Allocated`,
    `${gifsOk ? yes : no} GIF`,
    "",
    d.name ? `**Event Name:** ${d.name}` : "",
    meritsOk ? `**Merits Awarded:** ${d.merits}` : "",
    scheduleOk ? `**Schedule (PT):** ${d.schedule_begin} → ${d.schedule_end}` : "",
    roleOk ? `**Role Allocated:** ${d.check_in_role_key}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}


function opObjectiveUploadEmbed(session) {
  const base = getRender(UI.OP_OBJ_UPLOAD_MENU_TEXT).text ?? "";
  const prog = objectiveUploadProgressText(session.draft);
  return embedWithImageAndText(null, [base, "", prog].filter(Boolean).join("\n"));
}

function opObjectiveUploadComponents(session) {
  const d = session.draft;

  const nameDone = !!d.name;
  const meritsDone = typeof d.merits === "number";
  const schedDone = !!d.schedule_begin && !!d.schedule_end;
  const fileDone = !!d.file_type;
  const gifsDone = !!d.gifs.main && !!d.gifs.submit && !!d.gifs.back;

  const bName = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_NAME);
  const bMerits = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_MERITS, { disabled: !nameDone });
  const bSchedule = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_SCHEDULE, { disabled: !nameDone || !meritsDone });
  const bFileType = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_FILETYPE, { disabled: !nameDone || !meritsDone || !schedDone });
  const bGif = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_GIF, { disabled: !nameDone || !meritsDone || !schedDone || !fileDone });

  const canSubmit = nameDone && meritsDone && schedDone && fileDone && gifsDone;
  const bSubmit = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_SUBMIT, { disabled: !canSubmit });
  const bBack = makeUiButton(UI.OP_OBJ_UPLOAD_BTN_BACK);
  const bClose = btnClose(CUSTOM.OP_CLOSE, "Close");

  return buildRowsFromButtons([bName, bMerits, bSchedule, bFileType, bGif, bSubmit, bBack, bClose]);
}

/* =========================
   FILE TYPE MENU
========================= */
function opFileTypeEmbed(session) {
  const base = getRender(UI.OP_OBJ_FILETYPE_MENU).text ?? "Select file type:";
  const pick = session.tempFileType ? `\n\nSelected: **${session.tempFileType}**` : "";
  return embedWithImageAndText(null, (base + pick) || ZWSP);
}
function opFileTypeComponents(session) {
  const pick = session.tempFileType;

  const row1 = new ActionRowBuilder().addComponents(
    makeUiButton(UI.OP_OBJ_FILETYPE_IMAGE),
    makeUiButton(UI.OP_OBJ_FILETYPE_AUDIO),
    makeUiButton(UI.OP_OBJ_FILETYPE_TEXT),
    makeUiButton(UI.OP_OBJ_FILETYPE_TEXT_FILE)
  );
  const row2 = new ActionRowBuilder().addComponents(
    makeUiButton(UI.OP_OBJ_FILETYPE_SUBMIT, { disabled: !pick }),
    makeUiButton(UI.OP_OBJ_FILETYPE_BACK)
  );

  return [row1, row2];
}

/* =========================
   GIF MENU
========================= */
function gifStatusLine(d) {
  const yes = "✅";
  const no = "⬜";
  return [`${d.gifs.main ? yes : no} Main GIF`, `${d.gifs.submit ? yes : no} Submit GIF`, `${d.gifs.back ? yes : no} Back GIF`].join(
    "\n"
  );
}

function eventGifStatusLine(eventDraft) {
  const gifs = eventDraft?.gifs ?? {};
  const orig = eventDraft?.originalGifs ?? {};
  const box = (cur, origVal) => {
    if (!cur) return "⬜";
    if (origVal && cur !== origVal) return "🟦";
    return "🟩";
  };

  return [
    `${box(gifs.main, orig.main)} Main`,
    `${box(gifs.check_in, orig.check_in)} Check-in`,
    `${box(gifs.back, orig.back)} Back`,
  ].join("\n");
}

function eventGifWaitEmbed(realSlot) {
  const label = realSlot === "main" ? "Main" : realSlot === "check_in" ? "Check-in" : "Back";
  return embedWithImageAndText(
    null,
    `Waiting for **${label} GIF** upload...\n\nUpload a **.gif** file in this channel within **2 minutes**.`
  );
}

function opGifMenuEmbed(session) {
  const url = getRender(UI.OP_OBJ_GIF_MENU).url;
  const txt = "Upload required GIFs:\n\n" + gifStatusLine(session.draft);
  return embedWithImageAndText(url ?? null, txt);
}

function opGifMenuComponents(session) {
  const d = session.draft;
  const canMain = true;
  const canSubmit = !!d.gifs.main;
  const canBack = !!d.gifs.main && !!d.gifs.submit;
  const canFinish = !!d.gifs.main && !!d.gifs.submit && !!d.gifs.back;

  const bMain = makeUiButton(UI.OP_OBJ_GIF_MAIN, { disabled: !canMain });
  const bSubmitFile = makeUiButton(UI.OP_OBJ_GIF_SUBMIT, { disabled: !canSubmit });
  const bBackFile = makeUiButton(UI.OP_OBJ_GIF_BACK, { disabled: !canBack });

  const bSubmit = makeUiButton(UI.OP_OBJ_GIF_SUBMIT_BTN, { disabled: !canFinish });
  const bBackBtn = makeUiButton(UI.OP_OBJ_GIF_BACK_BTN);

  return buildRowsFromButtons([bMain, bSubmitFile, bBackFile, bSubmit, bBackBtn]);
}

function gifWaitEmbed(slot) {
  const label = slot === "main" ? "Main" : slot === "submit" ? "Submit" : "Back";
  return embedWithImageAndText(
    null,
    `Waiting for **${label} GIF** upload...\n\nUpload a **.gif** file in this channel within **2 minutes**.`
  );
}
function gifWaitComponents() {
  return buildRowsFromButtons([makeUiButton(UI.OP_OBJ_GIF_ANYFILE_BACK)]);
}

/* =========================
   OPERATOR HANDLERS
========================= */
async function handleOperatorAccess(interaction) {
  await safeDeferEphemeral(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opMenuEmbed(), opMenuComponents());
}

async function handleOperatorObjectives(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_objectives_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opObjectivesMenuEmbed(), opObjectivesMenuComponents());
}

async function handleOperatorObjectivesBack(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opMenuEmbed(), opMenuComponents());
}


/* ========================= EVENT UPLOAD FLOW ========================= */

function opEventsMenuEmbed() {
  const txt = getRender(UI.OP_EVT_MENU_TEXT).text ?? "";
  return embedWithImageAndText(null, txt || ZWSP);
}
function opEventsMenuComponents() {
  const bUpload = makeUiButton(UI.OP_EVT_BTN_UPLOAD);
  const bSched = makeUiButton(UI.OP_EVT_BTN_SCHEDULING);
  const bBack = makeUiButton(UI.OP_EVT_BTN_BACK);
  return buildRowsFromButtons([bUpload, bSched, bBack]);
}

function opCommandsMenuEmbed() {
  const txt = getRender(UI.OP_CMD_MENU_TEXT).text ?? "";
  return embedWithImageAndText(null, txt || ZWSP);
}
function opCommandsMenuComponents() {
  const bRoll = makeUiButton(UI.OP_CMD_BTN_ROLL_CALL);
  const bAnn = makeUiButton(UI.OP_CMD_BTN_ANNOUNCEMENT);
  const bTransfer = makeUiButton(UI.OP_CMD_BTN_MERIT_TRANSFER);
  const bBack = makeUiButton(UI.OP_CMD_BTN_BACK);
  return buildRowsFromButtons([bRoll, bAnn, bTransfer, bBack]);
}

function opRollCallEmbed(state) {
  const base = getRender(UI.OP_CMD_ROLL_MENU_TEXT).text ?? "";
  const ch = state?.target_channel_id ? `<#${state.target_channel_id}>` : "_Not selected_";
  const txt = [base, "", `Target Channel: ${ch}`].filter(Boolean).join("\n");
  return embedWithImageAndText(null, txt || ZWSP);
}
function opRollCallComponents(state) {
  const bTarget = makeUiButton(UI.OP_CMD_ROLL_BTN_TARGET_CHANNEL);
  const canInit = !!state?.target_channel_id;
  const bInit = makeUiButton(UI.OP_CMD_ROLL_BTN_INITIATE, { disabled: !canInit });
  const bBack = makeUiButton(UI.OP_CMD_ROLL_BTN_BACK);
  return buildRowsFromButtons([bTarget, bInit, bBack]);
}

function opRollCallTargetEmbed(state, availableKeys) {
  const base = getRender(UI.OP_CMD_ROLL_TARGET_MENU_TEXT).text ?? "";
  const lines = [base, ""];
  for (const key of ["EVENT_CHANNEL_1","EVENT_CHANNEL_2","EVENT_CHANNEL_3","EVENT_CHANNEL_4","EVENT_CHANNEL_5"]) {
    const id = resolveEventChannelIdFromKey(key);
    if (!id) continue;
    const ok = availableKeys.includes(key);
    lines.push(`${ok ? "✅" : "⬛"} <#${id}>`);
  }
  if (availableKeys.length === 0) lines.push("_No active event channels available._");
  const chosen = state?.pending_channel_key ? `\n\nSelected: **${state.pending_channel_key}**` : "";
  return embedWithImageAndText(null, (lines.join("\n") + chosen).trim() || ZWSP);
}
function opRollCallTargetComponents(state, availableKeys) {
  const chosen = state?.pending_channel_key ?? null;

  const mk = (ref, key) => makeUiButton(ref, { disabled: !availableKeys.includes(key) });

  const b1 = mk(UI.OP_CMD_ROLL_TARGET_CH1, "EVENT_CHANNEL_1");
  const b2 = mk(UI.OP_CMD_ROLL_TARGET_CH2, "EVENT_CHANNEL_2");
  const b3 = mk(UI.OP_CMD_ROLL_TARGET_CH3, "EVENT_CHANNEL_3");
  const b4 = mk(UI.OP_CMD_ROLL_TARGET_CH4, "EVENT_CHANNEL_4");
  const b5 = mk(UI.OP_CMD_ROLL_TARGET_CH5, "EVENT_CHANNEL_5");

  const bSubmit = makeUiButton(UI.OP_CMD_ROLL_TARGET_SUBMIT, { disabled: !chosen });
  const bBack = makeUiButton(UI.OP_CMD_ROLL_TARGET_BACK);

  return [
    new ActionRowBuilder().addComponents(b1, b2, b3, b4, b5),
    new ActionRowBuilder().addComponents(bSubmit, bBack),
  ];
}

function opAnnouncementMenuEmbed(state) {
  const base = getRender(UI.OP_CMD_ANN_MENU_TEXT).text ?? "";
  const ok = (v) => (v ? "✅" : "⬛");
  const lines = [
    base,
    "",
    `${ok(state?.audio_url)} Audio Uploaded`,
  ];
  return embedWithImageAndText(null, lines.join("\n").trim() || ZWSP);
}

function opAnnouncementMenuComponents(state) {
  const bUpload = makeUiButton(UI.OP_CMD_ANN_BTN_UPLOAD_AUDIO);
  const canInit = !!state?.audio_url;
    const bInit = makeUiButton(UI.OP_CMD_ANN_BTN_INITIATE, { disabled: !canInit });
  const bBack = makeUiButton(UI.OP_CMD_ANN_BTN_BACK);
  return buildRowsFromButtons([bUpload, bInit, bBack]);
}

function opAnnouncementUploadEmbed() {
  const base = getRender(UI.OP_CMD_ANN_UPLOAD_MENU_TEXT).text ?? "";
  const txt = `${base}\n\nUpload a **.mp3** or **.wav** file in this channel within **2 minutes**.`;
  return embedWithImageAndText(null, txt || ZWSP);
}
function opAnnouncementUploadComponents() {
  const bBack = makeUiButton(UI.OP_CMD_ANN_UPLOAD_BACK);
  return buildRowsFromButtons([bBack]);
}

function opEventUploadEmbed(s) {
  const txt = getRender(UI.OP_EVT_UPLOAD_MENU_TEXT).text ?? "";
  const d = s.eventDraft || {};

  const hasName = !!d.name;
  const hasMerits = Number.isInteger(d.merits);
  const hasSchedule = !!d.schedule_begin && !!d.schedule_end;
  const hasRole = !!d.check_in_role_key && !!d.resolved_role_id;
  const hasGifs = !!d.gifs?.main && !!d.gifs?.check_in && !!d.gifs?.back;

  const lines = [];
  lines.push(`${hasName ? "✅" : "⬜"} Event Name`);
  lines.push(`${hasMerits ? "✅" : "⬜"} Merits Awarded`);
  lines.push(`${hasSchedule ? "✅" : "⬜"} Schedule`);
  lines.push(`${hasRole ? "✅" : "⬜"} Role Allocated`);
  lines.push(`${hasGifs ? "✅" : "⬜"} GIFs`);
  lines.push(`${hasName && hasMerits && hasSchedule && hasRole && hasGifs ? "✅" : "⬜"} Submit`);

  const details = [];
  if (d.name) details.push(`**Event Name:** ${d.name}`);
  if (Number.isInteger(d.merits)) details.push(`**Merits Awarded:** ${d.merits}`);
  if (d.schedule_begin && d.schedule_end) details.push(`**Schedule:** ${d.schedule_begin} → ${d.schedule_end} (PT)`);
  if (d.check_in_role_key) details.push(`**Role Allocated:** ${d.check_in_role_key}`);

  const body = [txt, lines.join("\n"), details.length ? details.join("\n") : null].filter(Boolean).join("\n\n");
  return embedWithImageAndText(null, body || ZWSP);
}

function opEventUploadComponents(s) {
  const hasName = !!s.eventDraft?.name;
  const hasMerits = Number.isInteger(s.eventDraft?.merits);
  const hasSchedule = !!s.eventDraft?.schedule_begin && !!s.eventDraft?.schedule_end;
  const hasRole = !!s.eventDraft?.check_in_role_key && !!s.eventDraft?.resolved_role_id;
  const hasGifs = !!s.eventDraft?.gifs?.main && !!s.eventDraft?.gifs?.check_in && !!s.eventDraft?.gifs?.back;

  const bName = makeUiButton(UI.OP_EVT_UPLOAD_NAME_BTN);
  const bMerits = makeUiButton(UI.OP_EVT_UPLOAD_MERITS_BTN, { disabled: !hasName });
  const bSchedule = makeUiButton(UI.OP_EVT_UPLOAD_SCHEDULE_BTN, { disabled: !hasName || !hasMerits });
  const bRole = makeUiButton(UI.OP_EVT_UPLOAD_ROLE_BTN, { disabled: !hasName || !hasMerits || !hasSchedule });
  const bGif = makeUiButton(UI.OP_EVT_UPLOAD_GIF_BTN, { disabled: !hasName || !hasMerits || !hasSchedule || !hasRole });
  const bSubmit = makeUiButton(UI.OP_EVT_UPLOAD_SUBMIT_BTN, { disabled: !(hasName && hasMerits && hasSchedule && hasRole && hasGifs) });
  const bBack = makeUiButton(UI.OP_EVT_UPLOAD_BACK_BTN);
  return buildRowsFromButtons([bName, bMerits, bSchedule, bRole, bGif, bSubmit, bBack]);
}

function opEventRoleMenuEmbed(s) {
  const txt = getRender(UI.OP_EVT_ROLE_MENU_TEXT).text ?? "";
  const chosen = s.eventDraft?.check_in_role_key ? `\n\nSelected: **${s.eventDraft.check_in_role_key}**` : "";
  return embedWithImageAndText(null, (txt || ZWSP) + chosen);
}


function opEventEditRoleMenuEmbed(entry) {
  const txt = getRender(UI.OP_EVT_ROLE_MENU_TEXT).text ?? "";
  const chosen = entry?.draft?.check_in_role_key ? `\n\nSelected: **${entry.draft.check_in_role_key}**` : "";
  return embedWithImageAndText(null, (txt || ZWSP) + chosen);
}

async function opEventEditRoleMenuComponents(guildId, eventId, entry) {
  const currentKey = entry?.draft?.check_in_role_key;

  const used1 = await dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey: "EVENT_1_CHECKED_IN", excludeEventId: eventId }).catch(() => false);
  const used2 = await dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey: "EVENT_2_CHECKED_IN", excludeEventId: eventId }).catch(() => false);
  const used3 = await dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey: "EVENT_3_CHECKED_IN", excludeEventId: eventId }).catch(() => false);
  const used4 = await dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey: "EVENT_4_CHECKED_IN", excludeEventId: eventId }).catch(() => false);
  const used5 = await dbHasUpcomingEventWithRoleKeyExcludingEventId({ guildId, checkInRoleKey: "EVENT_5_CHECKED_IN", excludeEventId: eventId }).catch(() => false);

  const b1 = makeUiButton(UI.OP_EVT_ROLE_BTN_1, { disabled: used1 && currentKey !== "EVENT_1_CHECKED_IN" });
  const b2 = makeUiButton(UI.OP_EVT_ROLE_BTN_2, { disabled: used2 && currentKey !== "EVENT_2_CHECKED_IN" });
  const b3 = makeUiButton(UI.OP_EVT_ROLE_BTN_3, { disabled: used3 && currentKey !== "EVENT_3_CHECKED_IN" });
  const b4 = makeUiButton(UI.OP_EVT_ROLE_BTN_4, { disabled: used4 && currentKey !== "EVENT_4_CHECKED_IN" });
  const b5 = makeUiButton(UI.OP_EVT_ROLE_BTN_5, { disabled: used5 && currentKey !== "EVENT_5_CHECKED_IN" });

  const canSubmit = !!entry?.draft?.check_in_role_key && !!entry?.draft?.resolved_role_id;
  const bSubmit = makeUiButton(UI.OP_EVT_ROLE_SUBMIT_BTN, { disabled: !canSubmit });
  const bBack = makeUiButton(UI.OP_EVT_ROLE_BACK_BTN);

  return buildRowsFromButtons([b1, b2, b3, b4, b5, bSubmit, bBack]);
}

async function handleEventSchedulingEditRoleOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  s.screen = "evt_sched_edit_role_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opEventEditRoleMenuEmbed(entry), await opEventEditRoleMenuComponents(interaction.guildId, eventId, entry));
}

async function handleEventEditRolePick(interaction, checkInRoleKey, roleUiRef) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  entry.draft.check_in_role_key = checkInRoleKey;
  entry.draft.resolved_role_id = String(resolveRoleId(roleUiRef));
  entry.updatedAt = Date.now();

  await editEphemeral(interaction, opEventEditRoleMenuEmbed(entry), await opEventEditRoleMenuComponents(interaction.guildId, eventId, entry));
}

async function handleEventEditRoleBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  s.screen = "evt_sched_edit";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, eventEditMenuEmbed(cur, entry), eventEditMenuComponents(entry));
}

async function handleEventEditRoleSubmit(interaction) {
  return await handleEventEditRoleBack(interaction);
}

async function opEventRoleMenuComponents(guildId, s) {
  const used1 = await dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey: "EVENT_1_CHECKED_IN" }).catch(() => false);
  const used2 = await dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey: "EVENT_2_CHECKED_IN" }).catch(() => false);
  const used3 = await dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey: "EVENT_3_CHECKED_IN" }).catch(() => false);
  const used4 = await dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey: "EVENT_4_CHECKED_IN" }).catch(() => false);
  const used5 = await dbHasUpcomingEventWithRoleKey({ guildId, checkInRoleKey: "EVENT_5_CHECKED_IN" }).catch(() => false);

  const b1 = makeUiButton(UI.OP_EVT_ROLE_BTN_1, { disabled: used1 });
  const b2 = makeUiButton(UI.OP_EVT_ROLE_BTN_2, { disabled: used2 });
  const b3 = makeUiButton(UI.OP_EVT_ROLE_BTN_3, { disabled: used3 });
  const b4 = makeUiButton(UI.OP_EVT_ROLE_BTN_4, { disabled: used4 });
  const b5 = makeUiButton(UI.OP_EVT_ROLE_BTN_5, { disabled: used5 });

  const canSubmit = !!s.eventDraft?.check_in_role_key && !!s.eventDraft?.resolved_role_id;
  const bSubmit = makeUiButton(UI.OP_EVT_ROLE_SUBMIT_BTN, { disabled: !canSubmit });
  const bBack = makeUiButton(UI.OP_EVT_ROLE_BACK_BTN);
  return buildRowsFromButtons([b1, b2, b3, b4, b5, bSubmit, bBack]);
}

function opEventGifMenuEmbed(session) {
  const url = getRender(UI.OP_EVT_GIF_MENU_TEXT).url;
  const txt = "Upload required GIFs:\n\n" + eventGifStatusLine(session.eventDraft ?? { gifs: {} });
  return embedWithImageAndText(url ?? null, txt);
}

function opEventGifMenuComponents(session) {
  const d = session?.eventDraft ?? { gifs: {} };
  const canMain = true;
  const canCheckIn = !!(d.gifs?.main);
  const canBack = !!(d.gifs?.main) && !!d.gifs.check_in;
  const canFinish = !!(d.gifs?.main) && !!d.gifs.check_in && !!d.gifs.back;

  const bMain = makeUiButton(UI.OP_EVT_GIF_MAIN_BTN, { disabled: !canMain });
  const bCheckIn = makeUiButton(UI.OP_EVT_GIF_CHECKIN_BTN, { disabled: !canCheckIn });
  const bBackFile = makeUiButton(UI.OP_EVT_GIF_BACK_BTN, { disabled: !canBack });

  const bSubmit = makeUiButton(UI.OP_EVT_GIF_SUBMIT_BTN, { disabled: !canFinish });
  const bBackBtn = makeUiButton(UI.OP_EVT_GIF_BACK_MENU_BTN);

  return buildRowsFromButtons([bMain, bCheckIn, bBackFile, bSubmit, bBackBtn]);
}

async function handleOperatorEvents(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventsMenuEmbed(), opEventsMenuComponents());
}


function opEventEditGifMenuEmbed(entry) {
  const url = getRender(UI.OP_EVT_GIF_MENU_TEXT).url;
  const txt = "Upload GIFs (partial updates allowed):\n\n" + eventGifStatusLine(entry.draft);
  return embedWithImageAndText(url ?? null, txt);
}

function opEventEditGifMenuComponents(entry) {
  // In edit, allow picking any slot at any time (partial updates)
  const bMain = makeUiButton(UI.OP_EVT_GIF_MAIN_BTN, { disabled: false });
  const bCheckIn = makeUiButton(UI.OP_EVT_GIF_CHECKIN_BTN, { disabled: false });
  const bBackFile = makeUiButton(UI.OP_EVT_GIF_BACK_BTN, { disabled: false });

  // Use existing "submit" as "back to edit"
  const bSubmit = makeUiButton(UI.OP_EVT_GIF_SUBMIT_BTN, { disabled: false });
  const bBackBtn = makeUiButton(UI.OP_EVT_GIF_BACK_MENU_BTN);

  return buildRowsFromButtons([bMain, bCheckIn, bBackFile, bSubmit, bBackBtn]);
}

async function handleEventSchedulingEditGifOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  s.screen = "evt_sched_edit_gif_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opEventEditGifMenuEmbed(entry), opEventEditGifMenuComponents(entry));
}

async function handleEventSchedulingEditSubmit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  if (!eventEditHasChanges(entry)) {
    // No changes; just return to edit screen
    s.screen = "op_evt_sched_edit";
    s.uiInteraction = interaction;
    await editEphemeral(interaction, opEventSchedulingEditEmbed(entry), opEventSchedulingEditComponents(entry));
    return;
  }

  const gifMainUrl = entry.draft.gifs.main !== entry.base.gifs.main ? entry.draft.gifs.main : null;
  const gifCheckInUrl = entry.draft.gifs.check_in !== entry.base.gifs.check_in ? entry.draft.gifs.check_in : null;
  const gifBackUrl = entry.draft.gifs.back !== entry.base.gifs.back ? entry.draft.gifs.back : null;

  await dbUpdateEventAndMaybeGifs({
    guildId: interaction.guildId,
    eventId,
    name: entry.draft.name,
    meritsAwarded: Number(entry.draft.merits_awarded),
    checkInRoleKey: entry.draft.check_in_role_key,
    resolvedRoleId: entry.draft.resolved_role_id,
    beginUtc: entry.draft.schedule_begin_at,
    endUtc: entry.draft.schedule_end_at,
    gifMainUrl,
    gifCheckInUrl,
    gifBackUrl,
  });

  // Clear edit draft on successful commit
  const key = eventEditDraftKey(interaction.guildId, interaction.user.id, eventId);
  eventEditDrafts.delete(key);

  // Return to scheduling list (fresh cache)
  return await handleEventSchedulingOpen(interaction);
}


async function handleEventEditGifBackToEdit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  s.screen = "evt_sched_edit";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, eventEditMenuEmbed(cur, entry), eventEditMenuComponents(entry));
}

async function handleOperatorEventsBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opMenuEmbed(), opMenuComponents());
}

async function handleOpCommands(interaction) {
  await ackButton(interaction);
const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_cmd_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opCommandsMenuEmbed(), opCommandsMenuComponents());
}

async function handleOpBackToMenu(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opMenuEmbed(), opMenuComponents());
}


async function listActiveEventChannelKeys(guildId) {
  const now = nowUtcJsDate();
  const [rows] = await pool.query(
    `
    SELECT check_in_role_key
    FROM events
    WHERE guild_id = ?
      AND status <> 'deleted'
      AND schedule_begin_at <= ?
      AND schedule_end_at >= ?
  `,
    [String(guildId), now, now]
  );
  const keys = new Set();
  for (const r of rows ?? []) {
    const k = checkInRoleKeyToEventChannelKey(r.check_in_role_key);
    if (k) keys.add(k);
  }
  return Array.from(keys);
}

async function handleOpRollCallOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_cmd_roll";
  s.uiInteraction = interaction;

  const d = s.commandsDraft.roll_call;
  const state = { target_channel_key: d.target_channel_key, target_channel_id: d.resolved_channel_id };
  await editEphemeral(interaction, opRollCallEmbed(state), opRollCallComponents(state));
}

async function handleOpRollCallBack(interaction) {
  await ackButton(interaction);
  return await handleOpCommands(interaction);
}

async function handleOpRollCallTargetOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_cmd_roll_target";
  s.uiInteraction = interaction;

  const availableKeys = await listActiveEventChannelKeys(interaction.guildId);
  const d = s.commandsDraft.roll_call;
  const state = { pending_channel_key: d.pending_channel_key ?? null };
  await editEphemeral(interaction, opRollCallTargetEmbed(state, availableKeys), opRollCallTargetComponents(state, availableKeys));
}

async function handleOpRollCallTargetPick(interaction, channelKey) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const availableKeys = await listActiveEventChannelKeys(interaction.guildId);
  if (!availableKeys.includes(channelKey)) return await handleOpRollCallTargetOpen(interaction);

  s.commandsDraft.roll_call.pending_channel_key = channelKey;

  const state = { pending_channel_key: channelKey };
  await editEphemeral(interaction, opRollCallTargetEmbed(state, availableKeys), opRollCallTargetComponents(state, availableKeys));
}

async function handleOpRollCallTargetBack(interaction) {
  await ackButton(interaction);
  return await handleOpRollCallOpen(interaction);
}

async function handleOpRollCallTargetSubmit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const pendingKey = s.commandsDraft.roll_call.pending_channel_key ?? null;
  if (!pendingKey) return await handleOpRollCallTargetOpen(interaction);

  const resolvedId = resolveEventChannelIdFromKey(pendingKey);
  if (!resolvedId) return await handleOpRollCallTargetOpen(interaction);

  s.commandsDraft.roll_call.target_channel_key = pendingKey;
  s.commandsDraft.roll_call.resolved_channel_id = String(resolvedId);
  s.commandsDraft.roll_call.pending_channel_key = null;

  const state = { target_channel_key: pendingKey, target_channel_id: String(resolvedId) };
  await editEphemeral(interaction, opRollCallEmbed(state), opRollCallComponents(state));
}

async function handleOpRollCallInitiate(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);

  const d = s.commandsDraft.roll_call;
  const channelKey = d.target_channel_key ?? null;
  const channelId = d.resolved_channel_id ?? null;
  if (!channelKey || !channelId) return await handleOpRollCallOpen(interaction);

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel || !channel.isTextBased()) return await handleOpRollCallOpen(interaction);

  let msgText =
    getRender("[UI: Roll-Call message]")?.text ??
    "React with :white_check_mark: to confirm your attendance to this event & secure your Merits.";
  // Normalize any authored token variants so Discord renders the emoji
  // Handles: "white_check mark:", ":white_check mark:", ":white_check_mark:", etc.
  msgText = String(msgText).replace(/:?white_check[\s_]*mark:?/gi, ":white_check_mark:");

  const msg = await channel.send({ content: msgText });

  // Seed the roll call message with the correct reaction
  try {
    await msg.react("✅");
  } catch (e) {
    console.warn("Roll call react failed:", e?.message ?? e);
  }

  await dbInsertRollCall({
    guildId: interaction.guildId,
    operatorUserId: interaction.user.id,
    channelKey,
    resolvedChannelId: channelId,
    messageId: msg.id,
  });

  d.target_channel_key = null;
  d.resolved_channel_id = null;
  d.pending_channel_key = null;

  await editEphemeral(interaction, embedWithImageAndText(null, "Roll Call initiated."), opCommandsMenuComponents());
}


async function handleOpMeritTransferOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_cmd_merit_transfer";
  s.uiInteraction = interaction;

  const d = s.commandsDraft.merit_transfer;
  const state = { target_user_id: d.target_user_id, amount: d.amount, notes: d.notes };
  await editEphemeral(interaction, opMeritTransferMenuEmbed(state), opMeritTransferMenuComponents(state));
}

async function handleOpMeritTransferBack(interaction) {
  await ackButton(interaction);
  return await handleOpCommands(interaction);
}

function parseUserIdFromInput(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;
  const m = s.match(/^<@!?(\d{6,})>$/);
  if (m) return m[1];
  if (/^\d{6,}$/.test(s)) return s;
  return null;
}

function validateMeritTransferDraft(d) {
  const target = d?.target_user_id ? String(d.target_user_id) : null;
  const amount = Number(d?.amount);
  const notes = String(d?.notes ?? "").trim();

  if (!target) return { ok: false, reason: "Select a target user." };
  if (!Number.isInteger(amount) || amount < 1 || amount > 9999) return { ok: false, reason: "Amount must be an integer 1–9999." };
  if (notes.length < 5 || notes.length > 1000) return { ok: false, reason: "Notes must be 5–1000 characters." };
  return { ok: true };
}

function opMeritTransferMenuEmbed(state) {
  const target = state?.target_user_id ? `<@${state.target_user_id}>` : "_Not set_";
  const amount = state?.amount ? `**${state.amount}**` : "_Not set_";
  const notes = state?.notes ? String(state.notes).trim() : "_Not set_";
  const notesDisplay = notes.length > 900 ? `${notes.slice(0, 900)}…` : notes;

  const txt = [
    "**Initiate Merit Transfer**",
    "",
    `Target User: ${target}`,
    `Amount: ${amount}`,
    "",
    `Notes:`,
    notesDisplay || ZWSP,
  ].join("\n");

  return embedWithImageAndText(null, txt);
}

function opMeritTransferMenuComponents(state) {
  const bTarget = new ButtonBuilder()
    .setCustomId("op_cmd_mt_target")
    .setLabel("Target User")
    .setStyle(ButtonStyle.Primary);

  const bAmount = new ButtonBuilder()
    .setCustomId("op_cmd_mt_amount")
    .setLabel("Amount")
    .setStyle(ButtonStyle.Primary);

  const bNotes = new ButtonBuilder()
    .setCustomId("op_cmd_mt_notes")
    .setLabel("Notes")
    .setStyle(ButtonStyle.Primary);

  const d = { target_user_id: state?.target_user_id ?? null, amount: state?.amount ?? null, notes: state?.notes ?? null };
  const v = validateMeritTransferDraft(d);

  const bSubmit = new ButtonBuilder()
    .setCustomId("op_cmd_mt_submit")
    .setLabel("Submit")
    .setStyle(ButtonStyle.Success)
    .setDisabled(!v.ok);

  const bBack = new ButtonBuilder()
    .setCustomId("op_cmd_mt_back")
    .setLabel("Back")
    .setStyle(ButtonStyle.Secondary);

  // Layout: [Target User, Amount, Notes] then [Submit, Back]
  const row1 = new ActionRowBuilder().addComponents(bTarget, bAmount, bNotes);
  const row2 = new ActionRowBuilder().addComponents(bSubmit, bBack);
  return [row1, row2];
}


async function showMeritTransferTargetModal(interaction, draft) {
  const modal = new ModalBuilder().setCustomId(MODALS.MT_TARGET).setTitle("Merit Transfer");

  const inputTarget = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_TARGET)
    .setLabel("Target User (mention or user ID)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(6)
    .setMaxLength(64)
    .setRequired(true);

  if (draft?.target_user_id) {
    try { inputTarget.setValue(String(draft.target_user_id)); } catch (_) {}
  }

  const row1 = new ActionRowBuilder().addComponents(inputTarget);
  modal.addComponents(row1);

  // showModal ACKs the interaction — do NOT defer/reply beforehand.
  await interaction.showModal(modal);
}

async function showMeritTransferAmountModal(interaction, draft) {
  const modal = new ModalBuilder().setCustomId(MODALS.MT_AMOUNT).setTitle("Merit Transfer");

  const inputAmount = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_AMOUNT)
    .setLabel("Amount (1–9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true);

  if (draft?.amount != null) {
    try { inputAmount.setValue(String(draft.amount)); } catch (_) {}
  }

  const row1 = new ActionRowBuilder().addComponents(inputAmount);
  modal.addComponents(row1);

  // showModal ACKs the interaction — do NOT defer/reply beforehand.
  await interaction.showModal(modal);

}

async function showMeritTransferNotesModal(interaction, draft) {
  const modal = new ModalBuilder().setCustomId(MODALS.MT_NOTES).setTitle("Merit Transfer — Notes");

  const inputNotes = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_NOTES)
    .setLabel("Notes (5–1000 chars)")
    .setStyle(TextInputStyle.Paragraph)
    .setMinLength(5)
    .setMaxLength(1000)
    .setRequired(true);

  if (draft?.notes) {
    try { inputNotes.setValue(String(draft.notes).slice(0, 1000)); } catch (_) {}
  }

  modal.addComponents(new ActionRowBuilder().addComponents(inputNotes));

  // showModal ACKs the interaction — do NOT defer/reply beforehand.
  await interaction.showModal(modal);
}


async function showMeritTransferDetailsModal(interaction, draft) {
  const modal = new ModalBuilder().setCustomId(MODALS.MT_DETAILS).setTitle("Merit Transfer");

  const inputTarget = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_TARGET)
    .setLabel("Target User (mention or user ID)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(6)
    .setMaxLength(64)
    .setRequired(true);

  const inputAmount = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_AMOUNT)
    .setLabel("Amount (1–9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true);

  const inputNotes = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.MT_NOTES)
    .setLabel("Notes (5–1000 chars)")
    .setStyle(TextInputStyle.Paragraph)
    .setMinLength(5)
    .setMaxLength(1000)
    .setRequired(true);

  if (draft?.target_user_id) inputTarget.setValue(`<@${draft.target_user_id}>`);
  if (draft?.amount) inputAmount.setValue(String(draft.amount));
  if (draft?.notes) inputNotes.setValue(String(draft.notes).slice(0, 1000));

  modal.addComponents(
    new ActionRowBuilder().addComponents(inputTarget),
    new ActionRowBuilder().addComponents(inputAmount),
    new ActionRowBuilder().addComponents(inputNotes)
  );

  await interaction.showModal(modal);
}

async function refreshOperatorCommandsUI(guildId, userId) {
  const s = ensureOperatorSession(guildId, userId);
  if (!s.uiInteraction) return;

  try {
    if (s.screen === "op_cmd_merit_transfer") {
      const d = s.commandsDraft.merit_transfer;
      const state = { target_user_id: d.target_user_id, amount: d.amount, notes: d.notes };
      await editEphemeral(s.uiInteraction, opMeritTransferMenuEmbed(state), opMeritTransferMenuComponents(state));
    } else if (s.screen === "op_cmd_menu") {
      await editEphemeral(s.uiInteraction, opCommandsMenuEmbed(), opCommandsMenuComponents());
    } else if (s.screen === "op_cmd_roll_call") {
      const d = s.commandsDraft.roll_call;
      await editEphemeral(s.uiInteraction, opRollCallEmbed(d), opRollCallComponents(d));
    } else if (s.screen === "op_cmd_announcement") {
      const d = s.commandsDraft.announcement;
      const state = { audio_url: d.audio_url, audio_name: d.audio_name };
      await editEphemeral(s.uiInteraction, opAnnouncementMenuEmbed(state), opAnnouncementMenuComponents(state));
    }
  } catch (e) {
    console.error("Failed to refresh operator commands UI:", e);
  }
}

async function handleOpAnnouncementOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_cmd_announcement";
  s.uiInteraction = interaction;

  const d = s.commandsDraft.announcement;
  const state = { audio_url: d.audio_url, audio_name: d.audio_name };
  await editEphemeral(interaction, opAnnouncementMenuEmbed(state), opAnnouncementMenuComponents(state));
}

async function handleOpAnnouncementBack(interaction) {
  await ackButton(interaction);
  return await handleOpCommands(interaction);
}

async function handleOpAnnouncementUploadOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "op_cmd_announcement_upload";
  s.uiInteraction = interaction;

  s.audioWait = { kind: "announcement", channel_id: interaction.channelId };
  s.audioWaitUntilMs = Date.now() + 2 * 60 * 1000;

  await editEphemeral(interaction, opAnnouncementUploadEmbed(), opAnnouncementUploadComponents());
}

async function handleOpAnnouncementUploadBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.audioWait = null;
  s.audioWaitUntilMs = 0;
  return await handleOpAnnouncementOpen(interaction);
}


async function handleOpMeritTransferDetails(interaction) {
  // Legacy (unused): kept for compatibility. Do not ACK before showModal.
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;
  await showMeritTransferDetailsModal(interaction, d);
}

async function handleOpMeritTransferTarget(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;
  await showMeritTransferTargetModal(interaction, d);
}

async function handleOpMeritTransferAmount(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;
  await showMeritTransferAmountModal(interaction, d);
}

async function handleOpMeritTransferNotes(interaction) {
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;
  await showMeritTransferNotesModal(interaction, d);
}

async function handleOpMeritTransferSubmit(interaction) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const operatorUserId = interaction.user.id;

  const s = ensureOperatorSession(guildId, operatorUserId);
  const d = s.commandsDraft.merit_transfer;

  const v = validateMeritTransferDraft(d);
  if (!v.ok) {
    await editEphemeral(interaction, embedWithImageAndText(null, `Cannot submit: ${v.reason}`), opMeritTransferMenuComponents(d));
    return;
  }

  // Validate target exists in this guild (best-effort)
  const guild = await fetchEnvGuild().catch(() => null);
  if (!guild) {
    await editEphemeral(interaction, embedWithImageAndText(null, "Guild not available."), opMeritTransferMenuComponents(d));
    return;
  }
  const targetId = String(d.target_user_id);
  const member = await guild.members.fetch(targetId).catch(() => null);
  if (!member) {
    await editEphemeral(interaction, embedWithImageAndText(null, "Target user not found in this server."), opMeritTransferMenuComponents(d));
    return;
  }

  const reviewChannelId = resolveChannelId(UI.CH_LT_COMMAND_REVIEWS);
  const reviewChannel = await guild.channels.fetch(reviewChannelId).catch(() => null);
  if (!reviewChannel || !reviewChannel.isTextBased()) {
    await editEphemeral(interaction, embedWithImageAndText(null, "LT review channel not found or not text-based."), opMeritTransferMenuComponents(d));
    return;
  }

  const reqId = await dbInsertMeritTransferRequest({
    guildId,
    operatorUserId,
    targetUserId: targetId,
    amount: Number(d.amount),
    notes: String(d.notes).trim(),
    reviewChannelId,
  });

  const approveBtn = new ButtonBuilder()
    .setCustomId("mt_review_approve")
    .setLabel("Approve")
    .setStyle(ButtonStyle.Success);

  const denyBtn = new ButtonBuilder()
    .setCustomId("mt_review_deny")
    .setLabel("Deny")
    .setStyle(ButtonStyle.Danger);

  const row = new ActionRowBuilder().addComponents(approveBtn, denyBtn);

  const reviewEmbed = embedWithImageAndText(
    null,
    [
      "**Merit Transfer Request**",
      "",
      `Request ID: **${reqId}**`,
      `Guild: **${guildId}**`,
      `Operator: <@${operatorUserId}>`,
      `Target: <@${targetId}>`,
      `Amount: **${Number(d.amount)}**`,
      "",
      "**Notes**",
      String(d.notes).trim(),
      "",
      "_Pending LT decision_",
    ].join("\n")
  );

  const msg = await reviewChannel.send({ embeds: [reviewEmbed], components: [row] });

  await dbSetMeritTransferReviewMessage({ guildId, id: reqId, reviewMessageId: msg.id });

  // clear draft
  d.target_user_id = null;
  d.amount = null;
  d.notes = null;

  await editEphemeral(interaction, embedWithImageAndText(null, "Merit transfer submitted for LT review."), opCommandsMenuComponents());
}

async function handleMeritTransferReviewDecision(interaction, decision) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const msgId = interaction.message?.id;
  if (!guildId || !msgId) return;

  const req = await dbGetMeritTransferByReviewMessage(guildId, msgId);
  if (!req) return;

  if (req.status !== "pending") {
    try {
      await interaction.message.edit({ components: [] });
    } catch (_) {}
    return;
  }

  const status = decision === "approve" ? "approved" : "denied";
  await dbUpdateMeritTransferDecision({ guildId, id: req.id, status, decidedByUserId: interaction.user.id });

  let afterMerits = null;
  if (status === "approved") {
    afterMerits = await dbAddUserMerits({ guildId, userId: req.target_user_id, delta: Number(req.amount) });

    // DM recipient (fail silently if DMs are closed)
    try {
      const user = await client.users.fetch(req.target_user_id);
      await user.send({ content: `You have just received ${Number(req.amount)} Merits from an Operator.` });
    } catch (e) {
      // ignore DM failures
    }
  }

  const decidedText =
    status === "approved"
      ? `✅ **Approved** by <@${interaction.user.id}> — Target now has **${afterMerits}** merits.`
      : `⛔ **Denied** by <@${interaction.user.id}>.`;

  const updatedEmbed = embedWithImageAndText(
    null,
    [
      "**Merit Transfer Request**",
      "",
      `Request ID: **${req.id}**`,
      `Operator: <@${req.operator_user_id}>`,
      `Target: <@${req.target_user_id}>`,
      `Amount: **${Number(req.amount)}**`,
      "",
      "**Notes**",
      String(req.notes ?? "").trim() || ZWSP,
      "",
      decidedText,
    ].join("\n")
  );

  try {
    await interaction.message.edit({ embeds: [updatedEmbed], components: [] });
  } catch (e) {
    console.error("Failed to edit merit transfer review message:", e);
  }

  console.log("[MERIT_TRANSFER]", {
    guild_id: guildId,
    request_id: req.id,
    decision: status,
    decided_by: interaction.user.id,
    operator_user_id: req.operator_user_id,
    target_user_id: req.target_user_id,
    amount: Number(req.amount),
  });
      invalidateLeaderboardCache(guildId);
}

async function handleOpAnnouncementInitiate(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.announcement;

  if (!d.audio_url) return await handleOpAnnouncementOpen(interaction);

  const guild = await client.guilds.fetch(interaction.guildId);
  const reviewChannelId = resolveChannelId(UI.CH_LT_COMMAND_REVIEWS);
  const reviewCh = await guild.channels.fetch(reviewChannelId).catch(() => null);
  if (!reviewCh || !reviewCh.isTextBased()) {
    return await editEphemeral(interaction, embedWithImageAndText(null, "LT review channel not found."), opCommandsMenuComponents());
  }

  const baseTxt = getRender(UI.ANN_REVIEW_TEXT).text ?? "Operator <@*Discord user ID*> has just submitted an Announcement for review.";
  const txt = String(baseTxt).replace(/@\*Discord user ID\*/g, `<@${interaction.user.id}>`);

  const bApprove = makeUiButton(UI.ANN_REVIEW_BTN_APPROVE, ButtonStyle.Success);
  const bDeny = makeUiButton(UI.ANN_REVIEW_BTN_DENY, ButtonStyle.Danger);
  const row = new ActionRowBuilder().addComponents(bApprove, bDeny);

  const msg = await reviewCh.send({
    content: txt,
    files: [{ attachment: await fetchUrlToBuffer(d.audio_url), name: d.audio_name ?? "announcement.mp3" }],
    components: [row],
  });

  await dbInsertAnnouncementRequest({
    guildId: interaction.guildId,
    operatorUserId: interaction.user.id,
    audioUrl: d.audio_url,
    audioName: d.audio_name ?? "announcement.mp3",
    reviewChannelId: reviewChannelId,
    reviewMessageId: msg.id,
  });

  // clear draft
  d.audio_url = null;
  d.audio_name = null;
  s.audioWait = null;
  s.audioWaitUntilMs = 0;

  await editEphemeral(interaction, embedWithImageAndText(null, "Announcement submitted for LT review."), opCommandsMenuComponents());
}

async function handleAnnouncementReviewDecision(interaction, decision) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const msgId = interaction.message?.id;
  if (!guildId || !msgId) return;

  const req = await dbGetAnnouncementByReviewMessage(guildId, msgId);
  if (!req) return;

  if (req.status !== "pending") {
    // already decided: just disable buttons
    try {
      await interaction.message.edit({ components: [] });
    } catch (_) {}
    return;
  }

  const status = decision === "approve" ? "approved" : "denied";
  await dbUpdateAnnouncementDecision({ id: req.id, guildId, status, decidedByUserId: interaction.user.id });

  // update review message
  try {
    await interaction.message.edit({ content: `${interaction.message.content}\n\n**${status.toUpperCase()}**`, components: [] });
  } catch (_) {}

  if (status !== "approved") return;

  // On approve: DM all Core Signal users
  const guild = await client.guilds.fetch(guildId);
  const roleId = resolveRoleId(UI.ROLE_SIGNAL);
  const dmText = getRender(UI.ANN_DM_TEXT).text ?? "You have received a new signal from LT.LICKME..";

  // Fetch members (may require GuildMembers intent which you already have)
  await guild.members.fetch().catch(() => null);
  const members = guild.members.cache.filter((m) => m.roles.cache.has(roleId) && !m.user.bot);

  for (const member of members.values()) {
    try {
      await member.send({
        content: dmText,
        files: [{ attachment: await fetchUrlToBuffer(req.audio_url), name: req.audio_name ?? "announcement.mp3" }],
      });
    } catch (_) {
      // ignore DM failures
    }
  }
}






async function handleEventUploadOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_upload";
  s.uiInteraction = interaction;
  s.eventDraft = {
    name: null,
    merits: null,
    schedule_begin: null,
    schedule_end: null,
    check_in_role_key: null,
    resolved_role_id: null,
    gifs: { main: null, check_in: null, back: null },
  };
  await editEphemeral(interaction, opEventUploadEmbed(s), opEventUploadComponents(s));
}

async function handleEventUploadBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_menu";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventsMenuEmbed(), opEventsMenuComponents());
}

async function handleEventUploadRoleOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_upload_role";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventRoleMenuEmbed(s), await opEventRoleMenuComponents(interaction.guildId, s));
}

async function handleEventRoleBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_upload";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventUploadEmbed(s), opEventUploadComponents(s));
}

async function handleEventRolePick(interaction, checkInRoleKey, roleUiRef) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.eventDraft.check_in_role_key = checkInRoleKey;
  s.eventDraft.resolved_role_id = String(resolveRoleId(roleUiRef));
  await editEphemeral(interaction, opEventRoleMenuEmbed(s), await opEventRoleMenuComponents(interaction.guildId, s));
}

async function handleEventRoleSubmit(interaction) {
  return await handleEventRoleBack(interaction);
}

async function handleEventUploadGifOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_upload_gif";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventGifMenuEmbed(s), opEventGifMenuComponents(s));
}

async function handleEventGifBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_events_upload";
  s.uiInteraction = interaction;
  await editEphemeral(interaction, opEventUploadEmbed(s), opEventUploadComponents(s));
}

async function handleEventGifPick(interaction, slot) {
  await ackButton(interaction);
  const key = sessionKey(interaction.guildId, interaction.user.id);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  const raw = String(slot);
  const isEdit = raw.startsWith("event_edit_");
  const context = isEdit ? "event_edit" : "event_upload";
  const slotKey = raw.replace(/^event_edit_/, "").replace(/^event_/, "");

  if (context === "event_edit") {
    s.lastGifContext = "event_edit";
    s.screen = "evt_sched_edit_gif_wait";
  } else {
    s.lastGifContext = "event";
    s.screen = "evt_gif_wait";
  }

  pendingGifUploads.set(key, {
    channelId: interaction.channelId,
    slot: raw,
    slotKey,
    context,
    eventId: context === "event_edit" ? (s.eventEdit?.eventId ?? null) : null,
    expiresAt: Date.now() + UPLOAD_TIMEOUT_MS,
  });

  const embed = eventGifWaitEmbed(slotKey);
  await editEphemeral(interaction, embed, gifWaitComponents());
}

async function handleEventUploadSubmit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.eventDraft;

  const hasAll =
    !!d.name &&
    Number.isInteger(d.merits) &&
    !!d.schedule_begin &&
    !!d.schedule_end &&
    !!d.check_in_role_key &&
    !!d.resolved_role_id &&
    !!d.gifs.main &&
    !!d.gifs.check_in &&
    !!d.gifs.back;

  if (!hasAll) {
    await editEphemeral(interaction, embedWithImageAndText(null, "Event incomplete. Fill all steps first."), opEventUploadComponents(s));
    return;
  }

  const inUse = await dbHasUpcomingEventWithRoleKey({ guildId: interaction.guildId, checkInRoleKey: d.check_in_role_key }).catch(() => false);
  if (inUse) {
    await editEphemeral(interaction, embedWithImageAndText(null, "That Event role is already allocated to another upcoming event."), opEventUploadComponents(s));
    return;
  }

  let eventId;
  try {
    eventId = await dbInsertEventWithGifs({
      guildId: interaction.guildId,
      createdByUserId: interaction.user.id,
      name: d.name,
      meritsAwarded: d.merits,
      checkInRoleKey: d.check_in_role_key,
      resolvedRoleId: d.resolved_role_id,
      scheduleBeginRaw: d.schedule_begin,
      scheduleEndRaw: d.schedule_end,
      gifMainUrl: d.gifs.main,
      gifCheckInUrl: d.gifs.check_in,
      gifBackUrl: d.gifs.back,
    });
  } catch (e) {
    console.error("DB insert event failed:", e);
    await editEphemeral(interaction, embedWithImageAndText(null, "❌ Failed to save event to MySQL."), []);
    return;
  }

  const savedName = d.name;
  s.eventDraft = {
    name: null,
    merits: null,
    schedule_begin: null,
    schedule_end: null,
    check_in_role_key: null,
    resolved_role_id: null,
    gifs: { main: null, check_in: null, back: null },
  };

  await editEphemeral(interaction, embedWithImageAndText(null, `✅ Event submitted.\n\n**${savedName}**\nID: ${eventId}`), []);
}

async function handleObjectiveUploadOpen(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_upload_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
}

async function handleObjectiveUploadBack(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "op_objectives_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opObjectivesMenuEmbed(), opObjectivesMenuComponents());
}

/* =========================
   MODALS (Objective Upload)
========================= */
const MODALS = {
  OBJ_NAME: "modal_obj_name",
  OBJ_MERITS: "modal_obj_merits",
  OBJ_SCHEDULE: "modal_obj_schedule",
  EVT_NAME: "modal_evt_name",
  EVT_MERITS: "modal_evt_merits",
  EVT_SCHEDULE: "modal_evt_schedule",
  MT_DETAILS: "modal_mt_details",
  MT_TARGET: "modal_mt_target",
  MT_AMOUNT: "modal_mt_amount",
  MT_NOTES: "modal_mt_notes",
};

const MODAL_INPUTS = {
  OBJ_NAME: "input_obj_name",
  OBJ_MERITS: "input_obj_merits",
  OBJ_BEGIN: "input_obj_begin",
  OBJ_END: "input_obj_end",
  EVT_NAME: "input_evt_name",
  EVT_MERITS: "input_evt_merits",
  EVT_BEGIN: "input_evt_begin",
  EVT_END: "input_evt_end",
  MT_TARGET: "input_mt_target",
  MT_AMOUNT: "input_mt_amount",
  MT_NOTES: "input_mt_notes",
};

async function showNameModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.OBJ_NAME).setTitle("Objective Name");
  const input = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.OBJ_NAME)
    .setLabel("Objective Name")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(64)
    .setRequired(true);

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showMeritsModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.OBJ_MERITS).setTitle("Merits Awarded");
  const input = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.OBJ_MERITS)
    .setLabel("Merits Awarded (1–9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true);

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showScheduleModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.OBJ_SCHEDULE).setTitle("Objective Schedule (PT)");

  const begin = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.OBJ_BEGIN)
    .setLabel("Begin (PT) — YYYY-MM-DD or YYYY-MM-DD HH:mm")
    .setStyle(TextInputStyle.Short)
    .setMinLength(5)
    .setMaxLength(32)
    .setRequired(true);

  const end = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.OBJ_END)
    .setLabel("End (PT) — YYYY-MM-DD or YYYY-MM-DD HH:mm")
    .setStyle(TextInputStyle.Short)
    .setMinLength(5)
    .setMaxLength(32)
    .setRequired(true);

  modal.addComponents(new ActionRowBuilder().addComponents(begin), new ActionRowBuilder().addComponents(end));
  await interaction.showModal(modal);
}

async function showEventNameModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.EVT_NAME).setTitle("Event Name");
  const input = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.EVT_NAME)
    .setLabel("Event Name")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(64)
    .setRequired(true);
  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEventMeritsModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.EVT_MERITS).setTitle("Merits Awarded");
  const input = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.EVT_MERITS)
    .setLabel("Merits (1-9999)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(1)
    .setMaxLength(4)
    .setRequired(true);
  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function showEventScheduleModal(interaction) {
  const modal = new ModalBuilder().setCustomId(MODALS.EVT_SCHEDULE).setTitle("Event Schedule (PT)");
  const inputBegin = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.EVT_BEGIN)
    .setLabel("Begin (YYYY-MM-DD or YYYY-MM-DD HH:mm)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(10)
    .setMaxLength(16)
    .setRequired(true);
  const inputEnd = new TextInputBuilder()
    .setCustomId(MODAL_INPUTS.EVT_END)
    .setLabel("End (YYYY-MM-DD or YYYY-MM-DD HH:mm)")
    .setStyle(TextInputStyle.Short)
    .setMinLength(10)
    .setMaxLength(16)
    .setRequired(true);
  modal.addComponents(new ActionRowBuilder().addComponents(inputBegin), new ActionRowBuilder().addComponents(inputEnd));
  await interaction.showModal(modal);
}


async function refreshOperatorUploadUI(guildId, userId) {
  const s = ensureOperatorSession(guildId, userId);
  if (!s.uiInteraction) return;

  try {
    await editEphemeral(s.uiInteraction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
  } catch (e) {
    console.error("Failed to refresh operator UI:", e);
  }
}

async function refreshOpEventsUploadUI(guildId, userId) {
  const key = sessionKey(guildId, userId);
  const s = operatorSession.get(key);
  if (!s?.uiInteraction) return;
  try {
    await editEphemeral(s.uiInteraction, opEventUploadEmbed(s), opEventUploadComponents(s));
  } catch (e) {
    console.error("refreshOpEventsUploadUI failed:", e);
  }
}


/* =========================
   FILE TYPE HANDLERS
========================= */
async function handleFileTypeOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.screen = "obj_filetype";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opFileTypeEmbed(s), opFileTypeComponents(s));
}
async function handleFileTypePick(interaction, type) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.tempFileType = type;
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opFileTypeEmbed(s), opFileTypeComponents(s));
}
async function handleFileTypeSubmit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  if (!s.tempFileType) {
    await editEphemeral(interaction, opFileTypeEmbed(s), opFileTypeComponents(s));
    return;
  }

  s.draft.file_type = s.tempFileType;
  s.tempFileType = null;
  s.screen = "obj_upload_menu";
  await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
}
async function handleFileTypeBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.tempFileType = null;
  s.screen = "obj_upload_menu";
  s.uiInteraction = interaction;

  await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
}

/* =========================
   GIF UPLOAD HANDLERS
========================= */
function canOpenGifMenu(d) {
  return !!d.name && typeof d.merits === "number" && !!d.schedule_begin && !!d.schedule_end && !!d.file_type;
}
function startPendingGif(guildId, userId, channelId, slot) {
  const key = sessionKey(guildId, userId);
  pendingGifUploads.set(key, {
    slot,
    channelId,
    expiresAt: Date.now() + UPLOAD_TIMEOUT_MS,
  });
}
function clearPendingGif(guildId, userId) {
  pendingGifUploads.delete(sessionKey(guildId, userId));
}

async function handleGifMenuOpen(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  if (!canOpenGifMenu(s.draft)) {
    await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
    return;
  }

  s.screen = "obj_gif_menu";
  await editEphemeral(interaction, opGifMenuEmbed(s), opGifMenuComponents(s));
}

async function handleGifSlot(interaction, slot) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  const d = s.draft;
  if (slot === "submit" && !d.gifs.main) return;
  if (slot === "back" && (!d.gifs.main || !d.gifs.submit)) return;

  s.screen = "obj_gif_wait";
  startPendingGif(interaction.guildId, interaction.user.id, interaction.channelId, slot);

  await editEphemeral(interaction, gifWaitEmbed(slot), gifWaitComponents());
}

async function handleGifWaitBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  const key = sessionKey(interaction.guildId, interaction.user.id);
  const pending = pendingGifUploads.get(key);

  const slot = pending?.slot ? String(pending.slot) : null;

  // If we were waiting on an Event Scheduling Edit GIF upload, return to the edit GIF menu.
  if (slot && slot.startsWith("event_edit_") || s.lastGifContext === "event_edit" || s.screen === "evt_sched_edit_gif_wait" || s.screen === "evt_sched_edit_gif_menu") {
    clearPendingGif(interaction.guildId, interaction.user.id);

    const eventId = s.eventEdit?.eventId;
    if (eventId) {
      const cur = await dbGetEventWithGifs(interaction.guildId, eventId).catch(() => null);
      if (cur) {
        const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
        s.lastGifContext = "event_edit";
        s.screen = "evt_sched_edit_gif_menu";
        await editEphemeral(interaction, opEventEditGifMenuEmbed(entry), opEventEditGifMenuComponents(entry));
        return;
      }
    }

    // Fallback: go back to edit screen if we can't load the event
    s.screen = "evt_sched_edit";
    await editEphemeral(interaction, embedWithImageAndText(null, "Back."), []);
    return;
  }

  const isEventUpload = (slot && slot.startsWith("event_")) || s.lastGifContext === "event";

  clearPendingGif(interaction.guildId, interaction.user.id);

  if (isEventUpload) {
    s.screen = "op_events_upload_gif";
    await editEphemeral(interaction, opEventGifMenuEmbed(s), opEventGifMenuComponents(s));
  } else {
    s.screen = "obj_gif_menu";
    await editEphemeral(interaction, opGifMenuEmbed(s), opGifMenuComponents(s));
  }
}



async function handleGifMenuSubmit(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  s.screen = "obj_upload_menu";
  await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
}

async function handleGifMenuBack(interaction) {
  await ackButton(interaction);
  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  s.screen = "obj_upload_menu";
  await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
}

/* =========================
   OBJECTIVE SUBMIT (MYSQL)
========================= */
function draftIsSubmittable(d) {
  return (
    !!d.name &&
    typeof d.merits === "number" &&
    !!d.schedule_begin &&
    !!d.schedule_end &&
    !!d.file_type &&
    !!d.gifs.main &&
    !!d.gifs.submit &&
    !!d.gifs.back
  );
}

async function handleObjectiveSubmit(interaction) {
  await ackButton(interaction);

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;

  if (!draftIsSubmittable(s.draft)) {
    await editEphemeral(interaction, opObjectiveUploadEmbed(s), opObjectiveUploadComponents(s));
    return;
  }

  // UPDATED: schedule parsing rules:
  // - Begin accepts "YYYY-MM-DD" (defaults to 00:00 PT) or "YYYY-MM-DD HH:mm"
  // - End   accepts "YYYY-MM-DD" (defaults to 23:59 PT) or "YYYY-MM-DD HH:mm"
  // - Begin CAN be in the past (instant active / edit-safe)
  // - End must be strictly after Begin
  const beginUtc = parsePtToUtcJsDate(s.draft.schedule_begin, { dateOnlyMode: "start" });
  const endUtc = parsePtToUtcJsDate(s.draft.schedule_end, { dateOnlyMode: "end" });

  if (!beginUtc || !endUtc) {
    const embed = embedWithImageAndText(
      null,
      `❌ Invalid schedule format.\nUse:\n• **YYYY-MM-DD HH:mm** (PT)\n• or **YYYY-MM-DD** (date only)`
    );
    await editEphemeral(interaction, embed, []);
    return;
  }

  if (endUtc.getTime() <= beginUtc.getTime()) {
    const embed = embedWithImageAndText(null, `❌ End time must be after begin time.`);
    await editEphemeral(interaction, embed, []);
    return;
  }

  // Insert objective + gifs into MySQL
  let objectiveId;
  try {
    objectiveId = await dbInsertObjectiveWithGifs({
      guildId: interaction.guildId,
      createdByUserId: interaction.user.id,
      name: s.draft.name,
      meritsAwarded: s.draft.merits,
      proofType: s.draft.file_type,
      beginUtc,
      endUtc,
      gifMainUrl: s.draft.gifs.main,
      gifSubmitUrl: s.draft.gifs.submit,
      gifBackUrl: s.draft.gifs.back,
    });
  } catch (e) {
    console.error("DB insert objective failed:", e);
    const embed = embedWithImageAndText(null, "❌ Failed to save objective to MySQL.");
    await editEphemeral(interaction, embed, []);
    return;
  }

  const savedName = s.draft.name;
  clearDraft(s);

  const embed = embedWithImageAndText(null, `✅ Objective submitted.\n\n**${savedName}**\nID: ${objectiveId}`);
  await editEphemeral(interaction, embed, []);
}

/* =========================
   MESSAGE CREATE — GIF INTAKE (STRICT CHANNEL + S3)
========================= */
client.on(Events.MessageCreate, async (message) => {
  try {

// DM Objective proof intake
if (!message.guildId) {
  if (message.author.bot) return;
if (!message.channel || !message.channel.isDMBased?.()) return;

  const key = sessionKey(envGuildId(), message.author.id);
  const sess = objectiveSubmitSession.get(key);
  if (!sess) return;

  const vars = {
    "*Objective name + ID*": `${sess.objectiveName} ID: ${sess.objectiveId}`,
    "*Discord user ID*": message.author.id,
  };

  // timeout
  if (Date.now() > sess.expiresAt) {
    objectiveSubmitSession.delete(key);
    const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_TIMEOUT).text ?? "", vars) || ZWSP;
    try { await message.author.send({ content: txt }); } catch (_) {}
    return;
  }

  // already submitted
  if (sess.submitted) {
    const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY).text ?? "", vars) || ZWSP;
    const row = new ActionRowBuilder().addComponents(
      makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_A),
      makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_B)
    );
    try { await message.author.send({ content: txt, components: [row] }); } catch (_) {}
    return;
  }

  const proofType = sess.proofType;

  // TEXT PROOF
  if (proofType === "text") {
    const content = String(message.content ?? "").trim();
    if (!content) return;

    const maxChars = 1000;
    if (content.length > maxChars) {
      const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_CHAR).text ?? "", vars) || ZWSP;
      try { await message.author.send({ content: txt }); } catch (_) {}
      return;
    }

    let submissionId;
    try {
      submissionId = await dbInsertObjectiveSubmission({
        guildId: envGuildId(),
        objectiveId: sess.objectiveId,
        userId: message.author.id,
        proofType,
        proofUrl: null,
        proofText: content,
      });
    } catch (e) {
      console.error("objective submission insert failed:", e);
      const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_TYPE).text ?? "", vars) || ZWSP;
      try { await message.author.send({ content: txt }); } catch (_) {}
      return;
    }

    sess.submitted = true;
    objectiveSubmitSession.set(key, sess);

    const success = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_SUCCESS).text ?? "", vars) || ZWSP;
    try { await message.author.send({ content: success }); } catch (_) {}

    await postObjectiveSubmissionToReviewChannel({
      guildId: envGuildId(),
      objectiveName: sess.objectiveName,
      objectiveId: sess.objectiveId,
      userId: message.author.id,
      submissionId,
      proofUrl: null,
      proofText: content,
    });

    return;
  }

  // FILE PROOF TYPES
  const att = message.attachments.first();
  if (!att) return;

  if (!proofAttachmentMatchesType(proofType, att)) {
    const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_TYPE).text ?? "", vars) || ZWSP;
    try { await message.author.send({ content: txt }); } catch (_) {}
    return;
  }

  let storedUrl;
  try {
    const s3Key = buildStoredProofKey({
      guildId: envGuildId(),
      objectiveId: sess.objectiveId,
      userId: message.author.id,
      filename: att.name,
    });
    storedUrl = await uploadProofFromDiscordToS3(att.url, s3Key, att.contentType);
  } catch (e) {
    console.error("proof upload failed:", e);
    const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_SIZE).text ?? "", vars) || ZWSP;
    try { await message.author.send({ content: txt }); } catch (_) {}
    return;
  }

  let submissionId;
  try {
    submissionId = await dbInsertObjectiveSubmission({
      guildId: envGuildId(),
      objectiveId: sess.objectiveId,
      userId: message.author.id,
      proofType,
      proofUrl: storedUrl,
      proofText: null,
    });
  } catch (e) {
    console.error("objective submission insert failed:", e);
    const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_TYPE).text ?? "", vars) || ZWSP;
    try { await message.author.send({ content: txt }); } catch (_) {}
    return;
  }

  sess.submitted = true;
  objectiveSubmitSession.set(key, sess);

  const success = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_SUCCESS).text ?? "", vars) || ZWSP;
  try { await message.author.send({ content: success }); } catch (_) {}

  await postObjectiveSubmissionToReviewChannel({
    guildId: envGuildId(),
    objectiveName: sess.objectiveName,
    objectiveId: sess.objectiveId,
    userId: message.author.id,
    submissionId,
    proofUrl: storedUrl,
    proofText: null,
  });

  return;
}


if (message.author.bot) return;

    // Operator announcement audio intake (2-minute window)
    // Runs before GIF intake early-return so audio uploads don't get ignored.
    try {
      const sAnn = operatorSession.get(sessionKey(message.guildId, message.author.id)) ?? null;
      const isWaiting =
        (sAnn?.audioWait?.kind === "announcement") ||
        (sAnn?.screen === "op_cmd_announcement_upload");

      if (isWaiting) {
        const until = sAnn.audioWaitUntilMs ?? 0;
        const inTime = Date.now() <= until;
        const expectedChannelId =
          sAnn?.audioWait?.channel_id ??
          sAnn?.uiInteraction?.channelId ??
          null;

        const sameChannel = expectedChannelId
          ? String(expectedChannelId) === String(message.channel.id)
          : true;

        if (!inTime) {
          sAnn.audioWait = null;
          sAnn.audioWaitUntilMs = 0;
        } else if (!sameChannel) {
          // wrong channel; ignore but keep waiting
        } else {
          const atts = Array.from(message.attachments?.values?.() ?? []);
          const pick = atts.find((a) => {
            const n = String(a?.name ?? "").toLowerCase();
            return n.endsWith(".mp3") || n.endsWith(".wav") || n.endsWith(".m4a");
          });

                    if (pick) {
            const name = String(pick.name ?? "");
            const url = String(pick.proxyURL ?? pick.url ?? "");
            const lower = name.toLowerCase();

            let contentType = "application/octet-stream";
            if (lower.endsWith(".mp3")) contentType = "audio/mpeg";
            else if (lower.endsWith(".wav")) contentType = "audio/wav";
            else if (lower.endsWith(".m4a")) contentType = "audio/mp4";

            const s3Key = buildAnnouncementAudioS3Key({
              guildId: message.guildId,
              operatorUserId: message.author.id,
              filename: name,
            });

            const storedUrl = await uploadProofFromDiscordToS3(url, s3Key, contentType);

            sAnn.commandsDraft.announcement.audio_url = storedUrl;
            sAnn.commandsDraft.announcement.audio_name = name;
            sAnn.audioWait = null;
            sAnn.audioWaitUntilMs = 0;
            sAnn.screen = "op_cmd_announcement";

            try { await message.delete(); } catch (_) {}

            const state = { audio_url: storedUrl, audio_name: name };
            if (sAnn.uiInteraction) {
              await editEphemeral(
                sAnn.uiInteraction,
                opAnnouncementMenuEmbed(state),
                opAnnouncementMenuComponents(state)
              );
            }
            return;
          }
        }
      }
    } catch (e) {
      console.error("announcement audio intake error:", e);
    }



    const key = sessionKey(message.guildId, message.author.id);
    const pending = pendingGifUploads.get(key);
    if (!pending) return;

    // strict: same channel only
    if (pending.channelId !== message.channelId) return;

    // timeout
    if (Date.now() > pending.expiresAt) {
      try {
        await message.delete();
      } catch (_) {}

      pendingGifUploads.delete(key);

      const s = operatorSession.get(key);
      if (s?.uiInteraction) {
        const embed = embedWithImageAndText(null, "⏱️ Upload timed out.");
        await editEphemeral(s.uiInteraction, embed, gifWaitComponents());
      }
      return;
    }

    const att = message.attachments.first();
    if (!att) return;

    const filename = (att.name ?? "").toLowerCase();
    const isGif =
      (att.contentType && String(att.contentType).toLowerCase().includes("gif")) ||
      filename.endsWith(".gif");
    if (!isGif) return;

    const s = operatorSession.get(key);
    if (!s) return;

    const pendingMode = pending?.mode ? String(pending.mode) : null;
    let slot = pending?.slot ? String(pending.slot) : null;

    const isEventEditCtx =
      pendingMode === "event_edit" ||
      s?.lastGifContext === "event_edit" ||
      String(s?.screen ?? "").startsWith("evt_sched_edit_gif");

    // Normalize slot so event scheduling edit always uses event_edit_* storage/UI even if slot came through as event_*.
    if (isEventEditCtx && slot && slot.startsWith("event_") && !slot.startsWith("event_edit_")) {
      slot = slot.replace(/^event_/, "event_edit_");
    }

    // Upload to S3/Spaces
    let storedUrl;
    try {
      const s3Key = buildStoredGifKey({
        guildId: message.guildId,
        userId: message.author.id,
        slot,
      });
      storedUrl = await uploadGifFromDiscordToS3(att.url, s3Key);
    } catch (e) {
      console.error("[GIF] S3 upload failed:", e);
      if (s.uiInteraction) {
      if (String(slot).startsWith("edit_")) {
        s.screen = "obj_sched_edit_gif_menu";
        const objectiveId = s.objectiveEdit?.objectiveId;
        const cur = objectiveId ? await dbGetObjectiveWithGifs(message.guildId, objectiveId).catch(() => null) : null;
        if (cur) {
          const entry = getOrCreateEditDraft(message.guildId, message.author.id, cur);
          await editEphemeral(s.uiInteraction, opEditGifMenuEmbed(entry), opEditGifMenuComponents(entry));
        } else {
          await editEphemeral(s.uiInteraction, embedWithImageAndText(null, "✅ GIF uploaded."), []);
        }
      } else if (String(slot).startsWith("event_edit_")) {
        s.lastGifContext = "event_edit";
        s.screen = "evt_sched_edit_gif_menu";
        const eventId = s.eventEdit?.eventId;
        const cur = eventId ? await dbGetEventWithGifs(message.guildId, eventId).catch(() => null) : null;
        if (cur) {
          const entry = getOrCreateEventEditDraft(message.guildId, message.author.id, cur);
          await editEphemeral(s.uiInteraction, opEventEditGifMenuEmbed(entry), opEventEditGifMenuComponents(entry));
        } else {
          await editEphemeral(s.uiInteraction, embedWithImageAndText(null, "✅ GIF uploaded."), []);
        }
      } else if (String(slot).startsWith("event_")) {
        s.lastGifContext = "event";
        s.screen = "op_events_upload_gif";
        await editEphemeral(s.uiInteraction, opEventGifMenuEmbed(s), opEventGifMenuComponents(s));
      } else {
        s.lastGifContext = "objective";
        s.screen = "obj_gif_menu";
        await editEphemeral(s.uiInteraction, opGifMenuEmbed(s), opGifMenuComponents(s));
      }
    }
      return;
    }

    // Store persistent URL in draft (upload or scheduling edit)
    const pendingContext = String(pending?.context ?? pending?.mode ?? "");
    const slotKey = String(pending?.slotKey ?? "").trim() || String(slot ?? "")
      .replace(/^edit_/, "")
      .replace(/^event_edit_/, "")
      .replace(/^event_/, "");

    if (String(slot).startsWith("edit_")) {
      // Objective scheduling edit
      const objectiveId = s.objectiveEdit?.objectiveId;
      if (objectiveId) {
        const cur = await dbGetObjectiveWithGifs(message.guildId, objectiveId).catch(() => null);
        if (cur) {
          const entry = getOrCreateEditDraft(message.guildId, message.author.id, cur);
          entry.draft.gifs[slotKey] = storedUrl;
          entry.updatedAt = Date.now();
        }
      }
    } else if (pendingContext === "event_edit" || String(slot).startsWith("event_edit_")) {
      // Event scheduling edit (do NOT recreate draft after upload)
      const eventId = pending?.eventId ?? s.eventEdit?.eventId ?? null;
      if (eventId) {
        let entry = getEventEditDraftEntry(message.guildId, message.author.id, eventId);
        if (!entry) {
          const cur = await dbGetEventWithGifs(message.guildId, eventId).catch(() => null);
          if (cur) entry = getOrCreateEventEditDraft(message.guildId, message.author.id, cur);
        }
        if (entry) {
          entry.draft.gifs[slotKey] = storedUrl;
          entry.updatedAt = Date.now();
        }
      }
    } else if (pendingContext === "event_upload" || String(slot).startsWith("event_")) {
      // Event upload
      if (!s.eventDraft) s.eventDraft = { name: null, meritsAwarded: null, scheduleBeginAt: null, scheduleEndAt: null, roleKey: null, gifs: { main: null, check_in: null, back: null } };
      if (!s.eventDraft.gifs) s.eventDraft.gifs = { main: null, check_in: null, back: null };
      if (slotKey === "main") s.eventDraft.gifs.main = storedUrl;
      else if (slotKey === "check_in") s.eventDraft.gifs.check_in = storedUrl;
      else if (slotKey === "back") s.eventDraft.gifs.back = storedUrl;
    } else {
      // Objective upload
      s.draft.gifs[slotKey] = storedUrl;
    }

    // delete user upload message (clean channel)
    try {
      await message.delete();
    } catch (_) {}

    pendingGifUploads.delete(key);

    if (s.uiInteraction) {
      if (String(slot).startsWith("edit_")) {
        // Objective scheduling edit menu
        s.screen = "obj_sched_edit_gif_menu";
        const objectiveId = s.objectiveEdit?.objectiveId;
        const cur = objectiveId ? await dbGetObjectiveWithGifs(message.guildId, objectiveId).catch(() => null) : null;
        if (cur) {
          const entry = getOrCreateEditDraft(message.guildId, message.author.id, cur);
          await editEphemeral(s.uiInteraction, opEditGifMenuEmbed(entry), opEditGifMenuComponents(entry));
        } else {
          await editEphemeral(s.uiInteraction, embedWithImageAndText(null, "✅ GIF uploaded."), []);
        }
      } else if (pendingContext === "event_edit" || String(slot).startsWith("event_edit_")) {
        // Event scheduling edit menu (render existing edit draft)
        s.lastGifContext = "event_edit";
        s.screen = "evt_sched_edit_gif_menu";
        const eventId = pending?.eventId ?? s.eventEdit?.eventId ?? null;
        const entry = eventId ? getEventEditDraftEntry(message.guildId, message.author.id, eventId) : null;
        if (entry) {
          await editEphemeral(s.uiInteraction, opEventEditGifMenuEmbed(entry), opEventEditGifMenuComponents(entry));
        } else {
          const cur = eventId ? await dbGetEventWithGifs(message.guildId, eventId).catch(() => null) : null;
          if (cur) {
            const created = getOrCreateEventEditDraft(message.guildId, message.author.id, cur);
            await editEphemeral(s.uiInteraction, opEventEditGifMenuEmbed(created), opEventEditGifMenuComponents(created));
          } else {
            await editEphemeral(s.uiInteraction, embedWithImageAndText(null, "✅ GIF uploaded."), []);
          }
        }
      } else if (pendingContext === "event_upload" || String(slot).startsWith("event_")) {
        // Event upload menu
        s.lastGifContext = "event";
        s.screen = "op_events_upload_gif";
        await editEphemeral(s.uiInteraction, opEventGifMenuEmbed(s), opEventGifMenuComponents(s));
      } else {
        // Objective upload menu
        s.lastGifContext = "objective";
        s.screen = "obj_gif_menu";
        await editEphemeral(s.uiInteraction, opGifMenuEmbed(s), opGifMenuComponents(s));
      }
    }

    // Operator announcement audio intake (2-minute window)
    try {
      const guildId = message.guild?.id ?? null;
      if (guildId) {
                const s = operatorSession.get(sessionKey(guildId, message.author.id)) ?? null;
        if (s?.audioWait?.kind === "announcement") {
          const inTime = Date.now() <= (s.audioWaitUntilMs ?? 0);
          const sameChannel = String(s.audioWait.channel_id) === String(message.channel.id);
          if (inTime && sameChannel) {
            const att = message.attachments?.first?.() ?? null;
            if (att) {
              const name = String(att.name ?? "");
              const url = String(att.url ?? "");
              const lower = name.toLowerCase();
              const ok = lower.endsWith(".mp3") || lower.endsWith(".wav");
              if (ok) {
                s.commandsDraft.announcement.audio_url = url;
                s.commandsDraft.announcement.audio_name = name;
                s.audioWait = null;
                s.audioWaitUntilMs = 0;

                // tidy message
                try { await message.delete(); } catch (_) {}

                const state = { audio_url: url, audio_name: name };
                s.screen = "op_cmd_announcement";
                if (s.uiInteraction) {
                  await editEphemeral(s.uiInteraction, opAnnouncementMenuEmbed(state), opAnnouncementMenuComponents(state));
                }
                return;
              }
            }
          } else {
            // expired / wrong channel: clear wait
            s.audioWait = null;
            s.audioWaitUntilMs = 0;
          }
        }
      }
    } catch (e) {
      console.error("announcement audio intake error:", e);
    }

} catch (err) {
    console.error("gif intake error:", err);
  }
});
async function postObjectiveSubmissionToReviewChannel({
  guildId,
  objectiveName,
  objectiveId,
  userId,
  submissionId,
  proofUrl,
  proofText,
}) {
  try {
    const guild = await client.guilds.fetch(guildId);
    const channelId = resolveChannelId(UI.CH_OBJECTIVE_SUBMISSIONS);
    const channel = await guild.channels.fetch(channelId);
    if (!channel || typeof channel.send !== "function") return;

    const operatorRoleId = resolveRoleId(UI.ROLE_OPERATOR);
    const operatorMention = operatorRoleId ? `<@&${operatorRoleId}>` : "";
    const userMention = `<@${userId}>`;

    // Fetch objective details + main GIF (operator uploaded)
    const [rows] = await pool.query(
      `
      SELECT
        o.id,
        o.name,
        o.merits_awarded,
        o.proof_type,
        o.schedule_begin_at,
        o.schedule_end_at,
        g.main_url
      FROM objectives o
      LEFT JOIN objective_gifs g ON g.objective_id = o.id
      WHERE o.guild_id = ? AND o.id = ?
      LIMIT 1
      `,
      [guildId, objectiveId]
    );

    const obj = rows?.[0] ?? null;

    const safeName = obj?.name ?? objectiveName ?? "Objective";
    const headerLine = `${userMention} has just submitted **${safeName} ID: ${objectiveId}** for review.`;

    let detailsBlock = "";
    if (obj) {
      const beginPt = DateTime.fromJSDate(new Date(obj.schedule_begin_at), { zone: "utc" })
        .setZone(PT_ZONE)
        .toFormat("yyyy-MM-dd HH:mm");
      const endPt = DateTime.fromJSDate(new Date(obj.schedule_end_at), { zone: "utc" })
        .setZone(PT_ZONE)
        .toFormat("yyyy-MM-dd HH:mm");

      detailsBlock = [
        `**Merits Awarded:** ${obj.merits_awarded}`,
        `**Proof Type:** ${obj.proof_type}`,
        `**Schedule (PT):** ${beginPt} → ${endPt}`,
      ].join("\n");
    }

    const proofLines = [];
    if (proofUrl) {
      proofLines.push("**Objective proof:**");
      proofLines.push(proofUrl);
    }
    if (proofText) proofLines.push(proofText);

    const content = [
      operatorMention,
      "",
      headerLine,
      detailsBlock,
      "",
      ...proofLines,
      "",
      `**Submission ID:** ${submissionId}`,
    ]
      .map((v) => String(v ?? "").trim())
      .filter((v) => v.length > 0)
      .join("\n");

    const btnA = makeUiButton(UI.OBJ_REVIEW_BTN_A);
    try { btnA.setLabel("Approve"); } catch (_) {}
    const btnB = makeUiButton(UI.OBJ_REVIEW_BTN_B);
    const btnC = makeUiButton(UI.OBJ_REVIEW_BTN_C, { disabled: true });

    const payload = {
      content,
      components: buildRowsFromButtons([btnA, btnB, btnC]),
    };

    // Include objective main GIF (if present)
    if (obj?.main_url) {
      const embed = new EmbedBuilder().setImage(obj.main_url);
      payload.embeds = [embed];
    }

    const sent = await channel.send(payload);

    // Persist review-post location in DB so we can delete it later even after restarts
    try {
      await dbSetObjectiveSubmissionReviewPost({
        guildId,
        submissionId,
        reviewChannelId: sent.channelId,
        reviewMessageId: sent.id,
      });
    } catch (e) {
      console.error("Failed to persist objective submission review post ids:", e);
    }
  } catch (e) {
    console.error("postObjectiveSubmissionToReviewChannel failed:", e);
  }
}


function isMemberOperator(interaction) {
  const roleId = resolveRoleId(UI.ROLE_OPERATOR);
  if (!roleId) return false;
  const member = interaction.member;
  if (!member || !("roles" in member)) return false;
  return member.roles.cache?.has(roleId) ?? false;
}

async function updateObjectiveReviewMessage(interaction, { status, reviewerUserId }) {
  const canReview = status === "pending_review" || status === "undone";
  const btnA = makeUiButton(UI.OBJ_REVIEW_BTN_A, { disabled: !canReview });
  try { btnA.setLabel("Approve"); } catch (_) {}
  const btnB = makeUiButton(UI.OBJ_REVIEW_BTN_B, { disabled: !canReview });
  const btnC = makeUiButton(UI.OBJ_REVIEW_BTN_C, { disabled: canReview });

  const components = buildRowsFromButtons([btnA, btnB, btnC]);

  // Preserve existing embed (image), just update footer for status clarity
  const embeds = Array.isArray(interaction.message.embeds) ? interaction.message.embeds : [];
  let embed = null;
  if (embeds.length) {
    // clone minimal embed fields
    const e0 = embeds[0];
    embed = new EmbedBuilder(e0.data ?? e0);
  } else {
    embed = new EmbedBuilder();
  }

  const reviewerMention = reviewerUserId ? `<@${reviewerUserId}>` : "";
  let footerText = "";
  if ((status === "approved" || status === "accepted")) footerText = `APPROVED${reviewerMention ? ` by ${reviewerMention}` : ""}`;
  else if (status === "denied") footerText = `DENIED${reviewerMention ? ` by ${reviewerMention}` : ""}`;
  else if (status === "undone") footerText = `UNDONE${reviewerMention ? ` by ${reviewerMention}` : ""}`;
  else footerText = "PENDING REVIEW";

  embed.setFooter({ text: footerText });

  await interaction.message.edit({ components, embeds: [embed] }).catch(() => {});
}

async function handleObjectiveReviewApprove(interaction) {
  await ackButton(interaction);

  if (!isMemberOperator(interaction)) {
    try {
      await interaction.followUp({ content: "This action is only available to Operators.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  const sub = await dbFindObjectiveSubmissionByReviewMessage({
    guildId: interaction.guildId,
    reviewMessageId: interaction.message.id,
  });

  if (!sub) {
    try {
      await interaction.followUp({ content: "Submission not found for this review post.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  if (sub.status !== "pending_review" && sub.status !== "undone") {
    await updateObjectiveReviewMessage(interaction, { status: sub.status, reviewerUserId: sub.reviewed_by_user_id });
    return;
  }

  const merits = Number(sub.objective_merits_awarded) || 0;

  try {
    if (merits > 0) {
      await dbAddUserMerits({ guildId: interaction.guildId, userId: sub.submitted_by_user_id, delta: merits });
    }
    await dbSetObjectiveSubmissionDecision({
      guildId: interaction.guildId,
      submissionId: sub.id,
      status: "accepted",
      reviewerUserId: interaction.user.id,
    });
  } catch (e) {
    console.error("approve objective submission failed:", e);
    try {
      await interaction.followUp({ content: "Failed to approve submission.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  
  // DM submitter (approved)
  try {
    const user = await client.users.fetch(String(sub.submitted_by_user_id));
    const obj = await dbGetObjectiveWithGifs(interaction.guildId, sub.objective_id).catch(() => null);
    const objName = obj?.name ?? "Objective";
    const meritsAwarded = Number(sub.objective_merits_awarded) || 0;
    await safeSendDm(
      user,
      `Your submission for ${objName} ID: ${sub.objective_id} has been approved by the Operators. You have been credited ${meritsAwarded} Merits, well done Soldier – this Objective has now been completed.`
    );
  } catch (_) {}


  await updateObjectiveReviewMessage(interaction, { status: "accepted", reviewerUserId: interaction.user.id });
}

async function handleObjectiveReviewDeny(interaction) {
  await ackButton(interaction);

  if (!isMemberOperator(interaction)) {
    try {
      await interaction.followUp({ content: "This action is only available to Operators.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  const sub = await dbFindObjectiveSubmissionByReviewMessage({
    guildId: interaction.guildId,
    reviewMessageId: interaction.message.id,
  });

  if (!sub) {
    try {
      await interaction.followUp({ content: "Submission not found for this review post.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  if (sub.status !== "pending_review" && sub.status !== "undone") {
    await updateObjectiveReviewMessage(interaction, { status: sub.status, reviewerUserId: sub.reviewed_by_user_id });
    return;
  }

  try {
    await dbSetObjectiveSubmissionDecision({
      guildId: interaction.guildId,
      submissionId: sub.id,
      status: "denied",
      reviewerUserId: interaction.user.id,
    });
  } catch (e) {
    console.error("deny objective submission failed:", e);
    try {
      await interaction.followUp({ content: "Failed to deny submission.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  
  // DM submitter (denied)
  try {
    const user = await client.users.fetch(String(sub.submitted_by_user_id));
    const obj = await dbGetObjectiveWithGifs(interaction.guildId, sub.objective_id).catch(() => null);
    const objName = obj?.name ?? "Objective";
    let components = null;

    if (obj) {
      const st = computeObjectiveStatusFromRow(obj);
      if (st === "active") {
        const submitAgainId = `core_obj_submit_again:${String(interaction.guildId)}:${String(sub.objective_id)}`;
        components = [
          new ActionRowBuilder().addComponents(
            new ButtonBuilder().setCustomId(submitAgainId).setLabel("Submit Again").setStyle(ButtonStyle.Primary)
          ),
        ];
      }
    }

    await safeSendDm(
      user,
      `Your submission for ${objName} ID: ${sub.objective_id} has been denied by the Operators.`,
      components
    );
  } catch (_) {}



  await updateObjectiveReviewMessage(interaction, { status: "denied", reviewerUserId: interaction.user.id });
}

async function handleObjectiveReviewUndo(interaction) {
  await ackButton(interaction);

  if (!isMemberOperator(interaction)) {
    try {
      await interaction.followUp({ content: "This action is only available to Operators.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  const sub = await dbFindObjectiveSubmissionByReviewMessage({
    guildId: interaction.guildId,
    reviewMessageId: interaction.message.id,
  });

  if (!sub) {
    try {
      await interaction.followUp({ content: "Submission not found for this review post.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  if (sub.status === "pending_review") {
    // Nothing to undo yet
    await updateObjectiveReviewMessage(interaction, { status: "pending_review", reviewerUserId: sub.reviewed_by_user_id });
    return;
  }

  const merits = Number(sub.objective_merits_awarded) || 0;

  try {
    if ((sub.status === "approved" || sub.status === "accepted") && merits > 0) {
      await dbAddUserMerits({ guildId: interaction.guildId, userId: sub.submitted_by_user_id, delta: -merits });
    }
    await dbUndoObjectiveSubmissionDecision({
      guildId: interaction.guildId,
      submissionId: sub.id,
      undoUserId: interaction.user.id,
    });
  } catch (e) {
    console.error("undo objective submission failed:", e);
    try {
      await interaction.followUp({ content: "Failed to undo decision.", flags: MessageFlags.Ephemeral });
    } catch (_) {}
    return;
  }

  await updateObjectiveReviewMessage(interaction, { status: "undone", reviewerUserId: interaction.user.id });
}


/* =========================
   READY/* =========================
   READY
========================= */
client.once(Events.ClientReady, async () => {
  console.log(`✅ Bot is online as ${client.user.tag}`);

  try {
    await initDb();
    startSuspendLiftScheduler();
    startLeaderboardTop3AnnouncementScheduler();
  

    // Leaderboard cache refresher (every 2 hours)
    refreshLeaderboardCache(envGuildId(), true).catch((e) => console.error("leaderboard cache refresh error:", e));
    setInterval(() => {
      refreshLeaderboardCache(envGuildId(), true).catch((e) => console.error("leaderboard cache refresh error:", e));
    }, LEADERBOARD_CACHE_TTL_MS);
} catch (e) {
    console.error("❌ MySQL init failed:", e);
    // Still continue posting access posts so you can see bot is alive
  }

  // Event role lifecycle: periodic cleanup after events end
  setInterval(() => {
    runEventRoleLifecycleTick().catch((e) => console.error("event role lifecycle tick error:", e));
  }, EVENT_ROLE_LIFECYCLE_TICK_MS);

  try {
    await postWelcomeAccessPost();
  } catch (err) {
    console.error("❌ Failed to post welcome message:", err);
  }

  try {
    await postTerminalAccessPost();
  } catch (err) {
    console.error("❌ Failed to post terminal access message:", err);
  }

  try {
    await postOperatorAccessPost();
    await postLtTerminalAccessPost();
    await postLeaderboardAccessPost();
  } catch (err) {
    console.error("❌ Failed to post operator access message:", err);
  }


// Roll Call cleanup (close after 1 hour)
setInterval(async () => {
  try {
    await dbCloseExpiredRollCalls();
  } catch (e) {
    console.error("roll call close interval error:", e);
  }
}, 60 * 1000);

  startRollCallCloser();

});

/* =========================
   INTERACTIONS
========================= */

/* =========================
   LT TERMINAL — COMMANDS
========================= */

const LT_UI_MODAL_IDS = {
  ADJ_ADD_TARGET: "lt_adj_add_target_modal",
  ADJ_ADD_AMOUNT: "lt_adj_add_amount_modal",
  ADJ_ADD_NOTES: "lt_adj_add_notes_modal",
  ADJ_DEDUCT_TARGET: "lt_adj_deduct_target_modal",
  ADJ_DEDUCT_AMOUNT: "lt_adj_deduct_amount_modal",
  ADJ_DEDUCT_NOTES: "lt_adj_deduct_notes_modal",
  SUSPEND_TARGET: "lt_suspend_target_modal",
};

const LT_UI_INPUT_IDS = {
  TARGET: "lt_target_user",
  AMOUNT: "lt_amount",
  NOTES: "lt_notes",
  SUSPEND_TARGET: "lt_suspend_target_user",
};

function isInLtTerminalChannel(interaction) {
  const expected = resolveChannelId("[UI: Discord channel-lt-terminal]");
  return String(interaction.channelId) === String(expected);
}

function ensureLtSession(guildId, userId) {
  // Use the existing per-user session container (same lifetime behavior as Operator Terminal)
  const s = ensureOperatorSession(guildId, userId);
  if (!s.ltDraft) {
    s.ltDraft = {
      adjust_add: { target_user_id: null, amount: null, notes: null },
      adjust_deduct: { target_user_id: null, amount: null, notes: null },
      suspend_user: { target_user_id: null, duration_key: null },
    };
  }
  return s;
}

function ltMenuEmbed() {
  const txt = getRender("[UI: LT-Terminal-menu]").text ?? "";
  return new EmbedBuilder().setDescription(txt || ZWSP);
}

function ltMenuComponents() {
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-adjust-merits-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-button]")
    ),
  ];
}

async function refreshLtUI(guildId, userId) {
  const s = ensureLtSession(guildId, userId);
  if (!s.uiInteraction) return;

  try {
    if (s.screen === "lt_menu") {
      await editEphemeral(s.uiInteraction, ltMenuEmbed(), ltMenuComponents());
      return;
    }
    if (s.screen === "lt_adjust_merits_menu") {
      await editEphemeral(s.uiInteraction, ltAdjustMeritsMenuEmbed(), ltAdjustMeritsMenuComponents());
      return;
    }
    if (s.screen === "lt_adjust_add") {
      await editEphemeral(s.uiInteraction, ltAdjustAddEmbed(s.ltDraft.adjust_add), ltAdjustAddComponents(s.ltDraft.adjust_add));
      return;
    }
    if (s.screen === "lt_adjust_deduct") {
      await editEphemeral(s.uiInteraction, ltAdjustDeductEmbed(s.ltDraft.adjust_deduct), ltAdjustDeductComponents(s.ltDraft.adjust_deduct));
      return;
    }
    if (s.screen === "lt_suspend_user") {
      await editEphemeral(s.uiInteraction, ltSuspendEmbed(s.ltDraft.suspend_user), ltSuspendComponents(s.ltDraft.suspend_user));
      return;
    }
    if (s.screen === "lt_suspend_duration_menu") {
      const txt = getRender("[UI: LT-Terminal-suspend-user-duration-menu]").text ?? "";
      await editEphemeral(s.uiInteraction, new EmbedBuilder().setDescription(txt || ZWSP), ltSuspendDurationComponents());
      return;
    }
  } catch (e) {
    console.error("Failed to refresh LT UI:", e);
  }
}

async function handleLtAccess(interaction) {
  await safeDeferEphemeral(interaction);

  if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, {
      content: "This command is only available in #lt-terminal.",
      components: [],
    });
  }

  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_menu";
  await editEphemeral(interaction, ltMenuEmbed(), ltMenuComponents());
}

/* =========================
   LT — Adjust Merits (Menu)
========================= */
function ltAdjustMeritsMenuEmbed() {
  const txt = getRender("[UI: LT-Terminal-adjust-merits-menu]").text ?? "";
  return new EmbedBuilder().setDescription(txt || ZWSP);
}

function ltAdjustMeritsMenuComponents() {
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-back-button]")
    ),
  ];
}

async function handleLtAdjustMeritsMenu(interaction) {
  await ackButton(interaction);
if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, { content: "This command is only available in #lt-terminal.", components: [] });
  }
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_adjust_merits_menu";
  await editEphemeral(interaction, ltAdjustMeritsMenuEmbed(), ltAdjustMeritsMenuComponents());
}

/* =========================
   LT — Adjust Merits (Add)
========================= */
function ltAdjustAddEmbed(draft) {
  const txt = getRender("[UI: LT-Terminal-adjust-merits-add-merits-menu]").text ?? "";
  const t = draft?.target_user_id ? `<@${draft.target_user_id}>` : "—";
  const amt = draft?.amount != null ? String(draft.amount) : "—";
  const notes = draft?.notes ? "✅" : "—";
  return new EmbedBuilder().setDescription([txt || "", "", `**Target:** ${t}`, `**Amount:** ${amt}`, `**Notes:** ${notes}`].join("\n") || ZWSP);
}

function ltAdjustAddComponents(draft) {
  const canSubmit = !!(draft?.target_user_id && draft?.amount != null);
  const submit = makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-initiate-button]");
  submit.setDisabled(!canSubmit);
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-user-id-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-transfer-amount-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-lt-notes-button]")
    ),
    new ActionRowBuilder().addComponents(
      submit,
      makeUiButton("[UI: LT-Terminal-adjust-merits-add-merits-back-button]")
    ),
  ];
}

async function handleLtAdjustAddOpen(interaction) {
  await ackButton(interaction);
if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, { content: "This command is only available in #lt-terminal.", components: [] });
  }
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_adjust_add";
  await editEphemeral(interaction, ltAdjustAddEmbed(s.ltDraft.adjust_add), ltAdjustAddComponents(s.ltDraft.adjust_add));
}

/* =========================
   LT — Adjust Merits (Deduct)
========================= */
function ltAdjustDeductEmbed(draft) {
  const txt = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-menu]").text ?? "";
  const t = draft?.target_user_id ? `<@${draft.target_user_id}>` : "—";
  const amt = draft?.amount != null ? String(draft.amount) : "—";
  const notes = draft?.notes ? "✅" : "—";
  return new EmbedBuilder().setDescription([txt || "", "", `**Target:** ${t}`, `**Amount:** ${amt}`, `**Notes:** ${notes}`].join("\n") || ZWSP);
}

function ltAdjustDeductComponents(draft) {
  const canSubmit = !!(draft?.target_user_id && draft?.amount != null);
  const submit = makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-initiate-button]");
  submit.setDisabled(!canSubmit);
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-user-id-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-transfer-amount-button]"),
      makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-lt-notes-button]")
    ),
    new ActionRowBuilder().addComponents(
      submit,
      makeUiButton("[UI: LT-Terminal-adjust-merits-deduct-merits-back-button]")
    ),
  ];
}

async function handleLtAdjustDeductOpen(interaction) {
  await ackButton(interaction);
if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, { content: "This command is only available in #lt-terminal.", components: [] });
  }
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_adjust_deduct";
  await editEphemeral(interaction, ltAdjustDeductEmbed(s.ltDraft.adjust_deduct), ltAdjustDeductComponents(s.ltDraft.adjust_deduct));
}

/* =========================
   LT — Adjust Merits (Buttons -> Modals)
========================= */
async function showLtModalFromUi(interaction, opts) {
  const modal = new ModalBuilder().setCustomId(opts.customId).setTitle(opts.title);
  const input = new TextInputBuilder()
    .setCustomId(opts.inputId)
    .setLabel(opts.label)
    .setStyle(opts.style === "paragraph" ? TextInputStyle.Paragraph : TextInputStyle.Short)
    .setRequired(opts.required !== false);

  if (typeof opts.minLength === "number") input.setMinLength(opts.minLength);
  if (typeof opts.maxLength === "number") input.setMaxLength(opts.maxLength);

  if (typeof opts.value === "string" && opts.value.length) {
    try { input.setValue(opts.value); } catch (_) {}
  }

  modal.addComponents(new ActionRowBuilder().addComponents(input));
  await interaction.showModal(modal);
}

async function handleLtAdjustAddTarget(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_ADD_TARGET,
    title: "Add Merits — Target User",
    inputId: LT_UI_INPUT_IDS.TARGET,
    label: "Target user (mention or ID)",
    style: "short",
    minLength: 6,
    maxLength: 64,
    value: "",
  });
}

async function handleLtAdjustAddAmount(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_ADD_AMOUNT,
    title: "Add Merits — Amount",
    inputId: LT_UI_INPUT_IDS.AMOUNT,
    label: "Amount (1–9999)",
    style: "short",
    minLength: 1,
    maxLength: 4,
    value: sess.ltDraft.adjust_add.amount != null ? String(sess.ltDraft.adjust_add.amount) : "",
  });
}

async function handleLtAdjustAddNotes(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_ADD_NOTES,
    title: "Add Merits — Notes",
    inputId: LT_UI_INPUT_IDS.NOTES,
    label: "Notes (optional, max 1000)",
    style: "paragraph",
    minLength: 0,
    maxLength: 1000,
    value: sess.ltDraft.adjust_add.notes ? String(sess.ltDraft.adjust_add.notes).slice(0, 1000) : "",
  });
}

async function handleLtAdjustDeductTarget(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_DEDUCT_TARGET,
    title: "Deduct Merits — Target User",
    inputId: LT_UI_INPUT_IDS.TARGET,
    label: "Target user (mention or ID)",
    style: "short",
    minLength: 6,
    maxLength: 64,
    value: "",
  });
}

async function handleLtAdjustDeductAmount(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_DEDUCT_AMOUNT,
    title: "Deduct Merits — Amount",
    inputId: LT_UI_INPUT_IDS.AMOUNT,
    label: "Amount (1–9999)",
    style: "short",
    minLength: 1,
    maxLength: 4,
    value: sess.ltDraft.adjust_deduct.amount != null ? String(sess.ltDraft.adjust_deduct.amount) : "",
  });
}

async function handleLtAdjustDeductNotes(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.ADJ_DEDUCT_NOTES,
    title: "Deduct Merits — Notes",
    inputId: LT_UI_INPUT_IDS.NOTES,
    label: "Notes (optional, max 1000)",
    style: "paragraph",
    minLength: 0,
    maxLength: 1000,
    value: sess.ltDraft.adjust_deduct.notes ? String(sess.ltDraft.adjust_deduct.notes).slice(0, 1000) : "",
  });
}

/* =========================
   LT — Adjust Merits (Submit)
========================= */
async function handleLtAdjustAddSubmit(interaction) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const ltUserId = interaction.user.id;
  const s = ensureLtSession(guildId, ltUserId);
  const d = s.ltDraft.adjust_add;

  const amount = Number(d.amount);
  if (!d.target_user_id || !Number.isInteger(amount) || amount < 1 || amount > 9999) {
    return await interaction.followUp({ content: "Missing or invalid fields.", ephemeral: true });
  }

  await pool.query(
    `INSERT INTO user_merits (guild_id, user_id, merits, updated_at)
     VALUES (?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE merits = merits + VALUES(merits), updated_at = NOW()`,
    [guildId, d.target_user_id, amount]
  );

  await pool.query(
    `INSERT INTO merit_adjustment_requests (guild_id, lt_user_id, target_user_id, mode, amount, notes, created_at, decided_at)
     VALUES (?, ?, ?, 'add', ?, ?, NOW(), NOW())`,
    [guildId, ltUserId, d.target_user_id, amount, d.notes ?? null]
  );

  console.log("[LT_ADJUST_MERITS]", { guild_id: guildId, lt_user_id: ltUserId, target_user_id: d.target_user_id, mode: "add", amount });


  invalidateLeaderboardCache(guildId);
  // Notify recipient (best-effort; ignore if DMs closed)
  try {
    const u = await client.users.fetch(String(d.target_user_id));
    await u.send(`You have just received ${amount} Merits from LT.`);
  } catch (_) {}

  s.ltDraft.adjust_add = { target_user_id: null, amount: null, notes: null };
  s.screen = "lt_adjust_merits_menu";
  await editEphemeral(interaction, ltAdjustMeritsMenuEmbed(), ltAdjustMeritsMenuComponents());
}

async function handleLtAdjustDeductSubmit(interaction) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const ltUserId = interaction.user.id;
  const s = ensureLtSession(guildId, ltUserId);
  const d = s.ltDraft.adjust_deduct;

  const amount = Number(d.amount);
  if (!d.target_user_id || !Number.isInteger(amount) || amount < 1 || amount > 9999) {
    return await interaction.followUp({ content: "Missing or invalid fields.", ephemeral: true });
  }

  const [rows] = await pool.query(
    `SELECT merits FROM user_merits WHERE guild_id = ? AND user_id = ? LIMIT 1`,
    [guildId, d.target_user_id]
  );
  const current = rows?.[0]?.merits != null ? Number(rows[0].merits) : 0;
  const newVal = Math.max(0, current - amount);

  await pool.query(
    `INSERT INTO user_merits (guild_id, user_id, merits, updated_at)
     VALUES (?, ?, ?, NOW())
     ON DUPLICATE KEY UPDATE merits = VALUES(merits), updated_at = NOW()`,
    [guildId, d.target_user_id, newVal]
  );

  await pool.query(
    `INSERT INTO merit_adjustment_requests (guild_id, lt_user_id, target_user_id, mode, amount, notes, created_at, decided_at)
     VALUES (?, ?, ?, 'deduct', ?, ?, NOW(), NOW())`,
    [guildId, ltUserId, d.target_user_id, amount, d.notes ?? null]
  );

  console.log("[LT_ADJUST_MERITS]", { guild_id: guildId, lt_user_id: ltUserId, target_user_id: d.target_user_id, mode: "deduct", amount });


  invalidateLeaderboardCache(guildId);
  // Notify recipient (best-effort; ignore if DMs closed)
  try {
    const u = await client.users.fetch(String(d.target_user_id));
    await u.send(`${amount} Merits have been deducted by LT.`);
  } catch (_) {}

  s.ltDraft.adjust_deduct = { target_user_id: null, amount: null, notes: null };
  s.screen = "lt_adjust_merits_menu";
  await editEphemeral(interaction, ltAdjustMeritsMenuEmbed(), ltAdjustMeritsMenuComponents());
}

async function handleLtAdjustMeritsBackToMenu(interaction) {
  await ackButton(interaction);
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.screen = "lt_menu";
  await editEphemeral(interaction, ltMenuEmbed(), ltMenuComponents());
}

/* =========================
   LT — Suspend User
========================= */
function ltSuspendEmbed(draft) {
  const txt = getRender("[UI: LT-Terminal-suspend-user-menu]").text ?? "";
  const t = draft?.target_user_id ? `<@${draft.target_user_id}>` : "—";
  const dur = draft?.duration_key ? String(draft.duration_key) : "—";
  return new EmbedBuilder().setDescription([txt || "", "", `**Target:** ${t}`, `**Duration:** ${dur}`].join("\n") || ZWSP);
}

function ltSuspendComponents(draft) {
  const canSubmit = !!(draft?.target_user_id && draft?.duration_key);
  const submit = makeUiButton("[UI: LT-Terminal-suspend-user-initiate-button]");
  submit.setDisabled(!canSubmit);
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-suspend-user-user-id-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-button]")
    ),
    new ActionRowBuilder().addComponents(
      submit,
      makeUiButton("[UI: LT-Terminal-suspend-user-back-button]")
    ),
  ];
}

async function handleLtSuspendOpen(interaction) {
  await ackButton(interaction);
if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, { content: "This command is only available in #lt-terminal.", components: [] });
  }
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_suspend_user";
  await editEphemeral(interaction, ltSuspendEmbed(s.ltDraft.suspend_user), ltSuspendComponents(s.ltDraft.suspend_user));
}

function ltSuspendDurationComponents() {
  return [
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-12-hours-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-1-day-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-3-days-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-1-week-button]"),
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-2-weeks-button]")
    ),
    new ActionRowBuilder().addComponents(
      makeUiButton("[UI: LT-Terminal-suspend-user-duration-back-button]")
    ),
  ];
}

async function handleLtSuspendDurationMenu(interaction) {
  await ackButton(interaction);
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.uiInteraction = interaction;
  s.screen = "lt_suspend_duration_menu";
if (!isInLtTerminalChannel(interaction)) {
    return await editEphemeral(interaction, null, null, { content: "This command is only available in #lt-terminal.", components: [] });
  }
  const txt = getRender("[UI: LT-Terminal-suspend-user-duration-menu]").text ?? "";
  await editEphemeral(interaction, new EmbedBuilder().setDescription(txt || ZWSP), ltSuspendDurationComponents());
}

function suspensionDurationSpec(durationKey) {
  const map = {
    "12h": { duration: { hours: 12 }, role_ui: "[UI: Discord role Dishonourably Discharged 12 Hours]" },
    "1d": { duration: { days: 1 }, role_ui: "[UI: Discord role Dishonourably Discharged 1 Day]" },
    "3d": { duration: { days: 3 }, role_ui: "[UI: Discord role Dishonourably Discharged 3 Days]" },
    "1w": { duration: { days: 7 }, role_ui: "[UI: Discord role Dishonourably Discharged 1 Week]" },
    "2w": { duration: { days: 14 }, role_ui: "[UI: Discord role Dishonourably Discharged 2 Weeks]" },
  };
  return map[durationKey] ?? null;
}

async function handleLtSuspendTarget(interaction) {
  const sess = ensureLtSession(interaction.guildId, interaction.user.id);
  await showLtModalFromUi(interaction, {
    customId: LT_UI_MODAL_IDS.SUSPEND_TARGET,
    title: "Suspend User — Target User",
    inputId: LT_UI_INPUT_IDS.SUSPEND_TARGET,
    label: "Target user (mention or ID)",
    style: "short",
    minLength: 6,
    maxLength: 64,
    value: "",
  });
}

async function handleLtSuspendSetDuration(interaction, durationKey) {
  await ackButton(interaction);
  const s = ensureLtSession(interaction.guildId, interaction.user.id);
  s.ltDraft.suspend_user.duration_key = durationKey;
  await editEphemeral(interaction, ltSuspendEmbed(s.ltDraft.suspend_user), ltSuspendComponents(s.ltDraft.suspend_user));
}

async function handleLtSuspendSubmit(interaction) {
  await ackButton(interaction);

  const guildId = interaction.guildId;
  const ltUserId = interaction.user.id;

  const s = ensureLtSession(guildId, ltUserId);
  const d = s.ltDraft.suspend_user;

  if (!d?.target_user_id || !d?.duration_key) {
    return await interaction.followUp({ content: "Missing required fields.", ephemeral: true });
  }

  const spec = suspensionDurationSpec(d.duration_key);
  if (!spec) {
    return await interaction.followUp({ content: "Invalid duration.", ephemeral: true });
  }

  const roleId = resolveRoleId(spec.role_ui);
  if (!roleId) {
    return await interaction.followUp({ content: "Suspended role is not configured.", ephemeral: true });
  }

  const guild = await fetchEnvGuild();
  const member = await guild.members.fetch(d.target_user_id).catch(() => null);
  if (!member) {
    return await interaction.followUp({ content: "Target user not found in this server.", ephemeral: true });
  }

  const startsAt = DateTime.utc();
  const endsAt = startsAt.plus(spec.duration);

  try {
    await member.roles.add(roleId);
  } catch (e) {
    return await interaction.followUp({ content: "Failed to apply suspension role.", ephemeral: true });
  }

    // If user already has an active suspension, reset it (Option A)
  const [activeRows] = await pool.query(
    `SELECT id FROM user_suspensions
     WHERE guild_id = ? AND target_user_id = ? AND lifted_at IS NULL
     ORDER BY ends_at DESC
     LIMIT 1`,
    [guildId, d.target_user_id]
  );

  const activeId = activeRows && activeRows[0] ? Number(activeRows[0].id) : null;

  if (activeId) {
    await pool.query(
      `UPDATE user_suspensions
       SET lt_user_id = ?, duration_key = ?, suspended_role_id_snapshot = ?, starts_at = ?, ends_at = ?, lifted_at = NULL
       WHERE id = ?`,
      [
        ltUserId,
        d.duration_key,
        String(roleId),
        startsAt.toSQL({ includeOffset: false }),
        endsAt.toSQL({ includeOffset: false }),
        activeId,
      ]
    );
  } else {
    await pool.query(
      `INSERT INTO user_suspensions
       (guild_id, lt_user_id, target_user_id, duration_key, suspended_role_id_snapshot, starts_at, ends_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        guildId,
        ltUserId,
        d.target_user_id,
        d.duration_key,
        String(roleId),
        startsAt.toSQL({ includeOffset: false }),
        endsAt.toSQL({ includeOffset: false }),
      ]
    );
  }

console.log("[LT_SUSPEND_USER]", { guild_id: guildId, lt_user_id: ltUserId, target_user_id: d.target_user_id, duration_key: d.duration_key });

  s.ltDraft.suspend_user = { target_user_id: null, duration_key: null };
  s.screen = "lt_menu";
  await editEphemeral(interaction, ltMenuEmbed(), ltMenuComponents());
}


async function runSuspendLiftCheck() {
  const guildId = envGuildId();

  // Find expired suspensions not yet lifted
  const [rows] = await pool.query(
    `SELECT id, target_user_id, suspended_role_id_snapshot
     FROM user_suspensions
     WHERE guild_id = ? AND lifted_at IS NULL AND ends_at <= NOW()
     ORDER BY ends_at ASC
     LIMIT 50`,
    [guildId]
  );

  if (!rows || rows.length === 0) return;

  const guild = await fetchEnvGuild().catch(() => null);

  for (const r of rows) {
    const suspensionId = Number(r.id);
    const targetUserId = String(r.target_user_id);
    const roleId = String(r.suspended_role_id_snapshot);

    // Best-effort role removal (do not fail the whole loop)
    if (guild) {
      const member = await guild.members.fetch(targetUserId).catch(() => null);
      if (member) {
        try {
          await member.roles.remove(roleId);
        } catch (_) {
          // ignore (role missing, perms, already removed, etc.)
        }
      }
    }

    await pool.query(
      `UPDATE user_suspensions SET lifted_at = NOW() WHERE id = ?`,
      [suspensionId]
    );

    console.log("[LT_SUSPEND_LIFT]", {
      guild_id: guildId,
      target_user_id: targetUserId,
      role_id_snapshot: roleId,
      suspension_id: suspensionId,
    });
  }
}

function startSuspendLiftScheduler() {
  // Run once on boot (catch up after restarts)
  runSuspendLiftCheck().catch((e) => console.error("suspend lift check error:", e));

  // Then poll every 15 minutes (low usage; shortest duration is 12h)
  setInterval(() => {
    runSuspendLiftCheck().catch((e) => console.error("suspend lift check error:", e));
  }, 15 * 60 * 1000);
}


async function runSuspensionExpiryTick() {
  const guild = await fetchEnvGuild();
  const guildId = envGuildId();

  const [rows] = await pool.query(
    `SELECT id, target_user_id, suspended_role_id_snapshot
     FROM user_suspensions
     WHERE guild_id = ? AND removed_at IS NULL AND ends_at <= NOW()
     LIMIT 50`,
    [guildId]
  );

  for (const r of rows ?? []) {
    const userId = String(r.target_user_id);
    const roleId = String(r.suspended_role_id_snapshot);

    const member = await guild.members.fetch(userId).catch(() => null);
    if (member) {
      try { await member.roles.remove(roleId); } catch (_) {}
    }

    await pool.query(
      `UPDATE user_suspensions SET removed_at = NOW(), removed_by_user_id = 'system' WHERE id = ?`,
      [r.id]
    );
  }
}


client.on(Events.InteractionCreate, async (interaction) => {
  try {
    // MODALS
    if (interaction.isModalSubmit()) {
      const s = ensureOperatorSession(interaction.guildId, interaction.user.id);

      // LT MODALS (hardcoded IDs — refresh existing ephemeral UI)
      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_ADD_TARGET) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.TARGET);
        const uid = parseUserIdFromInput(raw);
        if (!uid) {
          await interaction.reply({ content: "Invalid user. Use a mention or user ID.", flags: MessageFlags.Ephemeral });
          return;
        }
        sess.ltDraft.adjust_add.target_user_id = uid;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_ADD_AMOUNT) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.AMOUNT);
        const n = Number(String(raw).trim());
        if (!Number.isInteger(n) || n < 1 || n > 9999) {
          await interaction.reply({ content: "Amount must be an integer 1–9999.", flags: MessageFlags.Ephemeral });
          return;
        }
        sess.ltDraft.adjust_add.amount = n;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_ADD_NOTES) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.NOTES);
        const note = String(raw ?? "").trim();
        sess.ltDraft.adjust_add.notes = note.length ? note.slice(0, 1000) : null;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_DEDUCT_TARGET) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.TARGET);
        const uid = parseUserIdFromInput(raw);
        if (!uid) {
          await interaction.reply({ content: "Invalid user. Use a mention or user ID.", flags: MessageFlags.Ephemeral });
          return;
        }
        sess.ltDraft.adjust_deduct.target_user_id = uid;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_DEDUCT_AMOUNT) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.AMOUNT);
        const n = Number(String(raw).trim());
        if (!Number.isInteger(n) || n < 1 || n > 9999) {
          await interaction.reply({ content: "Amount must be an integer 1–9999.", flags: MessageFlags.Ephemeral });
          return;
        }
        sess.ltDraft.adjust_deduct.amount = n;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.ADJ_DEDUCT_NOTES) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.NOTES);
        const note = String(raw ?? "").trim();
        sess.ltDraft.adjust_deduct.notes = note.length ? note.slice(0, 1000) : null;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === LT_UI_MODAL_IDS.SUSPEND_TARGET) {
        const sess = ensureLtSession(interaction.guildId, interaction.user.id);
        const raw = interaction.fields.getTextInputValue(LT_UI_INPUT_IDS.SUSPEND_TARGET);
        const uid = parseUserIdFromInput(raw);
        if (!uid) {
          await interaction.reply({ content: "Invalid user. Use a mention or user ID.", flags: MessageFlags.Ephemeral });
          return;
        }
        sess.ltDraft.suspend_user.target_user_id = uid;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshLtUI(interaction.guildId, interaction.user.id);
        return;
      }


      if (interaction.customId === MODALS.OBJ_NAME) {
        const name = interaction.fields.getTextInputValue(MODAL_INPUTS.OBJ_NAME)?.trim();
        s.draft.name = name || null;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshOperatorUploadUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === MODALS.OBJ_MERITS) {
        const raw = interaction.fields.getTextInputValue(MODAL_INPUTS.OBJ_MERITS)?.trim();
        const n = Number(raw);
        s.draft.merits = Number.isInteger(n) && n >= 1 && n <= 9999 ? n : null;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshOperatorUploadUI(interaction.guildId, interaction.user.id);
        return;
      }

      if (interaction.customId === MODALS.OBJ_SCHEDULE) {
        const begin = interaction.fields.getTextInputValue(MODAL_INPUTS.OBJ_BEGIN)?.trim();
        const end = interaction.fields.getTextInputValue(MODAL_INPUTS.OBJ_END)?.trim();
        s.draft.schedule_begin = begin || null;
        s.draft.schedule_end = end || null;

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await refreshOperatorUploadUI(interaction.guildId, interaction.user.id);
        return;
      }


      // OBJECTIVE SCHEDULING EDIT MODALS
      if (interaction.customId === EDIT_MODALS.NAME) {
        const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
        const objectiveId = s2.objectiveEdit?.objectiveId;
        if (!objectiveId) return;

        const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
        if (!cur) return;

        const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
        const name = interaction.fields.getTextInputValue(EDIT_MODAL_INPUTS.NAME)?.trim();
        entry.draft.name = name || entry.draft.name;
        entry.updatedAt = Date.now();

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await editEphemeral(s2.uiInteraction ?? interaction, editMenuEmbed(cur, entry), editMenuComponents(entry));
        return;
      }


// EVENT SCHEDULING EDIT MODALS
if (interaction.customId === EVENT_EDIT_MODALS.NAME) {
  const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s2.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  const name = interaction.fields.getTextInputValue(EVENT_EDIT_MODAL_INPUTS.NAME)?.trim();
  entry.draft.name = name || entry.draft.name;
  entry.updatedAt = Date.now();

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await editEphemeral(s2.uiInteraction ?? interaction, eventEditMenuEmbed(cur, entry), eventEditMenuComponents(entry));
  return;
}


if (interaction.customId === EVENT_EDIT_MODALS.MERITS) {
  const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s2.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
  const raw = interaction.fields.getTextInputValue(EVENT_EDIT_MODAL_INPUTS.MERITS)?.trim();
  const n = Number(raw);
  if (Number.isInteger(n) && n >= 1 && n <= 9999) {
    entry.draft.merits_awarded = n;
    entry.updatedAt = Date.now();
  }

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await editEphemeral(s2.uiInteraction ?? interaction, eventEditMenuEmbed(cur, entry), eventEditMenuComponents(entry));
  return;
}

if (interaction.customId === EVENT_EDIT_MODALS.SCHEDULE) {
  const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const eventId = s2.eventEdit?.eventId;
  if (!eventId) return;

  const cur = await dbGetEventWithGifs(interaction.guildId, eventId);
  if (!cur) return;

  const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);

  const beginRaw = interaction.fields.getTextInputValue(EVENT_EDIT_MODAL_INPUTS.BEGIN)?.trim();
  const endRaw = interaction.fields.getTextInputValue(EVENT_EDIT_MODAL_INPUTS.END)?.trim();

  const beginPt = DateTime.fromFormat(beginRaw, "yyyy-MM-dd HH:mm", { zone: PT_ZONE });
  const endPt = DateTime.fromFormat(endRaw, "yyyy-MM-dd HH:mm", { zone: PT_ZONE });

  if (beginPt.isValid && endPt.isValid && endPt > beginPt) {
    entry.draft.schedule_begin_at = beginPt.toUTC().toJSDate();
    entry.draft.schedule_end_at = endPt.toUTC().toJSDate();
    entry.updatedAt = Date.now();
  }

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await editEphemeral(s2.uiInteraction ?? interaction, eventEditMenuEmbed(cur, entry), eventEditMenuComponents(entry));
  return;
}

      if (interaction.customId === EDIT_MODALS.MERITS) {
        const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
        const objectiveId = s2.objectiveEdit?.objectiveId;
        if (!objectiveId) return;

        const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
        if (!cur) return;

        const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
        const raw = interaction.fields.getTextInputValue(EDIT_MODAL_INPUTS.MERITS)?.trim();
        const n = Number(raw);
        if (Number.isInteger(n) && n >= 1 && n <= 9999) entry.draft.merits_awarded = n;
        entry.updatedAt = Date.now();

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await editEphemeral(s2.uiInteraction ?? interaction, editMenuEmbed(cur, entry), editMenuComponents(entry));
        return;
      }

      if (interaction.customId === EDIT_MODALS.SCHEDULE) {
        const s2 = ensureOperatorSession(interaction.guildId, interaction.user.id);
        const objectiveId = s2.objectiveEdit?.objectiveId;
        if (!objectiveId) return;

        const cur = await dbGetObjectiveWithGifs(interaction.guildId, objectiveId);
        if (!cur) return;

        const entry = getOrCreateEditDraft(interaction.guildId, interaction.user.id, cur);
        const begin = interaction.fields.getTextInputValue(EDIT_MODAL_INPUTS.BEGIN)?.trim();
        const end = interaction.fields.getTextInputValue(EDIT_MODAL_INPUTS.END)?.trim();
        if (begin) entry.draft.schedule_begin = begin;
        if (end) entry.draft.schedule_end = end;
        entry.updatedAt = Date.now();

        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
        await interaction.deleteReply().catch(() => {});
        await editEphemeral(s2.uiInteraction ?? interaction, editMenuEmbed(cur, entry), editMenuComponents(entry));
        return;
      }

      // ===================== EVENTS: UPLOAD MODALS =====================
      if (interaction.customId === MODALS.EVT_NAME) {
        // ACK first to avoid Discord's "something went wrong"
        await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});
        try {
          const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
          const name = String(interaction.fields.getTextInputValue(MODAL_INPUTS.EVT_NAME) || "").trim();
          s.eventDraft = s.eventDraft || {};
          s.eventDraft.name = name || null;

          await refreshOpEventsUploadUI(interaction.guildId, interaction.user.id);
          await interaction.deleteReply().catch(() => {});
        } catch (e) {
          console.error("EVT_NAME modal error:", e);
          await interaction.editReply({ content: "Something went wrong. Try again." }).catch(() => {});
        }
        return;
      }

      if (interaction.customId === MODALS.EVT_MERITS) {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});
        try {
          const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
          const raw = String(interaction.fields.getTextInputValue(MODAL_INPUTS.EVT_MERITS) || "").trim();
          const n = Number(raw);
          s.eventDraft = s.eventDraft || {};
          s.eventDraft.merits = Number.isInteger(n) && n >= 1 && n <= 9999 ? n : null;

          await refreshOpEventsUploadUI(interaction.guildId, interaction.user.id);
          await interaction.deleteReply().catch(() => {});
        } catch (e) {
          console.error("EVT_MERITS modal error:", e);
          await interaction.editReply({ content: "Something went wrong. Try again." }).catch(() => {});
        }
        return;
      }

      if (interaction.customId === MODALS.EVT_SCHEDULE) {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral }).catch(() => {});
        try {
          const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
          const begin = String(interaction.fields.getTextInputValue(MODAL_INPUTS.EVT_BEGIN) || "").trim();
          const end = String(interaction.fields.getTextInputValue(MODAL_INPUTS.EVT_END) || "").trim();
          s.eventDraft = s.eventDraft || {};
          s.eventDraft.schedule_begin = begin || null;
          s.eventDraft.schedule_end = end || null;

          await refreshOpEventsUploadUI(interaction.guildId, interaction.user.id);
          await interaction.deleteReply().catch(() => {});
        } catch (e) {
          console.error("EVT_SCHEDULE modal error:", e);
          await interaction.editReply({ content: "Something went wrong. Try again." }).catch(() => {});
        }
        return;
      }

      if (interaction.customId === MODALS.MT_TARGET) {
  const rawTarget = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_TARGET)?.trim();

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;

  const targetId = parseUserIdFromInput(rawTarget);
  d.target_user_id = targetId || null;

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await refreshOperatorCommandsUI(interaction.guildId, interaction.user.id);
  return;
}

if (interaction.customId === MODALS.MT_AMOUNT) {
  const rawAmount = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_AMOUNT)?.trim();

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;

  const amt = Number(rawAmount);
  d.amount = Number.isFinite(amt) ? amt : null;

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await refreshOperatorCommandsUI(interaction.guildId, interaction.user.id);
  return;
}

if (interaction.customId === MODALS.MT_NOTES) {
  const notes = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_NOTES)?.trim();

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;

  d.notes = notes || null;

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await refreshOperatorCommandsUI(interaction.guildId, interaction.user.id);
  return;
}

// Backward-compatible legacy modal (not used by current UI)
if (interaction.customId === MODALS.MT_DETAILS) {
  const rawTarget = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_TARGET)?.trim();
  const rawAmount = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_AMOUNT)?.trim();
  const notes = interaction.fields.getTextInputValue(MODAL_INPUTS.MT_NOTES)?.trim();

  const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
  const d = s.commandsDraft.merit_transfer;

  const targetId = parseUserIdFromInput(rawTarget);
  const amt = Number(rawAmount);

  d.target_user_id = targetId || null;
  d.amount = Number.isFinite(amt) ? amt : null;
  d.notes = notes || null;

  await interaction.deferReply({ flags: MessageFlags.Ephemeral });
  await interaction.deleteReply().catch(() => {});
  await refreshOperatorCommandsUI(interaction.guildId, interaction.user.id);
  return;
}



      return;
    }

// BUTTONS
    if (!interaction.isButton()) return;

    const cid = interaction.customId;

    // DM: Objective denied -> Submit Again
    if (cid && typeof cid === "string" && cid.startsWith("core_obj_submit_again:")) {
      await ackButton(interaction);

      const parts = cid.split(":");
      const guildId = parts[1] || interaction.guildId;
      const objectiveId = Number(parts[2] || 0);

      await startObjectiveDmSubmission({
        guildId,
        user: interaction.user,
        objectiveId,
      });

      return;
    }


    // DM buttons
    if (!interaction.inGuild()) {
      const idDmA = getRender(UI.DM_BTN_A).custom_id;
      const idDmB = getRender(UI.DM_BTN_B).custom_id;

      const idAlreadyA = getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_A).custom_id;
      const idAlreadyB = getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_B).custom_id;

      // Comms opt in/out DM panel
      if (interaction.customId === idDmA || interaction.customId === idDmB) {
        await handleDmButton(interaction);
        return;
      }

      // Objective submit: already submitted prompt (Yes/No)
      if (interaction.customId === idAlreadyA || interaction.customId === idAlreadyB) {
        // Always ACK so the DM interaction never "fails"
        try {
          await interaction.deferUpdate();
        } catch (_) {}

        const guildId = envGuildId();
        const key = sessionKey(guildId, interaction.user.id);
        const sess = objectiveSubmitSession.get(key);

        // Clear buttons on the prompt message (no extra text)
        try {
          await interaction.editReply({ components: [] });
        } catch (_) {
          try {
            await interaction.message.edit({ components: [] });
          } catch (_) {}
        }

        // If we don't have an active session, nothing else to do
        if (!sess) return;


        // Guard: if objective already approved/accepted, do not allow cancel/resubmit
        const latestStatus = await dbGetLatestObjectiveSubmissionStatus({
          guildId,
          objectiveId: sess.objectiveId,
          userId: interaction.user.id,
        }).catch(() => "");

        if (latestStatus === "accepted" || latestStatus === "approved") {
          try {
            await interaction.user.send({ content: "This Objective has already been approved. You cannot submit it again." });
          } catch (_) {}
          return;
        }

        const clickedYes = interaction.customId === idAlreadyA;

        if (clickedYes) {
          // Cancel the current pending submission (pending_review) for this objective+user
          let meta = null;
          try {
            meta = await dbGetPendingObjectiveSubmissionMeta({
              guildId,
              objectiveId: sess.objectiveId,
              userId: interaction.user.id,
            });
          } catch (e) {
            console.error("fetch pending objective submission meta failed:", e);
          }

          // Delete the review post (persisted in DB) so this works across restarts
          if (meta?.review_channel_id && meta?.review_message_id) {
            try {
              const guild = await client.guilds.fetch(guildId);
              const ch = await guild.channels.fetch(meta.review_channel_id);
              if (ch && typeof ch.messages?.fetch === "function") {
                const msg = await ch.messages.fetch(meta.review_message_id).catch(() => null);
                if (msg) await msg.delete().catch(() => {});
              }
            } catch (e) {
              console.error("delete objective review post failed:", e);
            }
          }

          try {
            await dbDeletePendingObjectiveSubmission({
              guildId,
              objectiveId: sess.objectiveId,
              userId: interaction.user.id,
            });
          } catch (e) {
            console.error("cancel pending objective submission failed:", e);
          }

          sess.submitted = false;sess.submitted = false;
          sess.expiresAt = Date.now() + UPLOAD_TIMEOUT_MS;
          objectiveSubmitSession.set(key, sess);

          // Re-send the correct DM instructions (UI text) for this proof type
          const dmRef = dmUiRefForProofType(sess.proofType);
          const vars = {
            "*Objective name + ID*": `${sess.objectiveName} ID: ${sess.objectiveId}`,
            "*Discord user ID*": interaction.user.id,
          };

          const txt = applyTemplate(getRender(dmRef).text ?? "", vars) || ZWSP;
          try {
            await interaction.user.send({ content: txt });
          } catch (_) {}
        }

        return;
      }

      return;
    }

    const idBegin = getRender(UI.BEGIN_BTN).custom_id;
    const idAccept = getRender(UI.ACCEPT).custom_id;
    const idUnderstand = getRender(UI.UNDERSTAND).custom_id;
    const idBackIntro = getRender(UI.BACK_INTRO).custom_id;
    const idYes = getRender(UI.YES).custom_id;
    const idNo = getRender(UI.NO).custom_id;
    const idBackComms = getRender(UI.BACK_COMMS).custom_id;
    const idTryAgain = getRender(UI.TRY_AGAIN).custom_id;

    const idTerminalAccess = getRender(UI.TERMINAL_ACCESS_BTN).custom_id;
    const idTerminalObjectives = getRender(UI.T_BTN_OBJECTIVES).custom_id;
    const idTerminalEvents = getRender(UI.T_BTN_EVENTS).custom_id;

    const idLeaderboardAccess = getRender("[UI: Leaderboard-access-button]").custom_id;
    const idLeaderboardAllTime = getRender("[UI: Leaderboard-all-time-button]").custom_id;
    const idLeaderboardCycle = getRender("[UI: Leaderboard-current-cycle-button]").custom_id;


    const idTerminalObjSubmit = getRender(UI.T_OBJ_BTN_SUBMIT).custom_id;
    const idTerminalObjBack = getRender(UI.T_OBJ_BTN_BACK).custom_id;
    const idTerminalObjNext = getRender(UI.T_OBJ_BTN_NEXT).custom_id;
    const idTerminalObjPrev = getRender(UI.T_OBJ_BTN_PREV).custom_id;
    const idTerminalEvtCheckIn = "core_terminal_evt_checkin";
    const idTerminalEvtPrev = "core_terminal_evt_prev";
    const idTerminalEvtNext = "core_terminal_evt_next";

    const idObjSubmitTryAgain = getRender(UI.T_OBJ_SUBMIT_TRY_AGAIN).custom_id;
    const idObjSubmitBack = getRender(UI.T_OBJ_SUBMIT_BACK).custom_id;

    const idOpAccess = getRender(UI.OP_ACCESS_BTN).custom_id;

    // LT TERMINAL
    const idLtAccess = getRender("[UI: LT-Terminal-access-button]").custom_id;
    const idLtAdjustMerits = getRender("[UI: LT-Terminal-adjust-merits-button]").custom_id;
    const idLtSuspendUser = getRender("[UI: LT-Terminal-suspend-user-button]").custom_id;

    const idLtAdjMenuAdd = getRender("[UI: LT-Terminal-adjust-merits-add-merits-button]").custom_id;
    const idLtAdjMenuDeduct = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-button]").custom_id;
    const idLtAdjMenuBack = getRender("[UI: LT-Terminal-adjust-merits-back-button]").custom_id;

    const idLtAdjAddTarget = getRender("[UI: LT-Terminal-adjust-merits-add-merits-user-id-button]").custom_id;
    const idLtAdjAddAmount = getRender("[UI: LT-Terminal-adjust-merits-add-merits-transfer-amount-button]").custom_id;
    const idLtAdjAddNotes = getRender("[UI: LT-Terminal-adjust-merits-add-merits-lt-notes-button]").custom_id;
    const idLtAdjAddSubmit = getRender("[UI: LT-Terminal-adjust-merits-add-merits-initiate-button]").custom_id;
    const idLtAdjAddBack = getRender("[UI: LT-Terminal-adjust-merits-add-merits-back-button]").custom_id;

    const idLtAdjDeductTarget = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-user-id-button]").custom_id;
    const idLtAdjDeductAmount = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-transfer-amount-button]").custom_id;
    const idLtAdjDeductNotes = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-lt-notes-button]").custom_id;
    const idLtAdjDeductSubmit = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-initiate-button]").custom_id;
    const idLtAdjDeductBack = getRender("[UI: LT-Terminal-adjust-merits-deduct-merits-back-button]").custom_id;

    const idLtSuspendTarget = getRender("[UI: LT-Terminal-suspend-user-user-id-button]").custom_id;
    const idLtSuspendDuration = getRender("[UI: LT-Terminal-suspend-user-duration-button]").custom_id;
    const idLtSuspendSubmit = getRender("[UI: LT-Terminal-suspend-user-initiate-button]").custom_id;
    const idLtSuspendBack = getRender("[UI: LT-Terminal-suspend-user-back-button]").custom_id;

    const idLtSuspendDur12h = getRender("[UI: LT-Terminal-suspend-user-duration-12-hours-button]").custom_id;
    const idLtSuspendDur1d = getRender("[UI: LT-Terminal-suspend-user-duration-1-day-button]").custom_id;
    const idLtSuspendDur3d = getRender("[UI: LT-Terminal-suspend-user-duration-3-days-button]").custom_id;
    const idLtSuspendDur1w = getRender("[UI: LT-Terminal-suspend-user-duration-1-week-button]").custom_id;
    const idLtSuspendDur2w = getRender("[UI: LT-Terminal-suspend-user-duration-2-weeks-button]").custom_id;
    const idLtSuspendDurBack = getRender("[UI: LT-Terminal-suspend-user-duration-back-button]").custom_id;
    const idOpObjectives = getRender(UI.OP_BTN_OBJECTIVES).custom_id;
    const idOpEvents = getRender(UI.OP_BTN_EVENTS).custom_id;
const idOpCommands = getRender(UI.OP_BTN_COMMANDS).custom_id;

const idOpCmdRoll = getRender(UI.OP_CMD_BTN_ROLL_CALL).custom_id;
const idOpCmdBack = getRender(UI.OP_CMD_BTN_BACK).custom_id;

const idOpCmdRollTarget = getRender(UI.OP_CMD_ROLL_BTN_TARGET_CHANNEL).custom_id;
const idOpCmdRollInitiate = getRender(UI.OP_CMD_ROLL_BTN_INITIATE).custom_id;
const idOpCmdRollBack = getRender(UI.OP_CMD_ROLL_BTN_BACK).custom_id;

const idOpCmdRollCh1 = getRender(UI.OP_CMD_ROLL_TARGET_CH1).custom_id;
const idOpCmdRollCh2 = getRender(UI.OP_CMD_ROLL_TARGET_CH2).custom_id;
const idOpCmdRollCh3 = getRender(UI.OP_CMD_ROLL_TARGET_CH3).custom_id;
const idOpCmdRollCh4 = getRender(UI.OP_CMD_ROLL_TARGET_CH4).custom_id;
const idOpCmdRollCh5 = getRender(UI.OP_CMD_ROLL_TARGET_CH5).custom_id;
const idOpCmdRollTargetSubmit = getRender(UI.OP_CMD_ROLL_TARGET_SUBMIT).custom_id;
const idOpCmdRollTargetBack = getRender(UI.OP_CMD_ROLL_TARGET_BACK).custom_id;
    const idOpCmdAnn = getRender(UI.OP_CMD_BTN_ANNOUNCEMENT).custom_id;
    const idOpCmdMeritTransfer = getRender(UI.OP_CMD_BTN_MERIT_TRANSFER).custom_id;

    const idOpCmdAnnUpload = getRender(UI.OP_CMD_ANN_BTN_UPLOAD_AUDIO).custom_id;
    const idOpCmdAnnInit = getRender(UI.OP_CMD_ANN_BTN_INITIATE).custom_id;
    const idOpCmdAnnBack = getRender(UI.OP_CMD_ANN_BTN_BACK).custom_id;
    const idOpCmdAnnUploadBack = getRender(UI.OP_CMD_ANN_UPLOAD_BACK).custom_id;

    const idAnnReviewApprove = getRender(UI.ANN_REVIEW_BTN_APPROVE).custom_id;
    const idAnnReviewDeny = getRender(UI.ANN_REVIEW_BTN_DENY).custom_id;

    const idMtReviewApprove = "mt_review_approve";
    const idMtReviewDeny = "mt_review_deny";




    const idOpEvtUpload = getRender(UI.OP_EVT_BTN_UPLOAD).custom_id;
    const idOpEvtBack = getRender(UI.OP_EVT_BTN_BACK).custom_id;

    const idOpEvtSched = getRender(UI.OP_EVT_BTN_SCHEDULING).custom_id;

    const idEvtSchedEdit = getRender(UI.OP_EVT_SCHED_BTN_EDIT).custom_id;
    const idEvtSchedDelete = getRender(UI.OP_EVT_SCHED_BTN_DELETE).custom_id;
    const idEvtSchedDeleteConfirm = getRender(UI.OP_EVT_SCHED_DELETE_BTN_CONFIRM).custom_id;
    const idEvtSchedDeleteBack = getRender(UI.OP_EVT_SCHED_DELETE_BTN_BACK).custom_id;
    const idEvtSchedPrev = getRender(UI.OP_EVT_SCHED_BTN_PREV).custom_id;
    const idEvtSchedNext = getRender(UI.OP_EVT_SCHED_BTN_NEXT).custom_id;
    const idEvtSchedBack = getRender(UI.OP_EVT_SCHED_BTN_BACK).custom_id;

    const idEvtSchedEditNameBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_NAME).custom_id;
const idEvtSchedEditMerits = getRender(UI.OP_EVT_SCHED_EDIT_BTN_MERITS).custom_id;
const idEvtSchedEditSchedule = getRender(UI.OP_EVT_SCHED_EDIT_BTN_SCHEDULE).custom_id;
const idEvtSchedEditRole = getRender(UI.OP_EVT_SCHED_EDIT_BTN_ROLE).custom_id;
const idEvtSchedEditGif = getRender(UI.OP_EVT_SCHED_EDIT_BTN_GIF).custom_id;
const idEvtSchedEditSubmit = getRender(UI.OP_EVT_SCHED_EDIT_BTN_SUBMIT).custom_id;
const idEvtSchedEditMeritsBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_MERITS).custom_id;
const idEvtSchedEditScheduleBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_SCHEDULE).custom_id;
const idEvtSchedEditRoleBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_ROLE).custom_id;
const idEvtSchedEditGifBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_GIF).custom_id;
const idEvtSchedEditSubmitBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_SUBMIT).custom_id;
    const idEvtSchedEditBackBtn = getRender(UI.OP_EVT_SCHED_EDIT_BTN_BACK).custom_id;
    const idEvtUpName = getRender(UI.OP_EVT_UPLOAD_NAME_BTN).custom_id;
    const idEvtUpMerits = getRender(UI.OP_EVT_UPLOAD_MERITS_BTN).custom_id;
    const idEvtUpSchedule = getRender(UI.OP_EVT_UPLOAD_SCHEDULE_BTN).custom_id;
    const idEvtUpRole = getRender(UI.OP_EVT_UPLOAD_ROLE_BTN).custom_id;
    const idEvtUpGif = getRender(UI.OP_EVT_UPLOAD_GIF_BTN).custom_id;
    const idEvtUpSubmit = getRender(UI.OP_EVT_UPLOAD_SUBMIT_BTN).custom_id;
    const idEvtUpBack = getRender(UI.OP_EVT_UPLOAD_BACK_BTN).custom_id;

    const idEvtRole1 = getRender(UI.OP_EVT_ROLE_BTN_1).custom_id;
    const idEvtRole2 = getRender(UI.OP_EVT_ROLE_BTN_2).custom_id;
    const idEvtRole3 = getRender(UI.OP_EVT_ROLE_BTN_3).custom_id;
    const idEvtRole4 = getRender(UI.OP_EVT_ROLE_BTN_4).custom_id;
    const idEvtRole5 = getRender(UI.OP_EVT_ROLE_BTN_5).custom_id;
    const idEvtRoleSubmit = getRender(UI.OP_EVT_ROLE_SUBMIT_BTN).custom_id;
    const idEvtRoleBack = getRender(UI.OP_EVT_ROLE_BACK_BTN).custom_id;

    const idEvtGifMain = getRender(UI.OP_EVT_GIF_MAIN_BTN).custom_id;
    const idEvtGifCheck = getRender(UI.OP_EVT_GIF_CHECKIN_BTN).custom_id;
    const idEvtGifBackFile = getRender(UI.OP_EVT_GIF_BACK_BTN).custom_id;
    const idEvtGifSubmit = getRender(UI.OP_EVT_GIF_SUBMIT_BTN).custom_id;
    const idEvtGifBackMenu = getRender(UI.OP_EVT_GIF_BACK_MENU_BTN).custom_id;
    const idEvtGifAnyBack = getRender(UI.OP_EVT_GIF_ANY_BACK_BTN).custom_id;

        const idOpObjBack = getRender(UI.OP_OBJ_BTN_BACK).custom_id;
const idOpObjUpload = getRender(UI.OP_OBJ_BTN_UPLOAD).custom_id;

    const idObjReviewA = getRender(UI.OBJ_REVIEW_BTN_A).custom_id;
    const idObjReviewB = getRender(UI.OBJ_REVIEW_BTN_B).custom_id;
    const idObjReviewC = getRender(UI.OBJ_REVIEW_BTN_C).custom_id;

    const idOpObjSched = getRender(UI.OP_OBJ_BTN_SCHED).custom_id;

    const idSchedEdit = getRender(UI.OP_OBJ_SCHED_BTN_EDIT).custom_id;
    const idSchedDelete = getRender(UI.OP_OBJ_SCHED_BTN_DELETE).custom_id;
    const idSchedNext = getRender(UI.OP_OBJ_SCHED_BTN_NEXT).custom_id;
    const idSchedPrev = getRender(UI.OP_OBJ_SCHED_BTN_PREV).custom_id;
    const idSchedBack = getRender(UI.OP_OBJ_SCHED_BTN_BACK).custom_id;

    const idSchedDelConfirm = getRender(UI.OP_OBJ_SCHED_DELETE_BTN_CONFIRM).custom_id;
    const idSchedDelBack = getRender(UI.OP_OBJ_SCHED_DELETE_BTN_BACK).custom_id;

    const idEditName = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_NAME).custom_id;
    const idEditMerits = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_MERITS).custom_id;
    const idEditSchedule = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_SCHEDULE).custom_id;
    const idEditFileType = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_FILETYPE).custom_id;
    const idEditGif = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_GIF).custom_id;
    const idEditSubmit = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_SUBMIT).custom_id;
    const idEditBack = getRender(UI.OP_OBJ_SCHED_EDIT_BTN_BACK).custom_id;

    const idEditFileTypeImg = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_IMAGE).custom_id;
    const idEditFileTypeAud = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_AUDIO).custom_id;
    const idEditFileTypeTxt = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_TEXT).custom_id;
    const idEditFileTypeTxtFile = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_TEXT_FILE).custom_id;
    const idEditFileTypeSubmit = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_SUBMIT).custom_id;
    const idEditFileTypeBack = getRender(UI.OP_OBJ_SCHED_EDIT_FILETYPE_BACK).custom_id;

    const idEditGifMain = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_MAIN).custom_id;
    const idEditGifSubmitFile = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_SUBMIT).custom_id;
    const idEditGifBackFile = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_BACK).custom_id;
    const idEditGifSubmitBtn = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_SUBMIT_BTN).custom_id;
    const idEditGifBackBtn = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_BACK_BTN).custom_id;
    const idEditGifAnyBack = getRender(UI.OP_OBJ_SCHED_EDIT_GIF_ANYFILE_BACK).custom_id;

    const idUpName = getRender(UI.OP_OBJ_UPLOAD_BTN_NAME).custom_id;
    const idUpMerits = getRender(UI.OP_OBJ_UPLOAD_BTN_MERITS).custom_id;
    const idUpSchedule = getRender(UI.OP_OBJ_UPLOAD_BTN_SCHEDULE).custom_id;
    const idUpFileType = getRender(UI.OP_OBJ_UPLOAD_BTN_FILETYPE).custom_id;
    const idUpGif = getRender(UI.OP_OBJ_UPLOAD_BTN_GIF).custom_id;
    const idUpSubmit = getRender(UI.OP_OBJ_UPLOAD_BTN_SUBMIT).custom_id;
    const idUpBack = getRender(UI.OP_OBJ_UPLOAD_BTN_BACK).custom_id;

    const idFileTypeImg = getRender(UI.OP_OBJ_FILETYPE_IMAGE).custom_id;
    const idFileTypeAud = getRender(UI.OP_OBJ_FILETYPE_AUDIO).custom_id;
    const idFileTypeTxt = getRender(UI.OP_OBJ_FILETYPE_TEXT).custom_id;
    const idFileTypeTxtFile = getRender(UI.OP_OBJ_FILETYPE_TEXT_FILE).custom_id;
    const idFileTypeSubmit = getRender(UI.OP_OBJ_FILETYPE_SUBMIT).custom_id;
    const idFileTypeBack = getRender(UI.OP_OBJ_FILETYPE_BACK).custom_id;

    const idGifMain = getRender(UI.OP_OBJ_GIF_MAIN).custom_id;
    const idGifSubmitFile = getRender(UI.OP_OBJ_GIF_SUBMIT).custom_id;
    const idGifBackFile = getRender(UI.OP_OBJ_GIF_BACK).custom_id;
    const idGifSubmitBtn = getRender(UI.OP_OBJ_GIF_SUBMIT_BTN).custom_id;
    const idGifBackBtn = getRender(UI.OP_OBJ_GIF_BACK_BTN).custom_id;
    const idGifAnyBack = getRender(UI.OP_OBJ_GIF_ANYFILE_BACK).custom_id;

    if (interaction.customId === CUSTOM.TERMINAL_BACK) return await handleTerminalBack(interaction);
    if (interaction.customId === CUSTOM.TERMINAL_CLOSE) return await handleTerminalClose(interaction);

    if (interaction.customId === CUSTOM.OP_CLOSE) {
      await ackButton(interaction);
      await editEphemeral(interaction, embedWithImageAndText(null, "Closed."), []);
      return;
    }

    switch (interaction.customId) {
      case idBegin:
        return await handleBegin(interaction);
      case idAccept:
        return await handleAccept(interaction);
      case idBackIntro:
        return await handleBackToCharter(interaction);
      case idUnderstand:
        return await handleUnderstand(interaction);
      case idBackComms:
        return await handleBackToIntro(interaction);
      case idYes:
        return await handleYes(interaction);
      case idNo:
        return await handleNo(interaction);
      case idTryAgain:
        return await handleTryAgain(interaction);

      case idTerminalAccess:
        return await handleTerminalAccess(interaction);
      case idTerminalObjectives:
        return await handleTerminalObjectives(interaction);
      case idTerminalEvents:
        return await handleTerminalEvents(interaction);


      case idLeaderboardAccess:
        return await handleLeaderboardAccess(interaction);
      case idLeaderboardAllTime:
        return await handleLeaderboardSetView(interaction, "all_time");
      case idLeaderboardCycle:
        return await handleLeaderboardSetView(interaction, "cycle");

            case "core_terminal_evt_prev":
        return await handleTerminalEvtPrev(interaction);
      case "core_terminal_evt_next":
        return await handleTerminalEvtNext(interaction);
      case "core_terminal_evt_checkin":
        return await handleTerminalEvtCheckIn(interaction);
case "ui_terminal_events_button_check_in":
        return await handleTerminalEvtCheckIn(interaction);
case "ui_terminal_events_button_previous":
        return await handleTerminalEvtPrev(interaction);
case "ui_terminal_events_button_next":
        return await handleTerminalEvtNext(interaction);
case "ui_terminal_events_button_back":
        return await handleTerminalEvtBack(interaction);
case "ui_terminal_events_check_in_button_try_again":
        return await handleTerminalEvtCheckInTryAgain(interaction);
case "ui_terminal_events_check_in_button_back":
        return await handleTerminalEvtCheckInBack(interaction);


case idTerminalObjPrev:
        return await handleTerminalObjPrev(interaction);
      case idTerminalObjNext:
        return await handleTerminalObjNext(interaction);
      case idTerminalObjBack:
        return await handleTerminalObjBack(interaction);
      case idTerminalObjSubmit:
        return await handleTerminalObjSubmit(interaction);

      case idObjReviewA:
        return await handleObjectiveReviewApprove(interaction);
      case idObjReviewB:
        return await handleObjectiveReviewDeny(interaction);
      case idObjReviewC:
        return await handleObjectiveReviewUndo(interaction);




case idObjSubmitTryAgain: {
  await ackButton(interaction);

  const transEmbed = buildScreenEmbed([UI.T_OBJ_SUBMIT_TRANS_TRY_AGAIN]);
  await editEphemeral(interaction, transEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(UI.T_OBJ_SUBMIT_TRANS_TRY_AGAIN) * 1000));

  const submitGifUrl = getRender(UI.T_OBJ_SUBMIT_GIF).url ?? null;
  const submitEmbed = embedWithImageAndText(submitGifUrl, ZWSP);
  await editEphemeral(
    interaction,
    submitEmbed,
    buildRowsFromButtons([makeUiButton(UI.T_OBJ_SUBMIT_TRY_AGAIN), makeUiButton(UI.T_OBJ_SUBMIT_BACK)])
  );


  const key = sessionKey(interaction.guildId, interaction.user.id);
  let sess = objectiveSubmitSession.get(key);

  // Fallback: if the DM submission session is missing (expired/cleared), infer objective from terminal state.
  let objectiveId = sess?.objectiveId ?? null;
  let objectiveName = sess?.objectiveName ?? null;
  let proofType = sess?.proofType ?? null;

  if (!objectiveId) {
    try {
      const state = ensureTerminalSession(interaction.guildId, interaction.user.id);
      await refreshTerminalObjectiveCache(interaction);
      const obj = state.cachedObjectives[state.objectiveIndex];
      if (obj) {
        objectiveId = obj.id;
        objectiveName = obj.name;
        proofType = obj.proof_type;
      }
    } catch (_) {}
  }

  if (!objectiveId) {
    const submitGifUrl2 = getRender(UI.T_OBJ_SUBMIT_GIF).url ?? null;
    const submitEmbed2 = embedWithImageAndText(submitGifUrl2, "Session expired. Please click Submit Objective again.");
    await editEphemeral(interaction, submitEmbed2, buildRowsFromButtons([makeUiButton(UI.T_OBJ_SUBMIT_BACK)]));
    return;
  }

  const latestStatus = await dbGetLatestObjectiveSubmissionStatus({
    guildId: interaction.guildId,
    objectiveId,
    userId: interaction.user.id,
  }).catch(() => "");

  if ((latestStatus === "approved" || latestStatus === "accepted")) {
    const submitGifUrl2 = getRender(UI.T_OBJ_SUBMIT_GIF).url ?? null;
    const submitEmbed2 = embedWithImageAndText(
      submitGifUrl2,
      "This Objective has already been approved. You cannot submit it again."
    );
    await editEphemeral(interaction, submitEmbed2, buildRowsFromButtons([makeUiButton(UI.T_OBJ_SUBMIT_BACK)]));
    return;
  }

  // If session is missing, recreate it so DM intake works.
  if (!sess) {
    sess = {
      objectiveId,
      objectiveName: objectiveName ?? `Objective ${objectiveId}`,
      proofType: proofType ?? "text",
      submitted: false,
      expiresAt: Date.now() + UPLOAD_TIMEOUT_MS,
    };
    objectiveSubmitSession.set(key, sess);
  }


  if ((latestStatus === "approved" || latestStatus === "accepted")) {
    const submitGifUrl2 = getRender(UI.T_OBJ_SUBMIT_GIF).url ?? null;
    const submitEmbed2 = embedWithImageAndText(submitGifUrl2, "This Objective has already been approved. You cannot submit it again.");
    await editEphemeral(interaction, submitEmbed2, buildRowsFromButtons([makeUiButton(UI.T_OBJ_SUBMIT_BACK)]));
    return;
  }


  const vars = {
    "*Objective name + ID*": `${sess.objectiveName} ID: ${sess.objectiveId}`,
    "*Discord user ID*": interaction.user.id,
  };

  try {
    if (sess.submitted) {
      const txt = applyTemplate(getRender(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY).text ?? "", vars) || ZWSP;
      const row = new ActionRowBuilder().addComponents(
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_A),
        makeUiButton(UI.T_OBJ_SUBMIT_DM_ERR_ALREADY_B)
      );
      await interaction.user.send({ content: txt, components: [row] });
    } else {
      const dmRef = dmUiRefForProofType(sess.proofType);
  const txtBase = applyTemplate(getRender(dmRef).text ?? "", vars) || ZWSP;
  const txt = appendAcceptedFileTypesLine(txtBase, sess.proofType);
      await interaction.user.send({ content: txt });
    }
  } catch (_) {}
  return;
}

case idObjSubmitBack: {
  await ackButton(interaction);

  const transEmbed = buildScreenEmbed([UI.T_OBJ_SUBMIT_TRANS_BACK]);
  await editEphemeral(interaction, transEmbed, disabledRow());
  await sleep(Math.round(gifDurationSeconds(UI.T_OBJ_SUBMIT_TRANS_BACK) * 1000));

  await showTerminalObjective(interaction);
  return;
}

      case idOpAccess:
        return await handleOperatorAccess(interaction);

      // LT TERMINAL
      case idLtAccess:
        return await handleLtAccess(interaction);
      case idLtAdjustMerits:
        return await handleLtAdjustMeritsMenu(interaction);
      case idLtSuspendUser:
        return await handleLtSuspendOpen(interaction);

      // LT Adjust Merits menu
      case idLtAdjMenuAdd:
        return await handleLtAdjustAddOpen(interaction);
      case idLtAdjMenuDeduct:
        return await handleLtAdjustDeductOpen(interaction);
      case idLtAdjMenuBack:
        return await handleLtAccess(interaction);

      // LT Adjust Merits (Add)
      case idLtAdjAddTarget:
        return await handleLtAdjustAddTarget(interaction);
      case idLtAdjAddAmount:
        return await handleLtAdjustAddAmount(interaction);
      case idLtAdjAddNotes:
        return await handleLtAdjustAddNotes(interaction);
      case idLtAdjAddSubmit:
        return await handleLtAdjustAddSubmit(interaction);
      case idLtAdjAddBack:
        return await handleLtAdjustMeritsMenu(interaction);

      // LT Adjust Merits (Deduct)
      case idLtAdjDeductTarget:
        return await handleLtAdjustDeductTarget(interaction);
      case idLtAdjDeductAmount:
        return await handleLtAdjustDeductAmount(interaction);
      case idLtAdjDeductNotes:
        return await handleLtAdjustDeductNotes(interaction);
      case idLtAdjDeductSubmit:
        return await handleLtAdjustDeductSubmit(interaction);
      case idLtAdjDeductBack:
        return await handleLtAdjustMeritsMenu(interaction);

      // LT Suspend
      case idLtSuspendTarget:
        return await handleLtSuspendTarget(interaction);
      case idLtSuspendDuration:
        return await handleLtSuspendDurationMenu(interaction);
      case idLtSuspendSubmit:
        return await handleLtSuspendSubmit(interaction);
      case idLtSuspendBack:
        return await handleLtAccess(interaction);

      // LT Suspend duration selection
      case idLtSuspendDur12h:
        return await handleLtSuspendSetDuration(interaction, "12h");
      case idLtSuspendDur1d:
        return await handleLtSuspendSetDuration(interaction, "1d");
      case idLtSuspendDur3d:
        return await handleLtSuspendSetDuration(interaction, "3d");
      case idLtSuspendDur1w:
        return await handleLtSuspendSetDuration(interaction, "1w");
      case idLtSuspendDur2w:
        return await handleLtSuspendSetDuration(interaction, "2w");
      case idLtSuspendDurBack:
        return await handleLtSuspendOpen(interaction);
      case idOpObjectives:
        return await handleOperatorObjectives(interaction);


case idOpObjBack:
  return await handleOperatorObjectivesBack(interaction);

      case idOpEvents:
        return await handleOperatorEvents(interaction);


case idOpCommands:
        return await handleOpCommands(interaction);

      case idOpCmdBack:
        return await handleOpBackToMenu(interaction);
            case idOpCmdAnn:
        return await handleOpAnnouncementOpen(interaction);

      case idOpCmdMeritTransfer:
        return await handleOpMeritTransferOpen(interaction);
      case "op_cmd_mt_target":
        return await handleOpMeritTransferTarget(interaction);
      case "op_cmd_mt_amount":
        return await handleOpMeritTransferAmount(interaction);
      case "op_cmd_mt_notes":
        return await handleOpMeritTransferNotes(interaction);
      case "op_cmd_mt_submit":
        return await handleOpMeritTransferSubmit(interaction);
      case "op_cmd_mt_back":
        return await handleOpMeritTransferBack(interaction);

      case idMtReviewApprove:
        return await handleMeritTransferReviewDecision(interaction, "approve");
      case idMtReviewDeny:
        return await handleMeritTransferReviewDecision(interaction, "deny");
      case idOpCmdAnnUpload:
        return await handleOpAnnouncementUploadOpen(interaction);
      case idOpCmdAnnUploadBack:
        return await handleOpAnnouncementUploadBack(interaction);
      case idOpCmdAnnInit:
        return await handleOpAnnouncementInitiate(interaction);
      case idOpCmdAnnBack:
        return await handleOpAnnouncementBack(interaction);

      case idAnnReviewApprove:
        return await handleAnnouncementReviewDecision(interaction, "approve");
      case idAnnReviewDeny:
        return await handleAnnouncementReviewDecision(interaction, "deny");

case idOpCmdRoll:
        return await handleOpRollCallOpen(interaction);

      case idOpCmdRollTarget:
        return await handleOpRollCallTargetOpen(interaction);
      case idOpCmdRollInitiate:
        return await handleOpRollCallInitiate(interaction);
      case idOpCmdRollBack:
        return await handleOpRollCallBack(interaction);

      case idOpCmdRollCh1:
        return await handleOpRollCallTargetPick(interaction, "EVENT_CHANNEL_1");
      case idOpCmdRollCh2:
        return await handleOpRollCallTargetPick(interaction, "EVENT_CHANNEL_2");
      case idOpCmdRollCh3:
        return await handleOpRollCallTargetPick(interaction, "EVENT_CHANNEL_3");
      case idOpCmdRollCh4:
        return await handleOpRollCallTargetPick(interaction, "EVENT_CHANNEL_4");
      case idOpCmdRollCh5:
        return await handleOpRollCallTargetPick(interaction, "EVENT_CHANNEL_5");
      case idOpCmdRollTargetSubmit:
        return await handleOpRollCallTargetSubmit(interaction);
      case idOpCmdRollTargetBack:
        return await handleOpRollCallTargetBack(interaction);



case idOpEvtBack:
  return await handleOperatorEventsBack(interaction);


      case idOpObjSched:
        return await handleObjectiveSchedulingOpen(interaction);

      case idSchedBack:
        return await handleObjectiveSchedulingBack(interaction);
      case idSchedPrev:
        return await handleObjectiveSchedulingPrev(interaction);
      case idSchedNext:
        return await handleObjectiveSchedulingNext(interaction);
      case idSchedEdit:
        return await handleObjectiveSchedulingEdit(interaction);
      case idSchedDelete:
        return await handleObjectiveSchedulingDelete(interaction);

      case idSchedDelBack:
        return await handleObjectiveSchedulingDeleteBack(interaction);
      case idSchedDelConfirm:
        return await handleObjectiveSchedulingDeleteConfirm(interaction);

      case idEditBack:
        return await handleObjectiveSchedulingEditBack(interaction);
      case idEditSubmit:
        return await handleObjectiveSchedulingEditSubmit(interaction);
      case idEditName:
        return await handleObjectiveSchedulingEditName(interaction);
      case idEditMerits:
        return await handleObjectiveSchedulingEditMerits(interaction);
      case idEditSchedule:
        return await handleObjectiveSchedulingEditSchedule(interaction);
      case idEditFileType:
        return await handleObjectiveSchedulingEditFileTypeOpen(interaction);
      case idEditGif:
        return await handleObjectiveSchedulingEditGifOpen(interaction);

      case idEditFileTypeImg:
        return await handleObjectiveSchedulingEditFileTypePick(interaction, "image");
      case idEditFileTypeAud:
        return await handleObjectiveSchedulingEditFileTypePick(interaction, "audio");
      case idEditFileTypeTxt:
        return await handleObjectiveSchedulingEditFileTypePick(interaction, "text");
      case idEditFileTypeTxtFile:
        return await handleObjectiveSchedulingEditFileTypePick(interaction, "text_file");
      case idEditFileTypeSubmit:
        return await handleObjectiveSchedulingEditFileTypeSubmitButton(interaction);
      case idEditFileTypeBack:
        return await handleObjectiveSchedulingEditFileTypeBack(interaction);

      case idEditGifMain:
        return await handleObjectiveSchedulingEditGifMain(interaction);
      case idEditGifSubmitFile:
        return await handleObjectiveSchedulingEditGifSubmitFile(interaction);
      case idEditGifBackFile:
        return await handleObjectiveSchedulingEditGifBackFile(interaction);
      case idEditGifAnyBack:
        return await handleObjectiveSchedulingEditGifWaitBack(interaction);
      case idEditGifSubmitBtn:
        return await handleObjectiveSchedulingEditGifMenuSubmit(interaction);
      case idEditGifBackBtn:
        return await handleObjectiveSchedulingEditGifMenuBack(interaction);

      case idOpObjUpload:
        return await handleObjectiveUploadOpen(interaction);

case idOpEvtSched:
  return await handleEventSchedulingOpen(interaction);
case idEvtSchedBack:
  return await handleEventSchedulingBack(interaction);
case idEvtSchedPrev:
  return await handleEventSchedulingPrev(interaction);
case idEvtSchedNext:
  return await handleEventSchedulingNext(interaction);
case idEvtSchedEdit:
  return await handleEventSchedulingEdit(interaction);
case idEvtSchedEditBackBtn:
  return await handleEventSchedulingEditBack(interaction);
      // EVENT SCHEDULING — EDIT BUTTONS
      case idEvtSchedEditNameBtn:
        return await handleEventSchedulingEditName(interaction);

      case idEvtSchedEditMerits:
        return await handleEventSchedulingEditMerits(interaction);

      case idEvtSchedEditSchedule:
        return await handleEventSchedulingEditSchedule(interaction);

      case idEvtSchedEditRole:
        return await handleEventSchedulingEditRole(interaction);

      case idEvtSchedEditGif:
        return await handleEventSchedulingEditGif(interaction);

case idOpEvtUpload:
        return await handleEventUploadOpen(interaction);
      case idEvtUpBack:
        return await handleEventUploadBack(interaction);

      case idEvtUpName:
        return await showEventNameModal(interaction);
      case idEvtUpMerits:
        return await showEventMeritsModal(interaction);
      case idEvtUpSchedule:
        return await showEventScheduleModal(interaction);

      case idEvtUpRole:
        return await handleEventUploadRoleOpen(interaction);

      case idEvtRoleBack: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRoleBack(interaction);
        return await handleEventRoleBack(interaction);
      }
case idEvtRole1: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRolePick(interaction, "EVENT_1_CHECKED_IN", UI.ROLE_EVT1_CHECKEDIN);
        return await handleEventRolePick(interaction, "EVENT_1_CHECKED_IN", UI.ROLE_EVT1_CHECKEDIN);
      }
case idEvtRole2: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRolePick(interaction, "EVENT_2_CHECKED_IN", UI.ROLE_EVT2_CHECKEDIN);
        return await handleEventRolePick(interaction, "EVENT_2_CHECKED_IN", UI.ROLE_EVT2_CHECKEDIN);
      }
case idEvtRole3: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRolePick(interaction, "EVENT_3_CHECKED_IN", UI.ROLE_EVT3_CHECKEDIN);
        return await handleEventRolePick(interaction, "EVENT_3_CHECKED_IN", UI.ROLE_EVT3_CHECKEDIN);
      }
case idEvtRole4: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRolePick(interaction, "EVENT_4_CHECKED_IN", UI.ROLE_EVT4_CHECKEDIN);
        return await handleEventRolePick(interaction, "EVENT_4_CHECKED_IN", UI.ROLE_EVT4_CHECKEDIN);
      }
case idEvtRole5: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRolePick(interaction, "EVENT_5_CHECKED_IN", UI.ROLE_EVT5_CHECKEDIN);
        return await handleEventRolePick(interaction, "EVENT_5_CHECKED_IN", UI.ROLE_EVT5_CHECKEDIN);
      }
case idEvtRoleSubmit: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_role_menu") return await handleEventEditRoleSubmit(interaction);
        return await handleEventRoleSubmit(interaction);
      }
case idEvtUpGif:
        return await handleEventUploadGifOpen(interaction);
      case idEvtGifBackMenu: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_gif_menu") return await handleEventEditGifBackToEdit(interaction);
        return await handleEventGifBack(interaction);
      }
case idEvtGifAnyBack:
        return await handleEventUploadGifOpen(interaction);

      case idEvtGifMain: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_gif_menu") return await handleEventGifPick(interaction, "event_edit_main");
        return await handleEventGifPick(interaction, "event_main");
      }
case idEvtGifCheck: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_gif_menu") return await handleEventGifPick(interaction, "event_edit_check_in");
        return await handleEventGifPick(interaction, "event_check_in");
      }
case idEvtGifBackFile: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_gif_menu") return await handleEventGifPick(interaction, "event_edit_back");
        return await handleEventGifPick(interaction, "event_back");
      }
case idEvtGifSubmit: {
        const s = ensureOperatorSession(interaction.guildId, interaction.user.id);
        if (s.screen === "evt_sched_edit_gif_menu") return await handleEventEditGifBackToEdit(interaction);
        return await handleEventGifBack(interaction);
      }
case idEvtUpSubmit:
        return await handleEventUploadSubmit(interaction);
      case idUpBack:
        return await handleObjectiveUploadBack(interaction);

      case idUpName:
        return await showNameModal(interaction);
      case idUpMerits:
        return await showMeritsModal(interaction);
      case idUpSchedule:
        return await showScheduleModal(interaction);

      case idUpFileType:
        return await handleFileTypeOpen(interaction);
      case idFileTypeImg:
        return await handleFileTypePick(interaction, "image");
      case idFileTypeAud:
        return await handleFileTypePick(interaction, "audio");
      case idFileTypeTxt:
        return await handleFileTypePick(interaction, "text");
      case idFileTypeTxtFile:
        return await handleFileTypePick(interaction, "text_file");
      case idFileTypeSubmit:
        return await handleFileTypeSubmit(interaction);
      case idFileTypeBack:
        return await handleFileTypeBack(interaction);

      case idUpGif:
        return await handleGifMenuOpen(interaction);
      case idGifMain:
        return await handleGifSlot(interaction, "main");
      case idGifSubmitFile:
        return await handleGifSlot(interaction, "submit");
      case idGifBackFile:
        return await handleGifSlot(interaction, "back");
      case idGifAnyBack:
        return await handleGifWaitBack(interaction);
      case idGifSubmitBtn:
        return await handleGifMenuSubmit(interaction);
      case idGifBackBtn:
        return await handleGifMenuBack(interaction);

      case idUpSubmit:
        return await handleObjectiveSubmit(interaction);

            case idObjReviewA:
      case idObjReviewB:
      case idObjReviewC:
        await ackButton(interaction);
        await editEphemeral(interaction, embedWithImageAndText(null, "Review actions are not enabled yet."), []);
        return;

      
      // EVENT SCHEDULING — EDIT BUTTONS
      case idEvtSchedEditMerits: {
        const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
        const cur = currentEventSchedulingRow(st);
        if (!cur) { await ackButton(interaction); return; }
        const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
        await showEventEditMeritsModal(interaction, entry);
        return;
      }

      case idEvtSchedEditSchedule: {
        const st = ensureEventSchedulingState(interaction.guildId, interaction.user.id);
        const cur = currentEventSchedulingRow(st);
        if (!cur) { await ackButton(interaction); return; }
        const entry = getOrCreateEventEditDraft(interaction.guildId, interaction.user.id, cur);
        await showEventEditScheduleModal(interaction, entry);
        return;
      }

      case idEvtSchedEditRole: {
        await ackButton(interaction);
        await interaction.followUp({ content: "Role Allocated edit is not enabled yet.", flags: MessageFlags.Ephemeral }).catch(() => {});
        return;
      }

      case idEvtSchedEditGif: {
        await ackButton(interaction);
        await interaction.followUp({ content: "GIF edit is not enabled yet.", flags: MessageFlags.Ephemeral }).catch(() => {});
        return;
      }

default:
        break;
      case idEvtSchedDelete:
        await handleEventSchedulingDelete(interaction);
        break;
      case idEvtSchedDeleteConfirm:
        await handleEventSchedulingDeleteConfirm(interaction);
        break;
      case idEvtSchedDeleteBack:
        await handleEventSchedulingDeleteBack(interaction);
        break;
      case idEvtSchedEditSubmit:
        await handleEventSchedulingEditSubmit(interaction);
        break;
    }
  } catch (err) {
    console.error("interaction error:", err);
    try {
      if (interaction.isRepliable() && !interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: "Something went wrong.", flags: MessageFlags.Ephemeral });
      }
    } catch (_) {}
  }
});


client.on(Events.MessageReactionAdd, async (reaction, user) => {
  try {
    if (!reaction || !user || user.bot) return;

    // Fetch partials
    if (reaction.partial) reaction = await reaction.fetch().catch(() => null);
    if (!reaction) return;
    if (reaction.message?.partial) await reaction.message.fetch().catch(() => null);

    const emojiName = reaction.emoji?.name ?? null;
    if (emojiName !== "✅") return;

    const msg = reaction.message;
    const guildId = msg?.guild?.id ?? null;
    if (!guildId) return;

    const open = await dbGetOpenRollCallByMessage(guildId, msg.id);
    if (!open) return;

    // Close after 1 hour (hard guard even if the background closer didn't run yet)
    const createdAt = open.created_at instanceof Date ? open.created_at : new Date(open.created_at);
    if (Number.isFinite(createdAt?.getTime?.())) {
      const ageMs = Date.now() - createdAt.getTime();
      if (ageMs >= 60 * 60 * 1000) {
        await dbCloseExpiredRollCalls().catch(() => null);
        return;
      }
    }

    // Idempotency
    const already = await dbHasRollCallAward(open.id, user.id);
    if (already) return;

    // Resolve active event for this channel
    const channelKey = open.channel_key;
    const roleKey = channelKey ? `${channelKey.replace("EVENT_CHANNEL_", "EVENT_")}_CHECKED_IN` : null;
    if (!roleKey) return;

    const activeEvent = await dbGetActiveEventByCheckInRoleKey(guildId, roleKey);
    if (!activeEvent) return;

    const merits = Number(activeEvent.merits_awarded ?? 0);
    if (!Number.isFinite(merits) || merits <= 0) return;

    await dbInsertRollCallAward(open.id, guildId, user.id);
    await dbAddUserMerits({ guildId, userId: user.id, delta: merits });

    // Best-effort DM confirmation (do not fail the award if DM fails)
    try {
      await user.send(`✅ Roll Call confirmed for **${activeEvent.name}**. You have been awarded **${merits}** merits.`);
    } catch (_) {}
  } catch (e) {
    console.error("roll call reaction error:", e);
  }
});


// Close expired roll calls in the background (restart-safe; uses DB as source of truth)
function startRollCallCloser() {
  setInterval(async () => {
    try {
      await dbCloseExpiredRollCalls();
    } catch (e) {
      console.error("roll call close tick error:", e);
    }
  }, 60 * 1000);
}


client.login(process.env.DISCORD_BOT_TOKEN);