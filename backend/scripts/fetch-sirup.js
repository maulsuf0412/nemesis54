#!/usr/bin/env node
/**
 * fetch-sirup.js
 * Fetch data SiRUP via Cloudflare Worker proxy → dashboard.sqlite Nemesis
 */

const https = require("https");
const path = require("path");
const fs = require("fs");
const Database = require("better-sqlite3");

const TAHUN = process.env.AUDIT_DATASET_YEAR || "2026";
const DATA_DIR = path.resolve(__dirname, "../data");
const DB_PATH = path.join(DATA_DIR, "dashboard.sqlite");

// ⬇️ Ganti dengan URL Worker kamu
const WORKER_URL = process.env.SIRUP_PROXY_URL || "https://sirup-proxy.5554230075.workers.dev/";
const SIRUP_BASE = `${https://sirup-proxy.5554230075.workers.dev/}/sirup/ro/publicsipd`;

// ─── HTTP helper ──────────────────────────────────────────────────────────────

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { timeout: 30000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchJson(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode}: ${url}`));
      }
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message} | ${body.slice(0, 300)}`)); }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => { req.destroy(); reject(new Error(`Timeout: ${url}`)); });
  });
}

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try { return await fetchJson(url); }
    catch (e) {
      console.warn(`  Retry ${i + 1}/${retries}: ${e.message}`);
      if (i === retries - 1) throw e;
      await sleep(2000 * (i + 1));
    }
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// ─── Analisis anomali ─────────────────────────────────────────────────────────

const BATAS = [200_000_000, 500_000_000, 1_000_000_000, 2_500_000_000, 5_000_000_000];

function hitungRiskScore(paket) {
  let score = 0;
  const flags = { isMencurigakan: false, isPemborosan: false };
  const reasons = [];
  const pagu = paket.pagu || 0;
  const metode = (paket.metodePengadaan || "").toLowerCase();
  const nama = (paket.namaPaket || "").toLowerCase().trim();

  for (const batas of BATAS) {
    if (pagu >= batas * 0.92 && pagu <= batas) {
      score += 35; flags.isMencurigakan = true;
      reasons.push(`Pagu Rp${(pagu/1e6).toFixed(0)}jt mendekati batas Rp${(batas/1e6).toFixed(0)}jt`);
      break;
    }
  }
  if (metode.includes("penunjukan langsung") && pagu > 200_000_000) {
    score += 30; flags.isMencurigakan = true;
    reasons.push(`Penunjukan langsung Rp${(pagu/1e6).toFixed(0)}jt`);
  }
  if (nama.length < 15) {
    score += 15; flags.isMencurigakan = true;
    reasons.push("Nama paket terlalu generik");
  }
  if (pagu >= 500_000_000 && pagu % 100_000_000 === 0) {
    score += 20; flags.isPemborosan = true;
    reasons.push("Pagu angka bulat sempurna");
  }
  if (metode.includes("swakelola") && pagu > 1_000_000_000) {
    score += 15; flags.isPemborosan = true;
    reasons.push("Swakelola nilai besar");
  }

  const potentialWaste = (flags.isMencurigakan || flags.isPemborosan) ? Math.round(pagu * 0.15) : 0;
  let severity = "low";
  if (score >= 60) severity = "absurd";
  else if (score >= 40) severity = "high";
  else if (score >= 20) severity = "med";

  return {
    riskScore: Math.min(score, 100), severity, potentialWaste,
    reason: reasons.join("; ") || null,
    isMencurigakan: flags.isMencurigakan, isPemborosan: flags.isPemborosan,
    isFlagged: flags.isMencurigakan || flags.isPemborosan,
    isPriority: score >= 40,
  };
}

// ─── Owner & region ───────────────────────────────────────────────────────────

function getOwnerType(paket) {
  const kode = String(paket.kodeKlpd || paket.kdKlpd || "");
  const nama = (paket.namaKlpd || "").toLowerCase();
  if (kode.startsWith("K") || nama.includes("kementerian") || nama.includes("badan nasional")) return "central";
  if (nama.includes("provinsi") || nama.includes("prov.")) return "provinsi";
  if (nama.includes("kabupaten") || nama.includes("kota ") || nama.includes("kab.")) return "kabkota";
  return "other";
}

const PROV_MAP = {
  "aceh":"11","sumatera utara":"12","sumatera barat":"13","riau":"14",
  "jambi":"15","sumatera selatan":"16","bengkulu":"17","lampung":"18",
  "bangka belitung":"19","kepulauan riau":"21","dki jakarta":"31",
  "jawa barat":"32","jawa tengah":"33","di yogyakarta":"34","jawa timur":"35",
  "banten":"36","bali":"51","nusa tenggara barat":"52","nusa tenggara timur":"53",
  "kalimantan barat":"61","kalimantan tengah":"62","kalimantan selatan":"63",
  "kalimantan timur":"64","kalimantan utara":"65","sulawesi utara":"71",
  "sulawesi tengah":"72","sulawesi selatan":"73","sulawesi tenggara":"74",
  "gorontalo":"75","sulawesi barat":"76","maluku utara":"82","maluku":"81",
  "papua barat":"91","papua":"94",
};

function getRegionKey(paket) {
  const nama = (paket.namaKlpd || "").toLowerCase();
  for (const [k, v] of Object.entries(PROV_MAP)) {
    if (nama.includes(k)) return v;
  }
  return String(paket.kodeKlpd || paket.kdKlpd || "00");
}

function getProvinceName(namaKlpd) {
  const lower = namaKlpd.toLowerCase();
  for (const k of Object.keys(PROV_MAP)) {
    if (lower.includes(k)) return k.replace(/\b\w/g, c => c.toUpperCase());
  }
  return namaKlpd;
}

// ─── Schema ───────────────────────────────────────────────────────────────────

function setupSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS packages (
      id INTEGER PRIMARY KEY AUTOINCREMENT, inserted_order INTEGER,
      source_id TEXT UNIQUE, schema_version TEXT DEFAULT '1.0',
      owner_name TEXT, owner_type TEXT, satker TEXT, package_name TEXT,
      location_raw TEXT, budget REAL, funding_source TEXT,
      procurement_type TEXT, procurement_method TEXT, selection_date TEXT,
      potential_waste REAL DEFAULT 0, severity TEXT DEFAULT 'low', reason TEXT,
      is_mencurigakan INTEGER, is_pemborosan INTEGER,
      risk_score REAL DEFAULT 0, active_tag_count INTEGER DEFAULT 0,
      is_priority INTEGER DEFAULT 0, is_flagged INTEGER DEFAULT 0,
      mapped_region_count INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS regions (
      region_key TEXT PRIMARY KEY, code TEXT, province_name TEXT,
      region_name TEXT, region_type TEXT, display_name TEXT
    );
    CREATE TABLE IF NOT EXISTS provinces (
      province_key TEXT PRIMARY KEY, code TEXT,
      province_name TEXT, display_name TEXT
    );
    CREATE TABLE IF NOT EXISTS region_metrics (
      region_key TEXT PRIMARY KEY,
      total_packages INTEGER DEFAULT 0, total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0, total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0, avg_risk_score REAL DEFAULT 0, max_risk_score REAL DEFAULT 0,
      central_packages INTEGER DEFAULT 0, provincial_packages INTEGER DEFAULT 0,
      local_packages INTEGER DEFAULT 0, other_packages INTEGER DEFAULT 0,
      central_priority_packages INTEGER DEFAULT 0, provincial_priority_packages INTEGER DEFAULT 0,
      local_priority_packages INTEGER DEFAULT 0, other_priority_packages INTEGER DEFAULT 0,
      central_potential_waste REAL DEFAULT 0, provincial_potential_waste REAL DEFAULT 0,
      local_potential_waste REAL DEFAULT 0, other_potential_waste REAL DEFAULT 0,
      central_budget REAL DEFAULT 0, provincial_budget REAL DEFAULT 0,
      local_budget REAL DEFAULT 0, other_budget REAL DEFAULT 0,
      med_severity_packages INTEGER DEFAULT 0, high_severity_packages INTEGER DEFAULT 0,
      absurd_severity_packages INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS province_metrics (
      province_key TEXT PRIMARY KEY,
      total_packages INTEGER DEFAULT 0, total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0, total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0, avg_risk_score REAL DEFAULT 0, max_risk_score REAL DEFAULT 0,
      med_severity_packages INTEGER DEFAULT 0, high_severity_packages INTEGER DEFAULT 0,
      absurd_severity_packages INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS owner_metrics (
      owner_type TEXT, owner_name TEXT,
      total_packages INTEGER DEFAULT 0, total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0, total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0, med_severity_packages INTEGER DEFAULT 0,
      high_severity_packages INTEGER DEFAULT 0, absurd_severity_packages INTEGER DEFAULT 0,
      PRIMARY KEY (owner_type, owner_name)
    );
    CREATE TABLE IF NOT EXISTS package_regions (
      package_id INTEGER, region_key TEXT, PRIMARY KEY (package_id, region_key)
    );
    CREATE TABLE IF NOT EXISTS package_provinces (
      package_id INTEGER, province_key TEXT, PRIMARY KEY (package_id, province_key)
    );
    CREATE TABLE IF NOT EXISTS assets (key TEXT PRIMARY KEY, json TEXT);
    CREATE INDEX IF NOT EXISTS idx_packages_owner ON packages(owner_type, owner_name);
    CREATE INDEX IF NOT EXISTS idx_packages_severity ON packages(severity);
    CREATE INDEX IF NOT EXISTS idx_packages_priority ON packages(is_priority);
    CREATE INDEX IF NOT EXISTS idx_package_regions ON package_regions(region_key);
    CREATE INDEX IF NOT EXISTS idx_package_provinces ON package_provinces(province_key);
  `);
}

function seedRegions(db, packets) {
  const rMap = new Map();
  const pMap = new Map();
  for (const p of packets) {
    const kode = String(p.kodeKlpd || p.kdKlpd || "");
    const nama = p.namaKlpd || "";
    const ownerType = getOwnerType(p);
    const provKey = getRegionKey(p);
    const provName = getProvinceName(nama);
    if (ownerType === "kabkota" && kode && !rMap.has(kode)) {
      rMap.set(kode, { region_key: kode, code: kode, province_name: provName,
        region_name: nama, region_type: nama.toLowerCase().startsWith("kota") ? "Kota" : "Kabupaten",
        display_name: nama });
      if (!pMap.has(provKey))
        pMap.set(provKey, { province_key: provKey, code: provKey, province_name: provName, display_name: provName });
    } else if (ownerType === "provinsi" && kode && !pMap.has(kode)) {
      pMap.set(kode, { province_key: kode, code: kode, province_name: nama, display_name: nama });
    }
  }
  const iR = db.prepare(`INSERT OR IGNORE INTO regions (region_key,code,province_name,region_name,region_type,display_name) VALUES (@region_key,@code,@province_name,@region_name,@region_type,@display_name)`);
  const iP = db.prepare(`INSERT OR IGNORE INTO provinces (province_key,code,province_name,display_name) VALUES (@province_key,@code,@province_name,@display_name)`);
  db.transaction((rows) => { for (const r of rows) iR.run(r); })([...rMap.values()]);
  db.transaction((rows) => { for (const r of rows) iP.run(r); })([...pMap.values()]);
  console.log(`Regions: ${rMap.size}, Provinces: ${pMap.size}`);
}

function insertPackages(db, packets) {
  const ins = db.prepare(`
    INSERT OR REPLACE INTO packages (
      inserted_order,source_id,schema_version,owner_name,owner_type,satker,
      package_name,location_raw,budget,funding_source,procurement_type,
      procurement_method,selection_date,potential_waste,severity,reason,
      is_mencurigakan,is_pemborosan,risk_score,active_tag_count,
      is_priority,is_flagged,mapped_region_count
    ) VALUES (@inserted_order,@source_id,@schema_version,@owner_name,@owner_type,
      @satker,@package_name,@location_raw,@budget,@funding_source,@procurement_type,
      @procurement_method,@selection_date,@potential_waste,@severity,@reason,
      @is_mencurigakan,@is_pemborosan,@risk_score,@active_tag_count,
      @is_priority,@is_flagged,@mapped_region_count)`);
  const iReg = db.prepare(`INSERT OR IGNORE INTO package_regions (package_id,region_key) VALUES (?,?)`);
  const iProv = db.prepare(`INSERT OR IGNORE INTO package_provinces (package_id,province_key) VALUES (?,?)`);

  db.transaction((rows) => {
    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      const audit = hitungRiskScore(p);
      const ownerType = getOwnerType(p);
      const kode = String(p.kodeKlpd || p.kdKlpd || "");
      const provKey = getRegionKey(p);
      const res = ins.run({
        inserted_order: i,
        source_id: String(p.kodeRup || p.id || `${kode}-${i}`),
        schema_version: "1.0",
        owner_name: p.namaKlpd || "",
        owner_type: ownerType,
        satker: p.namaSatker || null,
        package_name: p.namaPaket || "",
        location_raw: p.namaKlpd || null,
        budget: p.pagu || 0,
        funding_source: p.sumberDana || null,
        procurement_type: p.jenisPengadaan || null,
        procurement_method: p.metodePengadaan || null,
        selection_date: p.tanggalSeleksi || null,
        potential_waste: audit.potentialWaste,
        severity: audit.severity,
        reason: audit.reason,
        is_mencurigakan: audit.isMencurigakan ? 1 : 0,
        is_pemborosan: audit.isPemborosan ? 1 : 0,
        risk_score: audit.riskScore,
        active_tag_count: 0,
        is_priority: audit.isPriority ? 1 : 0,
        is_flagged: audit.isFlagged ? 1 : 0,
        mapped_region_count: kode ? 1 : 0,
      });
      const pkgId = res.lastInsertRowid;
      if (kode) {
        iReg.run(pkgId, kode);
        if (ownerType === "provinsi") iProv.run(pkgId, kode);
        else if (ownerType === "kabkota") iProv.run(pkgId, provKey);
      }
    }
  })(packets);
  console.log(`Inserted: ${packets.length} packages`);
}

function rebuildMetrics(db) {
  console.log("Rebuilding metrics...");
  db.exec(`
    DELETE FROM region_metrics;
    INSERT INTO region_metrics SELECT r.region_key,
      COUNT(p.id),COALESCE(SUM(p.is_priority),0),COALESCE(SUM(p.is_flagged),0),
      ROUND(COALESCE(SUM(p.potential_waste),0),2),COALESCE(SUM(p.budget),0),
      ROUND(COALESCE(AVG(p.risk_score),0),2),COALESCE(MAX(p.risk_score),0),
      SUM(CASE WHEN p.owner_type='central' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='provinsi' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='kabkota' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='other' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='central' AND p.is_priority=1 THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='provinsi' AND p.is_priority=1 THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='kabkota' AND p.is_priority=1 THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='other' AND p.is_priority=1 THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.owner_type='central' THEN p.potential_waste ELSE 0 END),
      SUM(CASE WHEN p.owner_type='provinsi' THEN p.potential_waste ELSE 0 END),
      SUM(CASE WHEN p.owner_type='kabkota' THEN p.potential_waste ELSE 0 END),
      SUM(CASE WHEN p.owner_type='other' THEN p.potential_waste ELSE 0 END),
      SUM(CASE WHEN p.owner_type='central' THEN COALESCE(p.budget,0) ELSE 0 END),
      SUM(CASE WHEN p.owner_type='provinsi' THEN COALESCE(p.budget,0) ELSE 0 END),
      SUM(CASE WHEN p.owner_type='kabkota' THEN COALESCE(p.budget,0) ELSE 0 END),
      SUM(CASE WHEN p.owner_type='other' THEN COALESCE(p.budget,0) ELSE 0 END),
      SUM(CASE WHEN p.severity='med' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.severity='high' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.severity='absurd' THEN 1 ELSE 0 END)
    FROM regions r
    LEFT JOIN package_regions pr ON pr.region_key=r.region_key
    LEFT JOIN packages p ON p.id=pr.package_id
    GROUP BY r.region_key;

    DELETE FROM province_metrics;
    INSERT INTO province_metrics SELECT prov.province_key,
      COUNT(p.id),COALESCE(SUM(p.is_priority),0),COALESCE(SUM(p.is_flagged),0),
      ROUND(COALESCE(SUM(p.potential_waste),0),2),COALESCE(SUM(p.budget),0),
      ROUND(COALESCE(AVG(p.risk_score),0),2),COALESCE(MAX(p.risk_score),0),
      SUM(CASE WHEN p.severity='med' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.severity='high' THEN 1 ELSE 0 END),
      SUM(CASE WHEN p.severity='absurd' THEN 1 ELSE 0 END)
    FROM provinces prov
    LEFT JOIN package_provinces pp ON pp.province_key=prov.province_key
    LEFT JOIN packages p ON p.id=pp.package_id
    GROUP BY prov.province_key;

    DELETE FROM owner_metrics;
    INSERT INTO owner_metrics SELECT owner_type,owner_name,COUNT(*),
      COALESCE(SUM(is_priority),0),COALESCE(SUM(is_flagged),0),
      ROUND(COALESCE(SUM(potential_waste),0),2),COALESCE(SUM(budget),0),
      SUM(CASE WHEN severity='med' THEN 1 ELSE 0 END),
      SUM(CASE WHEN severity='high' THEN 1 ELSE 0 END),
      SUM(CASE WHEN severity='absurd' THEN 1 ELSE 0 END)
    FROM packages GROUP BY owner_type,owner_name;
  `);
  console.log("Metrics done.");
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n=== Fetch SiRUP via Cloudflare Proxy → SQLite (TA ${TAHUN}) ===`);
  console.log(`Proxy: ${SIRUP_BASE}\n`);

  fs.mkdirSync(DATA_DIR, { recursive: true });
  if (fs.existsSync(DB_PATH)) { fs.unlinkSync(DB_PATH); console.log("DB lama dihapus."); }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  setupSchema(db);

  const allPackets = [];
  let halaman = 1;
  const limit = 100;

  while (halaman <= 500) {
    const url = `${SIRUP_BASE}/penyedia/publik?tahun=${TAHUN}&halaman=${halaman}&limit=${limit}`;
    console.log(`  Halaman ${halaman}...`);
    let data;
    try {
      data = await fetchWithRetry(url);
    } catch (e) {
      console.warn(`  Stop: ${e.message}`);
      break;
    }
    const items = data?.data || data?.list || data?.result || data?.paket || [];
    if (!Array.isArray(items) || items.length === 0) { console.log("  Selesai."); break; }
    allPackets.push(...items);
    console.log(`  +${items.length} (total: ${allPackets.length})`);
    const totalPage = data?.totalHalaman || data?.totalPage || data?.last_page || 1;
    if (halaman >= totalPage) break;
    halaman++;
    await sleep(500);
  }

  if (!allPackets.length) {
    console.error("Tidak ada data! Cek URL proxy worker.");
    process.exit(1);
  }

  console.log(`\nTotal: ${allPackets.length} paket`);
  seedRegions(db, allPackets);
  insertPackages(db, allPackets);
  rebuildMetrics(db);

  db.prepare(`INSERT OR IGNORE INTO assets (key,json) VALUES (?,?)`).run("audit_geojson", JSON.stringify({ type:"FeatureCollection",features:[] }));
  db.prepare(`INSERT OR IGNORE INTO assets (key,json) VALUES (?,?)`).run("audit_province_geojson", JSON.stringify({ type:"FeatureCollection",features:[] }));
  db.close();

  const stat = fs.statSync(DB_PATH);
  console.log(`\nDone! ${(stat.size/1024/1024).toFixed(2)} MB → ${DB_PATH}`);
}

main().catch((e) => { console.error("Fatal:", e.message); process.exit(1); });
