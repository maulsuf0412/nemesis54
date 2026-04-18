#!/usr/bin/env node
/**
 * fetch-sirup.js
 * Fetch data SiRUP LKPP → build dashboard.sqlite untuk Nemesis
 */

const https = require("https");
const http = require("http");
const path = require("path");
const fs = require("fs");
const Database = require("better-sqlite3");

const TAHUN = process.env.AUDIT_DATASET_YEAR || "2026";
const DATA_DIR = path.resolve(__dirname, "../data");
const DB_PATH = path.join(DATA_DIR, "dashboard.sqlite");
const SIRUP_BASE = "https://sirup.lkpp.go.id/sirup/ro/publicsipd";

// ─── HTTP helper ─────────────────────────────────────────────────────────────

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    const req = client.get(url, { timeout: 30000 }, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        try {
          resolve(JSON.parse(body));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}\nURL: ${url}`));
        }
      });
    });
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error(`Timeout: ${url}`));
    });
  });
}

async function fetchWithRetry(url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fetchJson(url);
    } catch (e) {
      console.warn(`  Retry ${i + 1}/${retries}: ${e.message}`);
      if (i === retries - 1) throw e;
      await new Promise((r) => setTimeout(r, 2000 * (i + 1)));
    }
  }
}

// ─── API SiRUP ───────────────────────────────────────────────────────────────

async function fetchPaketList(idSatker, tahun, halaman = 1, limit = 100) {
  const url = `${SIRUP_BASE}/penyedia/publik?idSatker=${idSatker}&tahun=${tahun}&halaman=${halaman}&limit=${limit}`;
  return fetchWithRetry(url);
}

async function fetchDaftarSatker(tahun) {
  // Ambil semua satker aktif dari SiRUP
  const url = `${SIRUP_BASE}/satker/list?tahun=${tahun}&halaman=1&limit=500`;
  return fetchWithRetry(url);
}

// ─── Analisis anomali ─────────────────────────────────────────────────────────

const BATAS_PENGADAAN = [
  200_000_000,
  500_000_000,
  1_000_000_000,
  2_500_000_000,
  5_000_000_000,
];

function hitungRiskScore(paket) {
  let score = 0;
  const flags = { isMencurigakan: false, isPemborosan: false };
  const reasons = [];

  const pagu = paket.pagu || 0;
  const metode = (paket.metodePengadaan || "").toLowerCase();
  const nama = (paket.namaPaket || "").toLowerCase().trim();

  // 1. Pagu mendekati batas (pecah paket)
  for (const batas of BATAS_PENGADAAN) {
    if (pagu >= batas * 0.92 && pagu <= batas) {
      score += 35;
      flags.isMencurigakan = true;
      reasons.push(`Pagu Rp${(pagu / 1e9).toFixed(2)}M mendekati batas Rp${(batas / 1e9).toFixed(0)}M`);
      break;
    }
  }

  // 2. Penunjukan langsung nilai besar
  if (metode.includes("penunjukan langsung") && pagu > 200_000_000) {
    score += 30;
    flags.isMencurigakan = true;
    reasons.push(`Penunjukan langsung nilai Rp${(pagu / 1e6).toFixed(0)}jt`);
  }

  // 3. Nama paket generik / tidak informatif
  const generikPatterns = [
    /^pengadaan barang$/,
    /^jasa konsultansi$/,
    /^belanja modal$/,
    /^kegiatan \w+$/,
    /^pekerjaan \w+$/,
  ];
  if (nama.length < 15 || generikPatterns.some((p) => p.test(nama))) {
    score += 15;
    flags.isMencurigakan = true;
    reasons.push("Nama paket terlalu generik / tidak informatif");
  }

  // 4. Pagu bulat sempurna (indikasi mark-up)
  if (pagu >= 500_000_000 && pagu % 100_000_000 === 0) {
    score += 20;
    flags.isPemborosan = true;
    reasons.push("Pagu angka bulat sempurna (indikasi mark-up)");
  }

  // 5. Swakelola tapi pagu besar
  if (metode.includes("swakelola") && pagu > 1_000_000_000) {
    score += 15;
    flags.isPemborosan = true;
    reasons.push(`Swakelola nilai besar Rp${(pagu / 1e9).toFixed(2)}M`);
  }

  // Potensi pemborosan: estimasi 15% dari pagu jika ada flag
  const potentialWaste = (flags.isMencurigakan || flags.isPemborosan)
    ? Math.round(pagu * 0.15)
    : 0;

  // Severity berdasarkan skor
  let severity = "low";
  if (score >= 60) severity = "absurd";
  else if (score >= 40) severity = "high";
  else if (score >= 20) severity = "med";

  return {
    riskScore: Math.min(score, 100),
    severity,
    potentialWaste,
    reason: reasons.join("; ") || null,
    isMencurigakan: flags.isMencurigakan,
    isPemborosan: flags.isPemborosan,
    isFlagged: flags.isMencurigakan || flags.isPemborosan,
    isPriority: score >= 40,
  };
}

// ─── Klasifikasi owner type ────────────────────────────────────────────────────

function getOwnerType(paket) {
  const kode = String(paket.kodeKlpd || paket.kdKlpd || "");
  const nama = (paket.namaKlpd || "").toLowerCase();
  if (kode.startsWith("K") || nama.includes("kementerian") || nama.includes("badan nasional")) {
    return "central";
  }
  if (nama.includes("provinsi") || nama.includes("prov.")) {
    return "provinsi";
  }
  if (
    nama.includes("kabupaten") ||
    nama.includes("kota ") ||
    nama.includes("kab.")
  ) {
    return "kabkota";
  }
  return "other";
}

// ─── Parse lokasi ke region_key ───────────────────────────────────────────────

function parseRegionKey(paket) {
  // Gunakan kode satker/klpd sebagai region key fallback
  const namaKlpd = (paket.namaKlpd || "").toLowerCase();

  // Coba map ke kode BPS wilayah (simplified)
  const provinsiMap = {
    "aceh": "11", "sumatera utara": "12", "sumatera barat": "13",
    "riau": "14", "jambi": "15", "sumatera selatan": "16",
    "bengkulu": "17", "lampung": "18", "bangka belitung": "19",
    "kepulauan riau": "21", "dki jakarta": "31", "jawa barat": "32",
    "jawa tengah": "33", "di yogyakarta": "34", "jawa timur": "35",
    "banten": "36", "bali": "51", "nusa tenggara barat": "52",
    "nusa tenggara timur": "53", "kalimantan barat": "61",
    "kalimantan tengah": "62", "kalimantan selatan": "63",
    "kalimantan timur": "64", "kalimantan utara": "65",
    "sulawesi utara": "71", "sulawesi tengah": "72",
    "sulawesi selatan": "73", "sulawesi tenggara": "74",
    "gorontalo": "75", "sulawesi barat": "76", "maluku": "81",
    "maluku utara": "82", "papua barat": "91", "papua": "94",
  };

  for (const [key, code] of Object.entries(provinsiMap)) {
    if (namaKlpd.includes(key)) {
      return code; // gunakan kode provinsi sebagai region key
    }
  }

  return paket.kodeKlpd || paket.kdKlpd || "00";
}

// ─── Setup SQLite schema ──────────────────────────────────────────────────────

function setupSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS packages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      inserted_order INTEGER,
      source_id TEXT UNIQUE,
      schema_version TEXT DEFAULT '1.0',
      owner_name TEXT,
      owner_type TEXT,
      satker TEXT,
      package_name TEXT,
      location_raw TEXT,
      budget REAL,
      funding_source TEXT,
      procurement_type TEXT,
      procurement_method TEXT,
      selection_date TEXT,
      potential_waste REAL DEFAULT 0,
      severity TEXT DEFAULT 'low',
      reason TEXT,
      is_mencurigakan INTEGER,
      is_pemborosan INTEGER,
      risk_score REAL DEFAULT 0,
      active_tag_count INTEGER DEFAULT 0,
      is_priority INTEGER DEFAULT 0,
      is_flagged INTEGER DEFAULT 0,
      mapped_region_count INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS regions (
      region_key TEXT PRIMARY KEY,
      code TEXT,
      province_name TEXT,
      region_name TEXT,
      region_type TEXT,
      display_name TEXT
    );

    CREATE TABLE IF NOT EXISTS provinces (
      province_key TEXT PRIMARY KEY,
      code TEXT,
      province_name TEXT,
      display_name TEXT
    );

    CREATE TABLE IF NOT EXISTS region_metrics (
      region_key TEXT PRIMARY KEY,
      total_packages INTEGER DEFAULT 0,
      total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0,
      total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0,
      avg_risk_score REAL DEFAULT 0,
      max_risk_score REAL DEFAULT 0,
      central_packages INTEGER DEFAULT 0,
      provincial_packages INTEGER DEFAULT 0,
      local_packages INTEGER DEFAULT 0,
      other_packages INTEGER DEFAULT 0,
      central_priority_packages INTEGER DEFAULT 0,
      provincial_priority_packages INTEGER DEFAULT 0,
      local_priority_packages INTEGER DEFAULT 0,
      other_priority_packages INTEGER DEFAULT 0,
      central_potential_waste REAL DEFAULT 0,
      provincial_potential_waste REAL DEFAULT 0,
      local_potential_waste REAL DEFAULT 0,
      other_potential_waste REAL DEFAULT 0,
      central_budget REAL DEFAULT 0,
      provincial_budget REAL DEFAULT 0,
      local_budget REAL DEFAULT 0,
      other_budget REAL DEFAULT 0,
      med_severity_packages INTEGER DEFAULT 0,
      high_severity_packages INTEGER DEFAULT 0,
      absurd_severity_packages INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS province_metrics (
      province_key TEXT PRIMARY KEY,
      total_packages INTEGER DEFAULT 0,
      total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0,
      total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0,
      avg_risk_score REAL DEFAULT 0,
      max_risk_score REAL DEFAULT 0,
      med_severity_packages INTEGER DEFAULT 0,
      high_severity_packages INTEGER DEFAULT 0,
      absurd_severity_packages INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS owner_metrics (
      owner_type TEXT,
      owner_name TEXT,
      total_packages INTEGER DEFAULT 0,
      total_priority_packages INTEGER DEFAULT 0,
      total_flagged_packages INTEGER DEFAULT 0,
      total_potential_waste REAL DEFAULT 0,
      total_budget REAL DEFAULT 0,
      med_severity_packages INTEGER DEFAULT 0,
      high_severity_packages INTEGER DEFAULT 0,
      absurd_severity_packages INTEGER DEFAULT 0,
      PRIMARY KEY (owner_type, owner_name)
    );

    CREATE TABLE IF NOT EXISTS package_regions (
      package_id INTEGER,
      region_key TEXT,
      PRIMARY KEY (package_id, region_key)
    );

    CREATE TABLE IF NOT EXISTS package_provinces (
      package_id INTEGER,
      province_key TEXT,
      PRIMARY KEY (package_id, province_key)
    );

    CREATE TABLE IF NOT EXISTS assets (
      key TEXT PRIMARY KEY,
      json TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_packages_owner ON packages(owner_type, owner_name);
    CREATE INDEX IF NOT EXISTS idx_packages_severity ON packages(severity);
    CREATE INDEX IF NOT EXISTS idx_packages_priority ON packages(is_priority);
    CREATE INDEX IF NOT EXISTS idx_package_regions ON package_regions(region_key);
    CREATE INDEX IF NOT EXISTS idx_package_provinces ON package_provinces(province_key);
  `);
}

// ─── Seed regions & provinces dari data SiRUP ─────────────────────────────────

function seedRegionsFromData(db, allPackets) {
  // Kumpulkan semua wilayah unik dari data
  const regionMap = new Map();
  const provinceMap = new Map();

  for (const p of allPackets) {
    const namaKlpd = p.namaKlpd || "";
    const kodeKlpd = String(p.kodeKlpd || p.kdKlpd || "");
    const ownerType = getOwnerType(p);
    const regionKey = parseRegionKey(p);

    if (ownerType === "kabkota" && !regionMap.has(kodeKlpd)) {
      // Ekstrak nama provinsi dari namaKlpd
      const provinceName = extractProvinceName(namaKlpd);
      regionMap.set(kodeKlpd, {
        region_key: kodeKlpd,
        code: kodeKlpd,
        province_name: provinceName,
        region_name: namaKlpd,
        region_type: namaKlpd.toLowerCase().includes("kota") ? "Kota" : "Kabupaten",
        display_name: namaKlpd,
      });

      if (!provinceMap.has(regionKey)) {
        provinceMap.set(regionKey, {
          province_key: regionKey,
          code: regionKey,
          province_name: provinceName,
          display_name: provinceName,
        });
      }
    } else if (ownerType === "provinsi" && !provinceMap.has(kodeKlpd)) {
      provinceMap.set(kodeKlpd, {
        province_key: kodeKlpd,
        code: kodeKlpd,
        province_name: namaKlpd,
        display_name: namaKlpd,
      });
    }
  }

  const insertRegion = db.prepare(`
    INSERT OR IGNORE INTO regions (region_key, code, province_name, region_name, region_type, display_name)
    VALUES (@region_key, @code, @province_name, @region_name, @region_type, @display_name)
  `);
  const insertProvince = db.prepare(`
    INSERT OR IGNORE INTO provinces (province_key, code, province_name, display_name)
    VALUES (@province_key, @code, @province_name, @display_name)
  `);

  const insertManyRegions = db.transaction((rows) => {
    for (const row of rows) insertRegion.run(row);
  });
  const insertManyProvinces = db.transaction((rows) => {
    for (const row of rows) insertProvince.run(row);
  });

  insertManyRegions([...regionMap.values()]);
  insertManyProvinces([...provinceMap.values()]);

  console.log(`Seed: ${regionMap.size} regions, ${provinceMap.size} provinces`);
}

function extractProvinceName(namaKlpd) {
  const lower = namaKlpd.toLowerCase();
  const provinsiList = [
    "Aceh", "Sumatera Utara", "Sumatera Barat", "Riau", "Jambi",
    "Sumatera Selatan", "Bengkulu", "Lampung", "Bangka Belitung",
    "Kepulauan Riau", "DKI Jakarta", "Jawa Barat", "Jawa Tengah",
    "DI Yogyakarta", "Jawa Timur", "Banten", "Bali",
    "Nusa Tenggara Barat", "Nusa Tenggara Timur", "Kalimantan Barat",
    "Kalimantan Tengah", "Kalimantan Selatan", "Kalimantan Timur",
    "Kalimantan Utara", "Sulawesi Utara", "Sulawesi Tengah",
    "Sulawesi Selatan", "Sulawesi Tenggara", "Gorontalo",
    "Sulawesi Barat", "Maluku", "Maluku Utara", "Papua Barat", "Papua",
  ];
  for (const prov of provinsiList) {
    if (lower.includes(prov.toLowerCase())) return prov;
  }
  return namaKlpd;
}

// ─── Insert packages ──────────────────────────────────────────────────────────

function insertPackages(db, packets) {
  const insert = db.prepare(`
    INSERT OR REPLACE INTO packages (
      inserted_order, source_id, schema_version,
      owner_name, owner_type, satker, package_name,
      location_raw, budget, funding_source,
      procurement_type, procurement_method, selection_date,
      potential_waste, severity, reason,
      is_mencurigakan, is_pemborosan,
      risk_score, active_tag_count,
      is_priority, is_flagged, mapped_region_count
    ) VALUES (
      @inserted_order, @source_id, @schema_version,
      @owner_name, @owner_type, @satker, @package_name,
      @location_raw, @budget, @funding_source,
      @procurement_type, @procurement_method, @selection_date,
      @potential_waste, @severity, @reason,
      @is_mencurigakan, @is_pemborosan,
      @risk_score, @active_tag_count,
      @is_priority, @is_flagged, @mapped_region_count
    )
  `);

  const insertRegionLink = db.prepare(`
    INSERT OR IGNORE INTO package_regions (package_id, region_key) VALUES (?, ?)
  `);
  const insertProvinceLink = db.prepare(`
    INSERT OR IGNORE INTO package_provinces (package_id, province_key) VALUES (?, ?)
  `);

  const insertMany = db.transaction((rows, startOrder) => {
    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      const audit = hitungRiskScore(p);
      const ownerType = getOwnerType(p);
      const regionKey = parseRegionKey(p);
      const kodeKlpd = String(p.kodeKlpd || p.kdKlpd || "");

      const row = {
        inserted_order: startOrder + i,
        source_id: String(p.kodeRup || p.id || `${kodeKlpd}-${i}`),
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
        mapped_region_count: kodeKlpd ? 1 : 0,
      };

      const result = insert.run(row);
      const pkgId = result.lastInsertRowid;

      if (kodeKlpd) {
        insertRegionLink.run(pkgId, kodeKlpd);
        if (ownerType === "provinsi") {
          insertProvinceLink.run(pkgId, kodeKlpd);
        } else if (ownerType === "kabkota") {
          insertProvinceLink.run(pkgId, regionKey);
        }
      }
    }
  });

  insertMany(packets, 0);
}

// ─── Rebuild metrics ──────────────────────────────────────────────────────────

function rebuildMetrics(db) {
  console.log("Rebuilding metrics...");

  // Region metrics
  db.exec(`
    DELETE FROM region_metrics;
    INSERT INTO region_metrics
    SELECT
      r.region_key,
      COUNT(p.id) AS total_packages,
      SUM(p.is_priority) AS total_priority_packages,
      SUM(p.is_flagged) AS total_flagged_packages,
      ROUND(SUM(p.potential_waste), 2) AS total_potential_waste,
      SUM(COALESCE(p.budget, 0)) AS total_budget,
      ROUND(AVG(p.risk_score), 2) AS avg_risk_score,
      MAX(p.risk_score) AS max_risk_score,
      SUM(CASE WHEN p.owner_type = 'central' THEN 1 ELSE 0 END) AS central_packages,
      SUM(CASE WHEN p.owner_type = 'provinsi' THEN 1 ELSE 0 END) AS provincial_packages,
      SUM(CASE WHEN p.owner_type = 'kabkota' THEN 1 ELSE 0 END) AS local_packages,
      SUM(CASE WHEN p.owner_type = 'other' THEN 1 ELSE 0 END) AS other_packages,
      SUM(CASE WHEN p.owner_type = 'central' AND p.is_priority = 1 THEN 1 ELSE 0 END) AS central_priority_packages,
      SUM(CASE WHEN p.owner_type = 'provinsi' AND p.is_priority = 1 THEN 1 ELSE 0 END) AS provincial_priority_packages,
      SUM(CASE WHEN p.owner_type = 'kabkota' AND p.is_priority = 1 THEN 1 ELSE 0 END) AS local_priority_packages,
      SUM(CASE WHEN p.owner_type = 'other' AND p.is_priority = 1 THEN 1 ELSE 0 END) AS other_priority_packages,
      SUM(CASE WHEN p.owner_type = 'central' THEN p.potential_waste ELSE 0 END) AS central_potential_waste,
      SUM(CASE WHEN p.owner_type = 'provinsi' THEN p.potential_waste ELSE 0 END) AS provincial_potential_waste,
      SUM(CASE WHEN p.owner_type = 'kabkota' THEN p.potential_waste ELSE 0 END) AS local_potential_waste,
      SUM(CASE WHEN p.owner_type = 'other' THEN p.potential_waste ELSE 0 END) AS other_potential_waste,
      SUM(CASE WHEN p.owner_type = 'central' THEN COALESCE(p.budget, 0) ELSE 0 END) AS central_budget,
      SUM(CASE WHEN p.owner_type = 'provinsi' THEN COALESCE(p.budget, 0) ELSE 0 END) AS provincial_budget,
      SUM(CASE WHEN p.owner_type = 'kabkota' THEN COALESCE(p.budget, 0) ELSE 0 END) AS local_budget,
      SUM(CASE WHEN p.owner_type = 'other' THEN COALESCE(p.budget, 0) ELSE 0 END) AS other_budget,
      SUM(CASE WHEN p.severity = 'med' THEN 1 ELSE 0 END) AS med_severity_packages,
      SUM(CASE WHEN p.severity = 'high' THEN 1 ELSE 0 END) AS high_severity_packages,
      SUM(CASE WHEN p.severity = 'absurd' THEN 1 ELSE 0 END) AS absurd_severity_packages
    FROM regions r
    LEFT JOIN package_regions pr ON pr.region_key = r.region_key
    LEFT JOIN packages p ON p.id = pr.package_id
    GROUP BY r.region_key;
  `);

  // Province metrics
  db.exec(`
    DELETE FROM province_metrics;
    INSERT INTO province_metrics
    SELECT
      prov.province_key,
      COUNT(p.id) AS total_packages,
      SUM(p.is_priority) AS total_priority_packages,
      SUM(p.is_flagged) AS total_flagged_packages,
      ROUND(SUM(p.potential_waste), 2) AS total_potential_waste,
      SUM(COALESCE(p.budget, 0)) AS total_budget,
      ROUND(AVG(p.risk_score), 2) AS avg_risk_score,
      MAX(p.risk_score) AS max_risk_score,
      SUM(CASE WHEN p.severity = 'med' THEN 1 ELSE 0 END) AS med_severity_packages,
      SUM(CASE WHEN p.severity = 'high' THEN 1 ELSE 0 END) AS high_severity_packages,
      SUM(CASE WHEN p.severity = 'absurd' THEN 1 ELSE 0 END) AS absurd_severity_packages
    FROM provinces prov
    LEFT JOIN package_provinces pp ON pp.province_key = prov.province_key
    LEFT JOIN packages p ON p.id = pp.package_id
    GROUP BY prov.province_key;
  `);

  // Owner metrics
  db.exec(`
    DELETE FROM owner_metrics;
    INSERT INTO owner_metrics
    SELECT
      owner_type,
      owner_name,
      COUNT(*) AS total_packages,
      SUM(is_priority) AS total_priority_packages,
      SUM(is_flagged) AS total_flagged_packages,
      ROUND(SUM(potential_waste), 2) AS total_potential_waste,
      SUM(COALESCE(budget, 0)) AS total_budget,
      SUM(CASE WHEN severity = 'med' THEN 1 ELSE 0 END) AS med_severity_packages,
      SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_severity_packages,
      SUM(CASE WHEN severity = 'absurd' THEN 1 ELSE 0 END) AS absurd_severity_packages
    FROM packages
    GROUP BY owner_type, owner_name;
  `);

  console.log("Metrics rebuilt.");
}

// ─── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`\n=== Fetch SiRUP LKPP → Nemesis SQLite (TA ${TAHUN}) ===\n`);

  // Pastikan folder data ada
  fs.mkdirSync(DATA_DIR, { recursive: true });

  // Hapus DB lama, buat baru
  if (fs.existsSync(DB_PATH)) {
    fs.unlinkSync(DB_PATH);
    console.log("DB lama dihapus.");
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  setupSchema(db);

  // ── Fetch semua paket ──
  console.log("Fetching data dari SiRUP LKPP...");
  const allPackets = [];

  // Fetch berdasarkan daftar kode satker utama
  // Ini akan di-expand sesuai kebutuhan
  const KODE_KLPD_LIST = [
    // Tambahkan kode KLPD yang ingin di-fetch
    // Contoh: kementerian, BUMN, provinsi tertentu
    // Biarkan kosong = fetch semua (gunakan endpoint publik)
  ];

  let halaman = 1;
  const limit = 100;
  let total = 0;

  // Gunakan endpoint publik tanpa filter satker
  while (true) {
    const url = `https://sirup.lkpp.go.id/sirup/ro/publicsipd/penyedia/publik?tahun=${TAHUN}&halaman=${halaman}&limit=${limit}`;
    console.log(`  Halaman ${halaman}...`);

    let data;
    try {
      data = await fetchWithRetry(url);
    } catch (e) {
      console.warn(`  Gagal fetch halaman ${halaman}: ${e.message}`);
      break;
    }

    const items = data?.data || data?.list || data?.result || [];
    if (!items.length) {
      console.log("  Tidak ada data lagi.");
      break;
    }

    allPackets.push(...items);
    total += items.length;
    console.log(`  +${items.length} paket (total: ${total})`);

    // Cek total halaman
    const totalHalaman = data?.totalHalaman || data?.totalPage || 1;
    if (halaman >= totalHalaman || halaman >= 200) break; // max 200 halaman
    halaman++;

    // Jeda antar request
    await new Promise((r) => setTimeout(r, 300));
  }

  if (!allPackets.length) {
    console.error("\nTidak ada data yang berhasil di-fetch!");
    console.log("Cek endpoint SiRUP LKPP atau coba manual.");
    process.exit(1);
  }

  console.log(`\nTotal paket: ${allPackets.length}`);

  // ── Seed wilayah dari data ──
  seedRegionsFromData(db, allPackets);

  // ── Insert semua paket ──
  console.log("Inserting packages...");
  insertPackages(db, allPackets);

  // ── Rebuild metrics ──
  rebuildMetrics(db);

  // ── Asset kosong (GeoJSON diisi terpisah) ──
  db.prepare(`INSERT OR IGNORE INTO assets (key, json) VALUES (?, ?)`).run(
    "audit_geojson",
    JSON.stringify({ type: "FeatureCollection", features: [] })
  );
  db.prepare(`INSERT OR IGNORE INTO assets (key, json) VALUES (?, ?)`).run(
    "audit_province_geojson",
    JSON.stringify({ type: "FeatureCollection", features: [] })
  );

  db.close();

  const stat = fs.statSync(DB_PATH);
  console.log(`\nSelesai! DB size: ${(stat.size / 1024 / 1024).toFixed(2)} MB`);
  console.log(`Path: ${DB_PATH}`);
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
