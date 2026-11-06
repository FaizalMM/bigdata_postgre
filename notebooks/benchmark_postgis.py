"""
Tahap 4a: benchmark kueri Q1-Q9 pada PostgreSQL/PostGIS.

Aturan ukur (HARUS sama dengan MongoDB dan Spark):
  - 1 kali pemanasan (dibuang), lalu 5 kali ukur; dilaporkan median, min, max (ms)
  - hasil diambil penuh (fetchall), bukan hanya dieksekusi
  - definisi kueri dan parameter IDENTIK di semua sistem (lihat blok PARAMETER)
Jalankan SETELAH 02_muat_postgis_v2.py.   Pemakaian:
    python notebooks/03_benchmark_postgis.py [--ukur 5] [--pemanasan 1]
Password dibaca dari variabel lingkungan PGPASSWORD.
"""
import argparse
import os
import statistics
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

HASIL = Path("hasil")
HASIL.mkdir(exist_ok=True)

if not os.environ.get("PGPASSWORD"):
    sys.exit('PGPASSWORD belum diatur. Jalankan dulu:  $env:PGPASSWORD = "160105"')

DB = dict(host=os.environ.get("PGHOST", "localhost"), port=5432,
          dbname="gempa", user="postgres", password=os.environ["PGPASSWORD"])

# ------------------------- PARAMETER (sama di semua sistem) -------------------------
SURABAYA = (112.7521, -7.2575)            # (bujur, lintang)
PALU = (119.8707, -0.8917)
SULAWESI_BOX = (118.0, -6.0, 125.5, 2.0)  # bujur_min, lintang_min, bujur_maks, lintang_maks
R_Q5 = 100_000                            # meter
R_Q7 = 200_000
R_Q8 = 50_000
# jarak dihitung pada bola (use_spheroid = false) agar sebanding dengan MongoDB


def pt(lon, lat):
    return f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography"


QUERIES = [
    ("Q1", "magnitude >= 6",
     "SELECT COUNT(*) FROM gempa WHERE magnitude >= 6",
     lambda r: f"{r[0][0]} baris"),
    ("Q2", "gempa tahun 2019 (UTC)",
     "SELECT COUNT(*) FROM gempa WHERE \"datetime\" >= TIMESTAMPTZ '2019-01-01 00:00:00+00' "
     "AND \"datetime\" < TIMESTAMPTZ '2020-01-01 00:00:00+00'",
     lambda r: f"{r[0][0]} baris"),
    ("Q3", "M>=5, depth<70, 2020-2023",
     "SELECT COUNT(*) FROM gempa WHERE magnitude >= 5 AND depth < 70 "
     "AND \"datetime\" >= TIMESTAMPTZ '2020-01-01 00:00:00+00' "
     "AND \"datetime\" < TIMESTAMPTZ '2024-01-01 00:00:00+00'",
     lambda r: f"{r[0][0]} baris"),
    ("Q4", "jumlah & rata-rata magnitude per tahun",
     "SELECT EXTRACT(YEAR FROM \"datetime\" AT TIME ZONE 'UTC')::int AS tahun, "
     "COUNT(*) AS n, AVG(magnitude) AS rata2 FROM gempa GROUP BY 1 ORDER BY 1",
     lambda r: f"{len(r)} tahun; total={sum(x[1] for x in r)}"),
    ("Q5", "radius 100 km dari Surabaya",
     f"SELECT COUNT(*) FROM gempa WHERE ST_DWithin(geom::geography, {pt(*SURABAYA)}, {R_Q5}, false)",
     lambda r: f"{r[0][0]} baris"),
    ("Q6", "bounding box Sulawesi",
     "SELECT COUNT(*) FROM gempa WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)" % SULAWESI_BOX,
     lambda r: f"{r[0][0]} baris"),
    ("Q7", "M>=5, 200 km dari Palu, tahun 2018",
     f"SELECT COUNT(*) FROM gempa WHERE magnitude >= 5 "
     f"AND \"datetime\" >= TIMESTAMPTZ '2018-01-01 00:00:00+00' "
     f"AND \"datetime\" < TIMESTAMPTZ '2019-01-01 00:00:00+00' "
     f"AND ST_DWithin(geom::geography, {pt(*PALU)}, {R_Q7}, false)",
     lambda r: f"{r[0][0]} baris"),
    ("Q8", "stasiun dalam 50 km dari tiap gempa M>=6",
     "SELECT g.\"eventID\", COUNT(s.id) AS jumlah_stasiun FROM gempa g "
     f"LEFT JOIN sensor s ON ST_DWithin(g.geom::geography, s.geom::geography, {R_Q8}, false) "
     "WHERE g.magnitude >= 6 GROUP BY g.\"eventID\"",
     lambda r: f"{len(r)} gempa; total_baris_sensor={sum(x[1] for x in r)}"),
    ("Q9", "doubleCouple > 0.8 (focal mechanism)",
     "SELECT COUNT(*) FROM gempa WHERE \"doubleCouple\" > 0.8",
     lambda r: f"{r[0][0]} baris"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ukur", type=int, default=5)
    ap.add_argument("--pemanasan", type=int, default=1)
    a = ap.parse_args()

    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    cur = conn.cursor()

    # pastikan data sudah termuat
    try:
        cur.execute("SELECT COUNT(*) FROM gempa")
        n_g = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM sensor")
        n_s = cur.fetchone()[0]
    except psycopg2.Error:
        sys.exit("Tabel gempa/sensor belum ada. Jalankan dulu 02_muat_postgis_v2.py")
    print(f"Data: gempa={n_g}, sensor={n_s}")

    # catat konfigurasi (bahan laporan: spesifikasi dan konfigurasi)
    info = []
    cur.execute("SELECT version()"); info.append(cur.fetchone()[0])
    cur.execute("SELECT postgis_version()"); info.append("PostGIS " + cur.fetchone()[0])
    for p in ("shared_buffers", "work_mem", "max_parallel_workers_per_gather", "TimeZone"):
        cur.execute(f"SHOW {p}")
        info.append(f"{p} = {cur.fetchone()[0]}")
    (HASIL / "konfigurasi_postgis.txt").write_text("\n".join(info), encoding="utf-8")
    print("\n".join("  " + x for x in info))

    rows = []
    print(f"\nPemanasan {a.pemanasan}x (dibuang), ukur {a.ukur}x per kueri\n")
    for kode, desk, q, ringkas in QUERIES:
        # rencana eksekusi (sekali) untuk analisis: apakah memakai indeks?
        cur.execute("EXPLAIN " + q)
        plan = "\n".join(r[0] for r in cur.fetchall())
        (HASIL / f"plan_postgis_{kode}.txt").write_text(plan, encoding="utf-8")
        pakai_indeks = "Index" in plan

        for _ in range(a.pemanasan):
            cur.execute(q)
            cur.fetchall()
        waktu = []
        for _ in range(a.ukur):
            t0 = time.perf_counter()
            cur.execute(q)
            hasil = cur.fetchall()
            waktu.append((time.perf_counter() - t0) * 1000)
        ringkasan = ringkas(hasil)
        med = statistics.median(waktu)
        print(f"{kode} | {desk:<42} | median {med:9.2f} ms | min {min(waktu):9.2f} | "
              f"maks {max(waktu):9.2f} | indeks: {'ya' if pakai_indeks else 'tidak'} | {ringkasan}")
        row = {"kueri": kode, "deskripsi": desk, "hasil_ringkas": ringkasan,
               "median_ms": round(med, 3), "min_ms": round(min(waktu), 3),
               "maks_ms": round(max(waktu), 3), "pakai_indeks": pakai_indeks}
        row.update({f"ukur_{i+1}_ms": round(v, 3) for i, v in enumerate(waktu)})
        rows.append(row)

    pd.DataFrame(rows).to_csv(HASIL / "benchmark_postgis.csv", index=False)
    print("\nTersimpan: hasil/benchmark_postgis.csv, hasil/konfigurasi_postgis.txt, hasil/plan_postgis_Q*.txt")
    print("Catat kolom hasil_ringkas: angka itu harus SAMA di MongoDB dan Spark.")
    conn.close()


if __name__ == "__main__":
    main()