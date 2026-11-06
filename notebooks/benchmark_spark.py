"""
Benchmark kueri Q1-Q9 pada Spark (mode lokal) yang membaca file Parquet LANGSUNG.
Ini pembanding dasar (baseline): data tidak dimuat ke database, tidak ada indeks,
dan Parquet dibaca ulang pada setiap kueri (tanpa cache).

Aturan ukur sama dengan PostGIS/MongoDB: 1 pemanasan (dibuang), 5 ukur,
dilaporkan median/min/maks (ms). Semua waktu dihitung di zona UTC.

Karena Spark tidak punya indeks spasial, jarak dihitung dengan rumus haversine
(jari-jari bola 6371008.8 m, sama dengan mode bola PostGIS).

Pemakaian:  python notebooks/benchmark_spark.py [--ukur 5] [--pemanasan 1]
"""
import argparse
import os
import statistics
import sys
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

DATA = Path("data")
HASIL = Path("hasil")
HASIL.mkdir(exist_ok=True)

# Pastikan Spark memakai Python dari venv yang sedang aktif
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

R = 6371008.8
SURABAYA = (112.7521, -7.2575)            # (bujur, lintang)
PALU = (119.8707, -0.8917)
BOX = (118.0, -6.0, 125.5, 2.0)           # bujur_min, lintang_min, bujur_maks, lintang_maks

EXPECTED = {"Q1": "1775 baris", "Q2": "13635 baris", "Q3": "3744 baris",
            "Q4": "19 tahun; total=176384", "Q5": "151 baris", "Q6": "30556 baris",
            "Q7": "17 baris", "Q8": "1775 gempa; total_baris_sensor=375", "Q9": "3669 baris"}


def haversine(lon1, lat1, lon2, lat2):
    """Ekspresi SQL jarak (meter) antara dua titik; argumen berupa ekspresi/kolom/angka."""
    return (f"(2*{R}*asin(sqrt(pow(sin(radians(({lat2})-({lat1}))/2),2)"
            f"+cos(radians({lat1}))*cos(radians({lat2}))"
            f"*pow(sin(radians(({lon2})-({lon1}))/2),2))))")


def buat_spark():
    t0 = time.perf_counter()
    spark = (SparkSession.builder.master("local[*]").appName("gempa-baseline")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.caseSensitive", "true")   # sumber punya AzGapFM dan azgapFM sekaligus
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark, time.perf_counter() - t0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ukur", type=int, default=5)
    ap.add_argument("--pemanasan", type=int, default=1)
    a = ap.parse_args()

    spark, t_start = buat_spark()

    t0 = time.perf_counter()
    spark.read.parquet(str(DATA / "gempa.parquet")).createOrReplaceTempView("gempa_raw")
    spark.read.parquet(str(DATA / "sensor.parquet")).createOrReplaceTempView("sensor")
    # waktu disimpan sebagai teks ISO-8601 di sumber; di-cast saat dibaca (nilai tidak diubah)
    spark.sql("CREATE OR REPLACE TEMP VIEW gempa AS "
              "SELECT *, try_cast(datetime AS TIMESTAMP) AS ts FROM gempa_raw")
    t_reg = time.perf_counter() - t0

    n_g = spark.sql("SELECT COUNT(*) FROM gempa").collect()[0][0]
    n_s = spark.sql("SELECT COUNT(*) FROM sensor").collect()[0][0]
    n_ts_null = spark.sql("SELECT COUNT(*) FROM gempa WHERE ts IS NULL AND datetime IS NOT NULL").collect()[0][0]
    print(f"Data: gempa={n_g}, sensor={n_s} | waktu gagal di-cast: {n_ts_null}")
    if n_ts_null:
        print("PERINGATAN: ada nilai waktu yang gagal di-cast, hasil Q2-Q4/Q7 bisa meleset.")

    info = [f"Spark {spark.version} (mode lokal, local[*])",
            f"cores = {spark.sparkContext.defaultParallelism}",
            "driver memory = 2g", "shuffle partitions = 8", "timezone = UTC",
            f"Python {sys.version.split()[0]}",
            f"waktu start SparkSession = {t_start:.2f} s",
            f"waktu registrasi Parquet = {t_reg:.3f} s",
            f"ukuran gempa.parquet = {(DATA/'gempa.parquet').stat().st_size/1e6:.1f} MB",
            f"ukuran sensor.parquet = {(DATA/'sensor.parquet').stat().st_size/1e6:.1f} MB"]
    (HASIL / "konfigurasi_spark.txt").write_text("\n".join(info), encoding="utf-8")
    print("\n".join("  " + x for x in info))

    x0, y0, x1, y1 = BOX
    sql = {
        "Q1": "SELECT COUNT(*) FROM gempa WHERE magnitude >= 6",
        "Q2": ("SELECT COUNT(*) FROM gempa WHERE ts >= TIMESTAMP'2019-01-01 00:00:00' "
               "AND ts < TIMESTAMP'2020-01-01 00:00:00'"),
        "Q3": ("SELECT COUNT(*) FROM gempa WHERE magnitude >= 5 AND depth < 70 "
               "AND ts >= TIMESTAMP'2020-01-01 00:00:00' AND ts < TIMESTAMP'2024-01-01 00:00:00'"),
        "Q4": ("SELECT year(ts) AS tahun, COUNT(*) AS n, AVG(magnitude) AS rata2 "
               "FROM gempa GROUP BY year(ts) ORDER BY tahun"),
        "Q5": ("SELECT COUNT(*) FROM gempa WHERE "
               + haversine(SURABAYA[0], SURABAYA[1], "longitude", "latitude") + " <= 100000"),
        "Q6": (f"SELECT COUNT(*) FROM gempa WHERE longitude BETWEEN {x0} AND {x1} "
               f"AND latitude BETWEEN {y0} AND {y1}"),
        "Q7": ("SELECT COUNT(*) FROM gempa WHERE magnitude >= 5 "
               "AND ts >= TIMESTAMP'2018-01-01 00:00:00' AND ts < TIMESTAMP'2019-01-01 00:00:00' AND "
               + haversine(PALU[0], PALU[1], "longitude", "latitude") + " <= 200000"),
        "Q8": ("SELECT g.eventID, COUNT(s.longitude) AS n FROM gempa g "
               "LEFT JOIN sensor s ON "
               + haversine("g.longitude", "g.latitude", "s.longitude", "s.latitude") + " <= 50000 "
               "WHERE g.magnitude >= 6 GROUP BY g.eventID"),
        "Q9": "SELECT COUNT(*) FROM gempa WHERE doubleCouple > 0.8",
    }
    desk = {"Q1": "magnitude >= 6", "Q2": "gempa tahun 2019 (UTC)",
            "Q3": "M>=5, depth<70, 2020-2023", "Q4": "jumlah & rata-rata magnitude per tahun",
            "Q5": "radius 100 km dari Surabaya", "Q6": "bounding box Sulawesi",
            "Q7": "M>=5, 200 km dari Palu, tahun 2018",
            "Q8": "stasiun dalam 50 km dari tiap gempa M>=6",
            "Q9": "doubleCouple > 0.8 (focal mechanism)"}

    def ringkas(kode, rows):
        if kode == "Q4":
            return f"{len(rows)} tahun; total={sum(r['n'] for r in rows)}"
        if kode == "Q8":
            return f"{len(rows)} gempa; total_baris_sensor={sum(r['n'] for r in rows)}"
        return f"{rows[0][0]} baris"

    rows_out = []
    print(f"\nPemanasan {a.pemanasan}x (dibuang), ukur {a.ukur}x per kueri\n")
    for kode, q in sql.items():
        for _ in range(a.pemanasan):
            spark.sql(q).collect()
        waktu = []
        for _ in range(a.ukur):
            t0 = time.perf_counter()
            hasil = spark.sql(q).collect()
            waktu.append((time.perf_counter() - t0) * 1000)
        ring = ringkas(kode, hasil)
        cocok = "SAMA dgn PostGIS" if ring == EXPECTED[kode] else f"BEDA (PostGIS: {EXPECTED[kode]})"
        med = statistics.median(waktu)
        print(f"{kode} | {desk[kode]:<42} | median {med:9.2f} ms | min {min(waktu):9.2f} | "
              f"maks {max(waktu):9.2f} | indeks: tidak | {ring} | {cocok}")
        row = {"kueri": kode, "deskripsi": desk[kode], "hasil_ringkas": ring,
               "cocok_postgis": ring == EXPECTED[kode], "median_ms": round(med, 3),
               "min_ms": round(min(waktu), 3), "maks_ms": round(max(waktu), 3), "pakai_indeks": False}
        row.update({f"ukur_{i+1}_ms": round(v, 3) for i, v in enumerate(waktu)})
        rows_out.append(row)

    pd.DataFrame(rows_out).to_csv(HASIL / "benchmark_spark.csv", index=False)
    print("\nTersimpan: hasil/benchmark_spark.csv, hasil/konfigurasi_spark.txt")
    spark.stop()


if __name__ == "__main__":
    main()