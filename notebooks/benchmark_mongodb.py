"""
Benchmark kueri Q1-Q9 pada MongoDB (definisi sama dengan benchmark_postgis.py).

Aturan ukur: 1 kali pemanasan (dibuang), lalu 5 kali ukur; dilaporkan median, min, max (ms).
Jalankan SETELAH muat_mongodb.py.
Pemakaian:  python notebooks/benchmark_mongodb.py [--ukur 5] [--pemanasan 1]
"""
import argparse
import json
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

HASIL = Path("hasil")
HASIL.mkdir(exist_ok=True)
URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")

# ------------------------- PARAMETER (sama dengan PostGIS) -------------------------
SURABAYA = (112.7521, -7.2575)            # (bujur, lintang)
PALU = (119.8707, -0.8917)
SULAWESI_BOX = (118.0, -6.0, 125.5, 2.0)  # bujur_min, lintang_min, bujur_maks, lintang_maks
R_Q5, R_Q7, R_Q8 = 100_000, 200_000, 50_000   # meter
R_BUMI = 6_371_008.8                      # jari-jari bola (m), dipakai PostGIS pada mode bola


def utc(th, bl=1, hr=1):
    return datetime(th, bl, hr, tzinfo=timezone.utc)


def dekat(lonlat, meter):
    return {"geom": {"$geoWithin": {"$centerSphere": [list(lonlat), meter / R_BUMI]}}}


x0, y0, x1, y1 = SULAWESI_BOX
F = {
    "Q1": {"magnitude": {"$gte": 6}},
    "Q2": {"datetime": {"$gte": utc(2019), "$lt": utc(2020)}},
    "Q3": {"magnitude": {"$gte": 5}, "depth": {"$lt": 70},
           "datetime": {"$gte": utc(2020), "$lt": utc(2024)}},
    "Q5": dekat(SURABAYA, R_Q5),
    "Q6": {"geom": {"$geoWithin": {"$geometry": {"type": "Polygon", "coordinates": [
        [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]}}}},
    "Q7": {"magnitude": {"$gte": 5}, "datetime": {"$gte": utc(2018), "$lt": utc(2019)},
           **dekat(PALU, R_Q7)},
    "Q9": {"fm.doubleCouple": {"$gt": 0.8}},
}

# Hasil yang benar menurut PostGIS (cek silang kebenaran)
EXPECTED = {"Q1": "1775 baris", "Q2": "13635 baris", "Q3": "3744 baris",
            "Q4": "19 tahun; total=176384", "Q5": "151 baris", "Q6": "30556 baris",
            "Q7": "17 baris", "Q8": "1775 gempa; total_baris_sensor=375", "Q9": "3669 baris"}


def hitung(db, kode):
    return lambda: db.gempa.count_documents(F[kode])


def q4(db):
    return list(db.gempa.aggregate([
        {"$group": {"_id": {"$year": "$datetime"}, "n": {"$sum": 1}, "rata2": {"$avg": "$magnitude"}}},
        {"$sort": {"_id": 1}}]))


def q8(db):
    # join spasial dilakukan di sisi aplikasi: 1 kueri sensor per gempa M>=6
    hasil = []
    for e in db.gempa.find({"magnitude": {"$gte": 6}}, {"eventID": 1, "geom": 1}):
        lon, lat = e["geom"]["coordinates"]
        n = db.sensor.count_documents(
            {"geom": {"$geoWithin": {"$centerSphere": [[lon, lat], R_Q8 / R_BUMI]}}})
        hasil.append((e["eventID"], n))
    return hasil


def pakai_indeks(db, koleksi, filt):
    if filt is None:
        return False
    plan = db[koleksi].find(filt).explain()
    return "IXSCAN" in json.dumps(plan, default=str)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ukur", type=int, default=5)
    ap.add_argument("--pemanasan", type=int, default=1)
    a = ap.parse_args()

    client = MongoClient(URI, tz_aware=True, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client["gempa"]
    n_g, n_s = db.gempa.count_documents({}), db.sensor.count_documents({})
    if n_g == 0:
        raise SystemExit("Koleksi gempa kosong. Jalankan dulu muat_mongodb.py")
    print(f"Data: gempa={n_g}, sensor={n_s}")

    # catat konfigurasi (bahan laporan)
    info = []
    try:
        info.append("MongoDB " + client.server_info()["version"])
        ss = db.command("serverStatus")
        info.append("storageEngine = " + ss["storageEngine"]["name"])
        info.append("wiredTiger cache max = %.0f MB" %
                    (ss["wiredTiger"]["cache"]["maximum bytes configured"] / 1e6))
    except Exception as e:
        info.append(f"(sebagian konfigurasi tidak terbaca: {e})")
    (HASIL / "konfigurasi_mongodb.txt").write_text("\n".join(info), encoding="utf-8")
    print("\n".join("  " + x for x in info))

    # (kode, deskripsi, fungsi, ringkasan, koleksi+filter untuk cek indeks)
    cnt = lambda r: f"{r} baris"
    QUERIES = [
        ("Q1", "magnitude >= 6", hitung(db, "Q1"), cnt, ("gempa", F["Q1"])),
        ("Q2", "gempa tahun 2019 (UTC)", hitung(db, "Q2"), cnt, ("gempa", F["Q2"])),
        ("Q3", "M>=5, depth<70, 2020-2023", hitung(db, "Q3"), cnt, ("gempa", F["Q3"])),
        ("Q4", "jumlah & rata-rata magnitude per tahun", lambda: q4(db),
         lambda r: f"{len(r)} tahun; total={sum(x['n'] for x in r)}", None),
        ("Q5", "radius 100 km dari Surabaya", hitung(db, "Q5"), cnt, ("gempa", F["Q5"])),
        ("Q6", "bounding box Sulawesi", hitung(db, "Q6"), cnt, ("gempa", F["Q6"])),
        ("Q7", "M>=5, 200 km dari Palu, tahun 2018", hitung(db, "Q7"), cnt, ("gempa", F["Q7"])),
        ("Q8", "stasiun dalam 50 km dari tiap gempa M>=6", lambda: q8(db),
         lambda r: f"{len(r)} gempa; total_baris_sensor={sum(x[1] for x in r)}",
         ("sensor", dekat(SURABAYA, R_Q8))),
        ("Q9", "doubleCouple > 0.8 (focal mechanism)", hitung(db, "Q9"), cnt, ("gempa", F["Q9"])),
    ]

    rows = []
    print(f"\nPemanasan {a.pemanasan}x (dibuang), ukur {a.ukur}x per kueri\n")
    for kode, desk, fn, ringkas, ix in QUERIES:
        idx = pakai_indeks(db, *ix) if ix else False
        for _ in range(a.pemanasan):
            fn()
        waktu = []
        for _ in range(a.ukur):
            t0 = time.perf_counter()
            hasil = fn()
            waktu.append((time.perf_counter() - t0) * 1000)
        ring = ringkas(hasil)
        cocok = "SAMA dgn PostGIS" if ring == EXPECTED[kode] else f"BEDA (PostGIS: {EXPECTED[kode]})"
        med = statistics.median(waktu)
        print(f"{kode} | {desk:<42} | median {med:9.2f} ms | min {min(waktu):9.2f} | "
              f"maks {max(waktu):9.2f} | indeks: {'ya' if idx else 'tidak'} | {ring} | {cocok}")
        row = {"kueri": kode, "deskripsi": desk, "hasil_ringkas": ring, "cocok_postgis": ring == EXPECTED[kode],
               "median_ms": round(med, 3), "min_ms": round(min(waktu), 3),
               "maks_ms": round(max(waktu), 3), "pakai_indeks": idx}
        row.update({f"ukur_{i+1}_ms": round(v, 3) for i, v in enumerate(waktu)})
        rows.append(row)

    pd.DataFrame(rows).to_csv(HASIL / "benchmark_mongodb.csv", index=False)
    print("\nTersimpan: hasil/benchmark_mongodb.csv, hasil/konfigurasi_mongodb.txt")
    print("Kueri bertanda BEDA: laporkan apa adanya sebagai temuan (lihat catatan di balasan).")
    client.close()


if __name__ == "__main__":
    main()