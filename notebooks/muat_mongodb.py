"""
Memuat data mentah (Parquet) ke MongoDB dengan pengukuran waktu per tahap
yang setara dengan PostGIS (muat_postgis_v2.py).

Tahap yang diukur (nama tahap sejajar dengan PostGIS):
  A. baca Parquet           D. insert_many ke database (bulk)
  B. konversi tipe waktu    E. bangun indeks
  C. susun dokumen (Python) F. perapian (compact)

Prinsip "apa adanya": tidak ada baris dibuang, tidak ada nilai diubah/diisi.
Nilai kosong (NaN) tidak disimpan sebagai field (ciri khas dokumen).
Kolom focal mechanism (mulai dari datetimeFM) dikelompokkan di sub-dokumen "fm"
hanya bila barisnya punya nilai.

Pemakaian:  python notebooks/muat_mongodb.py [--ulang 3]
Alamat MongoDB dibaca dari MONGO_URI (default mongodb://localhost:27017).
"""
import argparse
import os
import statistics
import time
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

DATA = Path("data")
HASIL = Path("hasil")
HASIL.mkdir(exist_ok=True)

URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
NAMA_DB = "gempa"
TS_COLS = {"gempa": ["datetime", "datetimeFM"], "sensor": ["datetime"]}
BATCH = 10_000


def susun_dokumen(d, kolom_fm):
    """Ubah DataFrame menjadi daftar dokumen (geom GeoJSON [bujur, lintang])."""
    fm_set = set(kolom_fm)
    docs = []
    for r in d.to_dict("records"):
        doc, fm = {}, {}
        for k, v in r.items():
            if pd.isna(v):
                continue                      # nilai kosong tidak diisi
            if isinstance(v, pd.Timestamp):
                v = v.to_pydatetime(warn=False)
            (fm if k in fm_set else doc)[k] = v
        doc["geom"] = {"type": "Point",
                       "coordinates": [doc["longitude"], doc["latitude"]]}
        if fm:
            doc["fm"] = fm
        docs.append(doc)
    return docs


def satu_putaran(db, tampil=True):
    w = {}

    def catat(k, t0):
        w[k] = time.perf_counter() - t0
        if tampil:
            print(f"    {k}: {w[k]:.3f} s")

    # A. baca Parquet
    t0 = time.perf_counter()
    df = {n: pd.read_parquet(DATA / f"{n}.parquet") for n in ("gempa", "sensor")}
    catat("A. baca parquet", t0)

    # B. konversi TIPE waktu (nilai tetap sama; dicek jumlahnya)
    t0 = time.perf_counter()
    for n, d in df.items():
        for c in TS_COLS[n]:
            sebelum = int(d[c].notna().sum())
            d[c] = pd.to_datetime(d[c], utc=True, format="ISO8601", errors="coerce")
            assert sebelum == int(d[c].notna().sum()), f"{n}.{c}: ada waktu gagal terbaca"
    catat("B. konversi tipe waktu", t0)

    # C. susun dokumen
    t0 = time.perf_counter()
    kolom = list(df["gempa"].columns)
    kolom_fm = kolom[kolom.index("datetimeFM"):]
    docs = {"gempa": susun_dokumen(df["gempa"], kolom_fm),
            "sensor": susun_dokumen(df["sensor"], [])}
    catat("C. susun dokumen (Python)", t0)

    # D. kirim ke database (hapus lama + insert_many per batch)
    t0 = time.perf_counter()
    for n in ("gempa", "sensor"):
        db.drop_collection(n)
    for n, lst in docs.items():
        for i in range(0, len(lst), BATCH):
            db[n].insert_many(lst[i:i + BATCH], ordered=False)
    catat("D. insert_many ke database", t0)

    # E. indeks
    t0 = time.perf_counter()
    db.gempa.create_index([("eventID", 1)], unique=True)
    db.gempa.create_index([("geom", "2dsphere")])
    db.gempa.create_index([("datetime", 1)])
    db.gempa.create_index([("magnitude", 1)])
    db.gempa.create_index([("depth", 1)])
    db.sensor.create_index([("geom", "2dsphere")])
    catat("E. bangun indeks", t0)

    # F. perapian (padanan VACUUM FULL = compact; bila ditolak dicatat)
    t0 = time.perf_counter()
    for n in ("gempa", "sensor"):
        try:
            db.command("compact", n)
        except Exception as e:
            print(f"    (compact {n} ditolak/tidak didukung: {str(e)[:80]})")
    catat("F. compact", t0)

    w["TOTAL A-F"] = sum(w.values())
    w["Murni database D-F"] = (w["D. insert_many ke database"] + w["E. bangun indeks"]
                               + w["F. compact"])
    return w, df


def verifikasi(db, df):
    print("\nVerifikasi")
    ok = True
    g = df["gempa"]
    for n in ("gempa", "sensor"):
        jml = db[n].count_documents({})
        c = jml == len(df[n]); ok &= c
        print(f"  dokumen {n}: db={jml} sumber={len(df[n])} -> {'OK' if c else 'BEDA'}")

    dmax = next(db.gempa.aggregate([{"$group": {"_id": None, "m": {"$max": "$depth"}}}]))["m"]
    mnull = db.gempa.count_documents({"magnitude": None})   # None juga cocok untuk field yang tidak ada
    tmin = db.gempa.find_one(sort=[("datetime", 1)])["datetime"]
    tmax = db.gempa.find_one(sort=[("datetime", -1)])["datetime"]
    gnull = db.gempa.count_documents({"geom": {"$exists": False}})
    # MongoDB menyimpan waktu dengan ketelitian milidetik -> sumber dibandingkan pada milidetik
    smin = g["datetime"].min().floor("ms")
    smax = g["datetime"].max().floor("ms")
    cek = {
        f"depth max: {dmax}": dmax == g["depth"].max(),
        f"magnitude kosong: {mnull}": mnull == int(g["magnitude"].isna().sum()),
        f"rentang waktu: {tmin} s.d. {tmax}": (pd.Timestamp(tmin) == smin and pd.Timestamp(tmax) == smax),
        f"geom kosong: {gnull}": gnull == 0,
    }
    for k, v in cek.items():
        ok &= bool(v)
        print(f"  {k} -> {'OK' if v else 'BEDA'}")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ulang", type=int, default=3, help="jumlah pengulangan (default 3)")
    args = ap.parse_args()

    client = MongoClient(URI, tz_aware=True, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[NAMA_DB]

    semua = []
    for i in range(1, args.ulang + 1):
        print(f"\n=== Putaran {i}/{args.ulang} ===")
        w, df = satu_putaran(db)
        semua.append(w)

    ok = verifikasi(db, df)

    print("\nUkuran penyimpanan (setelah compact)")
    ukuran = {}
    for n in ("gempa", "sensor"):
        s = db.command("collStats", n)
        data, simpan, ix = s["size"], s["storageSize"], s["totalIndexSize"]
        ukuran[n] = (data, simpan, ix, simpan + ix)
        print(f"  {n}: data={data/1e6:.1f} MB | storage={simpan/1e6:.1f} MB | "
              f"indeks={ix/1e6:.1f} MB | total={(simpan+ix)/1e6:.1f} MB")

    keys = list(semua[0].keys())
    rows = []
    for k in keys:
        nilai = [round(w[k], 3) for w in semua]
        rows.append({"tahap": k, **{f"putaran_{j+1}": v for j, v in enumerate(nilai)},
                     "median": round(statistics.median(nilai), 3)})
    for n, (data, simpan, ix, tot) in ukuran.items():
        rows.append({"tahap": f"ukuran_{n}_data_MB", "median": round(data / 1e6, 2)})
        rows.append({"tahap": f"ukuran_{n}_storage_MB", "median": round(simpan / 1e6, 2)})
        rows.append({"tahap": f"ukuran_{n}_indeks_MB", "median": round(ix / 1e6, 2)})
        rows.append({"tahap": f"ukuran_{n}_total_MB", "median": round(tot / 1e6, 2)})
    pd.DataFrame(rows).to_csv(HASIL / "waktu_muat_mongodb.csv", index=False)

    print("\nMedian waktu (detik):")
    for k in keys:
        print(f"  {k}: {statistics.median([w[k] for w in semua]):.3f}")
    print("\nHasil verifikasi:", "SEMUA OK" if ok else "ADA YANG BEDA, jangan lanjut dulu")
    print("Tersimpan di hasil/waktu_muat_mongodb.csv")
    client.close()


if __name__ == "__main__":
    main()