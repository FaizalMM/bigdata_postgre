"""
Tahap 3a (v2): memuat data mentah (Parquet) ke PostgreSQL/PostGIS dengan
pengukuran waktu per tahap yang setara dengan MongoDB.

Tahap yang diukur:
  A. baca Parquet            D. kirim ke database (COPY, geom dihitung server)
  B. konversi tipe waktu     E. bangun indeks
  C. siapkan bentuk kiriman  F. perapian (ANALYZE + VACUUM FULL)
     (tulis CSV di memori)

Prinsip "apa adanya": tidak ada baris dibuang, tidak ada nilai diubah/diisi.
Pemakaian:  python notebooks/02_muat_postgis_v2.py [--ulang 3]   (default 3 putaran)
Password dibaca dari variabel lingkungan PGPASSWORD.
"""
import argparse
import io
import os
import statistics
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql

DATA = Path("data")
HASIL = Path("hasil")
HASIL.mkdir(exist_ok=True)

if not os.environ.get("PGPASSWORD"):
    sys.exit('PGPASSWORD belum diatur. Jalankan dulu:  $env:PGPASSWORD = "160105"')

DB = dict(host=os.environ.get("PGHOST", "localhost"), port=5432,
          dbname="gempa", user="postgres", password=os.environ["PGPASSWORD"])
TS_COLS = {"gempa": ["datetime", "datetimeFM"], "sensor": ["datetime"]}
GEOM_EXPR = "ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)"


def pgtype(col, dtype, ts_cols):
    if col in ts_cols:
        return "TIMESTAMPTZ"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    return "TEXT"


def buat_tabel(cur, nama, df, dengan_id, gen=True):
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(nama)))
    kolom = [sql.SQL("{} {}").format(sql.Identifier(c),
                                     sql.SQL(pgtype(c, df[c].dtype, TS_COLS[nama])))
             for c in df.columns]
    if dengan_id:
        kolom.insert(0, sql.SQL("id BIGSERIAL PRIMARY KEY"))
    if gen:   # geom dihitung server saat baris dimasukkan (tanpa UPDATE, jadi tanpa baris mati)
        kolom.append(sql.SQL("geom geometry(Point, 4326) GENERATED ALWAYS AS ({}) STORED")
                     .format(sql.SQL(GEOM_EXPR)))
    cur.execute(sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(nama), sql.SQL(", ").join(kolom)))


def satu_putaran(cur, tampil=True):
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

    # C. siapkan bentuk kiriman: tulis CSV di memori
    t0 = time.perf_counter()
    buf = {}
    for n, d in df.items():
        b = io.StringIO()
        d.to_csv(b, index=False, header=False)
        b.seek(0)
        buf[n] = b
    catat("C. siapkan CSV (Python)", t0)

    # D. kirim ke database (buat tabel + COPY; geom dihitung server)
    t0 = time.perf_counter()
    gen = True
    try:
        buat_tabel(cur, "gempa", df["gempa"], dengan_id=False, gen=True)
    except psycopg2.Error as e:
        print("    (kolom geom otomatis ditolak, pakai cadangan UPDATE):", str(e).strip().splitlines()[0])
        gen = False
        buat_tabel(cur, "gempa", df["gempa"], dengan_id=False, gen=False)
    buat_tabel(cur, "sensor", df["sensor"], dengan_id=True, gen=gen)
    for n, d in df.items():
        cols = sql.SQL(", ").join(sql.Identifier(c) for c in d.columns)
        cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')")
                        .format(sql.Identifier(n), cols).as_string(cur), buf[n])
    if not gen:
        for n in ("gempa", "sensor"):
            cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN geom geometry(Point, 4326)").format(sql.Identifier(n)))
            cur.execute(sql.SQL("UPDATE {} SET geom = {}").format(sql.Identifier(n), sql.SQL(GEOM_EXPR)))
    w["metode_geom_(1=otomatis,0=UPDATE)"] = 1.0 if gen else 0.0
    catat("D. COPY ke database", t0)

    # E. indeks
    t0 = time.perf_counter()
    cur.execute('ALTER TABLE gempa ADD PRIMARY KEY ("eventID")')
    cur.execute("CREATE INDEX gempa_geom_gist ON gempa USING GIST (geom)")
    cur.execute('CREATE INDEX gempa_waktu ON gempa ("datetime")')
    cur.execute("CREATE INDEX gempa_mag ON gempa (magnitude)")
    cur.execute("CREATE INDEX gempa_depth ON gempa (depth)")
    cur.execute("CREATE INDEX sensor_geom_gist ON sensor USING GIST (geom)")
    # indeks 'geography' agar kueri radius (meter) memakai indeks spasial
    cur.execute("CREATE INDEX gempa_geog_gist ON gempa USING GIST ((geom::geography))")
    cur.execute("CREATE INDEX sensor_geog_gist ON sensor USING GIST ((geom::geography))")
    catat("E. bangun indeks", t0)

    # F. perapian
    t0 = time.perf_counter()
    cur.execute("VACUUM FULL gempa")
    cur.execute("VACUUM FULL sensor")
    cur.execute("ANALYZE gempa")
    cur.execute("ANALYZE sensor")
    catat("F. VACUUM FULL + ANALYZE", t0)

    metode = w.pop("metode_geom_(1=otomatis,0=UPDATE)")
    w["TOTAL A-F"] = sum(w.values())
    w["Murni database D-F"] = w["D. COPY ke database"] + w["E. bangun indeks"] + w["F. VACUUM FULL + ANALYZE"]
    w["metode_geom_(1=otomatis,0=UPDATE)"] = metode
    return w, df


def verifikasi(cur, df):
    print("\nVerifikasi")
    ok = True
    g = df["gempa"]
    for n in ("gempa", "sensor"):
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(n)))
        jml = cur.fetchone()[0]
        c = jml == len(df[n]); ok &= c
        print(f"  baris {n}: db={jml} sumber={len(df[n])} -> {'OK' if c else 'BEDA'}")
    cur.execute('SELECT MAX(depth), COUNT(*) FILTER (WHERE magnitude IS NULL), '
                'MIN("datetime"), MAX("datetime"), COUNT(*) FILTER (WHERE geom IS NULL) FROM gempa')
    dmax, mnull, tmin, tmax, gnull = cur.fetchone()
    cek = {
        f"depth max: {dmax}": dmax == g["depth"].max(),
        f"magnitude NULL: {mnull}": mnull == int(g["magnitude"].isna().sum()),
        f"rentang waktu: {tmin} s.d. {tmax}": (pd.Timestamp(tmin) == g["datetime"].min()
                                                and pd.Timestamp(tmax) == g["datetime"].max()),
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

    conn = psycopg2.connect(**DB)
    conn.autocommit = True          # VACUUM tidak boleh di dalam transaksi
    cur = conn.cursor()

    semua = []
    for i in range(1, args.ulang + 1):
        print(f"\n=== Putaran {i}/{args.ulang} ===")
        w, df = satu_putaran(cur)
        semua.append(w)

    ok = verifikasi(cur, df)

    print("\nUkuran penyimpanan (setelah VACUUM FULL)")
    ukuran = {}
    for n in ("gempa", "sensor"):
        cur.execute("SELECT pg_relation_size(%s), pg_indexes_size(%s), pg_total_relation_size(%s)", (n, n, n))
        t, ix, tot = cur.fetchone()
        ukuran[n] = (t, ix, tot)
        print(f"  {n}: tabel={t/1e6:.1f} MB | indeks={ix/1e6:.1f} MB | total={tot/1e6:.1f} MB")

    # tabel hasil: tiap putaran + median
    keys = list(semua[0].keys())
    rows = []
    for k in keys:
        nilai = [round(w[k], 3) for w in semua]
        rows.append({"tahap": k, **{f"putaran_{j+1}": v for j, v in enumerate(nilai)},
                     "median": round(statistics.median(nilai), 3)})
    for n, (t, ix, tot) in ukuran.items():
        rows.append({"tahap": f"ukuran_{n}_tabel_MB", "median": round(t / 1e6, 2)})
        rows.append({"tahap": f"ukuran_{n}_indeks_MB", "median": round(ix / 1e6, 2)})
        rows.append({"tahap": f"ukuran_{n}_total_MB", "median": round(tot / 1e6, 2)})
    pd.DataFrame(rows).to_csv(HASIL / "waktu_muat_postgis_v2.csv", index=False)

    print("\nMedian waktu (detik):")
    for k in keys:
        print(f"  {k}: {statistics.median([w[k] for w in semua]):.3f}")
    print("\nHasil verifikasi:", "SEMUA OK" if ok else "ADA YANG BEDA, jangan lanjut dulu")
    print("Tersimpan di hasil/waktu_muat_postgis_v2.csv")
    conn.close()


if __name__ == "__main__":
    main()