"""
Tahap 3a: memuat data mentah (Parquet) ke PostgreSQL/PostGIS.

Prinsip "apa adanya":
  - tidak ada baris dibuang, tidak ada nilai diubah/diisi
  - nama kolom dipertahankan persis seperti di Parquet (pakai tanda kutip)
  - teks waktu disimpan sebagai TIMESTAMPTZ (momen yang sama, tipe yang benar)
  - kolom geom ditambahkan dari longitude/latitude (kolom asli tetap ada)
Password dibaca dari variabel lingkungan PGPASSWORD.
"""
import io
import os
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
    sys.exit("PGPASSWORD belum diatur. Jalankan dulu:  $env:PGPASSWORD = \"passwordmu\"")

DB = dict(host=os.environ.get("PGHOST", "localhost"), port=5432,
          dbname="gempa", user="postgres", password=os.environ["PGPASSWORD"])

TS_COLS = {"gempa": ["datetime", "datetimeFM"], "sensor": ["datetime"]}
waktu = {}          # catatan waktu tiap tahap (detik)


def catat(nama, t0):
    waktu[nama] = round(time.perf_counter() - t0, 3)
    print(f"  {nama}: {waktu[nama]} s")


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


def siapkan(nama):
    """Baca Parquet, ubah TIPE waktu saja, verifikasi tidak ada nilai hilang."""
    t0 = time.perf_counter()
    df = pd.read_parquet(DATA / f"{nama}.parquet")
    catat(f"{nama}: baca parquet", t0)

    t0 = time.perf_counter()
    for c in TS_COLS[nama]:
        sebelum = int(df[c].notna().sum())
        df[c] = pd.to_datetime(df[c], utc=True, format="ISO8601", errors="coerce")
        sesudah = int(df[c].notna().sum())
        assert sebelum == sesudah, f"{nama}.{c}: {sebelum - sesudah} waktu gagal terbaca"
    catat(f"{nama}: konversi tipe waktu", t0)
    return df


def buat_tabel(cur, nama, df, dengan_id):
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(nama)))
    kolom = [sql.SQL("{} {}").format(sql.Identifier(c),
                                     sql.SQL(pgtype(c, df[c].dtype, TS_COLS[nama])))
             for c in df.columns]
    if dengan_id:
        kolom.insert(0, sql.SQL("id BIGSERIAL PRIMARY KEY"))
    cur.execute(sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(nama), sql.SQL(", ").join(kolom)))


def muat(cur, nama, df):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)      # NaN/NaT -> kosong -> NULL
    buf.seek(0)
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '')")
                    .format(sql.Identifier(nama), cols).as_string(cur), buf)


def tambah_geom(cur, nama):
    cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN geom geometry(Point, 4326)")
                .format(sql.Identifier(nama)))
    cur.execute(sql.SQL("UPDATE {} SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")
                .format(sql.Identifier(nama)))


def main():
    gempa = siapkan("gempa")
    sensor = siapkan("sensor")

    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    cur = conn.cursor()

    print("Membuat tabel dan memuat data")
    t0 = time.perf_counter()
    buat_tabel(cur, "gempa", gempa, dengan_id=False)
    buat_tabel(cur, "sensor", sensor, dengan_id=True)
    catat("create table", t0)

    t0 = time.perf_counter()
    muat(cur, "gempa", gempa)
    muat(cur, "sensor", sensor)
    catat("COPY (muat data)", t0)

    t0 = time.perf_counter()
    tambah_geom(cur, "gempa")
    tambah_geom(cur, "sensor")
    catat("bentuk kolom geom", t0)

    t0 = time.perf_counter()
    cur.execute('ALTER TABLE gempa ADD PRIMARY KEY ("eventID")')
    cur.execute("CREATE INDEX gempa_geom_gist ON gempa USING GIST (geom)")
    cur.execute('CREATE INDEX gempa_waktu ON gempa ("datetime")')
    cur.execute("CREATE INDEX gempa_mag ON gempa (magnitude)")
    cur.execute("CREATE INDEX gempa_depth ON gempa (depth)")
    cur.execute("CREATE INDEX sensor_geom_gist ON sensor USING GIST (geom)")
    catat("bangun indeks", t0)

    t0 = time.perf_counter()
    cur.execute("ANALYZE gempa")
    cur.execute("ANALYZE sensor")
    catat("ANALYZE", t0)

    # ---------------- verifikasi ----------------
    print("\nVerifikasi")
    ok = True
    for nama, df in (("gempa", gempa), ("sensor", sensor)):
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(nama)))
        n = cur.fetchone()[0]
        cocok = (n == len(df)); ok &= cocok
        print(f"  baris {nama}: db={n} sumber={len(df)} -> {'OK' if cocok else 'BEDA'}")

    cur.execute('SELECT MAX(depth), COUNT(*) FILTER (WHERE magnitude IS NULL), '
                'MIN("datetime"), MAX("datetime") FROM gempa')
    dmax, mnull, tmin, tmax = cur.fetchone()
    c1 = (dmax == gempa["depth"].max()); c2 = (mnull == int(gempa["magnitude"].isna().sum()))
    c3 = (pd.Timestamp(tmin) == gempa["datetime"].min()) and (pd.Timestamp(tmax) == gempa["datetime"].max())
    ok &= c1 and c2 and c3
    print(f"  depth max: {dmax} -> {'OK' if c1 else 'BEDA'}")
    print(f"  magnitude NULL: {mnull} -> {'OK' if c2 else 'BEDA'}")
    print(f"  rentang waktu: {tmin} s.d. {tmax} -> {'OK' if c3 else 'BEDA'}")
    cur.execute("SELECT COUNT(*) FROM gempa WHERE geom IS NULL")
    c4 = cur.fetchone()[0] == 0; ok &= c4
    print(f"  geom kosong: {0 if c4 else 'ADA'} -> {'OK' if c4 else 'BEDA'}")

    # ---------------- ukuran penyimpanan ----------------
    ukuran = {}
    for nama in ("gempa", "sensor"):
        cur.execute("SELECT pg_total_relation_size(%s), pg_relation_size(%s)", (nama, nama))
        total, tabel = cur.fetchone()
        ukuran[nama] = (total, tabel)
        print(f"  ukuran {nama}: total={total/1e6:.1f} MB (tabel saja {tabel/1e6:.1f} MB)")

    waktu["TOTAL"] = round(sum(v for k, v in waktu.items()), 3)
    rows = [{"tahap": k, "detik": v} for k, v in waktu.items()]
    rows += [{"tahap": f"ukuran_total_{n}_MB", "detik": round(t / 1e6, 2)} for n, (t, _) in ukuran.items()]
    pd.DataFrame(rows).to_csv(HASIL / "waktu_muat_postgis.csv", index=False)
    print("\nHasil verifikasi:", "SEMUA OK" if ok else "ADA YANG BEDA, jangan lanjut dulu")
    print("Waktu tersimpan di hasil/waktu_muat_postgis.csv")
    conn.close()


if __name__ == "__main__":
    main()