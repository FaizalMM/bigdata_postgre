# Perbandingan PostgreSQL/PostGIS, MongoDB, dan Spark (Parquet) pada Data Gempa Indonesia

Eksperimen mata kuliah Big Data. Tiga cara menyimpan dan menanyai **data gempa yang sama** dibandingkan dari tiga sisi: **waktu muat data**, **ukuran penyimpanan**, dan **kecepatan sembilan kueri** (atribut, waktu, dan spasial).

## Daftar isi

1. [Ringkasan hasil](#1-ringkasan-hasil)
2. [Latar belakang dan tujuan](#2-latar-belakang-dan-tujuan)
3. [Data yang digunakan](#3-data-yang-digunakan)
4. [Arsitektur testbed dan konfigurasi](#4-arsitektur-testbed-dan-konfigurasi)
5. [Variabel dan metodologi](#5-variabel-dan-metodologi)
6. [Tahapan implementasi](#6-tahapan-implementasi)
7. [Definisi sembilan kueri](#7-definisi-sembilan-kueri)
8. [Hasil: waktu muat data](#8-hasil-waktu-muat-data)
9. [Hasil: ukuran penyimpanan](#9-hasil-ukuran-penyimpanan)
10. [Hasil: waktu kueri](#10-hasil-waktu-kueri)
11. [Analisis per kueri dan rencana eksekusi](#11-analisis-per-kueri-dan-rencana-eksekusi)
12. [Validitas hasil](#12-validitas-hasil)
13. [Temuan dan kesimpulan](#13-temuan-dan-kesimpulan)
14. [Batasan eksperimen](#14-batasan-eksperimen)
15. [Masalah yang ditemui dan solusinya](#15-masalah-yang-ditemui-dan-solusinya)
16. [Cara mereproduksi](#16-cara-mereproduksi)
17. [Struktur folder dan berkas hasil](#17-struktur-folder-dan-berkas-hasil)
18. [Anggota kelompok dan pembagian peran](#18-anggota-kelompok-dan-pembagian-peran)

---

## 1. Ringkasan hasil

| Aspek                      | PostGIS             | MongoDB                         | Spark (Parquet)                       |
| -------------------------- | ------------------- | ------------------------------- | ------------------------------------- |
| Waktu muat total (median)  | 24,39 detik         | **10,04 detik**                 | tidak ada (nyalakan Spark 6,41 detik) |
| Ukuran total tabel gempa   | 68,95 MB            | 33,95 MB                        | **13,1 MB** (file sumber)             |
| Kueri tercepat             | **8 dari 9**        | 0 dari 9                        | 1 dari 9 (Q9, selisih tipis)          |
| Memakai indeks             | ya (kecuali Q4, Q9) | ya (kecuali Q4, Q9)             | tidak                                 |
| Kebenaran hasil vs PostGIS | acuan               | 8 dari 9 sama (Q6 beda 9 baris) | 9 dari 9 sama                         |

Satu kalimat: **PostGIS unggul untuk kueri berindeks, MongoDB unggul untuk muat cepat dan penyimpanan hemat dibanding PostGIS, dan Spark paling praktis bila data tidak mau dimuat ke database, dengan biaya waktu dasar 60 sampai 200 ms per kueri.**

---

## 2. Latar belakang dan tujuan

Data gempa punya tiga ciri yang membuatnya menarik untuk perbandingan sistem penyimpanan: ada **atribut numerik** (magnitude, kedalaman), ada **dimensi waktu** (belasan tahun), dan ada **dimensi spasial** (koordinat titik). Tiap sistem punya pendekatan berbeda untuk ketiganya.

| Sistem               | Pendekatan                                                                |
| -------------------- | ------------------------------------------------------------------------- |
| PostgreSQL + PostGIS | Tabel relasional dengan tipe geometri dan indeks B-tree serta GiST        |
| MongoDB              | Dokumen JSON dengan indeks 2dsphere untuk titik GeoJSON                   |
| Spark + Parquet      | File kolumnar dibaca langsung oleh mesin pemroses paralel, tanpa database |

Pertanyaan yang dijawab:

1. Berapa lama dan berapa besar ruang yang dibutuhkan untuk memuat data ke tiap sistem?
2. Sistem mana yang paling cepat menjawab kueri atribut, waktu, dan spasial?
3. Apakah ketiganya menghasilkan jawaban yang sama?
4. Pada beban kerja seperti apa tiap sistem paling cocok?

Prinsip pemuatan: data dimuat **apa adanya**. Tidak ada baris dibuang, tidak ada nilai diubah atau diisi. Nilai kosong tetap kosong (di MongoDB, field kosong tidak disimpan).

---

## 3. Data yang digunakan

| Berkas                | Isi                                                        | Baris   | Kolom | Ukuran  |
| --------------------- | ---------------------------------------------------------- | ------- | ----- | ------- |
| `data/gempa.parquet`  | Kejadian gempa Indonesia, **2008-11-01 sampai 2026-07-14** | 176.384 | 37    | 13,1 MB |
| `data/sensor.parquet` | Stasiun sensor (kode, lokasi, elevasi)                     | 1.743   | 6     | 0,1 MB  |

### 3.1 Kolom tabel gempa

| Kelompok             | Kolom                                                                                                                                            | Keterangan                                                      |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| Identitas dan waktu  | `eventID`, `datetime`                                                                                                                            | `eventID` unik; `datetime` berupa teks ISO-8601 dengan zona UTC |
| Lokasi               | `latitude`, `longitude`, `location`, `agency`                                                                                                    | Koordinat selalu terisi                                         |
| Ukuran gempa         | `magnitude`, `mag_type`, `depth`, `phasecount`, `azimuth_gap`                                                                                    | `magnitude` kosong pada 1.405 baris                             |
| Focal mechanism (FM) | `datetimeFM` sampai `misfit` (24 kolom, antara lain `doubleCouple`, `clvd`, `scalarMoment`, `Mrr`…`Mtp`, `strikeNP1/2`, `dipNP1/2`, `rakeNP1/2`) | Hanya terisi pada **8.023 baris** (4,5 persen)                  |

### 3.2 Kolom tabel sensor

`code`, `latitude`, `longitude`, `elevation`, `location`, `datetime` (teks ISO-8601, tanpa zona).

### 3.3 Profil dan keanehan data

| Temuan                    | Nilai                                            | Dampak                                                                                                                               |
| ------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| Rentang magnitude         | 0,19 sampai 8,75 (rata-rata 3,75)                |                                                                                                                                      |
| Rentang kedalaman         | 0 sampai **90000**                               | Nilai 90000 jelas tidak realistis (bukan km). Tetap dimuat sesuai prinsip "apa adanya" dan tercatat di verifikasi                    |
| Magnitude kosong          | 1.405 baris                                      | Tetap kosong; diperiksa saat verifikasi                                                                                              |
| Kolom FM jarang terisi    | 8.023 dari 176.384 baris                         | Di MongoDB dikelompokkan di sub-dokumen `fm` hanya bila ada                                                                          |
| **Dua kolom kembar nama** | `AzGapFM` dan `azgapFM` (beda huruf besar-kecil) | Aman di PostgreSQL (nama dikutip) dan MongoDB, tapi **membuat Spark error** kecuali mode case-sensitive diaktifkan (lihat bagian 15) |
| `datetime` berupa teks    | Sumber menyimpan waktu sebagai string            | Diubah **tipenya** (bukan nilainya) menjadi timestamp UTC; jumlahnya diperiksa sama sebelum dan sesudah                              |

---

## 4. Arsitektur testbed dan konfigurasi

Semua sistem berjalan **di satu laptop yang sama**, dan hanya **satu sistem aktif per sesi pengukuran** supaya CPU dan RAM tidak terbagi.

```
                      ┌────────────────────────────────────────────┐
                      │          Laptop (Windows, satu mesin)       │
                      │                                            │
   data/gempa.parquet │   ┌──────────────┐   ┌──────────────────┐  │
   data/sensor.parquet│   │ Skrip Python │   │  PostgreSQL 16   │  │
          │           │   │  (venv, 3.13)│──▶│  + PostGIS 3.6   │  │
          ▼           │   │              │   │  port 5432 (C:)  │  │
   ┌────────────┐     │   │ pandas       │   └──────────────────┘  │
   │  Parquet   │─────┼──▶│ pyarrow      │   ┌──────────────────┐  │
   │ (sumber)   │     │   │ psycopg2     │──▶│  MongoDB 9.0.4   │  │
   └────────────┘     │   │ pymongo      │   │  port 27017 (D:) │  │
          │           │   │              │   └──────────────────┘  │
          │           │   │ pyspark      │   ┌──────────────────┐  │
          └───────────┼──▶│              │──▶│ Spark 4.3.0      │  │
                      │   └──────────────┘   │ local[*] 12 core │  │
                      │                      │ baca Parquet     │  │
                      │                      └──────────────────┘  │
                      └────────────────────────────────────────────┘
        Hanya SATU dari tiga kotak kanan yang aktif pada satu waktu.
```

### 4.1 Perangkat lunak dan konfigurasi

| Komponen   | Versi                                          | Konfigurasi                                                                                                                              |
| ---------- | ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL | 16.15 (Windows, build Visual C++ 1944, 64-bit) | `shared_buffers = 128MB`, `work_mem = 4MB`, `max_parallel_workers_per_gather = 2`, TimeZone `Asia/Bangkok` (kueri memakai UTC eksplisit) |
| PostGIS    | 3.6                                            | GEOS, PROJ, dan statistik aktif                                                                                                          |
| MongoDB    | 9.0.4                                          | Storage engine WiredTiger, cache maksimum 3602 MB, dijalankan manual dari berkas ZIP (data di `D:\mongodata`)                            |
| Spark      | 4.3.0                                          | Mode `local[*]` (12 core), driver memory 2g, shuffle partitions 8, timezone UTC, `caseSensitive = true`, UI dimatikan                    |
| Python     | 3.13.0                                         | Paket: pandas, pyarrow, psycopg2-binary, pymongo, pyspark                                                                                |
| Java       | 17 atau lebih baru                             | Dibutuhkan Spark                                                                                                                         |

### 4.2 Perangkat keras

| Komponen       | Nilai                  |
| -------------- | ---------------------- | --- |
| CPU            | Intel Core i5 (Gen 13) |
| RAM            | 8 GB                   |
| Disk           | SSD 512 GB             |
| Sistem operasi | Windows 11             |     |

### 4.3 Konfigurasi jaringan

Semua komponen berkomunikasi lewat **localhost** (satu mesin), jadi tidak ada latensi jaringan antar node. Ini berarti hasil mencerminkan kinerja mesin penyimpanan itu sendiri, bukan kinerja jaringan. Port: PostgreSQL 5432, MongoDB 27017. Spark berjalan di dalam proses Python (tanpa port klaster).

---

## 5. Variabel dan metodologi

### 5.1 Variabel

| Jenis       | Variabel                                                                                                                                                                  |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Bebas**   | Sistem penyimpanan (PostGIS, MongoDB, Spark/Parquet) dan jenis kueri (Q1 sampai Q9)                                                                                       |
| **Terikat** | Waktu muat per tahap (detik), ukuran penyimpanan (MB), waktu kueri (ms), kebenaran hasil (jumlah baris)                                                                   |
| **Kontrol** | Data yang sama, laptop yang sama, satu sistem aktif per sesi, konfigurasi bawaan (tanpa tuning), zona waktu kueri UTC, parameter kueri identik, hasil kueri diambil penuh |

### 5.2 Aturan pengukuran

| Pengukuran        | Aturan                                                                         |
| ----------------- | ------------------------------------------------------------------------------ |
| Muat data         | 3 putaran penuh (hapus lalu muat ulang), dilaporkan **median** per tahap       |
| Kueri             | 1 kali pemanasan (dibuang) lalu 5 kali ukur, dilaporkan **median, min, maks**  |
| Pengambilan hasil | Hasil diambil penuh (`fetchall`, `collect`), bukan hanya dieksekusi            |
| Kebenaran         | Jumlah hasil tiap kueri dibandingkan antar sistem dengan PostGIS sebagai acuan |
| Ukuran            | Diukur setelah perapian (`VACUUM FULL` di PostGIS, `compact` di MongoDB)       |

### 5.3 Tahap muat yang diukur (sejajar di PostGIS dan MongoDB)

| Tahap | PostGIS                   | MongoDB                      |
| ----- | ------------------------- | ---------------------------- |
| A     | Baca Parquet              | Baca Parquet                 |
| B     | Konversi tipe waktu       | Konversi tipe waktu          |
| C     | Tulis CSV di memori       | Susun dokumen di Python      |
| D     | `COPY` ke database        | `insert_many` (batch 10.000) |
| E     | Bangun indeks             | Bangun indeks                |
| F     | `VACUUM FULL` + `ANALYZE` | `compact`                    |

"Murni database" = tahap D + E + F (bagian yang dikerjakan mesin database, di luar persiapan Python).

---

## 6. Tahapan implementasi

### 6.1 Instalasi (Windows)

| Komponen        | Langkah ringkas                                                                                                                                                                                                                  |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Python dan venv | Pasang Python 3.10 ke atas, centang _Add to PATH_. Lalu `python -m venv venv`, `venv\Scripts\activate`, `pip install pandas pyarrow psycopg2-binary pymongo pyspark`                                                             |
| PostgreSQL 16   | Pasang dari installer EDB. Catat password, port 5432, locale default                                                                                                                                                             |
| PostGIS 3.6     | Lewat Stack Builder: _Spatial Extensions → PostGIS 3.6 Bundle for PostgreSQL 16_. Jangan centang _Create spatial database_                                                                                                       |
| Database        | Di SQL Shell: `CREATE DATABASE gempa;` lalu `\c gempa`, `CREATE EXTENSION postgis;`, `SELECT postgis_version();`                                                                                                                 |
| MongoDB         | Karena ruang drive C: tidak cukup untuk installer MSI, dipakai **versi ZIP** yang diekstrak ke D:. Jalankan: `mongod.exe --dbpath D:\mongodata` (jendela dibiarkan terbuka). Cek: `pymongo ... ping` mengembalikan `{'ok': 1.0}` |
| Java dan Spark  | Java 17 atau lebih baru ada di PATH. `pip install pyspark --no-cache-dir` dengan `TEMP` diarahkan ke D:                                                                                                                          |

### 6.2 Model data di PostGIS

Tipe kolom dipetakan otomatis dari Parquet (float menjadi `DOUBLE PRECISION`, integer menjadi `BIGINT`, waktu menjadi `TIMESTAMPTZ`, sisanya `TEXT`). Kolom geometri **dihitung server** saat baris masuk, sehingga tidak ada `UPDATE` dan tidak ada baris mati:

```sql
geom geometry(Point, 4326) GENERATED ALWAYS AS
     (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED
```

Indeks yang dibuat (tahap E):

| Indeks             | Tabel  | Kolom             | Jenis  | Dipakai untuk               |
| ------------------ | ------ | ----------------- | ------ | --------------------------- |
| Primary key        | gempa  | `eventID`         | B-tree | keunikan                    |
| `gempa_geom_gist`  | gempa  | `geom`            | GiST   | Q6 (kotak)                  |
| `gempa_waktu`      | gempa  | `datetime`        | B-tree | Q2, Q3, Q7                  |
| `gempa_mag`        | gempa  | `magnitude`       | B-tree | Q1, Q3, Q7, Q8              |
| `gempa_depth`      | gempa  | `depth`           | B-tree | Q3                          |
| `gempa_geog_gist`  | gempa  | `geom::geography` | GiST   | Q5, Q7 (radius dalam meter) |
| `sensor_geom_gist` | sensor | `geom`            | GiST   |                             |
| `sensor_geog_gist` | sensor | `geom::geography` | GiST   | Q8                          |

Indeks `geography` dibuat khusus agar kueri radius (satuan meter) bisa memakai indeks spasial.

### 6.3 Model data di MongoDB

Satu baris menjadi satu dokumen. Nilai kosong tidak disimpan. Kolom FM dikelompokkan di sub-dokumen `fm` hanya pada baris yang punya nilai FM:

```jsonc
{
  "eventID": "...", "datetime": ISODate("..."),
  "latitude": ..., "longitude": ..., "magnitude": ..., "depth": ...,
  // ...kolom non-FM lain yang terisi...
  "geom": { "type": "Point", "coordinates": [longitude, latitude] },  // GeoJSON
  "fm": { "doubleCouple": ..., "clvd": ..., /* hanya 8.023 dokumen */ }
}
```

Indeks: `eventID` (unik), `geom` (2dsphere), `datetime`, `magnitude`, `depth` pada koleksi `gempa`; `geom` (2dsphere) pada koleksi `sensor`.

### 6.4 Model data di Spark

Tidak ada tahap muat. Parquet didaftarkan sebagai tabel sementara (`gempa_raw`, `sensor`). Karena waktu di sumber berupa teks, dibuat tampilan `gempa` dengan kolom tambahan `ts = try_cast(datetime AS TIMESTAMP)` (zona sesi UTC). Nilai sumber tidak diubah, dan jumlah yang gagal di-cast dicek (hasilnya 0). Tidak ada indeks dan tidak ada cache, jadi **Parquet dibaca ulang pada setiap kueri**.

---

## 7. Definisi sembilan kueri

Parameter identik di semua sistem: Surabaya (112,7521; -7,2575), Palu (119,8707; -0,8917), kotak Sulawesi (bujur 118 sampai 125,5; lintang -6 sampai 2), jari-jari bumi bola 6371008,8 m.

| Kode | Pertanyaan                                | PostGIS                                     | MongoDB                                     | Spark                        |
| ---- | ----------------------------------------- | ------------------------------------------- | ------------------------------------------- | ---------------------------- |
| Q1   | Magnitude ≥ 6                             | `COUNT(*) WHERE magnitude >= 6`             | `count_documents({magnitude: {$gte: 6}})`   | SQL `COUNT(*)`               |
| Q2   | Tahun 2019 (UTC)                          | rentang `datetime`                          | rentang `datetime`                          | rentang `ts`                 |
| Q3   | M ≥ 5, kedalaman < 70, 2020 sampai 2023   | tiga syarat `AND`                           | tiga syarat                                 | tiga syarat                  |
| Q4   | Jumlah dan rata-rata magnitude per tahun  | `GROUP BY` tahun UTC                        | `$group` dengan `$year`                     | `GROUP BY year(ts)`          |
| Q5   | Radius 100 km dari Surabaya               | `ST_DWithin(geography)`                     | `$geoWithin $centerSphere`                  | rumus haversine              |
| Q6   | Kotak Sulawesi                            | `geom && ST_MakeEnvelope`                   | `$geoWithin $geometry Polygon`              | `BETWEEN` bujur dan lintang  |
| Q7   | M ≥ 5, 200 km dari Palu, 2018             | gabungan waktu, magnitude, radius           | gabungan                                    | gabungan                     |
| Q8   | Stasiun dalam 50 km dari tiap gempa M ≥ 6 | `LEFT JOIN` dengan `ST_DWithin`, satu kueri | 1 kueri sensor per gempa (join di aplikasi) | `LEFT JOIN` dengan haversine |
| Q9   | `doubleCouple` > 0,8                      | `WHERE "doubleCouple" > 0.8`                | `fm.doubleCouple > 0.8`                     | `WHERE doubleCouple > 0.8`   |

---

## 8. Hasil: waktu muat data

Detik, tiga putaran dan median. Laptop yang sama untuk semua angka.

### 8.1 PostGIS

| Tahap                        | Putaran 1 | Putaran 2 | Putaran 3 | **Median** |
| ---------------------------- | --------- | --------- | --------- | ---------- |
| A. Baca Parquet              | 0,215     | 0,051     | 0,043     | 0,051      |
| B. Konversi tipe waktu       | 0,094     | 0,091     | 0,080     | 0,091      |
| C. Siapkan CSV (Python)      | 2,192     | 2,426     | 1,968     | 2,192      |
| D. `COPY` ke database        | 2,906     | 3,401     | 3,410     | 3,401      |
| E. Bangun indeks             | 9,888     | 6,892     | 7,753     | 7,753      |
| F. `VACUUM FULL` + `ANALYZE` | 9,093     | 11,115    | 11,296    | 11,115     |
| **Total A-F**                | 24,388    | 23,976    | 24,549    | **24,388** |
| **Murni database D-F**       | 21,887    | 21,408    | 22,459    | **21,887** |

### 8.2 MongoDB

| Tahap                     | Putaran 1 | Putaran 2 | Putaran 3 | **Median** |
| ------------------------- | --------- | --------- | --------- | ---------- |
| A. Baca Parquet           | 0,135     | 0,050     | 0,049     | 0,050      |
| B. Konversi tipe waktu    | 0,114     | 0,079     | 0,076     | 0,079      |
| C. Susun dokumen (Python) | 3,476     | 3,039     | 3,048     | 3,048      |
| D. `insert_many`          | 2,691     | 1,871     | 2,190     | 2,190      |
| E. Bangun indeks          | 4,678     | 4,769     | 4,327     | 4,678      |
| F. `compact`              | 0,264     | 0,233     | 0,277     | 0,264      |
| **Total A-F**             | 11,357    | 10,041    | 9,966     | **10,041** |
| **Murni database D-F**    | 7,632     | 6,873     | 6,793     | **6,873**  |

### 8.3 Perbandingan

|                    | PostGIS                       | MongoDB                      | Selisih                             |
| ------------------ | ----------------------------- | ---------------------------- | ----------------------------------- |
| Total A-F          | 24,39 detik                   | 10,04 detik                  | PostGIS sekitar 2,4 kali lebih lama |
| Murni database D-F | 21,89 detik                   | 6,87 detik                   | PostGIS sekitar 3,2 kali lebih lama |
| Tahap terbesar     | F (`VACUUM FULL`, 11,1 detik) | C (susun dokumen, 3,0 detik) |                                     |

Pengamatan:

- Di PostGIS, tahap **F (perapian)** dan **E (indeks)** memakan sekitar 77 persen waktu muat. `COPY` sendiri hanya 3,4 detik.
- Di MongoDB, bagian terbesar justru **persiapan di Python** (membangun 176 ribu dokumen), dan `compact` nyaris tanpa biaya.
- Putaran pertama cenderung sedikit lebih lambat pada tahap A (cache berkas belum hangat).
- Spark tidak memuat data. Biaya awalnya: menyalakan SparkSession 6,41 detik dan mendaftarkan Parquet 2,29 detik.

---

## 9. Hasil: ukuran penyimpanan

### 9.1 Tabel gempa

| Sistem           | Data                           | Indeks    | **Total**    |
| ---------------- | ------------------------------ | --------- | ------------ |
| Parquet (sumber) | 13,1 MB                        | tidak ada | **13,1 MB**  |
| MongoDB          | 21,26 MB (terkompresi di disk) | 12,69 MB  | **33,95 MB** |
| PostGIS          | 32,96 MB                       | 35,98 MB  | **68,95 MB** |

MongoDB menyebut ukuran data mentah 57,46 MB, tetapi yang tersimpan di disk 21,26 MB karena kompresi WiredTiger. Angka yang dibandingkan adalah ukuran di disk.

### 9.2 Tabel sensor

| Sistem  | Data    | Indeks    | Total   |
| ------- | ------- | --------- | ------- |
| PostGIS | 0,25 MB | 0,27 MB   | 0,53 MB |
| MongoDB | 0,14 MB | 0,08 MB   | 0,22 MB |
| Parquet | 0,1 MB  | tidak ada | 0,1 MB  |

### 9.3 Pengamatan

- Total PostGIS sekitar **5,3 kali** ukuran Parquet dan sekitar **2 kali** ukuran MongoDB.
- Di PostGIS, **indeks (36 MB) lebih besar dari tabelnya (33 MB)**. Delapan indeks dibuat, termasuk dua indeks GiST tambahan untuk `geography`, yang menjadi harga kecepatan kueri radius.
- Parquet kolumnar dengan kompresi paling hemat, tetapi tanpa indeks.

---

## 10. Hasil: waktu kueri

Milidetik, **median (min sampai maks)** dari 5 ukur setelah 1 pemanasan. **Tebal** = tercepat.

| Kueri | PostGIS                  | MongoDB                  | Spark (Parquet)         |
| ----- | ------------------------ | ------------------------ | ----------------------- |
| Q1    | **0,21** (0,18–0,27)     | 4,55 (3,98–4,81)         | 161,94 (112,68–200,72)  |
| Q2    | **4,49** (2,93–5,79)     | 6,12 (5,53–7,23)         | 205,58 (181,60–222,37)  |
| Q3    | **10,53** (9,06–12,34)   | 79,39 (40,78–88,76)      | 146,63 (132,94–164,15)  |
| Q4    | **85,24** (66,93–101,42) | 320,39 (281,25–353,66)   | 257,18 (217,19–279,90)  |
| Q5    | **2,41** (1,67–3,28)     | 3,33 (2,24–4,90)         | 106,61 (93,75–122,28)   |
| Q6    | **10,48** (7,29–13,08)   | 237,90 (226,30–255,68)   | 109,29 (86,85–125,37)   |
| Q7    | **5,68** (4,83–9,83)     | 68,40 (55,12–209,92)     | 127,85 (115,32–149,83)  |
| Q8    | **58,46** (49,76–79,48)  | 1065,32 (987,50–1469,13) | 894,88 (799,95–957,53)  |
| Q9    | 69,69 (45,67–162,19)     | 128,65 (121,87–171,09)   | **64,07** (56,56–75,23) |

### 10.1 Rasio kecepatan terhadap PostGIS (median)

| Kueri | MongoDB lebih lambat | Spark lebih lambat                   |
| ----- | -------------------- | ------------------------------------ |
| Q1    | 21 kali              | sekitar 750 kali                     |
| Q2    | 1,4 kali             | 46 kali                              |
| Q3    | 7,5 kali             | 14 kali                              |
| Q4    | 3,8 kali             | 3,0 kali                             |
| Q5    | 1,4 kali             | 44 kali                              |
| Q6    | 23 kali              | 10 kali                              |
| Q7    | 12 kali              | 22 kali                              |
| Q8    | 18 kali              | 15 kali                              |
| Q9    | 1,8 kali             | 0,9 kali (Spark sedikit lebih cepat) |

### 10.2 Pemakaian indeks

| Kueri                      | PostGIS                  | MongoDB                  | Spark            |
| -------------------------- | ------------------------ | ------------------------ | ---------------- |
| Q1, Q2, Q3, Q5, Q6, Q7, Q8 | ya                       | ya                       | tidak ada indeks |
| Q4, Q9                     | tidak (penyisiran penuh) | tidak (penyisiran penuh) | tidak ada indeks |

---

## 11. Analisis per kueri dan rencana eksekusi

Rencana eksekusi PostGIS (`EXPLAIN`) tersimpan di `hasil/plan_postgis_Q*.txt`.

| Kueri      | Apa yang terjadi di PostGIS                                                                                                                             | Catatan lintas sistem                                                                                                                     |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Q1**     | `Index Only Scan` pada `gempa_mag`: jawaban dihitung dari indeks tanpa menyentuh tabel                                                                  | Terbesar kesenjangannya (0,21 ms lawan 4,55 ms dan 161,94 ms). Spark membaca ulang Parquet                                                |
| **Q2, Q3** | Memakai indeks waktu, magnitude, dan depth                                                                                                              | PostGIS dan MongoDB sama-sama baik pada Q2. MongoDB melambat pada Q3 (gabungan tiga indeks)                                               |
| **Q4**     | `Seq Scan` lalu `Sort` lalu `GroupAggregate`: menyisir seluruh 176 ribu baris                                                                           | Tidak ada indeks yang membantu. PostGIS tetap tercepat (85 ms). Spark (257 ms) mengalahkan MongoDB (320 ms) karena paralel                |
| **Q5**     | `Bitmap Index Scan` pada indeks `geography`, lalu `Filter` jarak persis                                                                                 | Plan memperkirakan 18 baris, hasil sebenarnya 151 (statistik PostgreSQL meremehkan sebaran spasial), tapi tetap cepat                     |
| **Q6**     | Memakai indeks GiST pada kotak                                                                                                                          | 30 ribu baris cocok. Pada MongoDB 237,90 ms. Dugaan: tiap dokumen yang cocok dicek geometri poligonnya. Ini dugaan, tidak diukur          |
| **Q7**     | Kombinasi indeks waktu dan radius                                                                                                                       | MongoDB punya satu ukur ekstrem (209,92 ms), menunjukkan variasi yang besar                                                               |
| **Q8**     | `Nested Loop Left Join`: gempa M≥6 dari indeks `magnitude`, lalu tiap gempa mencari sensor lewat indeks `sensor_geog_gist`. Berjalan paralel (1 worker) | Seluruh join selesai dalam **satu** kueri. MongoDB mengerjakannya lewat 1.775 kueri kecil dari aplikasi, sehingga paling lambat (1065 ms) |
| **Q9**     | `Parallel Seq Scan` dengan 2 worker (tidak ada indeks)                                                                                                  | Praktis seri dengan Spark. Variasi PostGIS antar ukur besar (45,67 sampai 162,19 ms), lebih besar dari selisih median dengan Spark        |

Pola umum:

- **Kueri pemilihan sempit dengan indeks**: urutan PostGIS, MongoDB, Spark, dengan jarak jauh ke Spark.
- **Kueri penyisiran penuh tanpa indeks (Q4, Q9)**: jarak menyempit, dan Spark yang memakai 12 core menjadi sebanding.
- **Kueri join spasial (Q8)**: arsitektur menentukan. Join yang dikerjakan di dalam mesin (PostGIS) jauh lebih cepat daripada join di sisi aplikasi (MongoDB).

---

## 12. Validitas hasil

### 12.1 Verifikasi setelah muat (PostGIS dan MongoDB)

Dicek otomatis di akhir skrip muat dan dinyatakan `SEMUA OK`:

- Jumlah baris gempa (176.384) dan sensor (1.743) sama dengan sumber.
- Kedalaman maksimum (90000) sama dengan sumber.
- Jumlah magnitude kosong (1.405) sama dengan sumber.
- Rentang waktu sama dengan sumber. Di MongoDB perbandingan dilakukan pada ketelitian milidetik, karena MongoDB menyimpan waktu sampai milidetik sedangkan sumber sampai mikrodetik.
- Tidak ada geometri kosong.

### 12.2 Jumlah hasil kueri antar sistem

| Kueri | PostGIS           | MongoDB           | Spark             | Status         |
| ----- | ----------------- | ----------------- | ----------------- | -------------- |
| Q1    | 1.775             | 1.775             | 1.775             | sama           |
| Q2    | 13.635            | 13.635            | 13.635            | sama           |
| Q3    | 3.744             | 3.744             | 3.744             | sama           |
| Q4    | 19 tahun; 176.384 | 19 tahun; 176.384 | 19 tahun; 176.384 | sama           |
| Q5    | 151               | 151               | 151               | sama           |
| Q6    | 30.556            | **30.565**        | 30.556            | MongoDB beda 9 |
| Q7    | 17                | 17                | 17                | sama           |
| Q8    | 1.775 gempa; 375  | 1.775 gempa; 375  | 1.775 gempa; 375  | sama           |
| Q9    | 3.669             | 3.669             | 3.669             | sama           |

### 12.3 Mengapa Q6 di MongoDB berbeda

PostGIS dan Spark memakai **kotak datar** (batas lintang -6 dan 2 berupa garis lurus pada peta). MongoDB dengan indeks 2dsphere menarik sisi kotak sebagai **busur pada permukaan bola**, sehingga sisi utara dan selatan sedikit melengkung. Gempa yang tepat di sekitar tepi ikut atau tidak ikut terhitung. Selisih 9 baris dari 30.556 (0,03 persen) ini adalah **perbedaan definisi geometri, bukan kesalahan data atau skrip**. Hal ini terbukti karena Spark, yang memakai batas datar, menghasilkan 30.556 persis seperti PostGIS.

---

## 13. Temuan dan kesimpulan

1. **PostGIS tercepat di 8 dari 9 kueri** berkat indeks, terutama kueri pemilihan dan spasial. Pada Q1 ia sekitar 21 kali lebih cepat dari MongoDB dan sekitar 750 kali dari Spark.
2. **Q9 praktis seri** antara PostGIS dan Spark (selisih sekitar 5,6 ms). Tidak ada indeks, dan variasi PostGIS (46 sampai 162 ms) lebih besar dari selisihnya, sehingga Spark tidak bisa dinyatakan unggul.
3. **MongoDB memuat data paling cepat** (10,04 detik lawan 24,39 detik) dan memakai ruang setengah PostGIS (33,95 MB lawan 68,95 MB). Waktu PostGIS banyak habis di `VACUUM FULL` dan pembuatan indeks.
4. **PostGIS punya jejak penyimpanan terbesar** karena indeksnya (36 MB) lebih besar dari tabelnya (33 MB). Parquet paling hemat (13,1 MB).
5. **Spark tidak butuh tahap muat**, tetapi tiap kueri punya biaya dasar sekitar 60 sampai 200 ms (membaca ulang Parquet, tanpa cache). Ia kompetitif pada kueri penyisiran penuh tanpa indeks (Q4, Q9) karena memakai 12 core.
6. **MongoDB mengungguli Spark pada kueri berindeks** (Q1, Q2, Q5), tetapi **kalah pada Q4, Q6, Q8, dan Q9** yang menyisir banyak data.
7. **Q8 adalah titik lemah MongoDB**: join gempa dan stasiun dikerjakan aplikasi lewat 1.775 kueri kecil, sedangkan PostGIS menyelesaikannya dalam satu kueri.
8. **Ketiga sistem menghasilkan jawaban yang sama**, kecuali Q6 di MongoDB (selisih 9 baris karena definisi sisi kotak yang berbeda).
9. **Rekomendasi berdasarkan beban kerja:**

| Kebutuhan                                                              | Pilihan       |
| ---------------------------------------------------------------------- | ------------- |
| Kueri berulang dengan indeks, terutama spasial dan join                | PostGIS       |
| Muat data cepat dan penyimpanan hemat dibanding PostGIS                | MongoDB       |
| Analisis sekali jalan tanpa memuat ke database, atau data sangat besar | Spark/Parquet |

---

## 14. Batasan eksperimen

- Semua sistem memakai **konfigurasi bawaan** tanpa tuning (misalnya `shared_buffers = 128MB` di PostgreSQL). Hasil bisa berubah jika disetel.
- Data hanya 176 ribu baris (13 MB Parquet). Kesimpulan belum tentu berlaku untuk data ratusan juta baris, terutama untuk Spark yang keunggulannya muncul pada skala besar dan terdistribusi.
- Spark berjalan **mode lokal** (satu mesin, 12 core), bukan klaster HDFS sungguhan. Karena itu "HDFS" tidak diuji, hanya kemampuan Spark membaca Parquet.
- **Q8 tidak setara sepenuhnya**: join di MongoDB dikerjakan aplikasi, di PostGIS dan Spark dikerjakan mesin kueri.
- Spark tidak punya indeks spasial, sehingga jarak dihitung dengan rumus haversine.
- Hanya **3 putaran muat dan 5 ukur kueri**. Variasi antar ukur terlihat jelas pada beberapa kueri (Q9 PostGIS, Q7 MongoDB), jadi selisih kecil tidak boleh dianggap bermakna.
- Cache sistem operasi ikut berperan. Pemanasan 1 kali mengurangi, tetapi tidak menghilangkan, pengaruhnya.
- Hasil dari satu laptop. Angka mutlak akan berbeda di perangkat lain; yang relevan adalah urutan dan rasionya.
- Zona waktu server PostgreSQL tercatat `Asia/Bangkok`. Tidak memengaruhi hasil karena semua kueri memakai UTC eksplisit.
- Penjelasan sebab (misalnya mengapa Q6 di MongoDB lambat) bersifat **dugaan** kecuali yang ditunjukkan rencana eksekusi.

---

## 15. Masalah yang ditemui dan solusinya

| Masalah                                                           | Penyebab                                                                              | Solusi                                                                                  |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Installer MongoDB gagal: _Out of Disk Space_                      | Drive C: hampir penuh (sisa 285 MB)                                                   | Pakai MongoDB versi **ZIP** di drive D: dan jalankan `mongod.exe --dbpath D:\mongodata` |
| `pip install pyspark`: _No space left on device_                  | pip mengunduh berkas sementara ke folder temp di C:                                   | Arahkan `$env:TEMP` dan `$env:TMP` ke `D:\temp`, pakai `--no-cache-dir`                 |
| Spark error `COLUMN_ALREADY_EXISTS: azgapfm`                      | Sumber punya `AzGapFM` dan `azgapFM`; Spark bawaan tidak membedakan huruf besar-kecil | `.config("spark.sql.caseSensitive", "true")`                                            |
| `activate` ditolak (execution policy)                             | Kebijakan skrip PowerShell                                                            | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`                                   |
| Salah ketik password di kolom _Server_ di psql                    | Enter harus ditekan kosong untuk Server, Database, Port, Username                     | Tutup lalu buka lagi SQL Shell                                                          |
| Selisih Q6 di MongoDB                                             | Sisi kotak sebagai busur bola                                                         | Bukan kesalahan; dijelaskan di bagian 12.3                                              |
| Peringatan Spark `winutils.exe`, `native-hadoop`, `pandas >= 3.0` | Umum di Windows                                                                       | Aman diabaikan karena hanya membaca Parquet                                             |

Catatan keamanan: skrip PostGIS membaca password dari variabel lingkungan `PGPASSWORD`. Pada contoh pesan error di dalam skrip ada password lama milik anggota lain; **hapus sebelum mengumpulkan atau membagikan kode**.

---

## 16. Cara mereproduksi

Prasyarat: bagian 6.1 sudah dikerjakan. Jalankan dari folder induk proyek (yang berisi `data`, `hasil`, `notebooks`) di PowerShell dengan venv aktif.

**PostGIS** (MongoDB dimatikan):

```powershell
$env:PYTHONNOUSERSITE = "1"
$env:PGPASSWORD = "<password postgres>"
python notebooks/muat_postgis_v2.py       # 3 putaran, tunggu "SEMUA OK"
python notebooks/benchmark_postgis.py
```

**MongoDB** (PostgreSQL dimatikan, `mongod` berjalan di jendela lain):

```powershell
python notebooks/muat_mongodb.py          # 3 putaran, tunggu "SEMUA OK"
python notebooks/benchmark_mongodb.py
```

**Spark** (tidak butuh database):

```powershell
python notebooks/benchmark_spark.py
```

Opsi uji cepat: `--ulang 1` pada skrip muat. Opsi benchmark: `--ukur 5 --pemanasan 1`.

Agar adil: tutup aplikasi lain, colok charger, dan jalankan satu sistem per sesi.

---

## 17. Struktur folder dan berkas hasil

```
bigdata_postgre/
├── data/
│   ├── gempa.parquet
│   └── sensor.parquet
├── notebooks/
│   ├── muat_postgis_v2.py        # muat + ukur waktu per tahap (PostGIS)
│   ├── benchmark_postgis.py      # Q1-Q9 di PostGIS
│   ├── muat_mongodb.py           # muat + ukur waktu per tahap (MongoDB)
│   ├── benchmark_mongodb.py      # Q1-Q9 di MongoDB
│   ├── benchmark_spark.py        # Q1-Q9 di Spark (baca Parquet langsung)
│   ├── muat_postgis.py           # versi lama, tidak dipakai
│   ├── note.txt
│   └── konteks uas big data.txt
└── hasil/
```

| Berkas hasil                                                                  | Isi                                                        |
| ----------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `waktu_muat_postgis_v2.csv`                                                   | Waktu per tahap (3 putaran dan median) dan ukuran, PostGIS |
| `waktu_muat_mongodb.csv`                                                      | Waktu per tahap (3 putaran dan median) dan ukuran, MongoDB |
| `benchmark_postgis.csv`, `benchmark_mongodb.csv`, `benchmark_spark.csv`       | Median, min, maks, 5 ukur, pemakaian indeks, jumlah hasil  |
| `konfigurasi_postgis.txt`, `konfigurasi_mongodb.txt`, `konfigurasi_spark.txt` | Versi dan konfigurasi tiap sistem                          |
| `plan_postgis_Q1.txt` … `Q9.txt`                                              | Rencana eksekusi (`EXPLAIN`) tiap kueri di PostGIS         |
| `waktu_muat_postgis.csv`                                                      | **Hasil lama (skrip versi pertama), jangan dipakai**       |

---

## 18. Anggota kelompok dan pembagian peran

**Judul eksperimen:** _isi_

| No  | Nama  | NIM   | Peran |
| --- | ----- | ----- | ----- |
| 1   | _isi_ | _isi_ | _isi_ |
| 2   | _isi_ | _isi_ | _isi_ |
| 3   | _isi_ | _isi_ | _isi_ |
| 4   | _isi_ | _isi_ | _isi_ |
| 5   | _isi_ | _isi_ | _isi_ |
