# Perbandingan PostgreSQL/PostGIS, MongoDB, dan Spark (Parquet) pada Data Gempa Indonesia

Eksperimen mata kuliah Big Data: membandingkan tiga cara menyimpan dan menanyai data gempa yang sama, dari sisi **waktu muat data**, **ukuran penyimpanan**, dan **kecepatan 9 kueri** (atribut, waktu, dan spasial).

| Sistem                      | Peran                           | Cara data diakses                                              |
| --------------------------- | ------------------------------- | -------------------------------------------------------------- |
| PostgreSQL 16 + PostGIS 3.6 | Basis data relasional + spasial | Data dimuat ke tabel, diberi indeks (B-tree dan GiST)          |
| MongoDB 9.0.4               | Basis data dokumen              | Data dimuat sebagai dokumen, diberi indeks (termasuk 2dsphere) |
| Spark 4.3.0 (mode lokal)    | Pembanding dasar (baseline)     | Membaca file Parquet langsung, tanpa indeks dan tanpa cache    |

## 1. Data

| Berkas                | Isi                                                                               | Ukuran  |
| --------------------- | --------------------------------------------------------------------------------- | ------- |
| `data/gempa.parquet`  | 176.384 kejadian gempa, 37 kolom (termasuk kolom focal mechanism berakhiran `FM`) | 13,1 MB |
| `data/sensor.parquet` | 1.743 baris stasiun sensor, 6 kolom                                               | 0,1 MB  |

Prinsip pemuatan: data dimuat **apa adanya**. Tidak ada baris dibuang dan tidak ada nilai diubah atau diisi. Nilai kosong tetap kosong (di MongoDB, field kosong tidak disimpan).

## 2. Struktur folder

```
bigdata_postgre/
├── data/                     # gempa.parquet, sensor.parquet
├── notebooks/
│   ├── muat_postgis_v2.py    # muat data ke PostGIS + ukur waktu per tahap
│   ├── benchmark_postgis.py  # kueri Q1-Q9 di PostGIS
│   ├── muat_mongodb.py       # muat data ke MongoDB + ukur waktu per tahap
│   ├── benchmark_mongodb.py  # kueri Q1-Q9 di MongoDB
│   └── benchmark_spark.py    # kueri Q1-Q9 di Spark (baca Parquet langsung)
└── hasil/                    # CSV dan teks hasil pengukuran
```

Catatan: `muat_postgis.py` dan `hasil/waktu_muat_postgis.csv` (tanpa `_v2`) adalah versi lama, **tidak dipakai** di laporan.

## 3. Variabel eksperimen

| Jenis            | Variabel                                                                                                                    |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Variabel bebas   | Sistem penyimpanan (PostGIS, MongoDB, Spark/Parquet) dan jenis kueri (Q1 sampai Q9)                                         |
| Variabel terikat | Waktu muat per tahap (detik), ukuran penyimpanan (MB), waktu kueri (ms), kebenaran hasil (jumlah baris)                     |
| Variabel kontrol | Data yang sama, laptop yang sama, hanya satu sistem aktif per sesi, konfigurasi bawaan (tanpa tuning), zona waktu kueri UTC |

Aturan pengukuran:

- **Muat data:** 3 putaran penuh, dilaporkan median per tahap.
- **Kueri:** 1 kali pemanasan (dibuang), lalu 5 kali ukur, dilaporkan median, min, dan maks.
- **Kebenaran:** jumlah hasil tiap kueri dibandingkan antar sistem.

## 4. Sembilan kueri

| Kode | Pertanyaan                                               |
| ---- | -------------------------------------------------------- |
| Q1   | Gempa dengan magnitude ≥ 6                               |
| Q2   | Gempa sepanjang tahun 2019 (UTC)                         |
| Q3   | Magnitude ≥ 5, kedalaman < 70 km, tahun 2020 sampai 2023 |
| Q4   | Jumlah gempa dan rata-rata magnitude per tahun           |
| Q5   | Gempa dalam radius 100 km dari Surabaya                  |
| Q6   | Gempa dalam kotak batas (bounding box) Sulawesi          |
| Q7   | Magnitude ≥ 5 dalam radius 200 km dari Palu, tahun 2018  |
| Q8   | Stasiun sensor dalam radius 50 km dari tiap gempa M ≥ 6  |
| Q9   | Focal mechanism dengan `doubleCouple` > 0,8              |

## 5. Konfigurasi pengujian

| Komponen        | Nilai                                                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------------------------------------- |
| PostgreSQL      | 16.15 (Windows), `shared_buffers = 128MB`, `work_mem = 4MB`, `max_parallel_workers_per_gather = 2`, TimeZone `Asia/Bangkok` |
| PostGIS         | 3.6 (GEOS, PROJ aktif)                                                                                                      |
| MongoDB         | 9.0.4, storage engine WiredTiger, cache maks 3602 MB                                                                        |
| Spark           | 4.3.0, mode `local[*]` dengan 12 core, driver memory 2g, shuffle partitions 8, timezone UTC                                 |
| Python          | 3.13.0                                                                                                                      |
| Perangkat keras | Windows 11, Intel Core i5 (Gen 13), RAM 8 GB, SSD 512 GB, GPU NVIDIA RTX 2050                                               |

## 6. Cara menjalankan (Windows, PowerShell)

Prasyarat: Python 3.10 ke atas, PostgreSQL 16 + PostGIS 3.6 (database `gempa` dengan `CREATE EXTENSION postgis;`), MongoDB, dan Java 17 (untuk Spark).

```powershell
python -m venv venv
venv\Scripts\activate
pip install pandas pyarrow psycopg2-binary pymongo pyspark
```

**PostGIS** (MongoDB dimatikan):

```powershell
$env:PYTHONNOUSERSITE = "1"
$env:PGPASSWORD = "<password postgres>"
python notebooks/muat_postgis_v2.py      # 3 putaran, tunggu "SEMUA OK"
python notebooks/benchmark_postgis.py
```

**MongoDB** (PostgreSQL dimatikan, `mongod` berjalan):

```powershell
python notebooks/muat_mongodb.py         # 3 putaran, tunggu "SEMUA OK"
python notebooks/benchmark_mongodb.py
```

**Spark** (tidak perlu database):

```powershell
python notebooks/benchmark_spark.py
```

Opsi uji cepat: tambahkan `--ulang 1` pada skrip muat untuk satu putaran saja.

## 7. Hasil: waktu muat data

Median dari 3 putaran, dalam detik.

| Tahap                     | PostGIS                           | MongoDB              |
| ------------------------- | --------------------------------- | -------------------- |
| A. Baca Parquet           | 0,05                              | 0,05                 |
| B. Konversi tipe waktu    | 0,09                              | 0,08                 |
| C. Siapkan data di Python | 2,19 (CSV)                        | 3,05 (dokumen)       |
| D. Kirim ke database      | 3,40 (`COPY`)                     | 2,19 (`insert_many`) |
| E. Bangun indeks          | 7,75                              | 4,68                 |
| F. Perapian               | 11,12 (`VACUUM FULL` + `ANALYZE`) | 0,26 (`compact`)     |
| **Total A-F**             | **24,39**                         | **10,04**            |
| **Murni database (D-F)**  | **21,89**                         | **6,87**             |

Spark tidak memuat data. Biaya awalnya: menyalakan SparkSession 6,41 detik dan mendaftarkan Parquet 2,29 detik.

## 8. Hasil: ukuran penyimpanan (tabel gempa)

| Sistem           | Data                          | Indeks    | Total   |
| ---------------- | ----------------------------- | --------- | ------- |
| Parquet (sumber) | 13,1 MB                       | tidak ada | 13,1 MB |
| MongoDB          | 21,3 MB (terkompresi di disk) | 12,7 MB   | 34,0 MB |
| PostGIS          | 33,0 MB                       | 36,0 MB   | 69,0 MB |

Tabel sensor sangat kecil: PostGIS 0,53 MB, MongoDB 0,22 MB, Parquet 0,1 MB.

## 9. Hasil: waktu kueri

Median dalam milidetik (makin kecil makin cepat). **Tebal** menandai yang tercepat.

| Kueri | PostGIS   | MongoDB | Spark (Parquet) | Jumlah hasil                  |
| ----- | --------- | ------- | --------------- | ----------------------------- |
| Q1    | **0,21**  | 4,55    | 161,94          | 1.775                         |
| Q2    | **4,49**  | 6,12    | 205,58          | 13.635                        |
| Q3    | **10,53** | 79,39   | 146,63          | 3.744                         |
| Q4    | **85,24** | 320,39  | 257,18          | 19 tahun; total 176.384       |
| Q5    | **2,41**  | 3,33    | 106,61          | 151                           |
| Q6    | **10,48** | 237,90  | 109,29          | 30.556 (MongoDB: 30.565)      |
| Q7    | **5,68**  | 68,40   | 127,85          | 17                            |
| Q8    | **58,46** | 1065,32 | 894,88          | 1.775 gempa; 375 baris sensor |
| Q9    | 69,69     | 128,65  | **64,07**       | 3.669                         |

Pemakaian indeks: PostGIS dan MongoDB memakai indeks di semua kueri kecuali Q4 dan Q9. Spark tidak memakai indeks sama sekali.

## 10. Temuan dan kesimpulan

1. **PostGIS tercepat di 8 dari 9 kueri** berkat indeks, terutama kueri pemilihan dan spasial. Pada Q1 ia sekitar 21 kali lebih cepat dari MongoDB dan sekitar 750 kali dari Spark.
2. **Q9 praktis seri** antara PostGIS dan Spark (selisih 5,6 ms). Tidak ada indeks di kueri ini, dan variasi PostGIS antar ukuran (46 sampai 162 ms) lebih besar dari selisihnya, jadi Spark tidak bisa dinyatakan unggul.
3. **MongoDB memuat data paling cepat** (10,0 detik berbanding 24,4 detik) dan memakai ruang setengah PostGIS. Waktu PostGIS banyak habis di `VACUUM FULL` dan pembuatan indeks.
4. **PostGIS memiliki jejak penyimpanan terbesar** karena indeksnya (36 MB) hampir sebesar tabelnya (33 MB). Parquet paling hemat (13,1 MB).
5. **Spark tidak butuh tahap muat**, tetapi tiap kueri punya biaya dasar sekitar 60 sampai 200 ms (membaca ulang Parquet, tanpa cache). Ia kompetitif pada kueri penyisiran penuh tanpa indeks (Q4, Q9) karena memakai 12 core.
6. **MongoDB unggul dari Spark di kueri berindeks** (Q1, Q2, Q5), tetapi **kalah di Q4, Q6, Q8, dan Q9** yang menyisir banyak data.
7. **Q8 adalah titik lemah MongoDB**: gabungan gempa dan stasiun dikerjakan di sisi aplikasi lewat 1.775 kueri kecil, sedangkan PostGIS menyelesaikannya dalam satu kueri.
8. **Perbedaan Q6 di MongoDB (30.565 vs 30.556)** bukan kesalahan data. MongoDB menghitung sisi kotak sebagai busur di permukaan bola, sedangkan PostGIS dan Spark memakai kotak datar.
9. **Pilihan sistem bergantung pada beban kerja:** kueri berulang dengan indeks cocok di PostGIS; muat cepat dan penyimpanan hemat cocok di MongoDB; analisis sekali jalan tanpa memuat ke database cocok di Spark.

## 11. Batasan eksperimen

- Semua sistem memakai **konfigurasi bawaan**, tanpa tuning (misalnya `shared_buffers = 128MB` di PostgreSQL).
- Data hanya 176 ribu baris (13 MB Parquet). Kesimpulan belum tentu berlaku untuk data ratusan juta baris, terutama untuk Spark.
- Spark berjalan **mode lokal** (satu mesin, 12 core), bukan klaster HDFS terdistribusi.
- **Q8 tidak setara sepenuhnya**: join di MongoDB dikerjakan aplikasi, di PostGIS dan Spark dikerjakan mesin kueri.
- Tidak ada indeks spasial di Spark, jadi jarak dihitung dengan rumus haversine (jari-jari bola 6371008,8 m).
- Waktu di MongoDB hanya sampai milidetik, sedangkan sumber sampai mikrodetik (diperhitungkan di verifikasi).
- Pengukuran dilakukan pada satu laptop dengan pengulangan terbatas (3 putaran muat, 5 ukur kueri). Variasi antar ukur terlihat jelas pada beberapa kueri (misalnya Q9 PostGIS dan Q7 MongoDB), jadi selisih kecil tidak boleh dianggap bermakna.

## 12. Berkas hasil

| Berkas                                                                        | Isi                                                                     |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `hasil/waktu_muat_postgis_v2.csv`                                             | Waktu muat per tahap dan ukuran, PostGIS                                |
| `hasil/waktu_muat_mongodb.csv`                                                | Waktu muat per tahap dan ukuran, MongoDB                                |
| `hasil/benchmark_postgis.csv`, `benchmark_mongodb.csv`, `benchmark_spark.csv` | Waktu kueri (median, min, maks, 5 ukur), pemakaian indeks, jumlah hasil |
| `hasil/konfigurasi_*.txt`                                                     | Versi dan konfigurasi tiap sistem                                       |
| `hasil/plan_postgis_Q1..Q9.txt`                                               | Rencana eksekusi (`EXPLAIN`) tiap kueri di PostGIS                      |

## 13. Anggota kelompok dan pembagian peran

| Nama  | Peran |
| ----- | ----- |
| _isi_ | _isi_ |
| _isi_ | _isi_ |
| _isi_ | _isi_ |
| _isi_ | _isi_ |
| _isi_ | _isi_ |
