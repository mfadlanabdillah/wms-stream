# Panduan Dev Day — dari nol sampai form terkirim

Panduan langkah-demi-langkah untuk submission Confluent Developer Day.
Nilai konkret di sini diambil dari environment yang sudah berjalan di
mesinmu, bukan contoh.

**Estimasi waktu: 45–60 menit.**

Urutan itu penting. Stream Lineage — yang diminta form — hanya tergambar
setelah data benar-benar mengalir. Jangan lompat ke screenshot.

---

## Checklist singkat

- [ ] 1. Cek Postgres demo + tunnel masih hidup
- [ ] 2. Buat cluster Confluent Cloud (Schema Registry otomatis)
- [ ] 3. Buat API key Kafka
- [ ] 4. Jalankan connector Postgres CDC
- [ ] 5. Verifikasi topic terbentuk
- [ ] 6. Jalankan Flink SQL
- [ ] 7. Picu alert dengan data baru ⏱
- [ ] 8. Jalankan HTTP Sink
- [ ] 9. Screenshot Stream Lineage ⏱ **dalam 10 menit setelah langkah 7**
- [ ] 10. Isi form

---

## Langkah 1 — Cek yang sudah jalan

Dua hal harus hidup: Postgres demo dan tunnel.

```bash
cd ~/workspace/wms-stream

# Postgres demo
docker ps --filter name=wms-cdc-demo --format '{{.Names}}\t{{.Status}}'

# Tunnel
pgrep -f "bore local 55433" >/dev/null && \
  grep -oE "bore\.pub:[0-9]+" /tmp/bore-wms.log | head -1 || echo "TUNNEL MATI"
```

Kalau Postgres mati:

```bash
DEMO_DB_PASSWORD=devday docker compose -f docker-compose.demo.yml up -d
```

Kalau tunnel mati, jalankan lagi di terminal terpisah dan **biarkan terbuka**:

```bash
./scripts/start_tunnel.sh
```

> **Port berubah setiap restart.** Saat dokumen ini ditulis endpoint-nya
> `bore.pub:11033`. Kalau kamu me-restart tunnel, angkanya lain — pakai
> yang dicetak skrip, dan update connector config.

Ambil password CDC (jangan di-paste ke chat/screenshot):

```bash
grep '^CDC_PASSWORD=' .env.demo.local | cut -d= -f2-
```

---

## Langkah 2 — Cluster Confluent Cloud

Console → **Environments** → pilih environment → **Create cluster**.

| Pilihan | Isi | Alasan |
|---|---|---|
| Type | **Basic** | Cukup untuk workload ini; kredit trial $400 lebih dari memadai |
| Region | terdekat dengan lokasimu | Menekan latensi CDC |
| Nama | `wms-dsp` | — |

### Schema Registry: tidak perlu diaktifkan manual

Tidak ada tombol "Enable Schema Registry" — pertanyaan yang wajar, karena
banyak tutorial lama masih menyebutkannya.

Yang sebenarnya terjadi:

- Paket **Stream Governance Essentials** sudah terpasang otomatis di
  setiap environment (gratis)
- Schema Registry **di-provision sendiri** begitu cluster Kafka pertama
  di environment itu selesai dibuat

Jadi setelah langkah di atas, Schema Registry sudah ada. Tidak ada aksi
tambahan.

Essentials mencakup 100 schema gratis; proyek ini memakai 6. Kamu tidak
akan kena biaya.

Untuk memastikan (opsional): **Environments** → pilih environment →
panel kanan menampilkan **Stream Governance** dengan package Essentials
dan endpoint Schema Registry. Kalau belum muncul, cluster-mu belum
selesai provisioning.

> **Region Schema Registry mengikuti cluster pertama, dan tidak bisa
> diubah.** Kalau nanti kamu ingin cluster di region lain, buat
> environment baru.

### Satu batasan yang memengaruhi langkah 9

Di paket Essentials, **Stream Lineage hanya menampilkan 10 menit
terakhir** (Advanced: 7 hari).

Konsekuensinya nyata: screenshot lineage harus diambil **dalam 10 menit
setelah data terakhir mengalir**. Kalau kamu jalankan Flink lalu istirahat
satu jam, grafiknya akan tampak kosong.

Karena itu langkah 7 (picu data live) ditaruh persis sebelum langkah 9.
Bukan kebetulan.

---

## Langkah 3 — API key Kafka

Cluster → **API Keys** → *Create key* → **Global access** → simpan.

Kamu dapat dua bagian: **key** dan **secret**. Secret hanya tampil sekali.

Keduanya masuk ke `kafka.api.key` / `kafka.api.secret` di **dua** file
connector.

---

## Langkah 4 — Connector Postgres CDC

Console → **Connectors** → cari *Postgres CDC Source V2*.

Wizard-nya 6 layar. Ada **dua cara** — pilih salah satu.

### Cara A (tercepat): Switch to JSON

Di layar wizard mana pun, cari tombol **Switch to JSON** di kanan atas.
Tempel konfigurasi berikut, lalu langsung lompat ke *Review and launch*.

```json
{
  "connector.class": "PostgresCdcSourceV2",
  "name": "wms-postgres-cdc-source",
  "tasks.max": "1",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "<key dari langkah 3>",
  "kafka.api.secret": "<secret dari langkah 3>",
  "database.hostname": "bore.pub",
  "database.port": "11033",
  "database.user": "confluent_cdc",
  "database.password": "<dari .env.demo.local>",
  "database.dbname": "wms",
  "database.sslmode": "prefer",
  "topic.prefix": "wms",
  "slot.name": "wms_cdc_slot",
  "publication.name": "wms_cdc_publication",
  "publication.autocreate.mode": "filtered",
  "plugin.name": "pgoutput",
  "table.include.list": "public.stock_movements,public.products,public.locations,public.warehouses",
  "snapshot.mode": "initial",
  "tombstones.on.delete": "false",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.tombstones.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,source.ts_ms"
}
```

Ini sama dengan `connectors/postgres-cdc-source.json` tetapi key
`_comment` dan `_sslmode_note` **sudah dibuang** — Confluent menolak
property yang tidak dikenal. Kalau kamu menempel dari file itu langsung,
hapus dulu dua key tersebut.

### Cara B: isi manual per layar

Kalau lebih nyaman lewat form, ini isian tiap layar:

**1. Topic selection**

| Field | Isi |
|---|---|
| Topic prefix | `wms` |
| Partitions | `1` (biarkan default) |

> Jangan naikkan partition di atas 1. Topic CDC kehilangan jaminan urutan
> untuk tabel tanpa primary key kalau partisinya lebih dari satu — dan
> urutan itu justru yang membuat perhitungan stok benar.

Klik **Continue**.

**2. Kafka access**

Pilih **Use an existing API key**, lalu tempel key + secret dari
langkah 3. (Opsi *My account* juga jalan untuk demo; *Service account*
untuk produksi.)

Klik **Continue**.

**3. Authentication** — ini layar koneksi database

| Field | Isi |
|---|---|
| Authentication method | `Password` |
| Database hostname | `bore.pub` |
| Database port | `11033` |
| Database username | `confluent_cdc` |
| Database password | dari `.env.demo.local` |
| Database name | `wms` |
| SSL mode | **`prefer`** |

> `prefer` itu wajib di sini. Postgres demo tidak punya sertifikat TLS,
> jadi `require` akan gagal konek. Kebetulan `prefer` juga default-nya.

Biarkan *Use secret manager* mati, dan semua field SSL certificate kosong.

Klik **Continue**.

**4. Configuration**

*Output messages:*

| Field | Isi |
|---|---|
| Output record value format | **AVRO** |
| Output record key format | **AVRO** |

AVRO penting — itu yang mendaftarkan schema ke Schema Registry, dan
Stream Governance adalah salah satu kriteria penilaian juri.

*Database config:*

| Field | Isi |
|---|---|
| Slot name | `wms_cdc_slot` |
| Publication name | `wms_cdc_publication` |

*Connector config:*

| Field | Isi |
|---|---|
| Snapshot mode | `initial` |
| Tables included | `public.stock_movements,public.products,public.locations,public.warehouses` |

Biarkan *Tables excluded* kosong — dua property itu tidak bisa dipakai
bersamaan.

Lalu buka **Show advanced configurations** → cari bagian **Single Message
Transforms** → tambah SMT:

| Field | Isi |
|---|---|
| Transform type | `ExtractNewRecordState` |
| **Transform name** | `wms_transform` (nama bebas) |
| Transformation version | biarkan default |
| **Handle delete records** | **kosongkan** (deprecated) |
| **Handle delete and tombstone records** | `rewrite` |
| Adds the specified field(s) to the message | `op,source.ts_ms` |
| **Drop tombstones** | **kosongkan** (deprecated) |
| sisa field | biarkan kosong |

Tiga hal yang perlu diperhatikan di layar ini:

**Ada dua field yang namanya mirip.** "Handle delete records"
(`delete.handling.mode`) dan "Drop tombstones" (`drop.tombstones`)
keduanya **sudah deprecated** — biarkan kosong. Yang benar adalah
"Handle delete **and tombstone** records"
(`delete.tombstones.handling.mode`) diisi `rewrite`. Mengisi yang
deprecated bisa bentrok dengan yang baru.

**Transform name menentukan nama property.** Kalau kamu namai
`wms_transform`, JSON-nya menjadi `transforms.wms_transform.type` dst.
Di Cara A aku memakai nama `unwrap`. Keduanya sama-sama benar — namanya
hanya label internal. Yang penting konsisten, jangan campur.

**Field sisanya biarkan kosong.** Field prefix, header, route by field
name — semuanya opsional dan tidak dipakai pipeline ini.

SMT ini membuka "envelope" Debezium sehingga isi topic menjadi baris yang
rata — cocok dengan Avro schema di `schemas/` dan bisa langsung dibaca
Flink. Tanpa ini, Flink akan melihat struktur bersarang
`before`/`after`/`source` dan query di langkah 6 tidak jalan.

Klik **Continue**.

**5. Sizing**

Connector ini hanya mendukung **1 task**. Tidak ada yang perlu diubah.

Klik **Continue**.

**6. Review and launch**

Periksa konfigurasinya, ganti nama connector kalau mau, lalu **Launch**.

Status akan berjalan dari **Provisioning** → **Running**. Biasanya 1–3
menit karena connector mengambil snapshot awal lebih dulu.

### Kalau gagal

| Gejala | Penyebab |
|---|---|
| connection timeout | tunnel mati, atau port sudah berganti — cek langkah 1 |
| SSL / TLS error | SSL mode masih `require`, harus `prefer` |
| `Unknown configuration` | key `_comment` / `_sslmode_note` ikut ter-paste |
| `replication slot already exists` | hapus dulu: `docker exec wms-cdc-demo psql -U postgres -d wms -c "SELECT pg_drop_replication_slot('wms_cdc_slot');"` |
| `permission denied for table` | prasyarat Postgres belum dijalankan — lihat README |

Pesan error lengkap ada di tab **Logs** pada halaman connector. Itu
biasanya spesifik dan langsung menunjuk penyebabnya.

---

## Langkah 5 — Verifikasi topic

Cluster → **Topics**. Harus muncul empat:

```
wms.public.stock_movements
wms.public.products
wms.public.locations
wms.public.warehouses
```

Klik `wms.public.warehouses` → tab **Messages**. Harus ada 15 record
(Jakarta, Balikpapan, Kendari, Sofifi, Angsana, 2 service point, 8 VHS).

### Kalau muncul `__raw__` alih-alih data

Kalau isi message tampak seperti ini:

```json
{ "__raw__": "AAABhqgMClNQTVRXMlNlcnZpY2UgUG9pbnQgTXVhcmEgVGV3ZWg..." }
```

**Datanya tidak rusak, dan tidak ada yang salah dengan connector-mu.**

Itu base64 dari byte mentah Kafka. Message browser menampilkannya apa
adanya karena belum berhasil mengambil schema untuk record itu — biasanya
karena viewer-nya membuka topic beberapa saat sebelum schema selesai
terdaftar.

Buktikan sendiri — salin isi `__raw__` lalu:

```bash
.venv/bin/python scripts/decode_message.py '<paste base64 di sini>' --table warehouses
```

Hasilnya:

```
magic byte  : 0  (0 = Confluent wire format, OK)
schema id   : 100008

decoded value:
{
  "id": 6,
  "code": "SPMTW",
  "name": "Service Point Muara Teweh",
  "created_at": "2026-09-22 09:43:15.087126+00:00",
  "__deleted": "false",
  "__op": "r",
  "__source_ts_ms": 1790072606903
}

__op = 'r'  ->  read (initial snapshot)
__source_ts_ms present -> the ExtractNewRecordState SMT is applied correctly
```

Justru ini kabar baik, tiga sekaligus:

- **Avro bekerja** — byte pertama `0` menandakan Confluent wire format,
  dan schema ID `100008` berarti schema sudah terdaftar di Schema Registry
- **SMT-mu benar** — field `__op` dan `__source_ts_ms` muncul, artinya
  `ExtractNewRecordState` dengan `add.fields` sudah aktif
- **`__op = 'r'`** berarti record ini dari snapshot awal, sesuai
  `snapshot.mode: initial`

Skripnya mendukung `--table warehouses|products|locations|stock_movements`.

Agar UI ikut menampilkannya terbaca, coba salah satu:

- **Refresh halaman** dan buka ulang topic — paling sering langsung beres
- Cek **Environments → Stream Governance → Schemas**; harus ada subject
  `wms.public.warehouses-value`. Kalau ada, Schema Registry sehat
- Pastikan kamu membuka topic dari cluster dan environment yang benar

Yang terpenting: **jangan ubah connector-mu.** Ini murni soal tampilan.
Flink membaca schema langsung dari Schema Registry, bukan dari message
browser, jadi langkah 6 akan jalan normal meski UI masih menampilkan
`__raw__`.

Kalau topic kosong padahal connector *Running*, cek tab **Logs** di
halaman connector — pesan errornya biasanya spesifik.

Tidak ada topic sama sekali berarti snapshot belum jalan; tunggu satu
menit lagi sebelum menduga ada masalah.

---

## Langkah 6 — Flink SQL

Console → **Stream Processing** → *Create compute pool* (ukuran terkecil
cukup) → buka **workspace**.

Set catalog = environment-mu, database = cluster `wms-dsp`.

Buka `flink/01_inventory_pipeline.sql` dan jalankan **satu per satu**,
jangan sekaligus:

**Statement 0 (sanity check).** Jalankan ini lebih dulu. Kalau tidak ada
baris keluar, CDC belum mengalir dan sisanya tidak ada gunanya —
kembali ke langkah 5.

**Statement 1** (`CREATE TABLE` + `INSERT INTO` untuk `wms.inventory.on_hand`).
Ini job berjalan terus. Biarkan hidup.

**Statement 2** (`wms.alerts.low_stock`). Sama, biarkan hidup.

Setelah keduanya jalan, cek hasilnya:

```sql
SELECT * FROM `wms.alerts.low_stock`;
```

Dengan data yang sudah ter-seed, kamu harus melihat 3 baris:

| warehouse_code | sku | on_hand | min_stock | severity |
|---|---|---|---|---|
| BPN | FLT-OIL-1045 | 12 | 60 | CRITICAL |
| SFF | FLT-OIL-1045 | 25 | 60 | CRITICAL |
| KDI | LUB-HD40-20L | 0 | 15 | STOCKOUT |

Jakarta tidak muncul — stoknya 380 dengan reorder point 40. Itu memang
benar, bukan data yang hilang.

Statement 3 dan 4 (velocity per jam, dan pantauan selisih opname)
opsional, tapi bagus untuk didemokan — keduanya memperlihatkan hal yang
tidak bisa diberikan laporan batch harian.

---

## Langkah 7 — Picu alert secara live

Ini bagian yang paling meyakinkan kalau kamu diminta demo langsung:
tunjukkan alert muncul saat kamu memasukkan data.

Biarkan query `SELECT * FROM wms.alerts.low_stock;` terbuka di Flink,
lalu di terminal lain:

```bash
docker exec wms-cdc-demo psql -U postgres -d wms -c "
INSERT INTO stock_movements (product_id, location_id, type, qty, reference_no, user_id, created_at)
SELECT p.id, l.id, 'out', -350, 'SO-DEMO-LIVE', 1, NOW()
FROM products p
JOIN warehouses w ON w.code = 'JKT'
JOIN locations  l ON l.warehouse_id = w.id
WHERE p.sku = 'BLT-V-A55';"
```

Jakarta turun dari 380 ke 30, di bawah reorder point 40 — alert baru
muncul dalam hitungan detik. Itu juga yang menghidupkan grafik lineage.

Hasil ini sudah kuverifikasi lewat dry-run (transaksi + rollback, jadi
baseline tetap utuh) — setelah insert, tabel alert berisi 4 baris:

| warehouse | sku | on_hand | min_stock | severity |
|---|---|---|---|---|
| BPN | FLT-OIL-1045 | 12 | 60 | CRITICAL |
| **JKT** | **BLT-V-A55** | **30** | **40** | **WARNING** ← baru |
| KDI | LUB-HD40-20L | 0 | 15 | STOCKOUT |
| SFF | FLT-OIL-1045 | 25 | 60 | CRITICAL |

Mau mencoba sendiri tanpa mengubah data? Jalankan dry-run-nya:

```bash
docker cp scripts/dryrun_live_demo.sql wms-cdc-demo:/tmp/
docker exec wms-cdc-demo psql -U postgres -d wms -f /tmp/dryrun_live_demo.sql
```

Perhatikan bahwa Jakarta jatuh ke WARNING, bukan CRITICAL: 30 masih di
atas setengah reorder point. Gradasi itu memang yang diinginkan — stok
menipis tidak layak membangunkan orang tengah malam.

---

## Langkah 8 — HTTP Sink

Belum punya endpoint penerima? Buka https://webhook.site, salin URL unik
yang diberikan. Itu cukup untuk demo dan kamu bisa melihat payload yang
masuk.

Edit `connectors/http-sink-alerts.json`:

```json
{
  "kafka.api.key":       "<dari langkah 3>",
  "kafka.api.secret":    "<dari langkah 3>",
  "http.api.base.url":   "https://webhook.site",
  "api1.http.api.path":  "/<token-unikmu>"
}
```

Kalau pakai webhook.site, hapus baris
`api1.http.request.sensitive.headers` — tidak ada token yang perlu
dikirim. Hapus juga `_comment`.

Connectors → *HTTP Sink V2* → **Switch to JSON** → tempel → Launch.

Begitu status *Running*, payload alert akan terlihat di webhook.site.
Itu bukti end-to-end: Postgres → CDC → Kafka → Flink → HTTP.

---

## Langkah 9 — Screenshot Stream Lineage

**Ini yang diminta form, dan urutannya sengaja ditaruh di akhir.**

Console → menu kiri → **Stream Lineage**.

> **Kerjakan ini dalam 10 menit setelah langkah 7.** Paket Essentials
> hanya menyimpan lineage point-in-time 10 menit terakhir. Lewat dari itu,
> grafiknya menyusut dan node connector/Flink bisa hilang. Kalau kamu
> sudah terlalu lama menunggu, ulangi langkah 7 untuk menyegarkan trafik.

Pilih topic `wms.public.stock_movements` sebagai titik masuk. Grafik akan
menampilkan jalur penuh:

```
Postgres → CDC connector → topics → Flink → alert topic → HTTP sink
```

Tunggu ±1 menit supaya semua node muncul. Grafik digambar dari trafik
yang teramati, bukan dari konfigurasi — kalau kamu screenshot terlalu
cepat, banyak node belum tampil.

Lalu:

1. Screenshot seluruh jendela (pastikan node connector dan Flink terlihat)
2. Upload ke https://imgbb.com — tanpa perlu akun
3. Salin **direct link**-nya

Kalau pakai Google Drive, set sharing ke *anyone with the link*, kalau
tidak juri akan melihat halaman izin.

> `[NO AI Usage Allowed]` berlaku di field ini: harus screenshot asli
> console Confluent Cloud milikmu. Jangan gambar hasil generate AI atau
> mock-up.

---

## Langkah 10 — Isi form

| Field | Isi |
|---|---|
| Current location | `Jakarta, Indonesia` (sesuaikan) |
| First / Last name | — |
| Email Confluent Cloud | email akun Confluent-mu |
| Job title | — |
| Company name | — |
| GitHub repo | `https://github.com/mfadlanabdillah/wms-stream` |
| Deskripsi app | versi **medium** di `docs/SUBMISSION.md` — sunting agar terdengar seperti kamu |
| Screenshot / Lineage | link imgbb dari langkah 9 |
| Connector(s) | `PostgreSQL CDC Source V2 (Debezium)` dan `HTTP Sink V2` |
| Paste schema | isi `schemas/stock_movements-value.avsc` |

Kalau kamu punya angka yang lebih presisi soal dampak bisnis — berapa
sering stockout terjadi, nilai order yang gagal karena stok telat
diketahui — masukkan ke deskripsi. Juri menilai "Most Impactful App",
dan angka nyata jauh lebih kuat daripada klaim kualitatif.

---

## Setelah selesai

Matikan yang mengekspos database ke internet:

```bash
# Ctrl-C di terminal tunnel, lalu:
docker compose -f docker-compose.demo.yml down
```

Hapus juga connector di Confluent Cloud kalau tidak dipakai lagi — kredit
trial terus terpakai selama connector berstatus *Running*.

---

## Kalau mentok

| Masalah | Langkah |
|---|---|
| Connector tidak bisa konek ke DB | Cek tunnel hidup dan port-nya masih sama; lihat langkah 1 |
| `replication slot already exists` | `docker exec wms-cdc-demo psql -U postgres -d wms -c "SELECT pg_drop_replication_slot('wms_cdc_slot');"` |
| Topic ada tapi Flink kosong | Set catalog/database di workspace Flink; jalankan statement 0 |
| Lineage tampak kosong | Belum ada trafik — jalankan langkah 7 lalu tunggu semenit |
| Alert tidak muncul | Cek `min_stock > 0` di tabel products; SKU dengan reorder point 0 memang diabaikan |

Kalau tunnel mati di tengah jalan, connector akan gagal tapi tidak hilang:
jalankan ulang tunnel, update `database.port` di connector config, lalu
*Resume*.
