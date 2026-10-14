# Trip-count spot check: top 11 cities

Checked 2026-10-12. This compares the per-year `trip_count` in `analysis/<city>/summary.json`
against published figures, using the official operator or government source wherever one exists
and news coverage otherwise.

## How to read this

- **Ours** is `trip_count` from `summary.json` (or, for sub-year periods, the same parquet data
  counted over that window).
- **Reference** is the published number. A leading `~` means the source gave a rounded figure
  ("over 5.8 million"), or the reference was derived from a daily average or growth rate. The
  Note column says which.
- **Diff** is `(ours − reference) / reference`. For a rounded reference, read anything within a
  few percent as agreement.
- Partial years are excluded unless the source covers the same window. Partial years come from
  data starting or ending mid-year (e.g. 2026 everywhere, Toronto 2016, Taipei 2020).
- Some references are built from the same open data we ingest: Boston's A Better City table, and
  the sum of Citi Bike's monthly reports in part. They check our *processing*, not whether the
  source is complete. TfL, SEMOVI, Taipei DBAS, Seoul, and the press figures are independent
  counts.

## Summary

| City | Verdict | Finding |
|---|---|---|
| Boston | ✅ Matches | Within 0.7% of Bluebikes' published annual totals for 2015–2024. 2018 is exact. |
| Seoul | ✅ Matches | Matches Seoul's published totals to the reported precision for 2019–2024. |
| Toronto | ✅ Matches | Within ~2% of every year-end figure for 2019–2025. |
| London | ✅ Matches, 2 known gaps | Within ~1.5% of TfL's own daily-hire series, except 2012 (−4%, source-side) and 2017 (−2.3%, one week not ingested). The 2023 drop to 8.5M is real. |
| Guadalajara | ✅ Matches | Within 0.5% of IIEG and WRI figures. Few annual references exist after 2019. |
| New York City | ✅ Matches | Within 0.05% of NYC DOT's annual totals for 2022–2024, and ~1–2% of Citi Bike's own monthly reports. 2013–14 run ~8% low because the public files drop trips under 60 s and staff trips. |
| Montreal | ✅ Plausible | Within ~4% of every BIXI figure, but all references are rounded. 2016–2021 run consistently 2–4% low. |
| Washington DC | ❌ **Pipeline bug** | 2018 and 2023 are each missing a month. The source misnamed two files, and `file_matcher` silently skipped them. Other years match to within tens of trips. |
| Mexico City | ✅ Fixed (2026-10-13) | Matches SEMOVI's official monthly table almost exactly for 2010–2024. 2025 is within 700 trips of the published total. The only remaining gap is Oct 2022 (−28%), which is short in the source file itself. |
| Taipei | ⚠️ **Source discontinuity** | 20–29% below official totals from H2 2023 to H1 2024, and 2–8% below otherwise. No days or stations are missing, so the per-trip open data appears to undercount. Needs investigation. |
| Chicago | ⚠️ Not verifiable after 2021 | The city publishes Divvy totals that include scooters, and the public trip data excludes them. One month (Sep 2024) does include 144,337 scooter rows. |

## Pipeline issues found

These are actionable. Each was confirmed against `raw/` or parquet, not just inferred from the
totals.

1. **Washington DC: two trip files skipped by `file_matcher`.** `file_matcher:
   capitalbikeshare-tripdata` doesn't match `202302-captialbikeshare-tripdata.csv.gz` (the source
   misspells "capital") or `201801_capitalbikeshare_tripdata.csv.gz` (underscores). Both sit in
   `raw/`, but no parquet is written for them. That loses 207,591 trips (Feb 2023) and 168,590 trips
   (Jan 2018). With the Feb file included, 2023 would be 4,467,334 vs GGWash's 4,469,358. Nothing
   reports a trip file that matched no pattern, and that silence is how this got through.
2. **Mexico City: wrong files linked from the source page (fixed 2026-10-13).** Ecobici's
   page linked January's file under the "2025-02" label, linked a May 2024 file that stops at
   May 15, and linked a Sep 2022 file with 129,541 Excel-mangled times (which left those trips
   with no departure time, so they fell into a `None` year). Fuller files for all three were on
   the server but unlinked; the WordPress media API (`/wp-json/wp/v2/media`) lists every upload.
   The downloader now takes per-month `url_overrides` from the city YAML, and fails if two month
   labels resolve to the same file. Oct 2022 is still 28% short because the source file itself
   has only 232,277 of SEMOVI's 322,787 trips, and no fuller copy is on the server.
3. **London: week 49 of 2017 not ingested.** TfL publishes
   `49JourneyDataExtract15Mar2017-21Mar2017` only as `.xlsx`. It's in `download/`, but the pipeline
   only reads CSV. That leaves 15–21 Mar 2017 missing, and March 2017 21% short (~173k trips).
4. **Chicago: e-scooter rows in a single month.** Divvy's Sep 2024 file includes 144,337
   `electric_scooter` rows, which end up with a null `bike_type`. No other month has scooters, so 2024
   is inflated relative to its neighbours. Either filter them or keep them consistently; today it's
   accidental.
5. **Taipei: under-count in the source data (not yet explained).** Official totals come from the
   Taipei DBAS bulletins, which count Taipei-city YouBike rentals. Against those we're −8% for H1
   2023, −20% for 2023, −29% for H1 2024, then −2% for Jan–Sep 2025. Every day is present, and the
   station count rises smoothly. Trips per day jump from ~126k (Jun 2024) to ~184k (Jul 2024) inside
   the source data. Possible causes worth checking: a change in what the open dataset includes in
   mid-2024 (cross-city trips, a bike class), or our parse dropping a category of rows.

## Per-city detail

### Taipei

Official figures come from the Taipei City Department of Budget, Accounting and Statistics weekly
bulletins (市政統計週報), citing the Department of Transportation. They cover rentals within Taipei
City only, and are published as partial-year totals or daily averages, so the windows below follow
the bulletins.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| Q1 2023 | 9,298,254 | ~10,080,000 | -7.8% | [T2] | 11.2萬/day × 90 days |
| H1 2023 | 19,667,222 | 21,362,000 | -7.9% | [T3] | 2,931.4萬 (H1 2024) − 795.2萬 (YoY increase) |
| 2023 | 36,818,842 | ~46,000,000 | -20.0% | [T1] | “4千6百萬次”, councillor's figure |
| H1 2024 | 20,720,084 | 29,314,000 | -29.3% | [T3] |  |
| Jan–Sep 2024 | 38,228,815 | 47,324,000 | -19.2% | [T4] | 5,874.4萬 (2025) − 1,142.0萬 (YoY increase) |
| Jan–Sep 2025 | 57,553,947 | 58,744,000 | -2.0% | [T4] |  |
| H1 2026 | 39,451,920 | ~42,354,000 | -6.9% | [T5] | 23.4萬/day × 181 days |

Sources:
- [T1] Mirror Media, 2024-07-04, quoting Taipei City Councillor 汪志冰: "去年4千6百萬次更是有始以來最高記錄". <https://www.mirrormedia.mg/story/amp/20240704soc002>
- [T2] 臺北市政府主計處 市政統計週報 第1230號, 2023-05-03. <https://www-ws.gov.taipei/001/Upload/367/relfile/46908/8974682/29644daa-32a3-4f1b-b100-33ba280aa47d.pdf>
- [T3] 市政統計週報 第1293號, 2024-07-31 ("113年1月至6月市區公共自行車租借達2,931.4萬次，較上年同期增加795.2萬次"). <https://www-ws.gov.taipei/001/Upload/367/relfile/46908/9250140/f1e853bc-8ba2-420e-9ba5-9a069588b945.pdf>
- [T4] 市政統計週報 第1359號, 2025-11-19 ("114年1至9月臺北市區公共自行車租借達5,874.4萬次，較上年同期增加1,142.0萬次"). <https://www-ws.gov.taipei/001/Upload/367/relfile/46908/9502476/61b91419-0e68-434d-8630-24c8b0f5bec4.pdf>
- [T5] 市政統計週報 第1380號, 2026-07-29 ("115年1至6月市區公共自行車平均每日租借23.4萬次"). <https://www-ws.gov.taipei/001/Upload/367/relfile/46908/9679852/02a2def1-a758-4dc8-93dd-8bbdb1431e23.pdf>

### New York City

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2013 (Jun–Dec) | 5,614,888 | 6,036,059 | -7.0% | [N1] | public files drop <60 s and staff/test trips |
| 2014 | 8,081,216 | 8,791,987 | -8.1% | [N1] | same as 2013 |
| 2015 | 9,937,969 | 10,068,280 | -1.3% | [N1] |  |
| 2016 (7 of 12 months) | 7,033,597 | 7,015,074 | +0.3% | [N1] | Apr/May/Jul/Aug/Sep reports are Google Drive links, not retrieved |
| 2017 | 16,364,657 | 16,347,284 | +0.1% | [N1] |  |
| 2018 | 17,548,339 | 17,742,410 | -1.1% | [N1] | Dec 2018 report −20% vs ours (all 31 days present; see notes) |
| 2019 | 20,552,228 | 20,984,695 | -2.1% | [N1] |  |
| 2020 | 19,562,792 | 19,783,517 | -1.1% | [N1] |  |
| 2021 | 27,129,753 | 27,353,093 | -0.8% | [N1] | July report has a typo (“3,0827,926”); used its 99,610/day × 31 |
| 2022 | 29,838,442 | 29,838,311 | +0.0% | [N2] | operating reports sum: 30,251,800 (−1.4%) |
| 2023 | 35,107,120 | 35,088,551 | +0.1% | [N2] | operating reports sum: 35,002,030 (+0.3%) |
| 2024 | 44,303,006 | 44,292,951 | +0.0% | [N2] | operating reports sum: 44,173,196 (+0.3%) |
| 2025 | 45,771,161 | 46,668,570 | -1.9% | [N1] | every month within 3%; uniform shortfall |

Notes:
- The [N1] figures sum the "There were N trips in <Month>" line from all 150 Citi Bike monthly
  operating reports I could retrieve.
- Dec 2018 (report 1,276,332 vs our 1,016,505) and Apr 2022 (2,742,568 vs 2,261,339) disagree
  sharply. Our data has every day of both months, and the Dec 2018 report's own stats look
  inconsistent (29-minute average trip covering 1.0 mile). I treat these as report-side.
- The 2025 shortfall is spread evenly. No month is off by more than 3%.

Sources:
- [N1] Citi Bike monthly operating reports (one PDF per month, Jun 2013 onward). <https://citibikenyc.com/system-data/operating-reports>
- [N2] NYC DOT Citi Bike quarterly reports, NYC Government Publications Portal (NYC-only trips over 60 seconds). 2022 = 29,838,311, 2023 = 35,088,551, 2024 = 44,292,951. <https://a860-gpp.nyc.gov/> The portal returned HTTP 403 for direct downloads, so these figures come from the reports' search-indexed text, and I couldn't confirm which document ID holds each one.

### Seoul

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2015 | 113,695 | ~113,000 | +0.6% | [S5] | “11만 3천건”, the launch year |
| 2019 | 19,074,622 | ~19,070,000 | +0.0% | [S1] | “1907만여 건” |
| 2020 | 23,705,064 | 23,705,000 | +0.0% | [S2] | “2,370만 5,000건” |
| 2021 | 32,053,322 | ~32,050,000 | +0.0% | [S3] | “3205만” |
| 2022 | 40,950,393 | ~40,950,000 | +0.0% | [S3] | “4095만 건” |
| 2023 | 44,905,158 | ~44,900,000 | +0.0% | [S1] | “4490만여 건” |
| 2024 | 43,851,938 | ~43,850,000 | +0.0% | [S4] | “4,385만여 건” |

No published annual totals were found for 2016–2018 or 2025.

Sources:
- [S1] 비즈한국, 2024-03-06 ("2019년 1907만여 건에서 2023년에는 2배가 넘는 4490만여 건"). <http://bizhankook.com/articles/27228.html>
- [S2] 서울시 미디어허브 (Seoul Metropolitan Government), 2021-01-20 ("2,370만 5,000건"). <https://mediahub.seoul.go.kr/archives/2000183>
- [S3] 뉴시스, 2023-05-21 ("지난해 4095만 건으로 2021년 3205건 대비 27.7% 증가"; the article prints 2021 as "3205건", which means 3205만). <https://mobile.newsis.com/view/NISX20230519_0002309932>
- [S4] 서울시 교통 news, Nov 2025 ("2024년 이용건수는 4,385만여 건으로, 10년 전(11만 3천건)에 비해 400배"). <https://news.seoul.go.kr/traffic/archives/515711>
- [S5] Same release as [S4], for the 2015 figure. Also reported by 헤럴드경제, 2025-11-11. <https://www.heraldk.com/article/2025111120450440618>

### London

The reference is TfL's own daily hire count, summed by calendar year. It's the official series,
covering 2010-07-30 to 2026-08-31.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2012 | 9,142,379 | 9,519,283 | -4.0% | [L1] | Apr–Aug 2012 run 6–10% low; all source files present |
| 2013 | 8,004,165 | 8,045,459 | -0.5% | [L1] |  |
| 2014 | 9,895,412 | 10,023,897 | -1.3% | [L1] |  |
| 2015 | 9,799,277 | 9,871,839 | -0.7% | [L1] |  |
| 2016 | 10,316,916 | 10,303,637 | +0.1% | [L1] |  |
| 2017 | 10,205,185 | 10,446,044 | -2.3% | [L1] | week 49 (15–21 Mar) published only as .xlsx; not ingested |
| 2018 | 10,441,884 | 10,567,540 | -1.2% | [L1] |  |
| 2019 | 10,310,063 | 10,424,955 | -1.1% | [L1] |  |
| 2020 | 10,287,040 | 10,434,167 | -1.4% | [L1] |  |
| 2021 | 10,821,289 | 10,941,264 | -1.1% | [L1] |  |
| 2022 | 11,419,294 | 11,505,872 | -0.8% | [L1] | TfL press release: 11,506,889 [L2] |
| 2023 | 8,514,455 | 8,531,168 | -0.2% | [L1] | the drop is real (TfL's own count) |
| 2024 | 8,755,152 | 8,768,238 | -0.1% | [L1] |  |
| 2025 | 9,068,241 | 9,066,554 | +0.0% | [L1] |  |

Sources:
- [L1] TfL, "Number of Bicycle Hires" (daily), London Datastore. <https://data.london.gov.uk/dataset/number-bicycle-hires> (file: `tfl-daily-cycle-hires.xlsx`). TfL notes that from Aug 2017, daily figures can change slightly for about two weeks after publication.
- [L2] TfL press release, Jan 2023: "record-breaking year in 2022 with 11,506,889 hires". <https://nile.tfl.gov.uk/info-for/media/press-releases/2023/january/london-s-iconic-santander-cycles-hire-scheme-sees-yet-another-record-breaking-year-in-2022>

### Mexico City

Updated 2026-10-13, after the `url_overrides` fix. The "Ours" column reflects the rebuilt data.

The reference is SEMOVI's official monthly trip table (Tabla 3), which covers Feb 2010 – Jul 2024,
with press figures citing SEMOVI or system statistics after that.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2010 (Feb–Dec) | 841,081 | 841,079 | +0.0% | [M1] |  |
| 2011 | 2,542,973 | 2,542,963 | +0.0% | [M1] |  |
| 2012 | 2,737,919 | 2,737,917 | +0.0% | [M1] |  |
| 2013 | 6,515,332 | 6,515,328 | +0.0% | [M1] |  |
| 2014 | 7,952,252 | 7,952,247 | +0.0% | [M1] |  |
| 2015 | 9,192,072 | 9,192,076 | -0.0% | [M1] |  |
| 2016 | 9,069,539 | 9,069,543 | -0.0% | [M1] |  |
| 2017 | 9,498,067 | 9,532,666 | -0.4% | [M1] | Apr 2017 −4.7% |
| 2018 | 8,540,264 | 8,540,258 | +0.0% | [M1] |  |
| 2019 | 8,415,437 | 8,434,936 | -0.2% | [M1] |  |
| 2020 | 4,031,937 | 4,031,802 | +0.0% | [M1] |  |
| 2021 | 4,295,438 | 4,295,491 | -0.0% | [M1] |  |
| 2022 | 5,213,813 | 5,312,012 | -1.8% | [M1] | Oct 2022 −28%: the source file is short (Sep fixed) |
| 2023 | 12,177,490 | 12,177,544 | -0.0% | [M1] | year total matches; individual months are offset (Jan +9%, Dec −16%) |
| Jan–Jul 2024 | 12,821,753 | 12,821,619 | +0.0% | [M1] | fixed: May 2024 now uses the full re-upload |
| 2024 | 22,242,891 | ~22,000,000 | +1.1% | [M2] | “más de 22 millones” |
| 2025 | 20,134,218 | 20,134,900 | -0.0% | [M3] | fixed: Feb 2025 was mislinked |
| 1 Jan–24 Jul 2026 | 10,516,788 | 10,530,278 | -0.1% | [M3] |  |

Sources:
- [M1] SEMOVI, "Datos del desempeño de ECOBICI 2019–2024", Oct 2024, Tabla 3 "Viajes mensuales de ECOBICI (2010-2024)". <https://ecobici.cdmx.gob.mx/wp-content/uploads/2024/10/Datos-del-desempeno-de-ECOBICI-2019-2024.pdf>
- [M2] Expansión Política, 2025-01-08, citing Semovi ("superó los 22 millones de viajes"; 2023 "poco más de 12 millones"). <https://politica.expansion.mx/cdmx/2025/01/08/el-sistema-ecobici-rompe-record-en-2024-alcanza-mas-de-22-millones-de-viajes>
- [M3] Expansión Política, 2026-07-31, citing system statistics ("Durante 2025 se realizaron 20 millones 134,900 viajes"; 10,530,278 trips 1 Jan – 24 Jul 2026). <https://politica.expansion.mx/cdmx/2026/07/31/el-pan-llama-a-ampliar-el-sistema-ecobici-con-30-000-bicicletas-en-la-cdmx>

### Montreal

All BIXI references are rounded press figures. From 2023 on, BIXI's totals may include its newer
off-island cities, and the 2025 figure is explicitly for all 13 cities.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2016 | 4,000,080 | ~4,100,000 | -2.4% | [B1] | “4,1 millions” |
| 2018 | 5,277,541 | ~5,370,000 | -1.7% | [B2] | derived: 2019 was “+8%” over 2018 |
| 2019 | 5,597,845 | ~5,800,000 | -3.5% | [B2] | “more than 5.8 million” |
| 2021 | 5,566,351 | ~5,800,000 | -4.0% | [B3] | “5,8 millions” |
| 2022 | 8,967,927 | ~8,990,000 | -0.2% | [B3] | derived: +55% over 2021 |
| 2023 | 11,790,439 | ~11,700,000 | +0.8% | [B4] | “11,7 millions” |
| 2024 | 13,274,911 | ~13,000,000 | +2.1% | [B5] | “more than 13 million” |
| 2025 | 14,249,953 | ~14,500,000 | -1.7% | [B6] | “quelque 14,5 millions”, across all 13 BIXI cities |

Sources:
- [B1] Pieuvre.ca, 2017-06-07, "BIXI renoue avec la croissance en 2016" (citing BIXI's financial report). <https://pieuvre.ca/?p=11106>
- [B2] Daily Hive, 2019-11-15, quoting BIXI: "more than 5.8 million trips this year, a significant increase (+8%) compared to last year's record". <https://dailyhive.com/montreal/bixi-bike-2019-season-records>
- [B3] Noovo, 2022-11-24: "le précédent record de 5,8 millions de déplacements enregistré en 2021", with 2022 ridership up 55%. <https://www.noovo.info/nouvelle/bixi-hausses-de-lachalandage-du-membership-et-du-nombre-dutilisateurs-en-2022.html> (2021's 5.8M is also in Noovo, 2022-05-18: <https://www.noovo.info/nouvelle/ca-roule-pour-bixi-qui-bat-plusieurs-records.html>)
- [B4] Noovo, 2024-04-02: "11,7 millions de déplacements". <https://www.noovo.info/nouvelle/les-bixi-toujours-aussi-populaires-le-projet-de-velo-hivernal-reconduit.html>
- [B5] Planetizen, "Montreal Bike Share Breaks Ridership Record With 13 Million Rides" (citing the Montreal Gazette). <https://planetizen.com/node/132969>
- [B6] Le Devoir, 2026: "Bixi a connu une année record en 2025, avec quelque 14,5 millions de déplacements enregistrés dans les 13 villes". <https://www.ledevoir.com/actualites/transports-urbanisme/969854/lancement-saison-hatif-bixi> The page returned 403, so I took the quote from search-indexed text. The figure first appeared as a projection on 98.5 FM: <https://www.985fm.ca/audio/742803/14-5-millions-de-deplacements-en-bixi-en-2025>

### Chicago

From 2022 on, the City of Chicago reports Divvy totals that combine bikes and Divvy e-scooters. The
public trip data contains bikes only, except for Sep 2024. So those totals can't validate our counts,
and the gap (~0.8–1.2M a year) is roughly the scooter volume. No bike-only official total was found for
2022–2025.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2017 | 3,829,014 | ~3,800,000 | +0.8% | [C1] | “3.8 million” |
| 2020 | 3,541,683 | ~3,400,000 | +4.2% | [C2] | “about 3.4 million” |
| 2021 | 5,595,063 | ~5,500,000 | +1.7% | [C2] | “roughly 5.5 million” |
| 2023 | 5,719,877 | 6,603,071 | -13.4% | [C3] | **not comparable**: city figure includes Divvy scooters |
| 2024 | 5,860,621 | 6,681,480 | -12.3% | [C3] | **not comparable**: includes scooters |
| 2025 | 5,552,964 | ~6,800,000 | -18.3% | [C4] | **not comparable**: includes scooters |

Sources:
- [C1] Streetsblog Chicago, 2018-01-05: "3.8 million trips were taken in 2017". <https://chi.streetsblog.org/2018/01/05/2017-was-another-record-year-for-divvy-ridership/>
- [C2] City Colleges of Chicago, 2022-01-05 (CDOT/Divvy announcement): roughly 5.5 million rides in 2021, up from about 3.4 million in 2020. <https://success.ccc.edu/2022/01/05/city-colleges-cdot-and-divvy-unveil-new-bikeshare-station-at-olive-harvey-college-offer-no-cost-divvy-memberships-to-students/> (taken from search-indexed text; the page wasn't fetched)
- [C3] CDOT via Streetsblog Chicago, 2025-01-09: 6,681,480 Divvy bike and scooter trips in 2024, up from 6,603,071 in 2023. <https://chi.streetsblog.org/2025/01/09/cdot-chicago-set-a-record-in-2024-with-10m-bike-and-scooter-share-rides-actually-make-that-11m>. Also CBS Chicago: <https://www.cbsnews.com/chicago/news/chicago-divvy-bikes-shared-scooters-new-record-2024/>
- [C4] WTTW, 2026-01-15: "6.8 million Divvy trips in 2025". <https://news.wttw.com/2026/01/15/divvy-and-lime-saw-highest-ridership-record-2025-nearly-13m-bike-and-scooter-trips>

### Washington DC

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2022 | 3,476,782 | ~3,478,100 | -0.0% | [D1] | derived: 2023 was “+28.5%” over 2022 |
| 2023 | 4,259,743 | 4,469,358 | -4.7% | [D1] | **Feb 2023 file skipped**; with it: 4,467,334 (−0.05%) |
| 2024 | 6,114,359 | 6,114,324 | +0.0% | [D2] |  |
| 2025 | 6,662,659 | 6,662,647 | +0.0% | [D3] |  |

2018 has no published reference, but our 2018 is short by the 168,590 trips in the skipped Jan 2018
file (see Pipeline issues). With that file, 2018 would be 3,542,684.

Sources:
- [D1] Greater Greater Washington, "Bikeshare Beat: CaBi wraps up a year of historic ridership in December": 4,469,358 rides in 2023, "an increase of 28.5% from 2022". <https://ggwash.org/view/92292/bikeshare-beat-cabi-wraps-up-a-year-of-historic-ridership-in-december>
- [D2] Greater Greater Washington, "Bikeshare Beat: Capital Bikeshare breaks all-time annual ridership record" (Nov 2025): the prior record was 6,114,324 in 2024. <https://ggwash.org/view/101853/bikeshare-beat-capital-bikeshare-breaks-all-time-annual-ridership-record>. DC.gov also says "more than 6 million" for 2024: <https://beta.dc.gov/news/bowser-administration-celebrates-15th-anniversary-capital-bikeshare>
- [D3] Greater Greater Washington, "Bikeshare Beat: CaBi broke annual record with nearly 6.7 million rides in 2025": 6,662,647. <https://ggwash.org/view/102205/bikeshare-beat-capital-bikeshare-broke-records-with-nearly-6.7-million-rides-in-2025>

### Toronto

2016 isn't comparable, because the open data starts 2016-07-01. Ours is 585,530, against 834,235 for the
full year. 2017–2018 have no independent reference: Wikipedia's figures for those years are taken from
the same open data.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2019 | 2,439,517 | ~2,400,000 | +1.6% | [R1] | “over 2.4 million” |
| 2020 | 2,911,308 | ~2,900,000 | +0.4% | [R2] | “2.9 million” |
| 2021 | 3,575,182 | ~3,500,000 | +2.1% | [R1] | “3.5 million” |
| 2022 | 4,620,469 | ~4,600,000 | +0.4% | [R1] | “4.6 million” |
| 2023 | 5,713,141 | ~5,700,000 | +0.2% | [R1] | “5.7 million” |
| 2024 | 6,953,094 | ~6,900,000 | +0.8% | [R3] | “6.9 million” |
| 2025 | 7,812,520 | ~7,800,000 | +0.2% | [R4] | “7.8 million” |

Sources:
- [R1] Bike Share Toronto year-end posts, as cited in Wikipedia's article: "2019: Celebrating a Year of Milestones" (2020-01-02), "An incredible 2021 for Bike Share Toronto" (2022-01-12), "A look back on 2022" (2023-01-17), and the 2023 review (2024-01-25). <https://en.wikipedia.org/wiki/Bike_Share_Toronto>
- [R2] Bike Share Toronto, "It feels great to say, 'See you later 2020!'": "closed off 2020 with 2.9 million trips". <https://bikesharetoronto.com/?p=5871> (taken from search-indexed text)
- [R3] City of Toronto background file (Toronto Parking Authority), 2025: "achieved ridership of 6.9 million (+1.2 m v 2023)". <https://www.toronto.ca/legdocs/mmis/2025/au/bgrd/backgroundfile-257027.pdf>
- [R4] City of Toronto background file, 2026: "Bike Share Toronto recorded 7.8 million rides in 2025". <https://www.toronto.ca/legdocs/mmis/2026/au/bgrd/backgroundfile-287722.pdf>. Also the TPA board deck: <https://www.toronto.ca/legdocs/mmis/2026/pa/bgrd/backgroundfile-285022.pdf>

### Guadalajara

Few annual totals are published. The only official full-year figure found is for 2019. IIEG's monthly
figures inside that report also match ours closely: Oct 2019 423,640 vs 422,473, and Dec 2019 318,538 vs
317,404.

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| Dec 2014–Nov 2019 | 11,675,023 | 11,738,700 | -0.5% | [G2] |  |
| Jan–Oct 2019 | 3,941,365 | 3,944,525 | -0.1% | [G3] |  |
| 2019 | 4,636,652 | 4,646,578 | -0.2% | [G1] |  |
| Launch–May 2026 (cumulative) | 37,451,700 | ~37,000,000 | +1.2% | [G4] | “más de 37 millones” |

Sources:
- [G1] IIEG Jalisco, "MiBici, enero a diciembre de 2019" (Jan 2020): "los usuarios de MiBici realizaron 4'646,578 viajes". <https://iieg.gob.mx/ns/wp-content/uploads/2020/01/MiBici_19.pdf>
- [G2] WRI México, "Análisis y beneficios de MiBici" (Dec 2019): "Viajes: 11,738,700 (en el periodo)", with the period given as Dec 2014 – Nov 2019. <https://es.wri.org/sites/default/files/uploads/Analisis%20y%20beneficios%20de%20MiBici.%20Diciembre%20de%202019.pdf>
- [G3] IIEG Jalisco, MiBici Jan–Oct 2019 (Nov 2019): 3,944,525 trips. <https://iieg.gob.mx/ns/wp-content/uploads/2019/11/MiBici19_1_rev.pdf> (taken from search-indexed text)
- [G4] Publimetro, 2026-06-03, citing the Jalisco government: "MiBici ha acumulado más de 37 millones de viajes". <https://www.publimetro.com.mx/jalisco/2026/06/03/guadalajara-mibici-supera-37-millones-de-viajes-y-registra-13-mil-traslados-diarios/>

### Boston

| Period | Ours | Reference | Diff | Source | Note |
|---|---:|---:|---:|---|---|
| 2015 | 1,122,558 | 1,122,544 | +0.0% | [E1] |  |
| 2016 | 1,236,203 | 1,236,236 | -0.0% | [E1] |  |
| 2017 | 1,313,774 | 1,313,837 | -0.0% | [E1] |  |
| 2018 | 1,767,806 | 1,767,806 | +0.0% | [E1] |  |
| 2019 | 2,522,771 | 2,520,418 | +0.1% | [E1] |  |
| 2020 | 2,073,448 | 2,065,292 | +0.4% | [E1] |  |
| 2021 | 2,934,378 | 2,954,624 | -0.7% | [E1] |  |
| 2022 | 3,757,281 | 3,781,611 | -0.6% | [E1] |  |
| 2023 | 3,693,193 | 3,686,979 | +0.2% | [E1] |  |
| 2024 | 4,751,823 | 4,734,042 | +0.4% | [E1] |  |
| 2025 | 4,614,297 | ~4,600,000 | +0.3% | [E2] | “more than 4.6 million” |

Bluebikes' own 2019 recap independently gives 2,520,418 for 2019, matching [E1].

Sources:
- [E1] A Better City, "Boosting Bluebikes" (Nov 2025), annual-trips table sourced to Bluebikes System Data. <https://www.abettercity.org/docs-new/boosting%20bluebikes%20report%20%20%2011%205%2025%20002.pdf>. The 2019 figure is also in the Bluebikes blog, "2019 Recap": <https://blog.bluebikes.com/blog/2019-recap>
- [E2] City of Boston, Bluebikes page: "more than 4.6 million in 2025 alone". It's unclear whether this is system-wide or Boston-only. <https://boston.gov/departments/transportation/bluebikes>
