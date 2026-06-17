package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	pmtiles "github.com/protomaps/go-pmtiles/pmtiles"
)

type TileJob struct {
	TileID uint64
	Offset uint64
	Length uint32
}

type TileRow struct {
	Z    int
	X    int
	TMSY int
	Data []byte
}

func xyzToTMSY(z int, y uint32) int {
	return (1 << z) - 1 - int(y)
}

func compressionName(c pmtiles.Compression) string {
	switch c {
	case pmtiles.NoCompression:
		return "none"
	case pmtiles.Gzip:
		return "gzip"
	case pmtiles.Brotli:
		return "brotli"
	case pmtiles.Zstd:
		return "zstd"
	default:
		return "unknown"
	}
}

func tileFormat(h pmtiles.HeaderV3) string {
	switch h.TileType {
	case pmtiles.Mvt:
		return "pbf"
	case pmtiles.Png:
		return "png"
	case pmtiles.Jpeg:
		return "jpg"
	case pmtiles.Webp:
		return "webp"
	case pmtiles.Avif:
		return "avif"
	default:
		return "pbf"
	}
}

func boundsString(h pmtiles.HeaderV3) string {
	return fmt.Sprintf(
		"%.7f,%.7f,%.7f,%.7f",
		float64(h.MinLonE7)/1e7,
		float64(h.MinLatE7)/1e7,
		float64(h.MaxLonE7)/1e7,
		float64(h.MaxLatE7)/1e7,
	)
}

func centerString(h pmtiles.HeaderV3) string {
	return fmt.Sprintf(
		"%.7f,%.7f,%d",
		float64(h.CenterLonE7)/1e7,
		float64(h.CenterLatE7)/1e7,
		h.CenterZoom,
	)
}

func mustExec(db *sql.DB, q string, args ...any) {
	if _, err := db.Exec(q, args...); err != nil {
		log.Fatalf("sqlite exec failed: %v\nSQL: %s", err, q)
	}
}

func readAt(f *os.File, off uint64, length uint64) ([]byte, error) {
	buf := make([]byte, length)
	_, err := f.ReadAt(buf, int64(off))
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

func main() {
	inPath := flag.String("in", "", "input .pmtiles")
	outPath := flag.String("out", "", "output .mbtiles")
	workers := flag.Int("workers", runtime.NumCPU(), "parallel PMTiles read workers")
	batchSize := flag.Int("batch", 1000, "SQLite insert batch size")
	noMetadataJSON := flag.Bool("no-json", false, "do not copy PMTiles metadata JSON into MBTiles metadata json row")
	flag.Parse()

	if *inPath == "" || *outPath == "" {
		log.Fatal("usage: pmtiles2mbtiles -in selection.pmtiles -out selection.mbtiles [-workers 16]")
	}

	start := time.Now()

	in, err := os.Open(*inPath)
	if err != nil {
		log.Fatal(err)
	}
	defer in.Close()

	headerBytes, err := readAt(in, 0, pmtiles.HeaderV3LenBytes)
	if err != nil {
		log.Fatal(err)
	}

	header, err := pmtiles.DeserializeHeader(headerBytes)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("input: %s", *inPath)
	log.Printf("tile entries: %d, addressed tiles: %d, contents: %d", header.TileEntriesCount, header.AddressedTilesCount, header.TileContentsCount)
	log.Printf("zoom: %d-%d, format=%s, tile_compression=%s", header.MinZoom, header.MaxZoom, tileFormat(header), compressionName(header.TileCompression))

	// Collect directory entries.
	// PMTiles entries point to tile payloads; entry.Offset is relative to header.TileDataOffset.
	var jobs []TileJob

	fetch := func(offset uint64, length uint64) ([]byte, error) {
		return readAt(in, offset, length)
	}

	err = pmtiles.IterateEntries(header, fetch, func(e pmtiles.EntryV3) {
		run := e.RunLength
		if run == 0 {
			run = 1
		}

		for i := uint32(0); i < run; i++ {
			jobs = append(jobs, TileJob{
				TileID: e.TileID + uint64(i),
				Offset: e.Offset,
				Length: e.Length,
			})
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("expanded tile rows: %d", len(jobs))

	tmpOut := *outPath + ".tmp"
	_ = os.Remove(tmpOut)

	db, err := sql.Open("sqlite3", tmpOut)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Speed-first settings for generated temporary output.
	mustExec(db, "PRAGMA journal_mode=OFF")
	mustExec(db, "PRAGMA synchronous=OFF")
	mustExec(db, "PRAGMA locking_mode=EXCLUSIVE")
	mustExec(db, "PRAGMA temp_store=MEMORY")
	mustExec(db, "PRAGMA cache_size=-524288")

	mustExec(db, "CREATE TABLE metadata (name TEXT, value TEXT)")
	mustExec(db, `
		CREATE TABLE tiles (
			zoom_level INTEGER,
			tile_column INTEGER,
			tile_row INTEGER,
			tile_data BLOB
		)
	`)

	mustExec(db, "INSERT INTO metadata VALUES ('name', ?)", "offline")
	mustExec(db, "INSERT INTO metadata VALUES ('format', ?)", tileFormat(header))
	mustExec(db, "INSERT INTO metadata VALUES ('minzoom', ?)", fmt.Sprintf("%d", header.MinZoom))
	mustExec(db, "INSERT INTO metadata VALUES ('maxzoom', ?)", fmt.Sprintf("%d", header.MaxZoom))
	mustExec(db, "INSERT INTO metadata VALUES ('bounds', ?)", boundsString(header))
	mustExec(db, "INSERT INTO metadata VALUES ('center', ?)", centerString(header))
	mustExec(db, "INSERT INTO metadata VALUES ('type', 'baselayer')")
	mustExec(db, "INSERT INTO metadata VALUES ('version', '1')")

	if !*noMetadataJSON && header.MetadataLength > 0 {
		rawMeta, err := readAt(in, header.MetadataOffset, header.MetadataLength)
		if err == nil {
			metaJSON, err := pmtiles.DeserializeMetadataBytes(bytes.NewReader(rawMeta), header.InternalCompression)
			if err == nil && len(metaJSON) > 0 {
				mustExec(db, "INSERT INTO metadata VALUES ('json', ?)", string(metaJSON))
			} else if err != nil {
				log.Printf("warning: could not decode metadata json: %v", err)
			}
		} else {
			log.Printf("warning: could not read metadata: %v", err)
		}
	}

	jobsCh := make(chan TileJob, *workers*4)
	rowsCh := make(chan TileRow, *workers*4)

	var readCount atomic.Uint64
	var readBytes atomic.Uint64

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobsCh {
				data, err := readAt(in, header.TileDataOffset+job.Offset, uint64(job.Length))
				if err != nil {
					log.Printf("read tile failed tileID=%d offset=%d len=%d: %v", job.TileID, job.Offset, job.Length, err)
					continue
				}

				z, x, y := pmtiles.IDToZxy(job.TileID)
				zi := int(z)

				rowsCh <- TileRow{
					Z:    zi,
					X:    int(x),
					TMSY: xyzToTMSY(zi, y),
					Data: data,
				}

				readCount.Add(1)
				readBytes.Add(uint64(len(data)))
			}
		}()
	}

	go func() {
		for _, job := range jobs {
			jobsCh <- job
		}
		close(jobsCh)
		wg.Wait()
		close(rowsCh)
	}()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO tiles VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatal(err)
	}

	inserted := 0
	batch := 0

	for row := range rowsCh {
		if _, err := stmt.Exec(row.Z, row.X, row.TMSY, row.Data); err != nil {
			log.Fatal(err)
		}

		inserted++
		batch++

		if batch >= *batchSize {
			if err := stmt.Close(); err != nil {
				log.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				log.Fatal(err)
			}

			tx, err = db.Begin()
			if err != nil {
				log.Fatal(err)
			}
			stmt, err = tx.Prepare("INSERT INTO tiles VALUES (?, ?, ?, ?)")
			if err != nil {
				log.Fatal(err)
			}
			batch = 0
		}
	}

	if err := stmt.Close(); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Printf("inserted rows: %d", inserted)
	log.Printf("read tile bytes: %.2f MB", float64(readBytes.Load())/1024/1024)

	log.Printf("creating index...")
	mustExec(db, "CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)")

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	_ = os.Remove(*outPath)
	if err := os.Rename(tmpOut, *outPath); err != nil {
		log.Fatal(err)
	}

	log.Printf("done: %s in %s", *outPath, time.Since(start).Round(time.Millisecond))
}
