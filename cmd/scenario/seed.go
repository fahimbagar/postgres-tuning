package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fahimbagar/postgres-tuning/internal/pg"
)

func init() {
	register("seed", "create the shared tables: cities, customers (1M), orders (10M)", seed)
}

// Ten countries with ten cities each. Every city has exactly one warehouse,
// so country, city and warehouse are fully dependent on each other. The
// statistics scenario relies on that.
var countries = map[string][]string{
	"ID": {"Jakarta", "Surabaya", "Bandung", "Medan", "Semarang", "Makassar", "Palembang", "Denpasar", "Yogyakarta", "Malang"},
	"MY": {"Kuala Lumpur", "George Town", "Ipoh", "Johor Bahru", "Kuching", "Kota Kinabalu", "Shah Alam", "Malacca", "Seremban", "Petaling Jaya"},
	"SG": {"Ang Mo Kio", "Bedok", "Bishan", "Clementi", "Hougang", "Jurong", "Punggol", "Sengkang", "Tampines", "Woodlands"},
	"PH": {"Manila", "Quezon City", "Cebu", "Davao", "Makati", "Pasig", "Taguig", "Iloilo", "Baguio", "Cagayan de Oro"},
	"TH": {"Bangkok", "Chiang Mai", "Phuket", "Pattaya", "Khon Kaen", "Hat Yai", "Udon Thani", "Nakhon Ratchasima", "Chon Buri", "Hua Hin"},
	"VN": {"Hanoi", "Ho Chi Minh City", "Da Nang", "Hai Phong", "Can Tho", "Nha Trang", "Hue", "Vung Tau", "Bien Hoa", "Quy Nhon"},
	"HK": {"Central", "Wan Chai", "Causeway Bay", "Mong Kok", "Tsim Sha Tsui", "Sha Tin", "Tsuen Wan", "Kowloon Tong", "Yuen Long", "Tuen Mun"},
	"TW": {"Taipei", "Kaohsiung", "Taichung", "Tainan", "Hsinchu", "Keelung", "Taoyuan", "Chiayi", "Hualien", "Yilan"},
	"AU": {"Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Hobart", "Darwin", "Gold Coast", "Newcastle"},
	"JP": {"Tokyo", "Osaka", "Nagoya", "Sapporo", "Fukuoka", "Kobe", "Kyoto", "Sendai", "Hiroshima", "Yokohama"},
}

func seed(ctx context.Context, _ []string) error {
	pool, err := pg.Pool(ctx, nil)
	if err != nil {
		return err
	}
	defer pool.Close()

	if done, err := pg.TableExists(ctx, pool, "orders", 10_000_000); err != nil {
		return err
	} else if done {
		fmt.Println("orders already seeded, nothing to do")
		return nil
	}

	var values []string
	id := 1
	for _, cc := range []string{"ID", "MY", "SG", "PH", "TH", "VN", "HK", "TW", "AU", "JP"} {
		for i, city := range countries[cc] {
			values = append(values, fmt.Sprintf("(%d, '%s', '%s', '%s-WH%d')", id, cc, city, cc, i+1))
			id++
		}
	}

	steps := []struct{ name, sql string }{
		{"drop old tables", `DROP TABLE IF EXISTS orders, customers, cities`},
		{"create tables", `
			CREATE TABLE cities (
				id        int PRIMARY KEY,
				country   text NOT NULL,
				city      text NOT NULL,
				warehouse text NOT NULL
			);
			CREATE TABLE customers (
				id         bigint PRIMARY KEY,
				email      text NOT NULL,
				tier       text NOT NULL,
				city_id    int NOT NULL REFERENCES cities,
				created_at timestamptz NOT NULL
			);
			CREATE TABLE orders (
				id          bigint PRIMARY KEY,
				customer_id bigint NOT NULL,
				status      text NOT NULL,
				country     text NOT NULL,
				city        text NOT NULL,
				warehouse   text NOT NULL,
				total       numeric(10,2) NOT NULL,
				created_at  timestamptz NOT NULL
			)`},
		{"insert cities", "INSERT INTO cities VALUES " + strings.Join(values, ", ")},
		// random() has to be evaluated in the inner SELECT; in a LATERAL that
		// doesn't reference g the planner may evaluate it only once.
		{"insert 1M customers", `
			INSERT INTO customers
			SELECT g, 'user' || g || '@example.com',
			       CASE WHEN r < 0.01 THEN 'vip' WHEN r < 0.10 THEN 'gold' ELSE 'standard' END,
			       1 + floor(random() * 100)::int,
			       timestamptz '2022-01-01' + g * interval '60 seconds'
			FROM (SELECT g, random() AS r FROM generate_series(1, 1000000) g) s`},
		// Status is skewed on purpose: 1 in 10,000 orders is refunded.
		// created_at follows id, the way it does in an append-only table.
		{"insert 10M orders", `
			INSERT INTO orders
			SELECT s.g,
			       1 + floor(s.r1 * 1000000)::bigint,
			       CASE WHEN s.r2 < 0.0001 THEN 'refunded'
			            WHEN s.r2 < 0.02   THEN 'cancelled'
			            WHEN s.r2 < 0.05   THEN 'pending'
			            WHEN s.r2 < 0.10   THEN 'shipped'
			            ELSE 'delivered' END,
			       c.country, c.city, c.warehouse,
			       round((5 + s.r3 * 495)::numeric, 2),
			       timestamptz '2024-10-01' + s.g * interval '6.3 seconds'
			FROM (SELECT g, random() r1, random() r2, random() r3,
			             1 + floor(random() * 100)::int AS city_id
			      FROM generate_series(1, 10000000) g) s
			JOIN cities c ON c.id = s.city_id`},
		// One validating pass is much faster than checking the FK per row
		// during the load.
		{"add foreign key", `ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers`},
		{"create indexes", `
			SET maintenance_work_mem = '256MB';
			CREATE INDEX orders_customer_id_idx ON orders (customer_id);
			CREATE INDEX orders_created_at_idx ON orders (created_at);
			CREATE INDEX orders_status_idx ON orders (status)`},
		// VACUUM sets the visibility map so index-only scans work from the start.
		{"vacuum analyze", `VACUUM (ANALYZE) cities, customers, orders`},
	}
	for _, s := range steps {
		start := time.Now()
		if _, err := pool.Exec(ctx, s.sql); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		fmt.Printf("%-20s %s\n", s.name, time.Since(start).Round(100*time.Millisecond))
	}

	for _, t := range []string{"customers", "orders"} {
		size, err := pg.RelationSize(ctx, pool, t)
		if err != nil {
			return err
		}
		fmt.Printf("%-10s %s (with indexes)\n", t, size)
	}
	return nil
}
