package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// Connect to database
	dsn := "host=localhost port=5432 user=postgres password=password123 dbname=simrs_db sslmode=disable"
	if os.Getenv("DB_HOST") != "" {
		dsn = fmt.Sprintf("host=%s port=5432 user=postgres password=password123 dbname=simrs_db sslmode=disable", os.Getenv("DB_HOST"))
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping DB: %v", err)
	}

	log.Println("Successfully connected to database.")

	action := "all"
	if len(os.Args) > 1 {
		action = os.Args[1]
	}

	if action == "all" || action == "migrate" {
		log.Println("Starting migration...")
		RunMigration(db)
		log.Println("Migration completed.")
	}

	if action == "all" || action == "seed" {
		log.Println("Starting seeder...")
		RunSeeder(db)
		log.Println("Seeder completed.")
	}
}
