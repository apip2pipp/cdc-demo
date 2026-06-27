package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

func RunSeeder(db *sql.DB) {
	gofakeit.Seed(0)

	log.Println("Seeding patients...")
	for i := 0; i < 50; i++ {
		_, err := db.Exec("INSERT INTO public.patient (name, dob, gender, address) VALUES ($1, $2, $3, $4)",
			gofakeit.Name(),
			gofakeit.DateRange(time.Now().AddDate(-80, 0, 0), time.Now().AddDate(-1, 0, 0)),
			gofakeit.RandomString([]string{"Male", "Female"}),
			gofakeit.Address().Address,
		)
		if err != nil {
			log.Printf("Failed to insert patient: %v\n", err)
		}
	}

	log.Println("Seeding doctors...")
	for i := 0; i < 10; i++ {
		_, err := db.Exec("INSERT INTO public.doctor (name, specialization, phone) VALUES ($1, $2, $3)",
			gofakeit.Name(),
			gofakeit.JobDescriptor(),
			gofakeit.Phone(),
		)
		if err != nil {
			log.Printf("Failed to insert doctor: %v\n", err)
		}
	}

	log.Println("Seeding rooms...")
	for i := 0; i < 5; i++ {
		_, err := db.Exec("INSERT INTO public.room (room_name, type, capacity) VALUES ($1, $2, $3)",
			"Room "+gofakeit.LetterN(3),
			gofakeit.RandomString([]string{"ICU", "UGD", "Poli Umum", "Poli Gigi"}),
			gofakeit.Number(1, 10),
		)
		if err != nil {
			log.Printf("Failed to insert room: %v\n", err)
		}
	}

	log.Println("Seeding visits and related entities...")
	for i := 0; i < 30; i++ {
		patientId := gofakeit.Number(1, 50)
		doctorId := gofakeit.Number(1, 10)

		// Insert visit
		var visitId int64
		err := db.QueryRow("INSERT INTO public.visit (patient_id, doctor_id, complaints) VALUES ($1, $2, $3) RETURNING id",
			patientId, doctorId, gofakeit.Sentence(5)).Scan(&visitId)
		if err != nil {
			continue
		}

		// Insert queue
		db.Exec("INSERT INTO public.queue (patient_id, doctor_id, queue_number, status) VALUES ($1, $2, $3, $4)",
			patientId, doctorId, gofakeit.Number(1, 100), gofakeit.RandomString([]string{"Waiting", "In Progress", "Completed"}),
		)

		// Insert medical record
		var mrId int64
		err = db.QueryRow("INSERT INTO public.medical_record (visit_id, patient_id, notes) VALUES ($1, $2, $3) RETURNING id",
			visitId, patientId, gofakeit.Sentence(10)).Scan(&mrId)
		if err == nil {
			// Insert diagnosis
			db.Exec("INSERT INTO public.diagnosis (medical_record_id, icd10_code, description) VALUES ($1, $2, $3)",
				mrId, "A0"+fmt.Sprintf("%d", gofakeit.Number(0, 9)), gofakeit.Sentence(3))

			// Insert prescription
			db.Exec("INSERT INTO public.prescription (medical_record_id, medicine_name, dosage) VALUES ($1, $2, $3)",
				mrId, gofakeit.Word(), "3x1")
		}

		// Insert laboratory
		db.Exec("INSERT INTO public.laboratory (visit_id, test_name, result) VALUES ($1, $2, $3)",
			visitId, "Blood Test", gofakeit.Word())

		// Insert billing
		db.Exec("INSERT INTO public.billing (visit_id, total_amount, status) VALUES ($1, $2, $3)",
			visitId, gofakeit.Float64Range(100000, 1000000), gofakeit.RandomString([]string{"Unpaid", "Paid"}))
	}
}
