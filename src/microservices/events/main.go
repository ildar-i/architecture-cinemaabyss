package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

func main() {

	// Set up HTTP routes
	http.HandleFunc("/api/movies/health", handleHealth)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081" // Note: Using a different port than the monolith
	}
	log.Printf("Starting movies microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]bool{"status": true})
	if err != nil {
		log.Println(err)

		return
	}
}
