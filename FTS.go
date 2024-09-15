package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// DB connection details
var db *sql.DB

func init() {
	// Read environment variables for PostgreSQL connection
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	// Check if any of these variables are empty
	if host == "" || port == "" || user == "" || password == "" || dbname == "" {
		panic("One or more environment variables are missing")

	// Create PostgreSQL connection string
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var err error
	db, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	// Test the connection
	if err = db.Ping(); err != nil {
		panic(err)
	}

	fmt.Println("Connected to the database!")
}

func main() {
	// Define API endpoints
	http.HandleFunc("/upload", uploadFileHandler)
	http.HandleFunc("/getFiles", getFilesHandler)
	http.HandleFunc("/download", downloadFileHandler)

	fmt.Println("Server running at http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}

// Upload file: Split into chunks and store in DB
func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	// Limit upload size to 10MB
	r.ParseMultipartForm(10 << 20)

	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Generate a unique file ID
	fileID := uuid.New()

	// Split the file into chunks
	const chunkSize = 1 << 20 // 1MB chunks
	var chunks [][]byte
	for {
		chunk := make([]byte, chunkSize)
		n, err := file.Read(chunk)
		if err != nil && err != io.EOF {
			http.Error(w, "Error reading file", http.StatusInternalServerError)
			return
		}
		if n == 0 {
			break
		}
		chunks = append(chunks, chunk[:n])
	}

	// Store file metadata
	totalChunks := len(chunks)
	_, err = db.Exec(`INSERT INTO file_metadata (file_id, filename, total_chunks) VALUES ($1, $2, $3)`, fileID, handler.Filename, totalChunks)
	if err != nil {
		http.Error(w, "Error storing file metadata", http.StatusInternalServerError)
		return
	}

	// Upload chunks in parallel
	var wg sync.WaitGroup
	wg.Add(totalChunks)

	for index, chunk := range chunks {
		go func(index int, chunk []byte) {
			defer wg.Done()
			_, err = db.Exec(`INSERT INTO file_chunks (file_id, chunk_index, chunk_data) VALUES ($1, $2, $3)`,
				fileID, index, chunk)
			if err != nil {
				fmt.Println("Error storing chunk:", err)
			}
		}(index, chunk)
	}

	// Wait for all chunks to be uploaded
	wg.Wait()

	// Respond with the file ID
	w.Write([]byte(fmt.Sprintf("File uploaded successfully with ID: %s", fileID)))
}

// Get uploaded files metadata
func getFilesHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`SELECT file_id, filename, total_chunks, upload_time FROM file_metadata`)
	if err != nil {
		http.Error(w, "Error retrieving files", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result string
	for rows.Next() {
		var fileID uuid.UUID
		var filename string
		var totalChunks int
		var uploadTime string
		err := rows.Scan(&fileID, &filename, &totalChunks, &uploadTime)
		if err != nil {
			http.Error(w, "Error reading file metadata", http.StatusInternalServerError)
			return
		}
		result += fmt.Sprintf("File ID: %s, Filename: %s, Chunks: %d, Uploaded: %s\n", fileID, filename, totalChunks, uploadTime)
	}

	w.Write([]byte(result))
}

// Download file: Retrieve and merge chunks
func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	// Get file ID from query parameters
	fileID := r.URL.Query().Get("id")
	if fileID == "" {
		http.Error(w, "File ID is required", http.StatusBadRequest)
		return
	}

	// Retrieve file metadata
	var filename string
	var totalChunks int
	err := db.QueryRow(`SELECT filename, total_chunks FROM file_metadata WHERE file_id = $1`, fileID).Scan(&filename, &totalChunks)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Retrieve chunks in parallel
	var wg sync.WaitGroup
	wg.Add(totalChunks)

	chunks := make([][]byte, totalChunks)

	for i := 0; i < totalChunks; i++ {
		go func(index int) {
			defer wg.Done()
			var chunkData []byte
			err := db.QueryRow(`SELECT chunk_data FROM file_chunks WHERE file_id = $1 AND chunk_index = $2`, fileID, index).Scan(&chunkData)
			if err != nil {
				fmt.Println("Error retrieving chunk:", err)
				return
			}
			chunks[index] = chunkData
		}(i)
	}

	wg.Wait()

	// Merge chunks back into a single file
	var mergedFile bytes.Buffer
	for _, chunk := range chunks {
		mergedFile.Write(chunk)
	}

	// Send the file to the client
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Write(mergedFile.Bytes())
}
