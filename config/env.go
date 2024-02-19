package config

import (
	"log"
	"os"

	"github.com/fatih/color"
	"github.com/joho/godotenv"
)

var (
	MongoHost1          string
	MongoHost2          string
	MongoPort1          string
	MongoPort2          string
	MongoDatabase       string
	MongoUsername       string
	MongoPassword       string
	MongoReplicaSetName string
	PostgresHost        string
	PostgresPort        string
	PostgresUser        string
	PostgresDatabase    string
	PostgresPassword    string
	BeginMongoRecord    int
	MongoNumberRecords  int
	Title               = color.New(color.Underline, color.FgWhite)
	APIDefaultRoute     string
	APITestRoute        string
	ServerConfig        string
	ServerLogDirectory  string
	ServerLogFile       string
	ServerPort          string
)

const (
	// TestMode test mode
	TestMode = "test"

	// ProductionMode product mode
	ProductionMode = "production"
)

func LoadEnv() {
	err := godotenv.Load(".env")
	if err != nil {
		const dotEnvError = "Error loading .env file"
		log.Fatal(dotEnvError)
		panic(dotEnvError)
	}

	APIDefaultRoute = os.Getenv("API_DEFAULT_ROUTE")
	APITestRoute = os.Getenv("API_TEST_ROUTE")
	ServerConfig = os.Getenv("SERVER_CONFIG")
	ServerLogDirectory = os.Getenv("SERVER_LOG_DIR")
	ServerLogFile = os.Getenv("SERVER_LOG_FILE")
	ServerPort = os.Getenv("SERVER_PORT")

	//mongo
	MongoHost1 = os.Getenv("MONGO_HOST_1")
	MongoHost2 = os.Getenv("MONGO_HOST_2")
	MongoPort1 = os.Getenv("MONGO_PORT_1")
	MongoPort2 = os.Getenv("MONGO_PORT_2")
	MongoDatabase = os.Getenv("MONGO_DATABASE")
	MongoUsername = os.Getenv("MONGO_USERNAME")
	MongoPassword = os.Getenv("MONGO_PASSWORD")
	MongoReplicaSetName = os.Getenv("MONGO_REPLICA_SET_NAME")

	//postgres
	PostgresHost = os.Getenv("POSTGRES_HOST")
	PostgresPort = os.Getenv("POSTGRES_PORT")
	PostgresUser = os.Getenv("POSTGRES_USER")
	PostgresDatabase = os.Getenv("POSTGRES_DATABASE")
	PostgresPassword = os.Getenv("POSTGRES_PASSWORD")

	// MONGO RECORDS
	BeginMongoRecord = 0
	MongoNumberRecords = 100

}

func GetLogPath() string {
	return ServerLogDirectory + ServerLogFile
}
