package config

import (
	"errors"
	"log/slog"
	"os"
	"path"
	"strings"
)

// TDConsumerConfig contains the RDM/Confluent Kafka settings for Train
// Describer messages. Empty broker and group settings disable the consumer.
type TDConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Username string
	Password string
}

// DarwinConsumerConfig contains the RDM Kafka settings for Darwin Push Port.
type DarwinConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Username string
	Password string
}

// GetScheduleFeedFilename retrieves the get schedule feed filename behavior.
func GetScheduleFeedFilename() string {
	filename := os.Getenv("SCHEDULE_FEED_FILENAME")
	if filename == "" {
		slog.Debug("No SCHEDULE_FEED_FILENAME environment variable set - defaulting to ./data/schedule.json")
		filename = "./data/schedule.json"
	}
	return filename
}

// GetDataDir retrieves the get data dir behavior.
func GetDataDir() string {
	dir := os.Getenv("DATA_DIR")
	if dir == "" {
		slog.Debug("No DATA_DIR environment variable set - defaulting to ./data")
		dir = "./data"
	}
	return dir
}

// GetDatabaseFilename retrieves the get database filename behavior.
func GetDatabaseFilename() string {
	return path.Join(GetDataDir(), "/ukra.db")
}

// GetDatabaseDSN returns the optional MySQL DSN. An empty value selects the
// SQLite file returned by GetDatabaseFilename, which remains useful for tests
// and standalone development.
// GetDatabaseDSN retrieves the get database dsn behavior.
func GetDatabaseDSN() string {
	return os.Getenv("SCHEDULE_DB_DSN")
}

// GetStompConnectionDetails retrieves the get stomp connection details behavior.
func GetStompConnectionDetails() (err error, url string, login string, password string) {
	url = os.Getenv("NR_STOMP_URL")
	if url == "" {
		slog.Debug("No NR_STOMP_URL environment variable set - defaulting to publicdatafeeds.networkrail.co.uk:61618")
		url = "publicdatafeeds.networkrail.co.uk:61618"
	}
	login = os.Getenv("NR_STOMP_LOGIN")
	password = os.Getenv("NR_STOMP_PASSWORD")
	if login == "" || password == "" {
		err = errors.New("NR_STOMP_LOGIN and NR_STOMP_PASSWORD environment variables must be set to connect to the VSTP feed")
		slog.Error(err.Error())
		return err, url, login, password
	}
	return nil, url, login, password
}

// GetTDConsumerConfig retrieves the get tdconsumer config behavior.
func GetTDConsumerConfig() (TDConsumerConfig, bool, error) {
	brokers := strings.TrimSpace(os.Getenv("TD_URL"))
	groupID := strings.TrimSpace(os.Getenv("TD_GROUP"))
	username := os.Getenv("TD_USER")
	password := os.Getenv("TD_PASSWORD")
	if brokers == "" && groupID == "" && username == "" && password == "" {
		return TDConsumerConfig{}, false, nil
	}
	missing := make([]string, 0, 4)
	for _, item := range []struct{ name, value string }{
		{"TD_URL", brokers}, {"TD_GROUP", groupID},
		{"TD_USER", username}, {"TD_PASSWORD", password},
	} {
		if strings.TrimSpace(item.value) == "" {
			missing = append(missing, item.name)
		}
	}
	if len(missing) > 0 {
		return TDConsumerConfig{}, false, errors.New("TD Kafka settings are incomplete; missing " + strings.Join(missing, ", "))
	}
	parsedBrokers := make([]string, 0, 2)
	for _, broker := range strings.Split(brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			parsedBrokers = append(parsedBrokers, broker)
		}
	}
	topic := strings.TrimSpace(os.Getenv("TD_TOPIC"))
	if topic == "" {
		topic = "TD_ALL_SIG_AREA"
	}
	return TDConsumerConfig{
		Brokers: parsedBrokers, Topic: topic, GroupID: groupID,
		Username: username, Password: password,
	}, true, nil
}

// GetDarwinConsumerConfig retrieves the get darwin consumer config behavior.
func GetDarwinConsumerConfig() (DarwinConsumerConfig, bool, error) {
	brokers := strings.TrimSpace(os.Getenv("DARWIN_URL"))
	groupID := strings.TrimSpace(os.Getenv("DARWIN_GROUP"))
	username := strings.TrimSpace(os.Getenv("DARWIN_USER"))
	password := os.Getenv("DARWIN_PASSWORD")
	topic := strings.TrimSpace(os.Getenv("DARWIN_TOPIC"))
	if topic == "" {
		topic = strings.TrimSpace(os.Getenv("DARWIN_FEED_TOPIC"))
	}
	if topic == "" {
		topic = strings.TrimSpace(os.Getenv("DARWIN_TOPIC_NAME"))
	}
	if brokers == "" && groupID == "" && username == "" && password == "" && topic == "" {
		return DarwinConsumerConfig{}, false, nil
	}
	if topic == "" {
		topic = "prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON"
	}
	missing := make([]string, 0, 5)
	for _, item := range []struct{ name, value string }{
		{"DARWIN_URL", brokers}, {"DARWIN_GROUP", groupID}, {"DARWIN_USER", username},
		{"DARWIN_PASSWORD", password}, {"DARWIN_TOPIC", topic},
	} {
		if strings.TrimSpace(item.value) == "" {
			missing = append(missing, item.name)
		}
	}
	if len(missing) > 0 {
		return DarwinConsumerConfig{}, false, errors.New("Darwin Kafka settings are incomplete; missing " + strings.Join(missing, ", "))
	}
	parsedBrokers := make([]string, 0, 2)
	for _, broker := range strings.Split(brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			parsedBrokers = append(parsedBrokers, broker)
		}
	}
	return DarwinConsumerConfig{Brokers: parsedBrokers, Topic: topic, GroupID: groupID, Username: username, Password: password}, true, nil
}

// GetHTTPListenAddress retrieves the get httplisten address behavior.
func GetHTTPListenAddress() string {
	addr := os.Getenv("LISTEN_ON")
	if addr == "" {
		slog.Debug("No LISTEN_ON environment variable set - defaulting to localhost:1333")
		addr = "localhost:1333"
	}
	return addr
}

// ShouldDeleteExpiredSchedulesAfterRefresh checks the should delete expired schedules after refresh behavior.
func ShouldDeleteExpiredSchedulesAfterRefresh() bool {
	return os.Getenv("DELETE_EXPIRED_SCHEDULES_ON_REFRESH") == "yes"
}
