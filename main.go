package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func fmtlog(params ...any) {
	fmt.Println(params...)
	log.Println(params...)
}

// פונקציה להרצת פרוצדורה מאוחסנת
func runStoredProcedure(spname string, params map[string]interface{}) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("EXEC %s ", spname)
	args := make([]interface{}, 0, len(params))

	for paramName, paramValue := range params {
		query += fmt.Sprintf("@%s = @%s,", paramName, paramName)
		args = append(args, sql.Named(paramName, paramValue))
	}
	query = query[:len(query)-1]

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		rowData := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			rowData[colName] = *val
		}
		result = append(result, rowData)
	}
	return result, nil
}

func readConfig() {
	// Open our jsonFile
	fmt.Println("Fetching config data")
	jsonFile, err := os.Open("config.json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println("error opening config.json", err)
		log.Fatalln("error opening config.json", err)
	}

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	bytevalue, _ := io.ReadAll(jsonFile)
	err = json.Unmarshal(bytevalue, &config)
	if err != nil {
		fmt.Println("Error reading config file", err)
		log.Fatalln("Error reading config file", err)
	} else {
		fmt.Println("Config data", config)
	}

}

type Config struct {
	DbServer struct {
		Driver           string `json:"driver"`
		ConnectionString string `json:"connectionstring"`
	} `json:"dbserver"`
	SapServer struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		Companydb       string `json:"companydb"`
		Servicelayerurl string `json:"servicelayerurl"`
	} `json:"sapserver"`
	Port          int    `json:"port"`
	WebSocketPort int    `json:"websocketPort"`
	RootApiPath   string `json:"rootapipath"`
}

var db *sql.DB
var config Config

func initDB() {
	var err error
	//connString := "server=10.8.2.88;user id=sa;password=B1Admin;database=Sanitec_2012_c"
	connString := config.DbServer.ConnectionString
	//db, err = sql.Open("sqlserver", connString)
	db, err = sql.Open(config.DbServer.Driver, connString)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("Cannot connect to database: %v", err)
	}
	log.Println("Connected to database")
}

type WhatsAppSettings struct {
	id                int64  `json:"id"`
	IncludeSenderName string `json:"IncludeSenderName"`
	AssignedMessage   string `json:"AssignedMessage"`
	ReminderMessage   string `json:"ReminderMessage"`
	CloseMesssage     string `json:"CloseMessage"`
	MessageTimeout    int64  `json:"messageTimeout"`
}

func getWhatsAppSettings() (*WhatsAppSettings, error) {
	data, err := runStoredProcedure("sp_getwhatsappsettings", nil)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("no settings found")
	}

	settings := &WhatsAppSettings{
		IncludeSenderName: getStringValue(data[0]["IncludeSenderName"]),
		AssignedMessage:   getStringValue(data[0]["AssignedMessage"]),
		CloseMesssage:     getStringValue(data[0]["CloseMessage"]),
		ReminderMessage:   getStringValue(data[0]["ReminderMessage"]),
		MessageTimeout:    getInt64Value(data[0]["MessageTimeout"]),
	}

	return settings, nil
}

// Helper functions to safely extract values
func getStringValue(val interface{}) string {
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

func getInt64Value(val interface{}) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func startContactChecker() {
	// Load settings first
	settings, err := getWhatsAppSettings()
	if err != nil {
		log.Printf("Failed to load settings: %v", err)
		return
	}

	//checkInterval := time.Duration(settings.MessageTimeout) * time.Second
	checkInterval := 5 * time.Minute

	if checkInterval == 0 {
		checkInterval = 60 * time.Second // default
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		checkContactsForReminders(settings)
	}
	log.Println("Ticker has been started")
}

func checkContactsForReminders(settings *WhatsAppSettings) {
	// Get all contacts (no pagination)
	contacts, err := runStoredProcedure("sp_getwhatsappcontacts", nil)
	if err != nil {
		log.Printf("Error fetching contacts: %v", err)
		return
	}

	currentTime := time.Now().UTC()

	for _, contact := range contacts {
		processContact(contact, currentTime, settings)
	}
}

func processContact(contact map[string]interface{}, currentTime time.Time, settings *WhatsAppSettings) {
	// Skip if no assigned user
	if contact["assignedUserid"] == nil || contact["assignedUserid"] == "" {
		return
	}

	lastRecv, ok := contact["lastmsgrecvtime"].(time.Time)
	if !ok {
		lastRecv = time.Time{} // or some default value
	}

	lastSent, ok := contact["lastmsgsenttime"].(time.Time)
	if !ok {
		lastSent = time.Time{} // or some default value
	}

	var lastReminder *time.Time
	if rt, ok := contact["remindertime"].(time.Time); ok {
		lastReminder = &rt
	}

	phone := contact["phone"].(string)
	senderID := contact["assignedUserid"].(string)

	reminderTimeout := time.Duration(settings.MessageTimeout) * time.Second
	closeTimeout := time.Duration(settings.MessageTimeout) * time.Second

	// Enhanced debug logging
	fmtlog("[TIME CHECKER]",
		"Phone:", phone,
		"Current:", currentTime.Format(time.RFC3339),
		"LastRecv:", lastRecv.Format(time.RFC3339),
		"LastSent:", lastSent.Format(time.RFC3339),
		"LastReminder:", func() string {
			if lastReminder != nil {
				return lastReminder.Format(time.RFC3339)
			}
			return "nil"
		}(),
		"ReminderTimeout:", reminderTimeout,
		"CloseTimeout:", closeTimeout,
	)

	fmtlog("[TIME DEBUG] Time since last sent:", currentTime.Sub(lastSent))
	fmtlog("[TIME DEBUG] Time since reminder:", currentTime.Sub(*lastReminder))

	// Check if customer hasn't responded after our last message
	customerNotResponded := lastSent.After(lastRecv)

	// Scenario 1: Send reminder
	needsReminder := (lastReminder == nil || lastReminder.Before(lastSent)) &&
		currentTime.Sub(lastSent) >= reminderTimeout &&
		customerNotResponded

	if needsReminder {
		fmtlog("[TIME CHECKER]", "SENDING REMINDER to", phone,
			"TimeSinceLastSent:", currentTime.Sub(lastSent))
		sendReminder(phone, senderID, settings)
		return
	}

	// Scenario 2: Need to close conversation
	needsClose := lastReminder != nil &&
		currentTime.Sub(*lastReminder) >= closeTimeout &&
		lastRecv.Before(*lastReminder)

	if needsClose {
		fmtlog("[TIME CHECKER]", "SENDING CLOSE MESSAGE to", phone,
			"TimeSinceReminder:", currentTime.Sub(*lastReminder))
		sendCloseMessage(phone, senderID, settings)
		return
	}

	// Log why no action was taken
	fmtlog("[TIME CHECKER]", "NO ACTION for", phone, "Reasons:",
		"NeedsReminder:", needsReminder,
		"NeedsClose:", needsClose,
		"CustomerNotResponded:", customerNotResponded,
		"TimeSinceLastSent:", currentTime.Sub(lastSent),
		"TimeSinceReminder:", func() time.Duration {
			if lastReminder != nil {
				return currentTime.Sub(*lastReminder)
			}
			return 0
		}(),
	)
}

func sendReminder(phone, senderID string, settings *WhatsAppSettings) {
	message := settings.ReminderMessage
	if settings.IncludeSenderName == "Y" {
		message = fmt.Sprintf("%s (%s)", message, senderID)
	}

	params := map[string]interface{}{
		"MessageType":    "Text",
		"MessageContent": message,
		"PhoneNumber":    phone,
		"senderid":       senderID,
	}

	if _, err := runStoredProcedure("SendWhatsAppMessage", params); err != nil {
		log.Printf("Failed to send reminder: %v", err)
	} else {
		updateReminderTime(phone)
	}
}

func sendCloseMessage(phone, senderID string, settings *WhatsAppSettings) {
	message := settings.CloseMesssage
	if settings.IncludeSenderName == "Y" {
		message = fmt.Sprintf("%s (%s)", message, senderID)
	}

	errChan := make(chan error, 2)

	// First stored procedure
	go func() {
		closeParams := map[string]interface{}{
			"phonenumber":  phone,
			"isautoclosed": "Y",
			"userid":       senderID,
		}
		log.Println("Running stored procedure sp_CloseWhatsAppAgentContact with params:", closeParams)
		_, err := runStoredProcedure("sp_CloseWhatsAppAgentContact", closeParams)
		if err != nil {
			log.Println("Error executing stored procedure sp_CloseWhatsAppAgentContact:", err)
		}
		errChan <- err
	}()

	// Second stored procedure
	go func() {
		params := map[string]interface{}{
			"MessageType":    "Text",
			"MessageContent": message,
			"PhoneNumber":    phone,
			"senderid":       senderID,
		}
		var err error
		if _, err = runStoredProcedure("SendWhatsAppMessage", params); err != nil {
			log.Printf("Failed to send close message: %v", err)
		} else {
			updateReminderTime(phone)
		}
		errChan <- err
	}()

	// Collect errors from both operations
	var errors []error
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			errors = append(errors, err)
		}
	}

	// Return combined errors if any
	if len(errors) > 0 {
		fmt.Errorf("stored procedure errors: %v", errors)
		return
	}
	return
}

func updateReminderTime(phone string) {
	params := map[string]interface{}{
		"phonenumber": phone,
	}
	_, _ = runStoredProcedure("sp_remindersent", params)
}

func main() {
	// אתחול מסד הנתונים
	readConfig()
	initDB()

	go func() {
		fmtlog("Starting WhatsApp contact checker")
		startContactChecker()
	}()

	select {}
}
