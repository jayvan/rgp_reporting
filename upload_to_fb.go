package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var config configuration

type configuration struct {
	FacebookAccessToken     string `json:"facebook_access_token"`
	FacebookConversionToken string `json:"facebook_conversion_token"`
	FacebookPixelId         string `json:"facebook_pixel_id"`
	Location                string `json:"location"`
	OfflineEventSetId       string `json:"offline_event_set_id"`
	DatabaseUser            string `json:"database_user"`
	DatabasePassword        string `json:"database_password"`
	DatabaseAddress         string `json:"database_address"`
	DatabaseName            string `json:"database_name"`
	DaysAgo                 int    `json:"days_ago"`
}

type matchKeys struct {
	Phone    []string `json:"phone"`
	Email    []string `json:"email"`
	Fn       string   `json:"fn"`
	Ln       string   `json:"ln"`
	Country  string   `json:"country"`
	ExternId int      `json:"extern_id"`
	DobY     string   `json:"doby"`
	DobM     string   `json:"dobm"`
	DobD     string   `json:"dobd"`
	Zip      string   `json:"zip"`
}

type purchase struct {
	MatchKeys matchKeys `json:"match_keys"`
	Currency  string    `json:"currency"`
	Value     float32   `json:"value"`
	EventName string    `json:"event_name"`
	OrderId   int       `json:"order_id"`
	EventTime int64     `json:"event_time"`
}

type conversionUserData struct {
	Email      []string `json:"em"`
	Phone      []string `json:"ph"`
	FirstName  string   `json:"fn"`
	LastName   string   `json:"ln"`
	Birthday   string   `json:"db"`
	Zipcode    string   `json:"zp"`
	Country    string   `json:"country"`
	ExternalId string   `json:"external_id"`
}

type conversionCustomData struct {
	Currency string  `json:"currency"`
	Value    float32 `json:"value"`
}

type conversion struct {
	EventName    string               `json:"event_name"`
	EventTime    int64                `json:"event_time"`
	ActionSource string               `json:"action_source"`
	UserData     conversionUserData   `json:"user_data"`
	CustomData   conversionCustomData `json:"custom_data"`
}

func main() {
	loadConfig()

	ReportOfflinePurchasesToFacebook()
	ReportOnlinePurchasesToFacebook()
}

func ReportOfflinePurchasesToFacebook() {
	offlinePurchases := fetchPurchases("POS")
	hashedOfflinePurchases := hashOfflineFacebookPurchases(offlinePurchases)
	uploadOfflinePurchasesToFacebook(hashedOfflinePurchases)
}

func ReportOnlinePurchasesToFacebook() {
	onlinePurchase := fetchPurchases("ONLINE")
	hashedOnlinePurchases := hashOnlineFacebookPurchases(onlinePurchase)
	uploadOnlinePurchasesToFacebook(hashedOnlinePurchases)
}

func loadConfig() {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}

	jsonFile, err := os.Open(filepath.Join(filepath.Dir(ex), "config.txt"))

	if err != nil {
		panic(err)
	}

	byteValue, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		panic(err)
	}
}

func uploadOfflinePurchasesToFacebook(purchases []purchase) {
	dayOfData := time.Now().AddDate(0, 0, -config.DaysAgo)
	dayOfData = time.Date(dayOfData.Year(), dayOfData.Month(), dayOfData.Day(), 0, 0, 0, 0, time.Local)

	jsonPurchases, err := json.Marshal(purchases)

	if err != nil {
		log.Fatal(err)
	}

	data := url.Values{
		"access_token": {config.FacebookAccessToken},
		"upload_tag":   {"rgp_upload_" + config.Location + "_" + dayOfData.Format("2006-01-02")},
		"data":         {string(jsonPurchases)},
	}

	resp, err := http.PostForm("https://graph.facebook.com/v12.0/"+config.OfflineEventSetId+"/events", data)

	if err != nil {
		log.Fatal(err)
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func uploadOnlinePurchasesToFacebook(conversions []conversion) {
	jsonConversions, err := json.Marshal(conversions)

	if err != nil {
		log.Fatal(err)
	}

	data := url.Values{
		"data": {string(jsonConversions)},
	}

	resp, err := http.PostForm("https://graph.facebook.com/v12.0/"+config.FacebookPixelId+"/events?access_token="+config.FacebookConversionToken, data)

	if err != nil {
		log.Fatal(err)
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func hashOfflineFacebookPurchases(purchases *sql.Rows) (hashedPurchases []purchase) {
	var (
		email      string
		cellPhone  string
		homePhone  string
		firstName  string
		lastName   string
		zip        string
		birthday   time.Time
		customerId int
		amount     float32
		invoiceId  int
		postdate   time.Time
	)

	columns := []interface{}{&email, &cellPhone, &homePhone, &firstName, &lastName, &zip, &birthday, &customerId, &amount, &invoiceId, &postdate}

	for purchases.Next() {
		if err := purchases.Scan(columns...); err != nil {
			panic(err)
		}

		hashedPurchase := purchase{
			MatchKeys: matchKeys{
				Phone:    formatPhoneNumbers(homePhone, cellPhone),
				Email:    formatEmail(email),
				Fn:       hexDigest(strings.ToLower(strings.TrimSpace(firstName))),
				Ln:       hexDigest(strings.ToLower(strings.TrimSpace(lastName))),
				Country:  hexDigest("ca"),
				ExternId: customerId,
				DobY:     hexDigest(strconv.Itoa(birthday.Year())),
				DobM:     hexDigest(birthday.Format("01")),
				DobD:     hexDigest(birthday.Format("02")),
				Zip:      hexDigest(strings.ToLower(strings.ReplaceAll(zip, " ", ""))),
			},
			Currency:  "CAD",
			Value:     amount,
			EventName: "Purchase",
			OrderId:   invoiceId,
			EventTime: postdate.Unix(),
		}

		hashedPurchases = append(hashedPurchases, hashedPurchase)
	}

	return hashedPurchases
}

func hashOnlineFacebookPurchases(purchases *sql.Rows) (conversionEvents []conversion) {
	var (
		email      string
		cellPhone  string
		homePhone  string
		firstName  string
		lastName   string
		zip        string
		birthday   time.Time
		customerId int
		amount     float32
		invoiceId  int
		postdate   time.Time
	)

	columns := []interface{}{&email, &cellPhone, &homePhone, &firstName, &lastName, &zip, &birthday, &customerId, &amount, &invoiceId, &postdate}

	for purchases.Next() {
		if err := purchases.Scan(columns...); err != nil {
			panic(err)
		}

		conversionEvent := conversion{
			EventName:    "Purchase",
			EventTime:    postdate.Unix(),
			ActionSource: "website",
			UserData: conversionUserData{
				Email:      formatEmail(email),
				Phone:      formatPhoneNumbers(homePhone, cellPhone),
				FirstName:  hexDigest(strings.ToLower(strings.TrimSpace(firstName))),
				LastName:   hexDigest(strings.ToLower(strings.TrimSpace(lastName))),
				Birthday:   hexDigest(birthday.Format("2006-02-01")),
				Zipcode:    hexDigest(strings.ToLower(strings.ReplaceAll(zip, " ", ""))),
				ExternalId: strconv.Itoa(customerId),
			},
			CustomData: conversionCustomData{
				Currency: "CAD",
				Value:    amount,
			},
		}

		conversionEvents = append(conversionEvents, conversionEvent)
	}

	return conversionEvents
}

func formatEmail(email string) []string {
	if len(email) == 0 {
		return []string{}
	}

	return []string{hexDigest(strings.ToLower(strings.TrimSpace(email)))}
}

func fetchPurchases(purchaseType string) *sql.Rows {
	db := connectToRgp()
	query := buildQuery(purchaseType)

	results, err := db.Query(query)

	if err != nil {
		panic(err)
	}

	return results
}

func formatPhoneNumbers(homePhone string, cellPhone string) (numbers []string) {
	if len(homePhone) > 0 {
		numbers = append(numbers, formatPhoneNumber(homePhone))
	}

	if len(cellPhone) > 0 {
		numbers = append(numbers, formatPhoneNumber(cellPhone))
	}

	return numbers
}

func formatPhoneNumber(number string) string {
	nonNumber := regexp.MustCompile("[^0-9]")

	number = nonNumber.ReplaceAllString(number, "")
	if len(number) == 10 {
		number = "1" + number
	}

	return hexDigest(number)
}

func buildQuery(purchaseType string) string {
	startOfDay := time.Now().AddDate(0, 0, -config.DaysAgo)
	startOfDay = time.Date(startOfDay.Year(), startOfDay.Month(), startOfDay.Day(), 0, 0, 0, 0, time.Local)
	endOfDay := startOfDay.AddDate(0, 0, 1)

	return `select
	customers.email,
		customers.cell_phone,
		customers.home_phone,
		customers.firstname,
		customers.lastname,
		customers.zip,
		customers.bday,
		customers.customer_id,
		invoices.amount,
		invoices.invoice_id,
		invoices.postdate
	from invoices
	left join customers on customers.customer_id = invoices.customer_id
	where invoices.voidedinvoice = 0
	and invoices.customer_id != 1008
	and amount > 0
	and invoices.invtype = '` + purchaseType + `'
	and invoices.postdate between '` + startOfDay.Format(time.RFC3339) + "' and '" + endOfDay.Format(time.RFC3339) + "'"
}

func connectToRgp() *sql.DB {
	var dbConfig mysql.Config
	dbConfig.User = config.DatabaseUser
	dbConfig.Passwd = config.DatabasePassword
	dbConfig.Net = "tcp"
	dbConfig.Addr = config.DatabaseAddress
	dbConfig.DBName = config.DatabaseName
	dbConfig.ParseTime = true
	dbConfig.AllowNativePasswords = true

	db, err := sql.Open("mysql", dbConfig.FormatDSN())

	if err != nil {
		panic(err)
	}

	return db
}

func hexDigest(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}
