package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const fbAccessToken = "EAAP2ZBKHoPa4BABvLZBQC28gOMDSZAcYu0tARGrkmtZCliZCDgLQVmMlNn5it8TCPmLlv3DQlsEV8CQrSfhv68FxG1RLIZCSrUdbB5ncf7vxUwRVXhRuxOiQOvhHSe9ZCKOWpDlf2qyu4dLMGhuhakzYHpFek2VxZBl6X8kWt7IalqJ8ZCBUn8EZClrqvcI4XOwEli233UcH6UEwZDZD"

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

func main() {
	daysAgo := 22
	// Step 1, fetch purchases from RGP
	purchases := fetchPurchases(daysAgo)

	// Step 2, hash data and prepare for upload to Facebook
	hashedPurchases := hashPurchases(purchases)

	// Step 3, upload the purchases to Facebook
	uploadPurchases(hashedPurchases, daysAgo)
}

func uploadPurchases(purchases []purchase, daysAgo int) {
	dayOfData := time.Now().AddDate(0, 0, -daysAgo)
	dayOfData = time.Date(dayOfData.Year(), dayOfData.Month(), dayOfData.Day(), 0, 0, 0, 0, time.Local)

	jsonPurchases, err := json.Marshal(purchases)

	if err != nil {
		log.Fatal(err)
	}

	data := url.Values{
		"access_token": {fbAccessToken},
		"upload_tag":   {"rgp_upload_" + "vgn" + "_" + dayOfData.Format("2006-01-02")},
		"data":         {string(jsonPurchases)},
	}

	resp, err := http.PostForm("https://graph.facebook.com/v12.0/323247912682990/events", data)

	if err != nil {
		log.Fatal(err)
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func hashPurchases(purchases *sql.Rows) (hashedPurchases []purchase) {
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

func formatEmail(email string) []string {
	if len(email) == 0 {
		return []string{}
	}

	return []string{hexDigest(strings.ToLower(strings.TrimSpace(email)))}
}

func fetchPurchases(daysAgo int) *sql.Rows {
	db := connectToRgp()
	query := buildQuery(daysAgo)

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

// Phone numbers can't have any punctuation and must have a leading 1.
// For example "(226) 600-1303" should be "12266001303"
func formatPhoneNumber(number string) string {
	nonNumber := regexp.MustCompile("[^0-9]")

	number = nonNumber.ReplaceAllString(number, "")
	if len(number) == 10 {
		number = "1" + number
	}

	return hexDigest(number)
}

func buildQuery(daysAgo int) string {
	startOfDay := time.Now().AddDate(0, 0, -daysAgo)
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
	and invoices.invtype = 'POS'
	and invoices.postdate between '` + startOfDay.Format(time.RFC3339) + "' and '" + endOfDay.Format(time.RFC3339) + "'"
}

func connectToRgp() *sql.DB {
	var dbConfig mysql.Config
	dbConfig.User = "readonly"
	dbConfig.Passwd = "IDOdL52bEs6SrtQ8Xpg1h56jZjIayt4I"
	dbConfig.Net = "tcp"
	dbConfig.Addr = "184.148.49.176"
	dbConfig.DBName = "aspireclimbingvaughan"
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
