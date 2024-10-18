package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := receiversFromDisk(ctx)
	mf := messagesFiles(ctx, r)
	rm := rawMessagesFromDisk(ctx, mf)
	pm := parsedMessages(ctx, rm)
	fc := filteredChannel(ctx, pm, func(msg Message) bool {
		return msg.amount.value > 0 && msg.timestamp.After(startTimestamp)
	})

	splitPerAccount(ctx, fc)
}

var rates = map[string]float64{
	"EUR": 19.80,
	"USD": 17.99,
	"BGN": 10.10,
	"RON": 3.95,
	"GBP": 23.78,
	"AMD": 0.062,
}
var startTimestamp = time.Date(2024, 10, 10, 0, 0, 0, 0, time.UTC)

const receiversPath = "./messages"

func allowedReceiver(receiver Receiver) bool {
	return receiver.name == "102" || receiver.name == "EXIMBANK"
}

type Receiver struct {
	name string
}

func receiversFromDisk(ctx context.Context) <-chan Receiver {
	entries, err := os.ReadDir(receiversPath)
	if err != nil {
		log.Fatal(err)
	}

	out := make(chan Receiver)

	go func() {
		defer close(out)
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			receiver := Receiver{name: e.Name()}
			if !allowedReceiver(receiver) {
				continue
			}
			select {
			case out <- receiver:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

type MessagesFile struct {
	receiver Receiver
	filepath string
}

func messagesFiles(ctx context.Context, in <-chan Receiver) <-chan MessagesFile {
	out := make(chan MessagesFile)

	go func() {
		defer close(out)
		for receiver := range in {
			folder := path.Join(receiversPath, receiver.name)
			entries, err := os.ReadDir(folder)
			if err != nil {
				log.Fatal(err)
			}

			for _, e := range entries {
				if e.IsDir() {
					continue
				}

				messagesFile := MessagesFile{receiver: receiver, filepath: path.Join(folder, e.Name())}
				select {
				case out <- messagesFile:
				case <-ctx.Done():
					return
				}
			}

		}
	}()

	return out
}

type RawMessage struct {
	receiver  Receiver
	timestamp time.Time
	body      string
}

func rawMessagesFromDisk(ctx context.Context, in <-chan MessagesFile) <-chan RawMessage {
	out := make(chan RawMessage)

	go func() {
		defer close(out)
		for mf := range in {
			data, err := os.ReadFile(mf.filepath)
			if err != nil {
				log.Fatal(err)
			}

			pattern := regexp.MustCompile(`\[(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\]([^\[\]]+)`)
			matches := pattern.FindAllStringSubmatch(string(data), -1)

			for _, m := range matches {
				if len(m) != 3 {
					continue
				}
				timestamp := parseTime(m[1])
				body := strings.Join(strings.Fields(strings.TrimSpace(m[2])), " ")
				message := RawMessage{receiver: mf.receiver, timestamp: timestamp, body: body}
				select {
				case out <- message:
				case <-ctx.Done():
					return
				}
			}

		}
	}()

	return out
}

func parseTime(timestamp string) time.Time {
	parsed_timestamp, err := time.Parse("2006-01-02 15:04:05 MST", strings.TrimSpace(timestamp)+" EEST")
	if err != nil {
		panic(err)
	}

	return parsed_timestamp
}

type Currency struct {
	value    float64
	currency string
}

func parseCurrency(amount string) Currency {
	parts := strings.Fields(strings.TrimSpace(amount))
	value, err := strconv.ParseFloat(strings.ReplaceAll(parts[0], ",", "."), 64)
	if err != nil {
		panic(err)
	}
	return Currency{value, parts[1]}
}

func exchangeToMdl(amount Currency) Currency {
	if amount.currency == "MDL" {
		return amount
	}

	return Currency{amount.value * rates[amount.currency], "MDL"}
}

type Message struct {
	RawMessage
	operation_type  string
	direction       string
	account         string
	original_amount Currency
	amount          Currency
	location        string
	memo            string
	status          string
}

func (msg Message) toCsv() []string {
	csv := make([]string, 4)
	csv[0] = msg.timestamp.Format("2006-01-02")
	csv[1] = msg.location
	csv[2] = msg.memo
	csv[3] = fmt.Sprintf("%s%.2f", msg.direction, msg.amount.value)
	return csv
}

func (msg Message) fancyAmount() int64 {
	value := msg.amount.value
	if msg.direction == "-" {
		value *= -1
	}
	return int64(value * 1000)
}

func (msg Message) fancyOriginalAmount() int64 {
	value := msg.original_amount.value
	if msg.direction == "-" {
		value *= -1
	}
	return int64(value * 1000)
}

func (msg Message) importId() string {
	return fmt.Sprintf("A:%d:%s:%s",
		msg.fancyOriginalAmount(),
		strings.ReplaceAll(msg.timestamp.Format("20060102150405"), " ", ""),
		msg.account)
}

func (msg Message) toJson(account_id string) string {
	return strings.TrimSpace(strings.Join(strings.Fields(fmt.Sprintf(`{
      "date":"%s", 
      "amount": %d, 
      "account_id": "%s",
      "payee_id": null,
      "category_id": null,
      "cleared": "cleared",
      "approved": true,
      "flag_color": null,
      "payee_name": "%s",
      "memo": "%s",
      "import_id": "%s"
      }`,
		msg.timestamp.Format("2006-01-02"),
		msg.fancyAmount(),
		account_id,
		msg.location,
		msg.memo,
		msg.importId())), ""))
}

func parsedMessages(ctx context.Context, in <-chan RawMessage) <-chan Message {
	out := make(chan Message)

	go func() {
		defer close(out)
		for rm := range in {
			var msg Message

			switch rm.receiver.name {
			case "102":
				msg = parseMaibMessage(rm)
			case "EXIMBANK":
				msg = parseEximMessage(rm)
			}

			if msg.operation_type == "" {
				continue
			}

			msg.receiver = rm.receiver
			msg.timestamp = rm.timestamp
			msg.body = rm.body

			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func parseMaibMessage(rm RawMessage) Message {
	var msg Message
	pattern := regexp.MustCompile(`Op: (.+) Karta: (.+) Status: (.+) Summa: (.+) Dost: (.+) Data/vremya: (.+) Adres: (.+)`)
	matches := pattern.FindStringSubmatch(rm.body)

	if len(matches) < 7 {
		return msg
	}

	msg.operation_type = matches[1]
	msg.status = matches[3]
	msg.direction = parseDirectionFromMaibMessage(msg)
	msg.account = matches[2]
	msg.original_amount = parseCurrency(matches[4])
	msg.amount = exchangeToMdl(parseCurrency(matches[4]))
	msg.location = parseMaibLocation(matches[7])
	if msg.location == "" {
		msg.location = "MAIB"
	}

	return msg
}

func parseMaibLocation(location string) string {
	if index := strings.Index(location, "Podderzhka: +373"); index > -1 {
		location = strings.TrimSpace(location[:index])
	}

	return location
}

func parseEximSpending(rm RawMessage) Message {
	var msg Message
	pattern := regexp.MustCompile(`(Tranzactie reusita|Anulare tranzactie), Data (.+), Card (.+), Suma (.+), Locatie (.+) Disponibil (.+)`)
	matches := pattern.FindStringSubmatch(rm.body)

	if len(matches) < 6 {
		return msg
	}

	msg.operation_type = matches[1]
	msg.direction = parseDirectionFromEximOperationType(msg.operation_type)
	msg.account = matches[3]
	msg.original_amount = parseCurrency(matches[4])
	msg.amount = exchangeToMdl(parseCurrency(matches[4]))
	msg.location = strings.TrimSuffix(strings.TrimSpace(matches[5]), ",")
	if msg.location == "" {
		msg.location = "EXIMBANK"
	}

	return msg
}

func parseEximTopup(rm RawMessage) Message {
	var msg Message
	pattern := regexp.MustCompile(`(Suplinire cont|Debitare cont) Card (.+), Data (.+), Suma (.+), Detalii (.+), Disponibil (.+)`)
	matches := pattern.FindStringSubmatch(rm.body)

	if len(matches) < 5 {
		return msg
	}

	msg.operation_type = matches[1]
	msg.direction = parseDirectionFromEximOperationType(msg.operation_type)
	msg.account = matches[2]
	msg.amount = parseCurrency(matches[4])
	msg.memo = strings.TrimSuffix(strings.TrimSpace(matches[5]), ",")
	if msg.location == "" {
		msg.location = "EXIMBANK"
	}

	return msg
}

func parseEximMessage(rm RawMessage) Message {
	msg := parseEximSpending(rm)
	if msg.account == "" {
		msg = parseEximTopup(rm)
	}
	return msg
}

func parseDirectionFromMaibMessage(msg Message) string {
	if msg.operation_type == "Popolnenie" || msg.status == "Uspeshnoe reversirovanie" {
		return "+"
	}

	return "-"
}

func parseDirectionFromEximOperationType(operation_type string) string {
	switch operation_type {
	case "Debitare cont":
		fallthrough
	case "Tranzactie reusita":
		return "-"
	case "Anulare tranzactie":
		fallthrough
	case "Suplinire cont":
		return "+"
	}

	return ""
}

func filteredChannel[T any](ctx context.Context, in <-chan T, fn func(T) bool) (out chan T) {
	out = make(chan T)

	go func() {
		defer close(out)
		for msg := range in {

			if !fn(msg) {
				continue
			}

			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func outputCsv(in <-chan Message) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	err := w.Write([]string{"Date", "Payee", "Memo", "Amount"})
	if err != nil {
		log.Fatal(err)
		return
	}

	for msg := range in {
		err := w.Write(msg.toCsv())
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}

func outputApi(in <-chan Message, budget_id, account_id string) {
	for msg := range in {
		sendYnabTransaction(msg.toJson(account_id), budget_id)
	}
}

func sendYnabTransaction(json string, budget_id string) {
	client := &http.Client{}
	url := fmt.Sprintf("https://api.ynab.com/v1/budgets/%s/transactions", budget_id)
	msgs := fmt.Sprintf(`{"transaction":%s}`, json)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(msgs)))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("TOKEN"))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}

func splitPerAccount(ctx context.Context, in <-chan Message) {
	var wg sync.WaitGroup
	wg.Add(3)
	ch1 := make(chan Message)
	ch2 := make(chan Message)
	ch3 := make(chan Message)

	go func() {
		defer close(ch1)
		defer close(ch2)
		defer close(ch3)
		for msg := range in {
			switch msg.account {
			case "*1972":
				select {
				case ch1 <- msg:
				case <-ctx.Done():
					return
				}
			case "*3632":
				fallthrough
			case "*2571":
				select {
				case ch2 <- msg:
				case <-ctx.Done():
					return
				}
			case "4..6345":
				select {
				case ch3 <- msg:
				case <-ctx.Done():
					return
				}
			default:
				panic(fmt.Sprintf("Unknown account: %s", msg.account))
			}
		}
	}()

	output := func(ch <-chan Message, budget_id, account_id string) {
		outputApi(ch, budget_id, account_id)
		wg.Done()
	}

	budget_id := "4f85f1ac-1bfe-4123-b3b9-e450bc6ed69e"

	go output(ch1, budget_id, "fd6f626b-7564-42bf-80ee-75feb70fd002")
	go output(ch2, budget_id, "76f338c8-b2d1-4edf-9a89-7331ebecc1bf")
	go output(ch3, budget_id, "079f089b-ccbc-4d37-87eb-c4dd86e23af3")

	wg.Wait()
}
