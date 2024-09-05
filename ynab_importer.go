package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
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
		return msg.amount.value >= 0 && msg.timestamp.After(startTimestamp)
	})
	fc2 := filteredChannel(ctx, fc, func(msg Message) bool {
		return true // msg.account == "*1972"
	})

	outputCsv(fc2)

	// for v := range out {
	// 	fmt.Println(v.timestamp, v.operation_type, v.account, v.direction, v.amount, v.location, v.memo)
	// }
}

var rates = map[string]float64{
	"EUR": 19.80,
	"USD": 17.80,
	"BGN": 9.90,
	"RON": 3.95,
}
var startTimestamp = time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)

const receiversPath = "./messages"

func allowedReceiver(receiver Receiver) bool {
	return receiver.name == "102" // || receiver.name == "EXIMBANK"
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
	return exchangeToMdl(Currency{value, parts[1]})
}

func exchangeToMdl(amount Currency) Currency {
	if amount.currency == "MDL" {
		return amount
	}

	return Currency{amount.value * rates[amount.currency], "MDL"}
}

type Message struct {
	RawMessage
	operation_type string
	direction      string
	account        string
	amount         Currency
	location       string
	memo           string
	status         string
}

func (msg Message) toCsv() []string {
	csv := make([]string, 4)
	csv[0] = msg.timestamp.Format("2006-01-02")
	csv[1] = msg.location
	csv[2] = msg.memo
	csv[3] = fmt.Sprintf("%s%.2f", msg.direction, msg.amount.value)
	return csv
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
	msg.amount = parseCurrency(matches[4])
	msg.location = parseMaibLocation(matches[7])

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
	msg.amount = parseCurrency(matches[4])
	msg.location = strings.TrimSuffix(strings.TrimSpace(matches[5]), ",")

	return msg
}

func parseEximTopup(rm RawMessage) Message {
	var msg Message
	pattern := regexp.MustCompile(`(Suplinire cont) Card (.+), Data (.+), Suma (.+), Detalii (.+), Disponibil (.+)`)
	matches := pattern.FindStringSubmatch(rm.body)

	if len(matches) < 5 {
		return msg
	}

	msg.operation_type = matches[1]
	msg.direction = parseDirectionFromEximOperationType(msg.operation_type)
	msg.account = matches[2]
	msg.amount = parseCurrency(matches[4])
	msg.memo = strings.TrimSuffix(strings.TrimSpace(matches[5]), ",")

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
	case "Tranzactie reusita":
		return "-"
	case "Anulare tranzactie":
		return "+"
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
		// fmt.Println(msg.timestamp, msg.operation_type, msg.account, msg.direction, msg.amount, msg.location, msg.memo)
		err := w.Write(msg.toCsv())
		if err != nil {
			log.Fatal(err)
			return
		}
	}
}
